defmodule Flezha.SDK.Effects do
  @moduledoc """
  The runner's copy of the effect results the controller keeps
  (`recorded-effects`): the ones a bind shipped, and the ones this runner's
  `Flezha.SDK.Ctx.effect/4` calls kept since. One public table per runner,
  keyed `{relay_id, cell, offset, identity}`.

  A Cell takes its inputs in offset order, so a call at a later offset drops
  the Cell's earlier entries; a re-run of those comes only with a new bind,
  which ships them again. An instance's entries go when it is removed.
  """

  @type table :: :ets.tid()

  @doc "A runner's table."
  @spec new() :: table()
  def new, do: :ets.new(:flezha_sdk_effects, [:public, :ordered_set, {:read_concurrency, true}])

  @doc """
  Resolve a call `identity_of.(ordinal)` names, from `ordinal` on, at `{relay_id,
  cell, offset}`: `{ordinal, identity, {:ok, payload} | :none}`.

  Several items of one Cell can take one offset, each counting its calls from
  0. A result an invocation kept or read is claimed, so another item's first
  attempt passes over it to the next ordinal, the order the items first ran
  in; a retry (`attempt` above 1) reads a claimed result as its own. The
  Cell's entries below the offset go first: it has moved past them.
  """
  @spec resolve(
          table(),
          {String.t(), binary(), non_neg_integer()},
          (non_neg_integer() -> String.t()),
          non_neg_integer(),
          pos_integer()
        ) :: {non_neg_integer(), String.t(), {:ok, binary()} | :none}
  def resolve(table, {relay_id, cell, offset} = at, identity_of, ordinal, attempt) do
    :ets.select_delete(table, [
      {{{relay_id, cell, :"$1", :_}, :_, :_}, [{:<, :"$1", offset}], [true]}
    ])

    resolve_from(table, at, identity_of, ordinal, attempt)
  end

  defp resolve_from(table, {relay_id, cell, offset} = at, identity_of, ordinal, attempt) do
    identity = identity_of.(ordinal)
    key = {relay_id, cell, offset, identity}

    case :ets.lookup(table, key) do
      [{_key, _payload, true}] when attempt <= 1 ->
        resolve_from(table, at, identity_of, ordinal + 1, attempt)

      [{_key, payload, _claimed}] ->
        :ets.update_element(table, key, {3, true})
        {ordinal, identity, {:ok, payload}}

      [] ->
        {ordinal, identity, :none}
    end
  end

  @doc "Keep `payload` for `identity`, claimed by the invocation that ran it."
  @spec put(table(), String.t(), binary(), non_neg_integer(), String.t(), binary()) :: :ok
  def put(table, relay_id, cell, offset, identity, payload),
    do: insert(table, {relay_id, cell, offset, identity}, payload, true)

  @doc "Take the results a bind shipped for `relay_id`; answers how many."
  @spec landed(table(), String.t(), [map()]) :: non_neg_integer()
  def landed(table, relay_id, results) do
    for r <- results,
        do: insert(table, {relay_id, r.cell_identity, r.offset, r.identity}, r.payload, false)

    length(results)
  end

  defp insert(table, key, payload, claimed) do
    :ets.insert_new(table, {key, payload, claimed})
    :ok
  end

  @doc "Drop everything kept for an instance this runner no longer holds."
  @spec forget(table(), String.t()) :: :ok
  def forget(table, relay_id) do
    :ets.match_delete(table, {{relay_id, :_, :_, :_}, :_, :_})
    :ok
  end
end
