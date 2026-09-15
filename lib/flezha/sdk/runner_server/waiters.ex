defmodule Flezha.SDK.RunnerServer.Waiters do
  @moduledoc """
  The callers a runner keeps waiting on a controller reply, by the reference id
  their request went out under: a workflow barrier, an admin call and a
  procedure's spawn request.

  One table, because each asks one question: which caller waits on this
  reference id. An answer is sent under the tag the caller waits for; a spawn's
  waiter holds its request in the tag's place, so its answer can also log a
  failure nobody read. Nothing but `settle/4` removes a waiter.
  """

  alias Flezha.SDK.Spawn

  @typedoc "A reference id to its caller, the caller's ref, and the tag or spawn request."
  @type t :: %{String.t() => {pid(), term(), atom() | Spawn.t()}}

  @doc "Remember that `caller` waits on `reference_id` for an answer under `tag`."
  @spec await(t(), String.t(), pid(), term(), atom() | Spawn.t()) :: t()
  def await(waiters, reference_id, caller, ref, tag),
    do: Map.put(waiters, reference_id, {caller, ref, tag})

  @doc "A reader asked for a spawn's answer, so a failure it carries is not also logged."
  @spec claim(t(), String.t()) :: t()
  def claim(waiters, reference_id) do
    Map.replace_lazy(waiters, reference_id, fn {caller, ref, spawn} ->
      {caller, ref, %{spawn | claimed?: true}}
    end)
  end

  @doc "Answer the caller waiting on `reference_id` and forget it; `:unknown` when none waits."
  @spec settle(t(), String.t(), term(), String.t()) :: {:ok, t()} | :unknown
  def settle(waiters, reference_id, result, error) do
    case Map.pop(waiters, reference_id) do
      {nil, _waiters} ->
        :unknown

      {{caller, _ref, %Spawn{} = spawn}, waiters} ->
        Spawn.answer(caller, spawn, result, error)
        {:ok, waiters}

      {{caller, ref, tag}, waiters} ->
        send(caller, {tag, ref, result, error})
        {:ok, waiters}
    end
  end
end
