defmodule Flezha.SDK.Ctx do
  @moduledoc """
  The invocation context handed to `Procedure.run/3`. `emit/2`,
  `emit_with_meta/3`, `emit_keyed/3` and `emit_keyed_with_meta/4` accumulate
  outputs for this invocation; `apply/3` folds a change into the state through
  the procedure's `apply_change/2` and records it for the Entity's record
  stream — one fold, one push, the only recorded mutation path; `now/1` is a
  wall-clock millisecond stamp captured once at invocation entry;
  `entity_key/1` is the owning Entity's key (or nil for a Single Entity).
  Emits and changes are held in the running process's dictionary under a
  per-invocation ref (so `emit/2` can side-effect through an immutable struct)
  and are taken by the Entity only when `run/3` returns `{:ok, _}` — a failing
  invocation discards both. Reserved `flz.`-prefixed metadata keys are refused.

  A key rides an emit only where the node's sink is declared `keyed/1`, and a
  keyless emit from an Entity with no key of its own on such a sink is refused
  here, at the call — the reference's `KeyRequiredOnKeyedChannel` — from two
  topology facts the Entity hands `new/4`.

  Timers ride the same dictionary: `timer_after/2` and `timer_at_event_time/2`
  register on a `Flezha.SDK.Timers` session over a copy of the Entity's pending
  set — opened on the first timer call, so an invocation that registers
  nothing pays nothing — answering the absolute timestamp, the handle the two
  cancels take; the Entity commits the session's ops with the changes when
  `run/3` (or `on_timer/4`) answers `{:ok, _}`, and a firing commits its own
  removal even when it registered nothing. `now/1` is the declared clock's value: the
  epoch millisecond, the durable seq, or the input's event time, evaluated
  once per invocation — and, inside a firing, the scheduled timestamp.

  Under the effectively-once tier two more slots ride the same dictionary:
  `checkpoint/1` raises the flag the Entity reads at the invocation's end to
  close its staged interval, and `stable_id/1` answers a replay-stable id
  derived from the Entity's wire identity, the change seq the invocation
  started at and a per-invocation ordinal.

  `spawn_workflow/5` asks the runner for a workflow instance and answers a
  `Flezha.SDK.Spawn` handle at once; an invocation of a replayed input asks
  nothing.
  """

  alias Flezha.SDK.{Procedure, Spawn, Timers}

  @enforce_keys [:module, :entity_key, :now_ms, :ref]
  defstruct [
    :module,
    :entity_key,
    :now_ms,
    :ref,
    :channel,
    :timers,
    :timer_policy,
    :firing,
    :runner,
    identity: <<>>,
    seq: 0,
    replay?: false,
    keyed_sink?: false,
    key_required?: false
  ]

  @zero_tally %{registered: 0, fired: 0, cancelled: 0, refused: 0}

  # FNV-1a-128, the constants `flz_core::operative::instance` fixes; the
  # rendered id is a cross-language contract, so the three fixture vectors are
  # the only thing that may change it.
  @fnv128_offset_basis 0x6C62272E07BB014262B821756295C58D
  @fnv128_prime 0x0000000001000000000000000000013B
  @fnv128_mask 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF

  @type t :: %__MODULE__{}

  @reserved_prefix "flz."

  @doc """
  A fresh ctx for one invocation of `module`. `now_ms` is captured once here;
  `opts` carries the node's channel name, whether its sink is keyed
  (`keyed_sink?`), whether a keyless emit has nothing to fall back on
  (`key_required?`), the Entity's pending timers (`timers`, empty by default),
  the procedure's timer policy (`timer_policy`, resolved from `module` by
  default), the firing this invocation is (`firing`, `{kind, at}` or nil), the
  runner a spawn request goes to (`runner`, nil for none) and whether the input
  is a replay (`replay`), which issues no spawn.
  """
  def new(module, entity_key, now_ms, opts \\ []) do
    # Nothing is written here: every reader of a slot defaults to empty, so an
    # invocation that emits nothing, applies nothing and registers no timer
    # touches the dictionary not at all — and one that never returns from here
    # leaves nothing behind to delete.
    %__MODULE__{
      module: module,
      entity_key: entity_key,
      now_ms: now_ms,
      ref: make_ref(),
      channel: Keyword.get(opts, :channel),
      timers: Keyword.get(opts, :timers, Timers.new()),
      timer_policy:
        Keyword.get_lazy(opts, :timer_policy, fn -> Procedure.timer_policy(module) end),
      firing: Keyword.get(opts, :firing),
      runner: Keyword.get(opts, :runner),
      identity: Keyword.get(opts, :identity, <<>>),
      seq: Keyword.get(opts, :seq, 0),
      replay?: Keyword.get(opts, :replay, false),
      keyed_sink?: Keyword.get(opts, :keyed_sink?, false),
      key_required?: Keyword.get(opts, :key_required?, false)
    }
  end

  @doc """
  Fold `change` into `state` with the procedure's `apply_change/2` and record
  it, so the Entity ships it after the invocation succeeds. The returned state
  is what `run/3` returns in its `{:ok, _}`.
  """
  @spec apply(t(), term(), term()) :: term()
  def apply(%__MODULE__{module: module, ref: ref}, state, change) do
    Process.put(changes_key(ref), [change | Process.get(changes_key(ref), [])])
    module.apply_change(state, change)
  end

  @doc "Emit a value on the procedure's outgoing channels."
  @spec emit(t(), term()) :: :ok | {:error, term()}
  def emit(%__MODULE__{} = ctx, value), do: emit_with_meta(ctx, value, %{})

  @doc "Emit a value with metadata. Reserved `flz.`-prefixed keys are refused."
  @spec emit_with_meta(t(), term(), map()) :: :ok | {:error, term()}
  def emit_with_meta(%__MODULE__{key_required?: true, channel: channel}, _value, _meta),
    do: {:error, {:key_required_on_keyed_channel, channel}}

  def emit_with_meta(%__MODULE__{} = ctx, value, meta) when is_map(meta),
    do: put_emit(ctx, value, meta, nil)

  @doc "Emit under the output's natural key — the sink must be declared `keyed/1`."
  @spec emit_keyed(t(), String.t(), term()) :: :ok | {:error, term()}
  def emit_keyed(%__MODULE__{} = ctx, key, value), do: emit_keyed_with_meta(ctx, key, value, %{})

  @doc "`emit_keyed/3` with metadata."
  @spec emit_keyed_with_meta(t(), String.t(), term(), map()) :: :ok | {:error, term()}
  def emit_keyed_with_meta(
        %__MODULE__{keyed_sink?: false, channel: channel},
        _key,
        _value,
        _meta
      ),
      do: {:error, {:key_not_allowed_on_non_keyed_channel, channel}}

  def emit_keyed_with_meta(%__MODULE__{} = ctx, key, value, meta)
      when is_binary(key) and is_map(meta),
      do: put_emit(ctx, value, meta, key)

  @doc "The declared clock's value, evaluated once at invocation entry; the scheduled timestamp inside a firing."
  def now(%__MODULE__{now_ms: ms}), do: ms

  @doc "Register a processing-time timer `delay_ms` after `now/1`; `{:ok, at}`, the cancellation handle."
  @spec timer_after(t(), integer()) :: {:ok, non_neg_integer()} | {:error, Timers.refusal()}
  def timer_after(%__MODULE__{} = ctx, delay_ms),
    do: with_session(ctx, &Timers.register_after(&1, delay_ms))

  @doc "Register an event-time timer at `at`; refused under a clock that is not `:event_time`."
  @spec timer_at_event_time(t(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, Timers.refusal()}
  def timer_at_event_time(%__MODULE__{} = ctx, at),
    do: with_session(ctx, &Timers.register(&1, :event_time, at))

  @doc "Cancel the processing-time timer `timer_after/2` answered; whether one was pending."
  @spec cancel_processing_time_timer(t(), non_neg_integer()) :: boolean()
  def cancel_processing_time_timer(%__MODULE__{} = ctx, at),
    do: cancel(ctx, :processing_time, at)

  @doc "Cancel the event-time timer `timer_at_event_time/2` answered; whether one was pending."
  @spec cancel_event_time_timer(t(), non_neg_integer()) :: boolean()
  def cancel_event_time_timer(%__MODULE__{} = ctx, at), do: cancel(ctx, :event_time, at)

  @doc "The owning Entity's key, or nil for a Single Entity."
  def entity_key(%__MODULE__{entity_key: k}), do: k

  @doc """
  Request spawn-or-deliver of a workflow instance in this runner's namespace:
  `instance_id` of `factory_id` starts with `payload` as its first input when
  the id is free; while it runs, `:deliver` hands it `payload` as one more
  input and `:error` resolves `already_running`. Answers a `Flezha.SDK.Spawn`
  handle without waiting, read with `Flezha.SDK.Spawn.await/2` or
  `Flezha.SDK.Spawn.try_result/1` inside this invocation, or not at all.
  """
  @spec spawn_workflow(t(), String.t(), String.t(), binary(), Spawn.if_running()) :: Spawn.t()
  def spawn_workflow(%__MODULE__{} = ctx, instance_id, factory_id, payload, if_running),
    do: Spawn.request(ctx.runner, ctx.replay?, instance_id, factory_id, payload, if_running)

  @doc """
  Close the staged checkpoint interval at the end of this invocation. A no-op
  on a class that did not declare checkpoint granularity — the Entity reads the
  flag only when it has an interval to close.
  """
  @spec checkpoint(t()) :: :ok
  def checkpoint(%__MODULE__{ref: ref}) do
    Process.put(checkpoint_key(ref), true)
    :ok
  end

  @doc false
  @spec checkpointed?(t()) :: boolean()
  def checkpointed?(%__MODULE__{ref: ref}), do: Process.get(checkpoint_key(ref), false)

  @doc """
  A replay-stable 128-bit id, 32 lowercase hex digits: FNV-1a-128 over the
  Entity's wire identity, the change seq this invocation started at and a
  per-invocation ordinal. The same invocation on a replay derives the same
  ids in the same order, which is what makes an effect idempotent.
  """
  @spec stable_id(t()) :: String.t()
  def stable_id(%__MODULE__{ref: ref} = ctx) do
    ordinal = Process.get(stable_id_key(ref), 0)
    Process.put(stable_id_key(ref), ordinal + 1)
    derive_stable_id(ctx.identity, ctx.seq, ordinal)
  end

  @doc false
  @spec derive_stable_id(binary(), non_neg_integer(), non_neg_integer()) :: String.t()
  def derive_stable_id(identity, seq, ordinal) do
    @fnv128_offset_basis
    |> fnv1a128(identity)
    |> fnv1a128(<<seq::64-big>>)
    |> fnv1a128(<<ordinal::32-big>>)
    |> Integer.to_string(16)
    |> String.downcase()
    |> String.pad_leading(32, "0")
  end

  defp fnv1a128(hash, <<byte, rest::binary>>),
    do: fnv1a128(Bitwise.band(Bitwise.bxor(hash, byte) * @fnv128_prime, @fnv128_mask), rest)

  defp fnv1a128(hash, <<>>), do: hash

  @doc false
  def take_emits(%__MODULE__{ref: ref}) do
    emits = Process.get(key(ref), []) |> Enum.reverse()
    Process.delete(key(ref))
    emits
  end

  @doc false
  def take_changes(%__MODULE__{ref: ref}) do
    changes = Process.get(changes_key(ref), []) |> Enum.reverse()
    Process.delete(changes_key(ref))
    changes
  end

  # An invocation that opened no session has nothing to commit; a firing
  # commits its removal, op 0 of a session opened here if `on_timer/4` opened
  # none.
  @doc false
  def take_timers(%__MODULE__{ref: ref, firing: nil} = ctx) do
    case Process.delete(timers_key(ref)) do
      nil -> {ctx.timers, [], @zero_tally}
      session -> Timers.commit(session)
    end
  end

  def take_timers(%__MODULE__{ref: ref} = ctx) do
    Timers.commit(Process.delete(timers_key(ref)) || open(ctx))
  end

  @doc false
  def discard(%__MODULE__{ref: ref}) do
    Process.delete(key(ref))
    Process.delete(changes_key(ref))
    Process.delete(timers_key(ref))
    Process.delete(checkpoint_key(ref))
    Process.delete(stable_id_key(ref))
  end

  defp with_session(%__MODULE__{ref: ref} = ctx, fun) do
    case fun.(session(ctx)) do
      {:ok, at, session} ->
        Process.put(timers_key(ref), session)
        {:ok, at}

      {:error, refusal, session} ->
        Process.put(timers_key(ref), session)
        {:error, refusal}
    end
  end

  defp cancel(%__MODULE__{ref: ref} = ctx, kind, at) do
    {changed?, session} = Timers.cancel(session(ctx), kind, at)
    Process.put(timers_key(ref), session)
    changed?
  end

  # The session open in this invocation, or one opened now.
  defp session(%__MODULE__{ref: ref} = ctx), do: Process.get(timers_key(ref)) || open(ctx)

  defp open(%__MODULE__{} = ctx),
    do: Timers.session(ctx.timers, ctx.timer_policy, ctx.now_ms, ctx.firing)

  defp put_emit(%__MODULE__{ref: ref}, value, meta, key) do
    case reserved_key(meta) do
      nil ->
        Process.put(key(ref), [{value, meta, key} | Process.get(key(ref), [])])
        :ok

      k ->
        {:error, {:reserved_metadata_key, k}}
    end
  end

  defp reserved_key(meta) do
    Enum.find_value(meta, fn {k, _v} ->
      ks = to_string(k)
      if String.starts_with?(ks, @reserved_prefix), do: ks
    end)
  end

  defp key(ref), do: {__MODULE__, ref}
  defp changes_key(ref), do: {__MODULE__, ref, :changes}
  defp timers_key(ref), do: {__MODULE__, ref, :timers}
  defp checkpoint_key(ref), do: {__MODULE__, ref, :checkpoint}
  defp stable_id_key(ref), do: {__MODULE__, ref, :stable_id}
end
