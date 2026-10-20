defmodule Flezha.SDK.Ctx do
  @moduledoc """
  The invocation context handed to `Tasklet.run/3`. `use Flezha.SDK.Tasklet`
  imports the functions below, so a tasklet writes `emit(ctx, value)` and
  `complete(ctx, result)`; `apply/3` is not imported, since it would shadow
  `Kernel.apply/3`, and a `use` tasklet never calls it: its `defchange`
  recorders do.

  `emit/2`, `emit_with_meta/3`, `emit_keyed/3` and `emit_keyed_with_meta/4`
  accumulate outputs for this invocation; `apply/3` folds a change into the
  state through the tasklet's `apply_change/2` and records it for the Cell's
  record stream, the only recorded mutation path; `now/1` is the declared
  clock's value, evaluated once by the core for this input, and the scheduled
  timestamp inside a firing; `cell_key/1` is the owning Cell's key (or nil for
  a Single Cell). Emits, changes, timer operations and the completion are held
  in the invoking worker's dictionary under a per-invocation ref and are taken
  only when `run/3` returns `{:ok, _}`: a failing invocation discards them all.
  Reserved `flz.`-prefixed metadata keys are refused.

  A key rides an emit only where the node's output is declared `keyed/1`, and a
  keyless emit there from a Cell with no key of its own is refused at the call.

  `timer_after/2` and `timer_at_event_time/2` record a registration and answer
  `{:ok, at}`, the absolute timestamp the two cancels take; a cancel answers
  `:ok`. `flz_core` judges the item's timer ops when it settles: past the
  tasklet's `max_timers_per_cell/0`, or an event-time timer under another
  clock, the item fails, not retried. Inside a `compensate/3` hook the four
  timer calls answer `runtime.timers_unavailable` and arm nothing.

  `spawn_steps/4`, `effect/4` and `part/2` reach `flz_core` from the live
  activation, and only from the worker running the invocation: the same call
  from another process is refused.
  """

  alias Flezha.SDK.{Error, Native, Spawn}

  @enforce_keys [:module, :cell_key, :now_ms, :ref]
  defstruct [
    :module,
    :cell_key,
    :now_ms,
    :ref,
    :channel,
    :core,
    :token,
    :owner,
    :offset,
    :part_encode,
    identity: <<>>,
    item: 0,
    seq: 0,
    replay?: false,
    keyed_sink?: false,
    timers?: true
  ]

  # FNV-1a-128, the constants `flz_core::relay::instance` fixes; the rendered
  # id is a cross-language contract.
  @fnv128_offset_basis 0x6C62272E07BB014262B821756295C58D
  @fnv128_prime 0x0000000001000000000000000000013B
  @fnv128_mask 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF

  @type t :: %__MODULE__{}

  @reserved_prefix "flz."

  # A refusal the caller can only fix in its code: `run/3` returns it as it is.
  @non_retryable_effect ~w(effect.bad_name effect.nested effect.outside_invocation effect.too_large)

  @doc """
  A fresh ctx for one invocation of `module`. `opts` carries the node's channel
  name and whether its output is keyed (`keyed_sink?`), `timers?: false`
  in a compensate hook, and the live
  activation the core calls are made from: `core`, `token`, `item`, `offset`,
  `replay`, the Cell's `identity` and the `seq` it was handed at.
  """
  def new(module, cell_key, now_ms, opts \\ []) do
    %__MODULE__{
      module: module,
      cell_key: cell_key,
      now_ms: now_ms,
      ref: make_ref(),
      channel: Keyword.get(opts, :channel),
      core: Keyword.get(opts, :core),
      token: Keyword.get(opts, :token),
      owner: self(),
      offset: Keyword.get(opts, :offset),
      part_encode: Keyword.get(opts, :part_encode),
      identity: Keyword.get(opts, :identity, <<>>),
      item: Keyword.get(opts, :item, 0),
      seq: Keyword.get(opts, :seq, 0),
      replay?: Keyword.get(opts, :replay, false),
      keyed_sink?: Keyword.get(opts, :keyed_sink?, false),
      timers?: Keyword.get(opts, :timers?, true)
    }
  end

  @doc """
  Fold `change` into `state` with the tasklet's `apply_change/2` and record
  it, so the Cell ships it after the invocation succeeds. The returned state
  is what `run/3` returns in its `{:ok, _}`.
  """
  @spec apply(t(), term(), term()) :: term()
  def apply(%__MODULE__{module: module, ref: ref}, state, change) do
    push(changes_key(ref), change)
    module.apply_change(state, change)
  end

  @doc "Emit a value on the tasklet's outgoing channels."
  @spec emit(t(), term()) :: :ok | {:error, Error.t()}
  def emit(%__MODULE__{} = ctx, value), do: emit_with_meta(ctx, value, %{})

  @doc "Emit a value with metadata. Reserved `flz.`-prefixed keys are refused."
  @spec emit_with_meta(t(), term(), map()) :: :ok | {:error, Error.t()}
  def emit_with_meta(
        %__MODULE__{keyed_sink?: true, cell_key: nil, channel: channel},
        _value,
        _meta
      ),
      do: {:error, Error.new("runtime.key_required_on_keyed_channel", channel: channel)}

  def emit_with_meta(%__MODULE__{} = ctx, value, meta) when is_map(meta),
    do: put_emit(ctx, value, meta, nil)

  @doc "Emit under the output's natural key — the output must be declared `keyed/1`."
  @spec emit_keyed(t(), String.t(), term()) :: :ok | {:error, Error.t()}
  def emit_keyed(%__MODULE__{} = ctx, key, value), do: emit_keyed_with_meta(ctx, key, value, %{})

  @doc "`emit_keyed/3` with metadata."
  @spec emit_keyed_with_meta(t(), String.t(), term(), map()) :: :ok | {:error, Error.t()}
  def emit_keyed_with_meta(
        %__MODULE__{keyed_sink?: false, channel: channel},
        _key,
        _value,
        _meta
      ),
      do: {:error, Error.new("runtime.key_not_allowed_on_non_keyed_channel", channel: channel)}

  def emit_keyed_with_meta(%__MODULE__{} = ctx, key, value, meta)
      when is_binary(key) and is_map(meta),
      do: put_emit(ctx, value, meta, key)

  @doc "The declared clock's value for this input; the scheduled timestamp inside a firing."
  def now(%__MODULE__{now_ms: ms}), do: ms

  @doc "Register a processing-time timer `delay_ms` after `now/1`; `{:ok, at}`, the cancellation handle."
  @spec timer_after(t(), integer()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def timer_after(%__MODULE__{now_ms: now} = ctx, delay_ms),
    do: timer(ctx, register_op(:processing_time, max(now + delay_ms, 0)))

  @doc "Register an event-time timer at `at`; `{:ok, at}`."
  @spec timer_at_event_time(t(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def timer_at_event_time(%__MODULE__{} = ctx, at), do: timer(ctx, register_op(:event_time, at))

  @doc "Cancel the processing-time timer `timer_after/2` answered; one not pending changes nothing."
  @spec cancel_processing_time_timer(t(), non_neg_integer()) :: :ok | {:error, Error.t()}
  def cancel_processing_time_timer(%__MODULE__{} = ctx, at),
    do: cancelled(timer(ctx, cancel_op(:processing_time, at)))

  @doc "Cancel the event-time timer `timer_at_event_time/2` answered; one not pending changes nothing."
  @spec cancel_event_time_timer(t(), non_neg_integer()) :: :ok | {:error, Error.t()}
  def cancel_event_time_timer(%__MODULE__{} = ctx, at),
    do: cancelled(timer(ctx, cancel_op(:event_time, at)))

  defp register_op(kind, at), do: %{kind: kind, scheduled_at: at, register: true}
  defp cancel_op(kind, at), do: %{kind: kind, scheduled_at: at, register: false}

  @doc "The owning Cell's key, or nil for a Single Cell."
  def cell_key(%__MODULE__{cell_key: k}), do: k

  @doc """
  Request spawn-or-deliver of a steps instance of `factory_id` in this
  runner's namespace, with `payload` as its first input: bytes as they are,
  anything else as JSON, and a term with no JSON form raises. Answers a
  `Flezha.SDK.Spawn` handle without waiting, read with
  `Flezha.SDK.Spawn.await/2` or `Flezha.SDK.Spawn.try_result/1` inside this
  invocation, or not at all.

  With `id:` the instance starts under that id when it is free; while it runs,
  `if_running: :deliver` (the default) hands it `payload` as one more input and
  `if_running: :error` resolves `already_running`. `if_running:` without `id:`
  resolves the core's `spawn.if_running_without_id` at the call and sends
  nothing.

  Without `id:` the controller generates one, and every call starts a new
  instance: a call reissued after a lost or timed-out answer starts a second
  one, and a replayed invocation resolves `skipped_on_replay`. A crash re-run
  of an input the Cell had not durably consumed asks again under the same
  identity, and the controller answers the instance the first ask started.
  """
  @spec spawn_steps(t(), String.t(), term(), [Spawn.option()]) :: Spawn.t()
  def spawn_steps(%__MODULE__{} = ctx, factory_id, payload, opts \\ []),
    do: Spawn.request(ctx, factory_id, plain_payload(payload), opts)

  @doc """
  Run `fun`, a call to anything outside the instance (a model, a tool, a
  service), and keep its result: when this invocation runs again (a crash
  re-run, a replay, a wake, a move), the call answers the kept result and
  `fun` does not run. The result is kept, synced on the controller, before
  this returns; a crash between `fun` finishing and that point runs `fun`
  again, so give the outside service `stable_id/1` as an idempotency key where
  it takes one.

  `name` names the call: a non-empty string of at most 255 bytes, counted per
  invocation. One call runs at a time; a concurrent one is refused. The result
  is encoded as `spawn_steps/4` encodes its payload and read back as JSON, else
  as the binary; `codec`, an `{encode, decode}` pair of one-argument
  functions, replaces both.

  An invocation that took no input-log offset runs `fun` each time and keeps
  nothing. A raise in `fun` propagates and nothing is kept. Answers
  `{:ok, result}`, or `{:error, reason}` for `run/3` to return as it is:
  `{:non_retryable, error}` for `effect.bad_name`, `effect.nested`,
  `effect.outside_invocation` and `effect.too_large`, and the retryable
  `effect.not_recorded` error.
  """
  @spec effect(t(), String.t(), (-> term()), {(term() -> binary()), (binary() -> term())} | nil) ::
          {:ok, term()} | {:error, Error.t() | {:non_retryable, Error.t()}}
  def effect(%__MODULE__{} = ctx, name, fun, codec \\ nil) when is_function(fun, 0) do
    {encode, decode} = codec || {&plain_payload/1, &plain_value/1}

    cond do
      not is_binary(name) ->
        effect_refused(Error.new("effect.bad_name", name: inspect(name)))

      self() != ctx.owner ->
        effect_refused(Error.new("effect.outside_invocation", name: name))

      true ->
        case Native.answered(Native.effect_begin(ctx.core, ctx.token, ctx.item, name)) do
          {:kept, kept} -> {:ok, decode.(kept)}
          :run -> run_effect(ctx, fun, encode, decode, true)
          :unrecorded -> run_effect(ctx, fun, encode, decode, false)
          {:error, error} -> effect_refused(error)
        end
    end
  end

  defp run_effect(ctx, fun, encode, decode, keep?) do
    encoded =
      try do
        encode.(fun.())
      catch
        kind, reason ->
          Native.effect_release(ctx.core, ctx.token, ctx.item)
          :erlang.raise(kind, reason, __STACKTRACE__)
      end

    if keep? do
      case Native.answered(Native.effect_end(ctx.core, ctx.token, ctx.item, encoded)) do
        nil -> {:ok, decode.(encoded)}
        first when is_binary(first) -> {:ok, decode.(first)}
        {:error, error} -> effect_refused(error)
      end
    else
      Native.effect_release(ctx.core, ctx.token, ctx.item)
      {:ok, decode.(encoded)}
    end
  end

  defp effect_refused(%Error{code: code} = error) when code in @non_retryable_effect,
    do: {:error, {:non_retryable, error}}

  defp effect_refused(%Error{} = error), do: {:error, error}

  defp plain_value(binary) do
    case Jason.decode(binary) do
      {:ok, value} -> value
      {:error, _not_json} -> binary
    end
  end

  @doc """
  End the instance this tasklet runs in, with `result` as the result its
  completion reports. `result` is encoded as `spawn_steps/4` encodes its
  payload; `nil` carries no result.

  In a class that runs in steps the rest of the iteration still runs and
  commits; once that commit is durable the instance ends instead of waiting
  for its next input. In a flow class the instance runs what its Cells already
  hold to its outputs, then ends; a later call before that replaces the result.
  """
  @spec complete(t(), term()) :: :ok
  def complete(%__MODULE__{ref: ref}, result) do
    Process.put(completion_key(ref), if(is_nil(result), do: "", else: plain_payload(result)))
    :ok
  end

  # Read by a runner in any language, so unlike an output's default this never
  # falls back to Erlang terms: a term with no JSON form raises.
  @doc false
  def plain_payload(v) when is_binary(v), do: v
  def plain_payload(v), do: Jason.encode!(v)

  @doc false
  @spec completion(t()) :: binary() | nil
  def completion(%__MODULE__{ref: ref}), do: Process.get(completion_key(ref))

  @doc """
  Close the staged checkpoint interval at the end of this invocation. A no-op
  on a class that did not declare checkpoint granularity.
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
  Cell's identity, the change seq this invocation started at and a
  per-invocation ordinal. The same invocation on a replay derives the same
  ids in the same order, which is what makes an effect idempotent.
  """
  @spec stable_id(t()) :: String.t()
  def stable_id(%__MODULE__{ref: ref} = ctx) do
    ordinal = Process.get(stable_id_key(ref), 0)
    Process.put(stable_id_key(ref), ordinal + 1)
    derive_stable_id(identity_bytes(ctx.identity), ctx.seq, ordinal)
  end

  # The dispatcher hands the ETS row the core's bytes are kept in; a ctx
  # built by hand carries the bytes.
  defp identity_bytes({cells, row}), do: :ets.lookup_element(cells, row, 2)
  defp identity_bytes(identity), do: identity

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

  @doc """
  Send `value` as a part: a preview of this invocation's output on its
  channel, which leaves now rather than when `run/3` returns, encoded as an
  emit of `value` would be. It never waits: a part the wire cannot take at once
  is dropped, and an invocation of a replayed input sends none.

  `:ok`, or `{:error, %Error{}}`: `part.no_output` when the node's channel does
  not end in an output (a `call` end is not one), `part.outside_invocation`
  through a ctx whose invocation has returned or from another process.
  """
  @spec part(t(), term()) :: :ok | {:error, Error.t()}
  def part(%__MODULE__{} = ctx, value) do
    payload = if ctx.part_encode, do: ctx.part_encode.(value), else: ""

    if self() != ctx.owner,
      do: {:error, Error.new("part.outside_invocation", tasklet: ctx.module.name())},
      else: Native.answered(Native.part(ctx.core, ctx.token, ctx.item, payload))
  end

  @doc false
  def take_emits(%__MODULE__{ref: ref}), do: take(key(ref))

  @doc false
  def take_changes(%__MODULE__{ref: ref}), do: take(changes_key(ref))

  @doc false
  # The ops to record, in call order; the core adds a firing's own removal.
  def take_timers(%__MODULE__{ref: ref}), do: take(timers_key(ref))

  @doc false
  def discard(%__MODULE__{ref: ref}) do
    Process.delete(key(ref))
    Process.delete(changes_key(ref))
    Process.delete(timers_key(ref))
    Process.delete(checkpoint_key(ref))
    Process.delete(stable_id_key(ref))
    Process.delete(completion_key(ref))
  end

  defp timer(%__MODULE__{timers?: false}, _op),
    do: {:error, Error.new("runtime.timers_unavailable")}

  defp timer(%__MODULE__{ref: ref}, op) do
    push(timers_key(ref), op)
    {:ok, op.scheduled_at}
  end

  defp cancelled({:ok, _at}), do: :ok
  defp cancelled(refused), do: refused

  defp put_emit(%__MODULE__{ref: ref}, value, meta, key) do
    case reserved_key(meta) do
      nil ->
        push(key(ref), {value, meta, key})
        :ok

      k ->
        {:error, Error.new("runtime.reserved_metadata_key", key: k)}
    end
  end

  defp reserved_key(meta) do
    Enum.find_value(meta, fn {k, _v} ->
      ks = to_string(k)
      if String.starts_with?(ks, @reserved_prefix), do: ks
    end)
  end

  # One invocation's list under `key`: pushed newest first, taken once in call order.
  defp push(key, item), do: Process.put(key, [item | Process.get(key, [])])
  defp take(key), do: Enum.reverse(Process.delete(key) || [])

  defp key(ref), do: {__MODULE__, ref}
  defp changes_key(ref), do: {__MODULE__, ref, :changes}
  defp timers_key(ref), do: {__MODULE__, ref, :timers}
  defp checkpoint_key(ref), do: {__MODULE__, ref, :checkpoint}
  defp stable_id_key(ref), do: {__MODULE__, ref, :stable_id}
  defp completion_key(ref), do: {__MODULE__, ref, :completion}
end
