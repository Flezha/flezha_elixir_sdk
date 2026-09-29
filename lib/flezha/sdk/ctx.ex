defmodule Flezha.SDK.Ctx do
  @moduledoc """
  The invocation context handed to `Tasklet.run/3`. `use Flezha.SDK.Tasklet`
  imports the functions below, so a tasklet writes `emit(ctx, value)` and
  `complete(ctx, result)`; `apply/3` is not imported, since it would shadow
  `Kernel.apply/3`, and a `use` tasklet never calls it: its `defchange`
  recorders do.

  `emit/2`, `emit_with_meta/3`, `emit_keyed/3` and `emit_keyed_with_meta/4`
  accumulate outputs for this invocation; `apply/3` folds a change into the
  state through the tasklet's `apply_change/2` and records it for the Cell's record
  stream — one fold, one push, the only recorded mutation path, which a
  reference-form tasklet calls by its full name; `now/1` is a
  wall-clock millisecond stamp captured once at invocation entry;
  `cell_key/1` is the owning Cell's key (or nil for a Single Cell).
  Emits and changes are held in the running process's dictionary under a
  per-invocation ref (so `emit/2` can side-effect through an immutable struct)
  and are taken by the Cell only when `run/3` returns `{:ok, _}` — a failing
  invocation discards both. Reserved `flz.`-prefixed metadata keys are refused.

  A key rides an emit only where the node's sink is declared `keyed/1`, and a
  keyless emit from a Cell with no key of its own on such a sink is refused
  here, at the call — the reference's `KeyRequiredOnKeyedChannel` — from two
  topology facts the Cell hands `new/4`.

  Timers ride the same dictionary: `timer_after/2` and `timer_at_event_time/2`
  register on a `Flezha.SDK.Timers` session over a copy of the Cell's pending
  set — opened on the first timer call, so an invocation that registers
  nothing pays nothing — answering the absolute timestamp, the handle the two
  cancels take; the Cell commits the session's ops with the changes when
  `run/3` (or `on_timer/4`) answers `{:ok, _}`, and a firing commits its own
  removal even when it registered nothing. `now/1` is the declared clock's value: the
  epoch millisecond, the durable seq, or the input's event time, evaluated
  once per invocation — and, inside a firing, the scheduled timestamp. Inside a
  bounded operator's tasklet timers are unavailable until they fire there: the Cell
  hands `timers: :unavailable`, and the four timer calls answer
  `runtime.timers_unavailable` and arm nothing.

  Two more slots ride the same dictionary: under the effectively-once tier
  `checkpoint/1` raises the flag the Cell reads at the invocation's end to
  close its staged interval, and on every Cell `stable_id/1` answers a
  replay-stable id derived from the Cell's wire identity, the change seq the
  invocation started at and a per-invocation ordinal.

  `part/2` sends a preview of the invocation's output at once, on the sink
  route of the node's channel (`preview`), through the Cell's stream seam
  (`hold`), which never holds one; its number rides the dictionary under the ref, opened by
  the Cell for the attempt (`attempt`) and cleared by `discard/1`, which is
  what makes a part through a kept ctx `part.outside_invocation`.

  `spawn_bounded/4` asks the runner for a bounded instance and answers a
  `Flezha.SDK.Spawn` handle at once; an invocation of a replayed input asks
  nothing. `complete/2` rides the dictionary too, and is taken with the emits.
  """

  alias Flezha.SDK.{Counters, Error, Hold, Knob, Tasklet, Spawn, Timers}

  @enforce_keys [:module, :cell_key, :now_ms, :ref]
  defstruct [
    :module,
    :cell_key,
    :now_ms,
    :ref,
    :channel,
    :timers,
    :timer_policy,
    :firing,
    :runner,
    :origin,
    :operator_id,
    :counters,
    :preview,
    identity: <<>>,
    hold: nil,
    attempt: 1,
    seq: 0,
    replay?: false,
    keyed_sink?: false,
    key_required?: false
  ]

  @zero_tally %{registered: 0, fired: 0, cancelled: 0, refused: 0}

  # FNV-1a-128, the constants `flz_core::operator::instance` fixes; the
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
  (`key_required?`), the Cell's pending timers (`timers`, empty by default,
  `:unavailable` inside a bounded operator),
  the tasklet's timer policy (`timer_policy`, resolved from `module` by
  default), the firing this invocation is (`firing`, `{kind, at}` or nil), the
  runner a spawn request goes to (`runner`, nil for none), whether the input
  is a replay (`replay`), which issues no spawn, and the Cell's identity with
  the input-log offset it is taking (`origin`, nil for none), which names a
  spawn across a re-run of the input.
  """
  def new(module, cell_key, now_ms, opts \\ []) do
    # Nothing is written here: every reader of a slot defaults to empty, so an
    # invocation that emits nothing, applies nothing and registers no timer
    # touches the dictionary not at all — and one that never returns from here
    # leaves nothing behind to delete.
    %__MODULE__{
      module: module,
      cell_key: cell_key,
      now_ms: now_ms,
      ref: make_ref(),
      channel: Keyword.get(opts, :channel),
      timers: Keyword.get(opts, :timers, Timers.new()),
      timer_policy: Keyword.get_lazy(opts, :timer_policy, fn -> Tasklet.timer_policy(module) end),
      firing: Keyword.get(opts, :firing),
      runner: Keyword.get(opts, :runner),
      origin: Keyword.get(opts, :origin),
      operator_id: Keyword.get(opts, :operator_id),
      counters: Keyword.get(opts, :counters),
      preview: Keyword.get(opts, :preview),
      identity: Keyword.get(opts, :identity, <<>>),
      seq: Keyword.get(opts, :seq, 0),
      replay?: Keyword.get(opts, :replay, false),
      keyed_sink?: Keyword.get(opts, :keyed_sink?, false),
      key_required?: Keyword.get(opts, :key_required?, false)
    }
  end

  @doc false
  # A Cell's ctx before any invocation: its static facts, built once at start and
  # updated per invocation with that invocation's own fields.
  @spec template(module(), term(), binary(), keyword()) :: t()
  def template(module, cell_key, identity, opts) do
    new(module, cell_key, 0, [
      {:timer_policy, Tasklet.timer_policy(module)},
      {:identity, identity} | opts
    ])
  end

  @doc """
  Fold `change` into `state` with the tasklet's `apply_change/2` and record
  it, so the Cell ships it after the invocation succeeds. The returned state
  is what `run/3` returns in its `{:ok, _}`.
  """
  @spec apply(t(), term(), term()) :: term()
  def apply(%__MODULE__{module: module, ref: ref}, state, change) do
    Process.put(changes_key(ref), [change | Process.get(changes_key(ref), [])])
    module.apply_change(state, change)
  end

  @doc "Emit a value on the tasklet's outgoing channels."
  @spec emit(t(), term()) :: :ok | {:error, Error.t()}
  def emit(%__MODULE__{} = ctx, value), do: emit_with_meta(ctx, value, %{})

  @doc "Emit a value with metadata. Reserved `flz.`-prefixed keys are refused."
  @spec emit_with_meta(t(), term(), map()) :: :ok | {:error, Error.t()}
  def emit_with_meta(%__MODULE__{key_required?: true, channel: channel}, _value, _meta),
    do: {:error, Error.new("runtime.key_required_on_keyed_channel", channel: channel)}

  def emit_with_meta(%__MODULE__{} = ctx, value, meta) when is_map(meta),
    do: put_emit(ctx, value, meta, nil)

  @doc "Emit under the output's natural key — the sink must be declared `keyed/1`."
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

  @doc "The declared clock's value, evaluated once at invocation entry; the scheduled timestamp inside a firing."
  def now(%__MODULE__{now_ms: ms}), do: ms

  @doc "Register a processing-time timer `delay_ms` after `now/1`; `{:ok, at}`, the cancellation handle."
  @spec timer_after(t(), integer()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def timer_after(%__MODULE__{} = ctx, delay_ms),
    do: with_session(ctx, &Timers.register_after(&1, delay_ms))

  @doc "Register an event-time timer at `at`; refused under a clock that is not `:event_time`."
  @spec timer_at_event_time(t(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def timer_at_event_time(%__MODULE__{} = ctx, at),
    do: with_session(ctx, &Timers.register(&1, :event_time, at))

  @doc "Cancel the processing-time timer `timer_after/2` answered; whether one was pending."
  @spec cancel_processing_time_timer(t(), non_neg_integer()) ::
          boolean() | {:error, Error.t()}
  def cancel_processing_time_timer(%__MODULE__{} = ctx, at),
    do: cancel(ctx, :processing_time, at)

  @doc "Cancel the event-time timer `timer_at_event_time/2` answered; whether one was pending."
  @spec cancel_event_time_timer(t(), non_neg_integer()) ::
          boolean() | {:error, Error.t()}
  def cancel_event_time_timer(%__MODULE__{} = ctx, at), do: cancel(ctx, :event_time, at)

  @doc "The owning Cell's key, or nil for a Single Cell."
  def cell_key(%__MODULE__{cell_key: k}), do: k

  @doc """
  Request spawn-or-deliver of a bounded instance of `factory_id` in this
  runner's namespace, with `payload` as its first input: bytes as they are,
  anything else as JSON, and a term with no JSON form raises. Answers a
  `Flezha.SDK.Spawn` handle without waiting, read with
  `Flezha.SDK.Spawn.await/2` or `Flezha.SDK.Spawn.try_result/1` inside this
  invocation, or not at all.

  With `id:` the instance starts under that id when it is free; while it runs,
  `if_running: :deliver` (the default) hands it `payload` as one more input and
  `if_running: :error` resolves `already_running`. `if_running:` without `id:`
  raises `ArgumentError` and sends nothing.

  Without `id:` the controller generates one, and every call starts a new
  instance: it never delivers into a running one, a call reissued after a lost
  or timed-out answer starts a second instance, and a replayed invocation
  resolves an empty id. A crash re-run of an input the Cell had not durably
  consumed asks again under the same identity, and the controller answers the
  instance the first ask started. Pass `id:` for at most one instance, or for
  the id on a replay.
  """
  @spec spawn_bounded(t(), String.t(), term(), [Spawn.option()]) :: Spawn.t()
  def spawn_bounded(%__MODULE__{} = ctx, factory_id, payload, opts \\ []) do
    payload = plain_payload(payload)
    identity = spawn_identity(ctx, factory_id, payload)
    Spawn.request(ctx.runner, ctx.replay?, factory_id, payload, opts, identity)
  end

  # FNV-1a-128 over the origin, the call's position in the invocation, the
  # factory and the payload, each part length-prefixed, as the shared core
  # derives it. Empty for an invocation that took no offset: nothing re-runs
  # it, and two of its inputs with one payload are two requests.
  defp spawn_identity(%__MODULE__{origin: {cell, offset}, ref: ref}, factory_id, payload)
       when is_binary(factory_id) and is_binary(payload) do
    position = Process.get(spawn_key(ref), 0)
    Process.put(spawn_key(ref), position + 1)

    [cell, <<offset::64-big>>, <<position::32-big>>, factory_id, payload]
    |> Enum.reduce(@fnv128_offset_basis, fn part, hash ->
      hash |> fnv1a128(<<byte_size(part)::64-big>>) |> fnv1a128(part)
    end)
    |> hex128()
  end

  defp spawn_identity(%__MODULE__{}, _factory_id, _payload), do: ""

  @doc """
  End the bounded instance this tasklet runs in, with `result` as the
  result its completion reports. The rest of the iteration still runs and
  commits; once that commit is durable the instance ends instead of waiting
  for its next input, re-seeding a chained loop or parking. In a `:once`
  bounded operator it only sets the result the pass ends with. `result` is
  encoded as `spawn_bounded/4` encodes its payload; `nil` carries no result.

  Called more than once in one iteration, the instance ends once, with the
  result of the tasklet latest in the chain's order and, within it, of the
  Cell whose key sorts last. A failing invocation discards its call as it
  discards its emits, and an iteration that fails discards the call with it.

  An unbounded operator never completes: an unbounded tasklet calling this fails its input,
  without a retry, with an error beginning `CompleteOutsideBounded`.
  """
  @spec complete(t(), term()) :: :ok
  def complete(%__MODULE__{ref: ref}, result) do
    Process.put(completion_key(ref), if(is_nil(result), do: "", else: plain_payload(result)))
    :ok
  end

  # Read by a runner in any language, so unlike a sink's default this never
  # falls back to Erlang terms: a term with no JSON form raises.
  defp plain_payload(v) when is_binary(v), do: v
  defp plain_payload(v), do: Jason.encode!(v)

  @doc false
  @spec completion(t()) :: binary() | nil
  def completion(%__MODULE__{ref: ref}), do: Process.get(completion_key(ref))

  @doc """
  Close the staged checkpoint interval at the end of this invocation. A no-op
  on a class that did not declare checkpoint granularity — the Cell reads the
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
  Cell's wire identity, the change seq this invocation started at and a
  per-invocation ordinal. The same invocation on a replay derives the same
  ids in the same order, which is what makes an effect idempotent. The seq
  moves on only when an invocation records a change, a timer op or an effect,
  so on a Cell that records nothing every invocation answers the same ids.
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
    |> hex128()
  end

  defp hex128(hash),
    do: hash |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(32, "0")

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
  # none. A Cell whose timers are unavailable never armed one, so its set is
  # the empty one it was seeded with.
  @doc false
  def take_timers(%__MODULE__{timers: :unavailable}), do: {Timers.new(), [], @zero_tally}

  def take_timers(%__MODULE__{ref: ref, firing: nil} = ctx) do
    case Process.delete(timers_key(ref)) do
      nil -> {ctx.timers, [], @zero_tally}
      session -> Timers.commit(session)
    end
  end

  def take_timers(%__MODULE__{ref: ref} = ctx) do
    Timers.commit(Process.delete(timers_key(ref)) || open(ctx))
  end

  @doc """
  Send `value` as a part: a preview of this invocation's output on its
  channel, which leaves now rather than when `run/3` returns, encoded as an
  emit of `value` would be. It never waits: a part the wire cannot take at once
  is dropped and counted, and an invocation of a replayed input sends none.

  `:ok`, or `{:error, %Error{}}`: `part.no_sink` when the node's channel does
  not end in a sink (a `response()` end is not one), `part.outside_invocation`
  through a ctx whose invocation has returned.
  """
  @spec part(t(), term()) :: :ok | {:error, Error.t()}
  def part(%__MODULE__{preview: nil} = ctx, _value) do
    {:error,
     Error.new("part.no_sink",
       tasklet: ctx.module.name(),
       operator: ctx.operator_id,
       channel: ctx.channel
     )}
  end

  def part(%__MODULE__{ref: ref} = ctx, value) do
    case Process.get(part_key(ref)) do
      nil ->
        {:error, Error.new("part.outside_invocation", tasklet: ctx.module.name())}

      _next when ctx.replay? ->
        :ok

      next ->
        outputs = ctx.preview.transform.(value)
        Process.put(part_key(ref), next + length(outputs))
        outputs |> Enum.with_index(next) |> Enum.each(fn {out, n} -> send_part(ctx, out, n) end)
    end
  end

  @doc false
  # Opened by the Cell for each attempt of a node that can send parts, so a
  # node that cannot pays nothing.
  def open_parts(%__MODULE__{preview: nil}), do: :ok

  def open_parts(%__MODULE__{ref: ref}) do
    Process.put(part_key(ref), 0)
    :ok
  end

  defp send_part(%__MODULE__{} = ctx, value, number) do
    bytes = ctx.preview.serializer.(value)

    # A local run's owner reads only its operator's outputs off its in-process
    # stream, so a part sent there is dropped by the owner.
    outcome =
      cond do
        not Hold.bound?(ctx.hold) ->
          :parts_dropped_no_stream

        byte_size(bytes) > Knob.get(:max_frame_bytes) ->
          :parts_dropped_too_large

        true ->
          write_part(ctx, bytes, number)
      end

    if ctx.counters, do: Counters.inc_part(ctx.counters, outcome)
    :ok
  end

  defp write_part(ctx, bytes, number) do
    case Hold.write("output_part", ctx.hold, {bytes, part_stamps(ctx, number), ctx.operator_id}) do
      {:ok, _hold} -> :parts_sent
      {_refused, _hold} -> :parts_dropped_refused
    end
  end

  defp part_stamps(%__MODULE__{} = ctx, number) do
    stamps = %{
      "channel" => ctx.preview.channel,
      "flz.part" => Integer.to_string(number),
      "flz.part_attempt" => Integer.to_string(ctx.attempt),
      "flz.cell" => "#{ctx.operator_id}/#{ctx.module.name()}/#{ctx.cell_key || ""}",
      "flz.operator" => ctx.operator_id
    }

    if ctx.cell_key, do: Map.put(stamps, "flz.key", ctx.cell_key), else: stamps
  end

  @doc false
  def discard(%__MODULE__{ref: ref}) do
    Process.delete(key(ref))
    Process.delete(changes_key(ref))
    Process.delete(timers_key(ref))
    Process.delete(checkpoint_key(ref))
    Process.delete(stable_id_key(ref))
    Process.delete(spawn_key(ref))
    Process.delete(completion_key(ref))
    Process.delete(part_key(ref))
  end

  defp with_session(%__MODULE__{timers: :unavailable}, _fun), do: {:error, timers_unavailable()}

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

  defp cancel(%__MODULE__{timers: :unavailable}, _kind, _at), do: {:error, timers_unavailable()}

  defp cancel(%__MODULE__{ref: ref} = ctx, kind, at) do
    {changed?, session} = Timers.cancel(session(ctx), kind, at)
    Process.put(timers_key(ref), session)
    changed?
  end

  defp timers_unavailable, do: Error.new("runtime.timers_unavailable")

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
        {:error, Error.new("runtime.reserved_metadata_key", key: k)}
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
  defp spawn_key(ref), do: {__MODULE__, ref, :spawn}
  defp completion_key(ref), do: {__MODULE__, ref, :completion}
  defp part_key(ref), do: {__MODULE__, ref, :part}
end
