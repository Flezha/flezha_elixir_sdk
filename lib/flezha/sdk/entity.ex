defmodule Flezha.SDK.Entity do
  @moduledoc """
  One process per Entity: a bounded input queue, the retry-then-park dispatch
  ladder, and per-Entity ordering (arrival order out of the queue). Inputs under
  `:drop_nothing` arrive as a `GenServer.call` the Entity answers only when
  there is room — never dropped, never refused (the caller, the Operative
  router, waits, which is the documented co-tenancy cost);
  `:drop_newest`/`:drop_oldest` arrive as casts. A completed dispatch bumps `dispatches`; an exhausted ladder parks
  the input and HOLDS the Entity until the controller's `ParkPayloadAck`.

  A changeful Entity (its procedure exports `apply_change/2`) is also the
  writer of its own record stream: after a successful dispatch it ships one
  `entity_state_change` frame per recorded change at the next seq on one
  secondary stream, before the dispatch's outputs leave, then writes its
  `{state, seq}` into the Operative's slot table so a replacement started
  after an untrappable kill continues on the same seq line. It answers a
  restore (fold, re-arm), a compaction (the snapshot after its records), a
  live read and a state apply, each in mailbox order so no dispatch
  interleaves. With the class's persistence off the seq advances and nothing
  ships. Its outputs reach the Operative with their metadata, their keys and
  the seq the dispatch started at — what a keyed sink's stamps read.

  Its timers fire through the same queue and ladder as its inputs, with
  `on_timer/4` in place of `run/3`: a processing-time timer is one
  `Process.send_after/3` on the BEAM's timer service — the wake list no
  thread of this SDK owns — whose due message folds every timer due by then
  onto the queue, ascending; an event-time timer fires at admission, when an
  input's event time advances the high-water past it, ahead of that input. A
  firing's ops and outputs ship as a dispatch's do, its removal committed
  even when it parks; a restore, a slot and an eviction re-arm, re-arm and
  discard the armed set with the pending one.
  """

  use GenServer
  require Logger

  alias Flezha.SDK.{Counters, Ctx, Jitter, Park, Procedure, Timers}
  alias Flezha.SDK.Entity.{Records, State}
  alias Flezha.SDK.Operative.Inbox

  @retry_backoff_cap_ms 5_000
  # A due message that finds no room comes back this much later, never drops.
  @wake_retry_backoff_ms 5

  # A due timer: on the queue beside the inputs, dispatched through the same
  # ladder with `on_timer/4` in place of `run/3`.
  defmodule Fire do
    @moduledoc false
    defstruct [:kind, :at]
  end

  # --- API (used by the Operative router) ---

  @doc "Start an Entity, unlinked: its Operative monitors it, and it monitors its Operative."
  def start(opts), do: GenServer.start(__MODULE__, opts)

  @doc "Enqueue under `:drop_nothing` — the caller waits until there is room."
  def enqueue_block(pid, value), do: GenServer.call(pid, {:enqueue, value}, :infinity)

  @doc "Enqueue under a drop policy — non-blocking."
  def enqueue_drop(pid, value, policy), do: GenServer.cast(pid, {:enqueue_drop, value, policy})

  @doc "Release a held (parked) Entity after its `ParkPayloadAck`."
  def release_park(pid, park_id), do: send(pid, {:park_released, park_id})

  @doc "Rebuild the state from a `RestoreInitState` payload; `{:ok, terminal_seq, migrated?}`."
  @spec restore(pid(), binary(), binary(), binary(), non_neg_integer()) ::
          {:ok, non_neg_integer(), boolean()} | {:error, String.t()}
  def restore(pid, snapshot_header, snapshot_bytes, change_log_bytes, terminal_hint),
    do:
      GenServer.call(
        pid,
        {:restore, snapshot_header, snapshot_bytes, change_log_bytes, terminal_hint},
        :infinity
      )

  @doc "Ship the state as an `entity_state_snapshot`; `{:ok, snapshot_seq, body_size}`."
  @spec snapshot(pid()) :: {:ok, non_neg_integer(), non_neg_integer()} | {:error, String.t()}
  def snapshot(pid), do: GenServer.call(pid, :snapshot, :infinity)

  @doc """
  The state and the seq it was read at; `{:error, :busy}` when a dispatch holds
  the Entity past `timeout` — the reference's serialize gate.
  """
  @spec read(pid(), timeout()) :: {:ok, term(), non_neg_integer()} | {:error, :busy | :gone}
  def read(pid, timeout) do
    GenServer.call(pid, :read, timeout)
  catch
    :exit, {:timeout, _} -> {:error, :busy}
    :exit, _gone -> {:error, :gone}
  end

  @doc "Replace the state with a JSON object through the procedure's `state_from_json/1`."
  @spec apply_state(pid(), map()) :: :ok | {:error, String.t()}
  def apply_state(pid, json), do: GenServer.call(pid, {:apply_state, json}, :infinity)

  @doc """
  Whether the Entity has been idle past `ttl_ms` holding no work at all — an
  empty queue, no dispatch in flight, no park held, no deferred push. A probe
  that times out (a dispatch in flight) is `false`, and its late reply is
  inert to the caller.
  """
  @spec idle_for?(pid(), pos_integer(), timeout()) :: boolean()
  def idle_for?(pid, ttl_ms, timeout) do
    GenServer.call(pid, {:idle?, ttl_ms}, timeout)
  catch
    :exit, _ -> false
  end

  @doc """
  Evict an Entity the caller proved idle: it ships `delete_entity_state` when
  it persists, drops its slot so a successor is a cold start, and stops.
  """
  @spec evict(pid()) :: :evicted | :gone
  def evict(pid) do
    GenServer.call(pid, :evict, :infinity)
  catch
    :exit, _ -> :gone
  end

  @doc """
  An Entity's slot identity in `Flezha.SDK.Counters`. Its Operative computes the
  same value to release the slot of an Entity that died mid-dispatch — the one
  unwind an `after` inside the Entity cannot reach.
  """
  @spec identity(binary(), term(), term()) :: term()
  def identity(operative_id, node_ref, entity_key),
    do: {operative_id, node_ref, entity_key}

  # --- server ---

  @impl true
  def init(opts) do
    module = Keyword.fetch!(opts, :module)
    entity_key = Keyword.get(opts, :entity_key)
    operative_id = Keyword.fetch!(opts, :operative_id)
    identity = Park.identity(operative_id, module.name(), entity_key || "")
    changeful? = Procedure.changeful?(module)
    slots = Keyword.get(opts, :slots)
    {proc_state, change_seq, timers} = Records.seed(slots, identity, module, entity_key)
    operative = Keyword.fetch!(opts, :operative)
    Process.monitor(operative)

    state = %State{
      operative: operative,
      operative_id: operative_id,
      node_ref: Keyword.fetch!(opts, :node_ref),
      node_name: Keyword.get(opts, :node_name),
      input_channel: Keyword.get(opts, :input_channel),
      module: module,
      entity_key: entity_key,
      capacity: Keyword.fetch!(opts, :capacity),
      policy: Keyword.fetch!(opts, :policy),
      retry_max: Keyword.fetch!(opts, :retry_max),
      retry_backoff_ms: Keyword.fetch!(opts, :retry_backoff_ms),
      counters: Keyword.fetch!(opts, :counters),
      identity: identity,
      slots: slots,
      credit: Keyword.get(opts, :credit),
      # The static ctx facts: the node's, from the Operative, and the timer
      # policy this procedure declares.
      ctx_opts:
        [timer_policy: Procedure.timer_policy(module)] ++ Keyword.get(opts, :ctx_opts, []),
      timers: %{
        pending: timers,
        clock: clock_of(module),
        on_timer?: Procedure.on_timer?(module),
        refs: %{},
        high_water: nil
      },
      changeful?: changeful?,
      persistent?: changeful? and Keyword.get(opts, :persistence_enabled, true),
      schema_version: Procedure.schema_version(module),
      proc_state: proc_state,
      change_seq: change_seq,
      last_input_ms: now_ms()
    }

    {:ok, state |> Records.put_secondaries(Keyword.get(opts, :secondaries, [])) |> rearm()}
  end

  @impl true
  def handle_call({:enqueue, value}, from, %State{} = s) do
    if s.size < s.capacity do
      # Room now — accept immediately (the caller does not block).
      {:reply, :ok, kick(enqueue(s, value))}
    else
      # No room — defer the reply until a dispatch frees a slot (block policy).
      Counters.inc_blocked_parks(s.counters)
      # An accepted-but-unqueued push is invisible to `dispatches` and to the
      # handout report, so it carries its own: the age reported is the oldest
      # one's, which is why marking is idempotent while any is held.
      Counters.deferred_started(s.counters, identity(s.operative_id, s.node_ref, s.entity_key))
      {:noreply, %{s | waiters: :queue.in({from, value}, s.waiters)}}
    end
  end

  def handle_call({:restore, header, snapshot, log, hint}, _from, %State{} = s) do
    case Records.restore(s, header, snapshot, log, hint) do
      {:ok, s, terminal, migrated} -> {:reply, {:ok, terminal, migrated}, rearm(s)}
      {:error, reason} -> {:reply, {:error, reason}, s}
    end
  end

  def handle_call(:snapshot, _from, %State{} = s) do
    case Records.snapshot(s) do
      {:ok, s, seq, size} -> {:reply, {:ok, seq, size}, s}
      {:error, reason} -> {:reply, {:error, reason}, s}
    end
  end

  def handle_call(:read, _from, %State{} = s),
    do: {:reply, {:ok, s.proc_state, s.change_seq}, s}

  def handle_call({:idle?, ttl_ms}, _from, %State{} = s) do
    idle? =
      s.size == 0 and not s.processing? and not s.held? and :queue.is_empty(s.waiters) and
        now_ms() - s.last_input_ms > ttl_ms

    {:reply, idle?, s}
  end

  def handle_call(:evict, _from, %State{} = s) do
    Records.delete(s)
    {:stop, :normal, :evicted, s}
  end

  def handle_call({:apply_state, json}, _from, %State{} = s) do
    {:reply, :ok,
     Records.write_slot(%{s | proc_state: Procedure.state_from_json(s.module, json)})}
  rescue
    e -> {:reply, {:error, "apply_state: #{Exception.message(e)}"}, s}
  end

  @impl true
  def handle_cast({:enqueue_drop, value, policy}, %State{} = s) do
    cond do
      s.size < s.capacity ->
        {:noreply, kick(enqueue(s, value))}

      policy == :drop_oldest ->
        {{:value, _dropped}, q} = :queue.out(s.queue)
        record_drop(s, :drop_oldest)
        {:noreply, kick(enqueue(%{s | queue: q, size: s.size - 1}, value))}

      true ->
        record_drop(s, :drop_newest)
        {:noreply, s}
    end
  end

  @impl true
  def handle_info(:process, %State{} = s), do: {:noreply, process_next(s)}

  def handle_info({:secondaries, secondaries}, %State{} = s),
    do: {:noreply, Records.put_secondaries(s, secondaries)}

  def handle_info({:park_released, park_id}, %State{park_id: park_id} = s) do
    {:noreply, process_next(%{s | held?: false, park_id: nil})}
  end

  def handle_info({:park_released, _stale}, s), do: {:noreply, s}

  # The BEAM's timer service is the wake list: a due message folds every
  # processing-time timer due by now onto the queue, ascending, so the order
  # never depends on the order two same-millisecond messages arrive in; one
  # that finds no room comes back later rather than dropping.
  def handle_info({:timer_due, at}, %State{size: size, capacity: capacity} = s)
      when size >= capacity do
    Process.send_after(self(), {:timer_due, at}, @wake_retry_backoff_ms)
    {:noreply, s}
  end

  def handle_info({:timer_due, at}, %State{} = s) do
    upto = max(at, System.system_time(:millisecond))

    s =
      s.timers.pending
      |> Timers.due(:processing_time, upto)
      |> Enum.reduce(s, &enqueue(&2, %Fire{kind: :processing_time, at: &1}))

    {:noreply, kick(s)}
  end

  # The Operative is gone — stopped or crashed — so this Entity ends with it,
  # quietly, through `terminate/2`. Not a link: a link discards a `:normal`
  # exit signal, which is exactly how a removed operative's Entities outlived it.
  def handle_info({:DOWN, _ref, :process, operative, _reason}, %State{operative: operative} = s),
    do: {:stop, :normal, s}

  def handle_info(_msg, s), do: {:noreply, s}

  # Evicted, or its Operative gone: the armed timers go with the process.
  @impl true
  def terminate(_reason, %State{} = s), do: disarm_all(s)

  # --- internals ---

  defp enqueue(%State{} = s, value),
    do: %{s | queue: :queue.in(value, s.queue), size: s.size + 1, last_input_ms: now_ms()}

  defp kick(%State{processing?: false, held?: false, size: size} = s) when size > 0 do
    send(self(), :process)
    %{s | processing?: true}
  end

  defp kick(s), do: s

  defp process_next(%State{held?: true} = s), do: s

  defp process_next(%State{} = s) do
    case :queue.out(s.queue) do
      {:empty, _} ->
        admit_waiter(%{s | processing?: false})

      {{:value, item}, q} ->
        s = %{s | queue: q, size: s.size - 1}
        s = admit(s, item)
        s = admit_waiter(s)

        if s.held? do
          %{s | processing?: false}
        else
          # keep draining
          send(self(), :process)
          %{s | processing?: true}
        end
    end
  end

  # A freed slot admits the next blocked caller (reply :ok, enqueue its value).
  defp admit_waiter(%State{} = s) do
    if s.size < s.capacity do
      case :queue.out(s.waiters) do
        {:empty, _} ->
          note_no_deferral(s)

        {{:value, {from, value}}, waiters} ->
          GenServer.reply(from, :ok)
          s = %{s | waiters: waiters} |> enqueue(value)
          if :queue.is_empty(waiters), do: note_no_deferral(s), else: s
      end
    else
      s
    end
  end

  # Release this Entity's deferred-push slot: nothing is held any more.
  # Idempotent, so the drain need not know whether one was ever taken.
  defp note_no_deferral(%State{} = s) do
    Counters.deferred_finished(s.counters, identity(s.operative_id, s.node_ref, s.entity_key))
    s
  end

  # A firing whose timer has left the pending set fires nothing. A value under an
  # event-time clock advances the high-water first, and the event-time timers
  # it makes due go ahead of it on the queue, ascending — the drain comes back
  # to the value behind them. Anything else dispatches.
  defp admit(%State{} = s, %Fire{kind: kind, at: at} = fire) do
    if Timers.contains?(s.timers.pending, kind, at), do: dispatch(s, fire), else: s
  end

  defp admit(%State{timers: %{clock: {:event_time, extract}} = timers} = s, value) do
    case safe_extract(extract, value) do
      {:ok, event_time} ->
        high_water = max(timers.high_water || -1, event_time)
        s = %{s | timers: %{timers | high_water: high_water}}

        case Timers.due(timers.pending, :event_time, high_water) do
          [] -> dispatch(s, value)
          due -> requeue(s, Enum.map(due, &%Fire{kind: :event_time, at: &1}) ++ [value])
        end

      # The ladder meets the raise and parks the value as any bad input.
      :error ->
        dispatch(s, value)
    end
  end

  defp admit(%State{} = s, value), do: dispatch(s, value)

  # Items at the head, in order; the size overshoots the capacity by internal
  # items only, bounded by the pending set.
  defp requeue(%State{} = s, items) do
    queue = items |> Enum.reverse() |> Enum.reduce(s.queue, &:queue.in_r/2)
    %{s | queue: queue, size: s.size + length(items)}
  end

  defp safe_extract(extract, value) do
    {:ok, extract.(value)}
  rescue
    _ -> :error
  catch
    _, _ -> :error
  end

  # `after` is the contract, not politeness: the in-flight report is what makes
  # ONE wedged Entity visible to the controller, so a dispatch that unwinds
  # (raise, throw, exit) must release its slot on the way out or the runner
  # would report a stall that healed long ago.
  defp dispatch(%State{} = s, item) do
    identity = identity(s.operative_id, s.node_ref, s.entity_key)
    Counters.dispatch_started(s.counters, identity)

    try do
      case run_ladder(s, item, 1) do
        {:ok, new_proc_state, emits, changes, timers} ->
          complete(s, new_proc_state, emits, changes, timers)

        {:park, error, attempts} ->
          s |> park_firing(item) |> park(item, error, attempts)
      end
    after
      Counters.dispatch_finished(s.counters, identity)
    end
  end

  # The records leave first (flush-before-emit, and the outputs are stamped
  # with the seq the dispatch started at), then the timers are armed, then the
  # outputs leave: an output on the wire means its dispatch's timers are armed
  # and counted, for every reader.
  defp complete(%State{} = s, new_proc_state, emits, changes, {pending, ops, tally}) do
    Counters.inc_dispatches(s.counters)
    handout_seq = s.change_seq
    s = %{s | proc_state: new_proc_state, timers: %{s.timers | pending: pending}}
    s = s |> Records.ship(changes, ops) |> reflect(ops, tally)
    flush_emits(s, emits, handout_seq)
    s
  end

  # A firing that exhausted its ladder still commits its removal, so a poison
  # timer parks once instead of firing for ever.
  defp park_firing(%State{} = s, %Fire{kind: kind, at: at}) do
    policy = Keyword.fetch!(s.ctx_opts, :timer_policy)

    {pending, ops, tally} =
      s.timers.pending |> Timers.session(policy, at, {kind, at}) |> Timers.commit()

    s = %{s | timers: %{s.timers | pending: pending}}
    s |> Records.ship([], ops) |> reflect(ops, tally)
  end

  defp park_firing(%State{} = s, _value), do: s

  # The clock is read inside the attempt: an extractor that raises on an input
  # is that input failing, and it rides the ladder like any other failure.
  defp run_ladder(%State{} = s, item, attempt) do
    outcome =
      try do
        ctx = Ctx.new(s.module, s.entity_key, clock_now(s, item), ctx_opts(s, item))
        {:result, ctx, call(s, item, ctx)}
      rescue
        e -> {:raised, Exception.message(e)}
      catch
        kind, reason -> {:raised, "#{kind}: #{inspect(reason)}"}
      end

    case outcome do
      {:result, ctx, {:ok, new_state}} ->
        {:ok, new_state, Ctx.take_emits(ctx), Ctx.take_changes(ctx), Ctx.take_timers(ctx)}

      {:result, ctx, {:error, {:non_retryable, reason}}} ->
        Ctx.discard(ctx)
        {:park, format_error(reason), attempt}

      {:result, ctx, {:error, reason}} ->
        Ctx.discard(ctx)
        retry_or_park(s, item, attempt, format_error(reason))

      {:raised, message} ->
        retry_or_park(s, item, attempt, message)
    end
  end

  # `on_timer/4` on a firing, `run/3` on an input; a firing on a procedure that
  # exports no handler parks by name, without the retries a raise would climb.
  defp call(%State{timers: %{on_timer?: true}} = s, %Fire{kind: kind, at: at}, ctx),
    do: s.module.on_timer(at, kind, s.proc_state, ctx)

  defp call(%State{module: module}, %Fire{}, _ctx) do
    {:error,
     {:non_retryable,
      "procedure `#{module.name()}` registered a timer but does not export on_timer/4"}}
  end

  defp call(%State{} = s, value, ctx), do: s.module.run(value, s.proc_state, ctx)

  # The declared clock, evaluated once per attempt: the epoch millisecond, the
  # durable seq this dispatch starts at, or the input's own event time — and,
  # inside a firing, the scheduled timestamp under every clock.
  defp clock_now(_s, %Fire{at: at}), do: at
  defp clock_now(%State{timers: %{clock: :wall}}, _value), do: System.system_time(:millisecond)
  defp clock_now(%State{timers: %{clock: :logical}, change_seq: seq}, _value), do: seq
  defp clock_now(%State{timers: %{clock: {:event_time, extract}}}, value), do: extract.(value)

  defp clock_of(module) do
    case Procedure.clock(module) do
      :event_time -> {:event_time, &module.event_time_of/1}
      clock -> clock
    end
  end

  # The invocation's timer session opens over the live pending set; a firing's
  # opens on its own removal.
  defp ctx_opts(%State{} = s, item),
    do: [timers: s.timers.pending, firing: firing(item)] ++ s.ctx_opts

  defp firing(%Fire{kind: kind, at: at}), do: {kind, at}
  defp firing(_value), do: nil

  # Committed ops onto the BEAM's timer service — one `send_after` per
  # processing-time registration, cancelled per removal — the counters and
  # the telemetry. A dispatch that touched no timer — the common one — pays
  # nothing here.
  defp reflect(%State{} = s, [], %{registered: 0, fired: 0, cancelled: 0, refused: 0}), do: s

  defp reflect(%State{} = s, ops, tally) do
    s =
      Enum.reduce(ops, s, fn
        %{kind: :processing_time, register: true, scheduled_at: at}, acc -> arm(acc, at)
        %{kind: :processing_time, register: false, scheduled_at: at}, acc -> disarm(acc, at)
        _event_time, acc -> acc
      end)

    if tally.fired > 0, do: Counters.inc_timers_fired(s.counters)
    Enum.each(tally, fn {event, n} -> if n > 0, do: timers_event(s, event, n) end)
    s
  end

  defp arm(%State{} = s, at) do
    %State{timers: timers} = s = disarm(s, at)
    delay = max(at - System.system_time(:millisecond), 0)
    ref = Process.send_after(self(), {:timer_due, at}, delay)
    Counters.add_timers_armed(s.counters, 1)
    %{s | timers: %{timers | refs: Map.put(timers.refs, at, ref)}}
  end

  defp disarm(%State{timers: timers} = s, at) do
    case Map.pop(timers.refs, at) do
      {nil, _refs} ->
        s

      {ref, refs} ->
        Process.cancel_timer(ref)
        Counters.add_timers_armed(s.counters, -1)
        %{s | timers: %{timers | refs: refs}}
    end
  end

  defp disarm_all(%State{timers: timers} = s),
    do: Enum.reduce(Map.keys(timers.refs), s, &disarm(&2, &1))

  # What a restore folded or a slot seeded, armed afresh; an overdue one fires
  # at once.
  defp rearm(%State{} = s) do
    s = disarm_all(s)
    Enum.reduce(Timers.all(s.timers.pending, :processing_time), s, &arm(&2, &1))
  end

  defp timers_event(%State{} = s, event, count) do
    :telemetry.execute([:flezha, :sdk, :timers], %{count: count}, %{
      operative_id: s.operative_id,
      procedure: s.module.name(),
      event: event
    })
  end

  defp retry_or_park(%State{retry_max: max}, _value, attempt, error) when attempt >= max,
    do: {:park, error, attempt}

  defp retry_or_park(%State{} = s, value, attempt, error) do
    Logger.debug(
      "[Flezha.SDK] #{State.label(s)} attempt #{attempt}/#{s.retry_max} failed: #{error}; retrying"
    )

    :telemetry.execute([:flezha, :sdk, :retry], %{count: 1}, %{
      operative_id: s.operative_id,
      attempt: attempt
    })

    Process.sleep(backoff(s.retry_backoff_ms, attempt))
    run_ladder(s, value, attempt + 1)
  end

  # exp = base * 2^(attempt-1), capped at 5s, jittered uniform in [exp/2, exp*1.5).
  defp backoff(base_ms, attempt) do
    exp = min(base_ms * Bitwise.bsl(1, min(attempt - 1, 16)), @retry_backoff_cap_ms)
    min(Jitter.jitter(exp), @retry_backoff_cap_ms)
  end

  defp park(%State{} = s, value, error, attempts) do
    seq = s.park_seq
    park_id = "#{State.label(s)}##{seq}"

    channel = s.input_channel || s.node_name || s.operative_id
    body = Park.body(park_id, channel, error, attempts, value)

    Park.ship(
      s.operative,
      self(),
      Park.identity(s.operative_id, s.module.name(), s.entity_key),
      body
    )

    :telemetry.execute([:flezha, :sdk, :parked], %{count: 1}, %{operative_id: s.operative_id})

    %{s | held?: true, park_id: park_id, park_seq: seq + 1}
  end

  defp flush_emits(_s, [], _handout_seq), do: :ok

  defp flush_emits(%State{} = s, emits, handout_seq),
    do: Inbox.emitted(s.operative, s.node_ref, s.entity_key, emits, handout_seq)

  # Two readers, one event: telemetry is the in-node handle, the counter is what
  # leaves the runner on the heartbeat — a drop nobody can see from outside the
  # process is the defect this closes.
  defp record_drop(%State{} = s, policy) do
    Counters.inc_drops(s.counters, policy)

    :telemetry.execute([:flezha, :sdk, :mailbox], %{count: 1}, %{
      operative_id: s.operative_id,
      policy: policy
    })
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  defp format_error(reason) when is_binary(reason), do: reason
  defp format_error(reason), do: inspect(reason)
end
