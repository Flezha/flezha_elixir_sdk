defmodule Flezha.SDK.Entity do
  @moduledoc """
  One process per Entity: a bounded input queue, the retry-then-park dispatch
  ladder, and per-Entity ordering (arrival order out of the queue). Inputs under
  `:drop_nothing` arrive as a `GenServer.call` the Entity answers only when
  there is room — never dropped, never refused (the caller, the Operative
  router, waits, which is the documented co-tenancy cost);
  `:drop_newest`/`:drop_oldest` arrive as casts. A completed dispatch bumps `dispatches`; an exhausted ladder parks
  the input and HOLDS the Entity until the controller's `ParkPayloadAck`.

  An Entity that writes a record stream — a changeful one, whose procedure
  exports `apply_change/2`, and under the effectively-once tier a changeless
  one too, because there the record carries the outputs and the consumed
  offset rather than only changes — is the writer of it: after a successful dispatch it ships one
  `entity_state_change` frame per recorded change at the next seq on one
  secondary stream, before the dispatch's outputs leave, then writes its
  `{state, seq}` into the Operative's slot table so a replacement started
  after an untrappable kill continues on the same seq line. A frame no stream
  will take is held and replayed by `rebind/2`, so "before the outputs leave"
  survives a reconnect as well; an eviction hands what is still held, and the
  delete it owes, to the Operative, because a hold that dies with the process
  that owned it is not a hold. It answers a restore (fold, re-arm), a
  compaction (the snapshot after its records), a live read and a state apply,
  each in mailbox order so no dispatch interleaves. With the class's persistence off the seq advances and nothing
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

  alias Flezha.SDK.{Call, Counters, Ctx, Hold, Knob, Park, Procedure, Timers}
  alias Flezha.SDK.Entity.{Clock, Records, State, Tier}
  alias Flezha.SDK.Operative.Inbox

  # The live read's two verdicts. Every other refusal is `Flezha.SDK.Call`'s.
  @read_verdict %{timeout: {:error, :busy}, other: {:error, :gone}}

  # A due timer: on the queue beside the inputs, dispatched through the same
  # ladder with `on_timer/4` in place of `run/3`.
  defmodule Fire do
    @moduledoc false
    defstruct [:kind, :at]
  end

  # The deadline's close, on the queue in mailbox order behind the items that
  # preceded it, running no author code. A close that already happened leaves
  # this one to find nothing open.
  defmodule Close do
    @moduledoc false
    defstruct [:opened_at]
  end

  # A saga undo, on the queue like a firing is: `compensate/3` in place of
  # `run/3`, over the outputs that Entity released. Best-effort — a hook that
  # fails is logged and the plan moves on, so it never parks.
  defmodule Undo do
    @moduledoc false
    defstruct [:outputs]
  end

  # A workflow stage's barrier, on the queue behind that stage's batch. It
  # runs no author code: reaching it is the answer, because the queue is FIFO
  # and every item before it has been dispatched. An invocation that emits
  # nothing reports nothing, so a driver cannot count outputs to know its
  # stage finished — it enqueues one of these and waits for the reply.
  defmodule Barrier do
    @moduledoc false
    defstruct [:ref, :reply_to]
  end

  # An input under the effectively-once tier: the value with the durable
  # offset the controller stamped on it, and `replay` — `:member` while it
  # waits for its slice to close, `:ordered` once the flush re-queued it in
  # event-time order. Off the tier the queue holds the raw value, so nothing
  # there pays for this.
  defmodule Input do
    @moduledoc false
    defstruct [:value, :offset, :replay]
  end

  # --- API (used by the Operative router) ---

  @doc "Start an Entity, unlinked: its Operative monitors it, and it monitors its Operative."
  def start(opts), do: GenServer.start(__MODULE__, opts)

  @doc """
  Enqueue under `:drop_nothing` — the caller waits until there is room, with no
  bound: the policy promises an input is never dropped and never refused.
  """
  def enqueue_block(pid, value), do: GenServer.call(pid, {:enqueue, value}, :infinity)

  @doc "Run this Entity's `compensate/3` over the outputs it released."
  def compensate(pid, outputs), do: enqueue_block(pid, %Undo{outputs: outputs})

  @doc """
  Enqueue a barrier behind whatever this Entity already holds: `{:stage_done,
  ref, entity_pid}` reaches `reply_to` once the queue has drained past it.
  """
  def barrier(pid, ref, reply_to),
    do: enqueue_block(pid, %Barrier{ref: ref, reply_to: reply_to})

  @doc "Enqueue under a drop policy — non-blocking."
  def enqueue_drop(pid, value, policy), do: GenServer.cast(pid, {:enqueue_drop, value, policy})

  @doc "Release a held (parked) Entity after its `ParkPayloadAck`."
  def release_park(pid, park_id), do: send(pid, {:park_released, park_id})

  @doc """
  Rebuild from a `RestoreInitState` payload; `{:ok, terminal_seq, migrated?}`.
  The fold grows with the change log, so it waits its own row.
  """
  @spec restore(pid(), binary(), binary(), binary(), non_neg_integer()) ::
          {:ok, non_neg_integer(), boolean()} | {:error, String.t()}
  def restore(pid, snapshot_header, snapshot_bytes, change_log_bytes, terminal_hint) do
    request = {:restore, snapshot_header, snapshot_bytes, change_log_bytes, terminal_hint}
    Call.entity(pid, request, timeout: Knob.get(:sdk_entity_restore_ms))
  end

  @doc """
  Ship the state as an `entity_state_snapshot`; `{:ok, snapshot_seq, body_size}`.
  Serialising grows with the state, so it waits inside the Controller's own
  compaction bound.
  """
  @spec snapshot(pid()) :: {:ok, non_neg_integer(), non_neg_integer()} | {:error, String.t()}
  def snapshot(pid), do: Call.entity(pid, :snapshot, timeout: Knob.get(:sdk_entity_snapshot_ms))

  @doc """
  The state and the seq it was read at; `{:error, :busy}` when a dispatch holds
  the Entity past `timeout` — the reference's serialize gate.
  """
  @spec read(pid(), timeout()) :: {:ok, term(), non_neg_integer()} | {:error, :busy | :gone}
  # `log: false`: the walk names the Procedure and the key in a line of its own.
  def read(pid, timeout),
    do: Call.entity(pid, :read, timeout: timeout, log: false, on_error: @read_verdict)

  @doc """
  Replace the state with a JSON object through the procedure's
  `state_from_json/1`, inside the Controller's own await for the reply.
  """
  @spec apply_state(pid(), map()) :: :ok | {:error, String.t()}
  def apply_state(pid, json),
    do: Call.entity(pid, {:apply_state, json}, timeout: Knob.get(:sdk_entity_apply_ms))

  @doc """
  Whether the Entity has been idle past `ttl_ms` holding no work at all — an
  empty queue, no dispatch in flight, no park held, no deferred push. A probe
  that times out (a dispatch in flight) is `false`, and its late reply is
  inert to the caller.
  """
  @spec idle_for?(pid(), pos_integer(), timeout()) :: boolean()
  # `log: false`: the reaper probes every Entity on every sweep, so a probe that
  # does not answer is routine.
  def idle_for?(pid, ttl_ms, timeout),
    do: Call.entity(pid, {:idle?, ttl_ms}, timeout: timeout, log: false, on_error: false)

  @doc """
  Evict an Entity the caller proved idle: it ships `delete_entity_state` when
  it persists, drops its slot so a successor is a cold start, and stops. What
  no stream would take goes to the Operative before the reply, so the caller
  knows the handoff is queued there by the time it sees `:evicted`.
  """
  @spec evict(pid()) :: :evicted | :busy | :gone
  def evict(pid), do: Call.entity(pid, :evict, on_error: %{timeout: :busy, other: :gone})

  @doc """
  An Entity's slot identity in `Flezha.SDK.Counters`. Its Operative computes the
  same value to release the slot of an Entity that died mid-dispatch — the one
  unwind an `after` inside the Entity cannot reach.
  """
  @spec identity(binary(), term(), term()) :: term()
  def identity(operative_id, node_ref, entity_key),
    do: {operative_id, node_ref, entity_key}

  @doc """
  Bind the Entity onto fresh secondaries and replay the records it held while
  there was none. Synchronous on purpose: the caller replays this Entity's
  OUTPUTS after it returns, and a dispatch's records have to reach the wire
  first. A gone Entity has nothing to rebind and answers for itself.

  Not bounded, and not by omission: giving up would let the caller replay this
  Entity's outputs ahead of its own records, and both callers discard the answer
  so there is no refusal for either to act on.
  """
  @spec rebind(pid(), [term()]) :: :ok
  def rebind(pid, secondaries),
    do: Call.entity(pid, {:rebind, secondaries}, timeout: :infinity, log: false, on_error: :ok)

  # --- server ---

  @impl true
  def init(opts) do
    module = Keyword.fetch!(opts, :module)
    entity_key = Keyword.get(opts, :entity_key)
    operative_id = Keyword.fetch!(opts, :operative_id)
    identity = Park.identity(operative_id, module.name(), entity_key || "")
    changeful? = Procedure.changeful?(module)
    tier = Map.merge(%State{}.tier, Keyword.get(opts, :tier, %{}))
    slots = Keyword.get(opts, :slots)
    slot = Records.seed(slots, identity, module, entity_key)
    operative = Keyword.fetch!(opts, :operative)
    counters = Keyword.fetch!(opts, :counters)
    credit = Keyword.get(opts, :credit)
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
      counters: counters,
      identity: identity,
      park: %{%State{}.park | enabled?: Keyword.get(opts, :park_on_exhaustion, true)},
      slots: slots,
      credit: credit,
      # The static ctx facts: the node's, from the Operative, and the timer
      # policy this procedure declares.
      ctx_opts:
        [timer_policy: Procedure.timer_policy(module)] ++ Keyword.get(opts, :ctx_opts, []),
      timers: Clock.seed(module, slot),
      changeful?: changeful?,
      tier: tier,
      persistent?: persists?(changeful?, tier, opts),
      schema_version: Procedure.schema_version(module),
      proc_state: slot.state,
      change_seq: slot.seq,
      last_input_ms: now_ms(),
      stream: Hold.new(cap: Keyword.get(opts, :hold_cap), counters: counters, credit: credit)
    }

    state = seed_tier(state, slot)
    {:ok, state |> Records.rebind(Keyword.get(opts, :secondaries, [])) |> Clock.rearm()}
  end

  # Off the tier a record stream exists only to carry changes, so a changeless
  # procedure writes none. Under the tier the record carries the OUTPUTS and
  # the consumed offset as well — and the router delivers no sink for a tier
  # class — so a changeless one writes them too. The class's own opt-out
  # silences both: the seq advances and the frames are discarded.
  defp persists?(changeful?, tier, opts),
    do: Keyword.get(opts, :persistence_enabled, true) and (changeful? or tier.on?)

  # The slot's interval and floor over the struct's defaults: a replacement
  # re-holds what its predecessor had staged but not yet made durable.
  defp seed_tier(%State{} = s, held),
    do: %{s | tier: %{s.tier | staged: held.staged, floor: held.floor}}

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
      {:ok, s, terminal, migrated} ->
        # A workflow driver holds its first procedure until every Entity the
        # bind owes has been rebuilt, so a resumed run reads persisted state
        # instead of cold-starting. A stream Operative has nothing to hold and
        # ignores this.
        send(s.operative, {:restored, self()})
        {:reply, {:ok, terminal, migrated}, Clock.rearm(Tier.cancel_deadline(s))}

      {:error, reason} ->
        {:reply, {:error, reason}, s}
    end
  end

  # The refused snapshot answers with the state too: the frame it held lives
  # there, and replying with the state from before the write would drop it.
  def handle_call(:snapshot, _from, %State{} = s) do
    case Records.snapshot(s) do
      {:ok, s, seq, size} -> {:reply, {:ok, seq, size}, s}
      {:error, s, reason} -> {:reply, {:error, reason}, s}
    end
  end

  def handle_call(:read, _from, %State{} = s),
    do: {:reply, {:ok, s.proc_state, s.change_seq}, s}

  def handle_call({:idle?, ttl_ms}, _from, %State{} = s) do
    idle? =
      s.size == 0 and not s.processing? and not s.park.held? and :queue.is_empty(s.waiters) and
        now_ms() - s.last_input_ms > ttl_ms

    {:reply, idle?, s}
  end

  def handle_call(:evict, _from, %State{} = s) do
    Records.delete(s)
    {:stop, :normal, :evicted, s}
  end

  def handle_call({:rebind, secondaries}, _from, %State{} = s),
    do: {:reply, :ok, Records.rebind(s, secondaries)}

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

  def handle_info({:park_released, park_id}, %State{park: %{id: park_id}} = s) do
    {:noreply, process_next(%{s | park: %{s.park | held?: false, id: nil}})}
  end

  def handle_info({:park_released, _stale}, s), do: {:noreply, s}

  # The BEAM's timer service is the wake list: a due message folds every
  # processing-time timer due by now onto the queue, ascending, so the order
  # never depends on the order two same-millisecond messages arrive in; one
  # that finds no room comes back later rather than dropping.
  def handle_info({:timer_due, at}, %State{size: size, capacity: capacity} = s)
      when size >= capacity do
    # A due message that finds no room comes back later, never drops.
    Process.send_after(
      self(),
      {:timer_due, at},
      Knob.arm(:entity_wake_retry, 1, s.operative_id)
    )

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

  # The interval's delay elapsed: the close goes on the queue rather than
  # running here, so it lands behind the inputs that preceded the deadline.
  def handle_info({:close_interval, opened}, %State{} = s) do
    s = %{s | tier: %{s.tier | deadline: nil}}
    {:noreply, kick(enqueue(s, %Close{opened_at: opened}))}
  end

  # The controller closed the replayed slice. The marker goes on the queue
  # rather than flushing here, so it lands behind every member that preceded it.
  def handle_info(:end_replay_slice, %State{} = s),
    do: {:noreply, kick(enqueue(s, %Tier.SliceEnd{}))}

  # Every inter-procedure push of one invocation has been admitted: its record
  # may become durable now.
  def handle_info({:edges_delivered, ref}, %State{} = s),
    do: {:noreply, Records.release(s, ref)}

  def handle_info(_msg, s), do: {:noreply, s}

  # Evicted, or its Operative gone: the armed timers go with the process.
  @impl true
  def terminate(_reason, %State{} = s), do: s |> Tier.cancel_deadline() |> Clock.disarm_all()

  # --- internals ---

  defp enqueue(%State{} = s, value),
    do: %{s | queue: :queue.in(value, s.queue), size: s.size + 1, last_input_ms: now_ms()}

  defp kick(%State{processing?: false, park: %{held?: false}, size: size} = s) when size > 0 do
    send(self(), :process)
    %{s | processing?: true}
  end

  defp kick(s), do: s

  defp process_next(%State{park: %{held?: true}} = s), do: s

  defp process_next(%State{} = s) do
    case :queue.out(s.queue) do
      {:empty, _} ->
        admit_waiter(%{s | processing?: false})

      {{:value, item}, q} ->
        s = %{s | queue: q, size: s.size - 1}
        s = admit(s, item)
        s = admit_waiter(s)

        if s.park.held? do
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
    if Timers.contains?(s.timers.pending, kind, at), do: dispatch(s, fire, nil), else: s
  end

  defp admit(%State{} = s, %Close{opened_at: at}), do: Tier.close(s, at)

  defp admit(%State{} = s, %Undo{} = undo), do: dispatch(s, undo, nil)

  defp admit(%State{} = s, %Barrier{ref: ref, reply_to: pid}) do
    send(pid, {:stage_done, ref, self()})
    s
  end

  # Defence in depth, and no test distinguishes it: the slice's order is already
  # guaranteed by `Operative.push/6` pushing a member with `enqueue_block/2`, a
  # synchronous call, so the drain has buffered every member before the close
  # can arrive. Taking the close off the queue instead of flushing on the
  # message removes that dependency on another module's choice of call.
  defp admit(%State{} = s, %Tier.SliceEnd{}), do: Tier.flush_slice(s, &requeue/2)

  # A re-forwarded input the tier already made durable is a COMPLETED
  # dispatch, never a re-run: the effect record that consumed it is on disk.
  defp admit(%State{tier: %{floor: floor}} = s, %Input{offset: offset})
       when is_integer(offset) and offset <= floor do
    Counters.inc_dispatches(s.counters)
    s
  end

  defp admit(%State{} = s, %Input{value: value, offset: offset, replay: replay}),
    do: admit_value(s, value, offset, replay)

  defp admit(%State{} = s, value), do: admit_value(s, value, nil, nil)

  defp admit_value(
         %State{timers: %{clock: {:event_time, extract}} = timers} = s,
         value,
         offset,
         replay
       ) do
    with {:ok, event_time} <- Clock.extract(extract, value),
         :advance <- Tier.admit(s, event_time, replay) do
      s = Clock.advance(s, event_time)

      case Timers.due(timers.pending, :event_time, s.timers.high_water) do
        [] ->
          dispatch(s, value, offset, replay)

        due ->
          fires = Enum.map(due, &%Fire{kind: :event_time, at: &1})
          requeue(s, fires ++ [rewrap(value, offset, replay)])
      end
    else
      # A correction was requested for it, or the beyond-window policy drops it.
      :withhold -> s
      {:buffer, at} -> Tier.buffer(s, rewrap(value, offset, :member), at)
      # A raising extractor rides the ladder and parks as any bad input; a
      # beyond-window arrival dispatches without moving the high-water.
      _other -> dispatch(s, value, offset, replay)
    end
  end

  defp admit_value(%State{} = s, value, offset, replay), do: dispatch(s, value, offset, replay)

  # A value going back on the queue keeps what it arrived with.
  defp rewrap(value, nil, nil), do: value
  defp rewrap(value, offset, replay), do: %Input{value: value, offset: offset, replay: replay}

  # Items at the head, in order; the size overshoots the capacity by internal
  # items only, bounded by the pending set.
  defp requeue(%State{} = s, items) do
    queue = items |> Enum.reverse() |> Enum.reduce(s.queue, &:queue.in_r/2)
    %{s | queue: queue, size: s.size + length(items)}
  end

  # `after` is the contract, not politeness: the in-flight report is what makes
  # ONE wedged Entity visible to the controller, so a dispatch that unwinds
  # (raise, throw, exit) must release its slot on the way out or the runner
  # would report a stall that healed long ago.
  defp dispatch(%State{} = s, item, offset, replay \\ nil) do
    identity = identity(s.operative_id, s.node_ref, s.entity_key)
    Counters.dispatch_started(s.counters, identity)

    try do
      case run_ladder(s, item, ctx_opts(s, item, replay), 1) do
        {:ok, new_proc_state, facts} ->
          complete(s, new_proc_state, facts, offset)

        {:park, error, attempts} ->
          s |> park_firing(item) |> exhausted(item, error, attempts, offset)
      end
    after
      Counters.dispatch_finished(s.counters, identity)
    end
  end

  defp complete(%State{tier: %{on?: true}} = s, new_proc_state, facts, offset),
    do: settle_tier(s, new_proc_state, facts, offset)

  # The records leave first (flush-before-emit, and the outputs are stamped
  # with the seq the dispatch started at), then the timers are armed, then the
  # outputs leave: an output on the wire means its dispatch's timers are armed
  # and counted, for every reader.
  defp complete(%State{} = s, new_proc_state, facts, _offset) do
    {pending, ops, tally} = facts.timers
    Counters.inc_dispatches(s.counters)
    handout_seq = s.change_seq
    s = %{s | proc_state: new_proc_state, timers: %{s.timers | pending: pending}}
    s = s |> Records.ship(facts.changes, ops) |> Clock.reflect(ops, tally)
    flush_emits(s, facts.emits, handout_seq, nil)
    s
  end

  # Under the tier the invocation's whole effect is ONE record: its changes,
  # the outputs it captured itself, the offset it consumed and its timer ops,
  # so state, outputs and the offset can never tear. Only an inter-procedure
  # emit still leaves through the router.
  defp settle_tier(%State{} = s, new_proc_state, facts, offset) do
    {pending, ops, tally} = facts.timers
    Counters.inc_dispatches(s.counters)
    handout_seq = s.change_seq
    s = %{s | proc_state: new_proc_state, timers: %{s.timers | pending: pending}}

    {s, report} = Tier.settle(s, facts, ops, offset)
    s = Clock.reflect(s, ops, tally)
    if s.tier.edges?, do: flush_emits(s, facts.emits, handout_seq, offset, report)
    s
  end

  # A firing that exhausted its ladder still commits its removal, so a poison
  # timer parks once instead of firing for ever. Under the tier the removal
  # rides the park's own record instead, which is what makes it atomic.
  defp park_firing(%State{tier: %{on?: true}} = s, _item), do: s

  defp park_firing(%State{} = s, item) do
    {s, ops, tally} = firing_removal(s, item)
    if ops == [], do: s, else: s |> Records.ship([], ops) |> Clock.reflect(ops, tally)
  end

  # The op a parked firing commits: its own removal, op 0 of the session.
  defp firing_removal(%State{} = s, %Fire{kind: kind, at: at}) do
    policy = Keyword.fetch!(s.ctx_opts, :timer_policy)

    {pending, ops, tally} =
      s.timers.pending |> Timers.session(policy, at, {kind, at}) |> Timers.commit()

    {%{s | timers: %{s.timers | pending: pending}}, ops, tally}
  end

  defp firing_removal(%State{} = s, _value), do: {s, [], nil}

  # The clock is read inside the attempt: an extractor that raises on an input
  # is that input failing, and it rides the ladder like any other failure. The
  # ctx is bound before the call, so a raise inside `run/3` still reaches the
  # arm that discards it — the ref is the only handle its dictionary slots
  # have.
  defp run_ladder(%State{} = s, item, opts, attempt) do
    outcome =
      with {:ok, ctx} <-
             guarded(fn -> Ctx.new(s.module, s.entity_key, clock_now(s, item), opts) end),
           {:ok, result} <- guarded(fn -> call(s, item, ctx) end, ctx) do
        {:result, ctx, result}
      end

    case outcome do
      {:result, ctx, {:ok, new_state}} ->
        facts = %{
          emits: Ctx.take_emits(ctx),
          changes: Ctx.take_changes(ctx),
          timers: Ctx.take_timers(ctx),
          event_time: ctx.now_ms,
          # A class with no checkpoint granularity never reads the flag, so
          # `Ctx.checkpoint/1` on one is a no-op by construction.
          checkpointed?: s.tier.checkpoint != nil and Ctx.checkpointed?(ctx)
        }

        # The takes clear what they take; the slots an invocation writes only
        # when it uses them — the checkpoint flag, the stable-id ordinal — are
        # cleared here, so nothing of this ref outlives the invocation.
        Ctx.discard(ctx)
        {:ok, new_state, facts}

      {:result, ctx, {:error, {:non_retryable, reason}}} ->
        Ctx.discard(ctx)
        {:park, format_error(reason), attempt}

      {:result, ctx, {:error, reason}} ->
        Ctx.discard(ctx)
        retry_or_park(s, item, opts, attempt, format_error(reason))

      {:raised, ctx, message} ->
        if ctx, do: Ctx.discard(ctx)
        retry_or_park(s, item, opts, attempt, message)
    end
  end

  # Anything author code raises becomes `{:raised, ctx, message}` so the ladder
  # can climb it and the ctx's slots still go. A ctx that never came back is
  # `nil` and has written nothing.
  defp guarded(fun, ctx \\ nil) do
    {:ok, fun.()}
  rescue
    e -> {:raised, ctx, Exception.message(e)}
  catch
    kind, reason -> {:raised, ctx, "#{kind}: #{inspect(reason)}"}
  end

  # `on_timer/4` on a firing, `run/3` on an input; a firing on a procedure that
  # exports no handler parks by name, without the retries a raise would climb.
  defp call(%State{} = s, %Undo{outputs: outputs}, ctx),
    do: s.module.compensate(outputs, s.proc_state, ctx)

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
  defp clock_now(%State{} = s, value), do: Clock.now(s, value)
  # The invocation's timer session opens over the live pending set; a firing's
  # opens on its own removal. A replayed input's spawn requests issue nothing.
  defp ctx_opts(%State{} = s, item, replay),
    do: [timers: s.timers.pending, firing: firing(item), replay: replay != nil] ++ s.ctx_opts

  defp firing(%Fire{kind: kind, at: at}), do: {kind, at}
  defp firing(_value), do: nil

  defp retry_or_park(%State{retry_max: max}, _value, _opts, attempt, error) when attempt >= max,
    do: {:park, error, attempt}

  defp retry_or_park(%State{} = s, value, opts, attempt, error) do
    Logger.debug(
      "[Flezha.SDK] #{State.label(s)} attempt #{attempt}/#{s.retry_max} failed: #{error}; retrying"
    )

    :telemetry.execute([:flezha, :sdk, :retry], %{count: 1}, %{
      operative_id: s.operative_id,
      attempt: attempt
    })

    Knob.wait(:sdk_retry, attempt, s.operative_id, base: s.retry_backoff_ms)
    run_ladder(s, value, opts, attempt + 1)
  end

  # Under the tier a park IS a record: the entry, the offset it consumed and a
  # parked firing's removal ride one frame, and nothing is held — the record is
  # the durability, so there is no ack to wait for.
  # A workflow stage's Entity does not park: its exhausted ladder fails the
  # procedure, and the driver unwinds the run. A stream's Entity parks to the
  # DLQ, which is the only behaviour that existed before workflows.
  defp exhausted(%State{park: %{enabled?: false}} = s, _value, error, _attempts, _offset) do
    GenServer.cast(s.operative, {:stage_failed, s.node_ref, format_error(error)})
    s
  end

  defp exhausted(%State{} = s, value, error, attempts, offset),
    do: park(s, value, error, attempts, offset)

  defp park(%State{tier: %{on?: true}} = s, value, error, attempts, offset) do
    {s, ops, tally} = firing_removal(s, value)
    dlq = Park.dlq_entry(park_channel(s), error, attempts, value)
    s = Tier.park(s, dlq, ops, offset, clock_now(s, value))
    s = if ops == [], do: s, else: Clock.reflect(s, ops, tally)
    parked_event(s)
  end

  defp park(%State{} = s, value, error, attempts, _offset) do
    seq = s.park.seq
    park_id = "#{State.label(s)}##{seq}"
    body = Park.body(park_id, park_channel(s), error, attempts, value)

    Park.ship(
      s.operative,
      self(),
      Park.identity(s.operative_id, s.module.name(), s.entity_key),
      body
    )

    parked_event(%{s | park: %{held?: true, id: park_id, seq: seq + 1}})
  end

  defp park_channel(%State{} = s), do: s.input_channel || s.node_name || s.operative_id

  defp parked_event(%State{} = s) do
    :telemetry.execute([:flezha, :sdk, :parked], %{count: 1}, %{operative_id: s.operative_id})
    s
  end

  defp flush_emits(s, emits, handout_seq, offset, report \\ nil)
  defp flush_emits(_s, [], _handout_seq, _offset, _report), do: :ok

  defp flush_emits(%State{} = s, emits, handout_seq, offset, report),
    do: Inbox.emitted(s.operative, s.node_ref, s.entity_key, emits, handout_seq, offset, report)

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
