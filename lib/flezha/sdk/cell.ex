defmodule Flezha.SDK.Cell do
  @moduledoc """
  One process per Cell: a bounded input queue, the retry-then-park dispatch
  ladder, and per-Cell ordering (arrival order out of the queue). Inputs under
  `:drop_nothing` arrive as a `GenServer.call` the Cell answers only when
  there is room — never dropped, never refused (the caller, the Operator
  router, waits, which is the documented co-tenancy cost);
  `:drop_newest`/`:drop_oldest` arrive as casts. A completed dispatch bumps `dispatches`; an exhausted ladder parks
  the input and HOLDS the Cell until the controller's `ParkPayloadAck`.

  A Cell that writes a record stream — a changeful one, whose tasklet
  exports `apply_change/2`, and under the effectively-once tier a changeless
  one too, because there the record carries the outputs and the consumed
  offset rather than only changes — is the writer of it: after a successful dispatch it ships one
  `cell_state_change` frame per recorded change at the next seq on one
  secondary stream, before the dispatch's outputs leave, then writes its
  `{state, seq}` into the Operator's slot table so a replacement started
  after an untrappable kill continues on the same seq line. A frame no stream
  will take is held and replayed by `rebind/2`, so "before the outputs leave"
  survives a reconnect as well; an eviction hands what is still held, and the
  delete it owes, to the Operator, because a hold that dies with the process
  that owned it is not a hold. It answers a restore (fold, re-arm), a
  compaction (the snapshot after its records), a live read and a state apply,
  each in mailbox order so no dispatch interleaves. With the class's persistence off the seq advances and nothing
  ships. Its outputs reach the Operator with their metadata, their keys and
  the seq the dispatch started at — what a keyed sink's stamps read.

  Its timers fire through the same queue and ladder as its inputs, with
  `on_timer/4` in place of `run/3`: a processing-time timer is one
  `Process.send_after/3` on the BEAM's timer service — the wake list no
  thread of this SDK owns — whose due message folds every timer due by then
  onto the queue, ascending; an event-time timer fires at admission, when an
  input's event time advances the high-water past it, ahead of that input. A
  firing's ops and outputs ship as a dispatch's do, its removal committed
  even when it parks; a restore, a slot and an eviction re-arm, re-arm and
  discard the armed set with the pending one. A bounded stage's Cell hands a
  due processing-time firing to its driver, which decides when it runs, and
  when its class journals it holds a served firing's removal until the
  driver releases it on the stage's commit.
  """

  use GenServer
  require Logger

  alias Flezha.SDK.{Call, Counters, Ctx, Hold, Knob, Park, Prefix, Tasklet, Timers}
  alias Flezha.SDK.Cell.{Clock, Records, State, Tier}
  alias Flezha.SDK.Operator.Inbox

  # The live read's two verdicts. Every other refusal is `Flezha.SDK.Call`'s.
  @read_verdict %{timeout: {:error, :busy}, other: {:error, :gone}}

  # What a removal written outside a dispatch adds to the timer counters: the
  # dispatch that served the firing already counted it.
  @no_tally %{registered: 0, fired: 0, cancelled: 0, refused: 0}

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
  # `run/3`, over the outputs that Cell released. Best-effort — a hook that
  # fails is logged and the plan moves on, so it never parks.
  defmodule Undo do
    @moduledoc false
    defstruct [:outputs]
  end

  # A bounded stage's barrier, on the queue behind that stage's batch. It
  # runs no author code: reaching it is the answer, because the queue is FIFO
  # and every item before it has been dispatched. An invocation that emits
  # nothing reports nothing, so a driver cannot count outputs to know its
  # stage finished — it enqueues one of these and waits for the reply.
  defmodule Barrier do
    @moduledoc false
    defstruct [:ref, :reply_to]
  end

  # An input with the durable offset the controller stamped on it, or its
  # producer's input took: `replay` is `:member` while it waits for its slice
  # to close, `:ordered` once the flush re-queued it in event-time order, and
  # `forwarded?` says the controller forwarded it rather than a tasklet of the
  # instance emitting it. An input with no offset off the tier is the raw
  # value, so nothing there pays for this.
  defmodule Input do
    @moduledoc false
    defstruct [:value, :offset, :replay, forwarded?: false]
  end

  # --- API (used by the Operator router) ---

  @doc "Start a Cell, unlinked: its Operator monitors it, and it monitors its Operator."
  def start(opts), do: GenServer.start(__MODULE__, opts)

  @doc """
  Enqueue under `:drop_nothing` — the caller waits until there is room, with no
  bound: the policy promises an input is never dropped and never refused.
  """
  def enqueue_block(pid, value), do: GenServer.call(pid, {:enqueue, value}, :infinity)

  @doc "Run this Cell's `compensate/3` over the outputs it released."
  def compensate(pid, outputs), do: enqueue_block(pid, %Undo{outputs: outputs})

  @doc """
  Enqueue a barrier behind whatever this Cell already holds: `{:stage_done,
  ref, cell_pid, written}` reaches `reply_to` once the queue has drained past
  it. `written` names the effect records a bounded stage's Cell wrote since
  its last barrier (`%{cell_id, first_seq, seq, channels}`), `nil` when none.
  """
  def barrier(pid, ref, reply_to),
    do: enqueue_block(pid, %Barrier{ref: ref, reply_to: reply_to})

  @doc "The earliest processing-time timer pending on this Cell, or `nil`."
  @spec earliest_timer(pid()) :: non_neg_integer() | nil
  def earliest_timer(pid), do: Call.cell(pid, :earliest_timer, log: false, on_error: nil)

  @doc "Drop restored firings whose stage the journal records committed, writing their removal."
  @spec drop_firings(pid(), [{Timers.kind(), non_neg_integer()}]) :: :ok
  def drop_firings(pid, fired), do: Call.cell(pid, {:drop_firings, fired}, on_error: :ok)

  @doc "The stage that served this Cell's firings committed: write their held removals."
  def release_firings(pid), do: send(pid, :release_firings)

  @doc "Enqueue under a drop policy — non-blocking."
  def enqueue_drop(pid, value, policy), do: GenServer.cast(pid, {:enqueue_drop, value, policy})

  @doc "Release a held (parked) Cell after its `ParkPayloadAck`."
  def release_park(pid, park_id), do: send(pid, {:park_released, park_id})

  @doc """
  Rebuild from a `RestoreInitState` payload; `{:ok, terminal_seq, migrated?}`.
  The fold grows with the change log, so it waits its own row.
  """
  @spec restore(pid(), binary(), binary(), binary(), non_neg_integer()) ::
          {:ok, non_neg_integer(), boolean()} | {:error, String.t()}
  def restore(pid, snapshot_header, snapshot_bytes, change_log_bytes, terminal_hint) do
    request = {:restore, snapshot_header, snapshot_bytes, change_log_bytes, terminal_hint}
    Call.cell(pid, request, timeout: Knob.get(:sdk_cell_restore_ms))
  end

  @doc """
  Ship the state as a `cell_state_snapshot`; `{:ok, snapshot_seq, body_size}`.
  Serialising grows with the state, so it waits inside the Controller's own
  compaction bound.
  """
  @spec snapshot(pid()) :: {:ok, non_neg_integer(), non_neg_integer()} | {:error, String.t()}
  def snapshot(pid), do: Call.cell(pid, :snapshot, timeout: Knob.get(:sdk_cell_snapshot_ms))

  @doc """
  The state and the seq it was read at; `{:error, :busy}` when a dispatch holds
  the Cell past `timeout` — the reference's serialize gate.
  """
  @spec read(pid(), timeout()) :: {:ok, term(), non_neg_integer()} | {:error, :busy | :gone}
  # `log: false`: the walk names the Tasklet and the key in a line of its own.
  def read(pid, timeout),
    do: Call.cell(pid, :read, timeout: timeout, log: false, on_error: @read_verdict)

  @doc """
  Replace the state with a JSON object through the tasklet's
  `state_from_json/1`, inside the Controller's own await for the reply.
  """
  @spec apply_state(pid(), map()) :: :ok | {:error, String.t()}
  def apply_state(pid, json),
    do: Call.cell(pid, {:apply_state, json}, timeout: Knob.get(:sdk_cell_apply_ms))

  @doc """
  Whether the Cell has been idle past `ttl_ms` holding no work at all — an
  empty queue, no dispatch in flight, no park held, no deferred push. A probe
  that times out (a dispatch in flight) is `false`, and its late reply is
  inert to the caller.
  """
  @spec idle_for?(pid(), pos_integer(), timeout()) :: boolean()
  # `log: false`: the reaper probes every Cell on every sweep, so a probe that
  # does not answer is routine.
  def idle_for?(pid, ttl_ms, timeout),
    do: Call.cell(pid, {:idle?, ttl_ms}, timeout: timeout, log: false, on_error: false)

  @doc """
  `{:quiet, last_input_ms}` when the Cell holds no work, no record it could
  not ship and no pending timer; `:busy` otherwise, and for a probe that times
  out. An instance that sleeps asks it of every Cell.
  """
  @spec quiet_since(pid(), timeout()) :: {:quiet, integer()} | :busy
  def quiet_since(pid, timeout),
    do: Call.cell(pid, :quiet_since, timeout: timeout, log: false, on_error: :busy)

  @doc """
  Evict a Cell the caller proved idle: it ships `delete_cell_state` when
  it persists, drops its slot so a successor is a cold start, and stops. What
  no stream would take goes to the Operator before the reply, so the caller
  knows the handoff is queued there by the time it sees `:evicted`.
  """
  @spec evict(pid()) :: :evicted | :busy | :gone
  def evict(pid), do: Call.cell(pid, :evict, on_error: %{timeout: :busy, other: :gone})

  @doc """
  A Cell's slot identity in `Flezha.SDK.Counters`. Its Operator computes the
  same value to release the slot of a Cell that died mid-dispatch — the one
  unwind an `after` inside the Cell cannot reach.
  """
  @spec identity(binary(), term(), term()) :: term()
  def identity(operator_id, node_ref, cell_key),
    do: {operator_id, node_ref, cell_key}

  @doc """
  Bind the Cell onto fresh secondaries and replay the records it held while
  there was none. Synchronous on purpose: the caller replays this Cell's
  OUTPUTS after it returns, and a dispatch's records have to reach the wire
  first. A gone Cell has nothing to rebind and answers for itself.

  Not bounded, and not by omission: giving up would let the caller replay this
  Cell's outputs ahead of its own records, and both callers discard the answer
  so there is no refusal for either to act on.
  """
  @spec rebind(pid(), [term()]) :: :ok
  def rebind(pid, secondaries),
    do: Call.cell(pid, {:rebind, secondaries}, timeout: :infinity, log: false, on_error: :ok)

  # --- server ---

  @impl true
  def init(opts) do
    module = Keyword.fetch!(opts, :module)
    cell_key = Keyword.get(opts, :cell_key)
    operator_id = Keyword.fetch!(opts, :operator_id)
    identity = Park.identity(operator_id, module.name(), cell_key || "")
    changeful? = Tasklet.changeful?(module)
    tier = Map.merge(%State{}.tier, Keyword.get(opts, :tier, %{}))
    slots = Keyword.get(opts, :slots)
    slot = Records.seed(slots, identity, module, cell_key)
    operator = Keyword.fetch!(opts, :operator)
    counters = Keyword.fetch!(opts, :counters)
    credit = Keyword.get(opts, :credit)
    Process.monitor(operator)

    state = %State{
      operator: operator,
      operator_id: operator_id,
      node_ref: Keyword.fetch!(opts, :node_ref),
      node_name: Keyword.get(opts, :node_name),
      input_channel: Keyword.get(opts, :input_channel),
      module: module,
      cell_key: cell_key,
      capacity: Keyword.fetch!(opts, :capacity),
      policy: Keyword.fetch!(opts, :policy),
      retry_max: Keyword.fetch!(opts, :retry_max),
      retry_backoff_ms: Keyword.fetch!(opts, :retry_backoff_ms),
      counters: counters,
      identity: identity,
      park: %{%State{}.park | enabled?: Keyword.get(opts, :park_on_exhaustion, true)},
      slots: slots,
      credit: credit,
      # The static ctx facts: the node's, from the Operator, and the timer
      # policy this tasklet declares.
      ctx_opts: [timer_policy: Tasklet.timer_policy(module)] ++ Keyword.get(opts, :ctx_opts, []),
      timers: Clock.seed(module, slot, firing_gate(opts)),
      changeful?: changeful?,
      tier: tier,
      persistent?: persists?(changeful?, tier, opts),
      schema_version: Tasklet.schema_version(module),
      task_state: slot.state,
      change_seq: slot.seq,
      last_input_ms: now_ms(),
      stream: Hold.new(cap: Keyword.get(opts, :hold_cap), counters: counters, credit: credit)
    }

    state = seed_tier(state, slot)
    state = %{state | stream: %{state.stream | prefix: tier.prefix}}
    {:ok, state |> Records.rebind(Keyword.get(opts, :secondaries, [])) |> Clock.rearm()}
  end

  # A bounded stage's Cell hands its due firings to the driver, which decides
  # when each runs; one whose class journals holds a served firing's removal
  # until the stage's commit.
  defp firing_gate(opts) do
    cond do
      not Keyword.get(opts, :gate_firings, false) -> nil
      Keyword.get(opts, :persistence_enabled, true) -> :held
      true -> :settled
    end
  end

  # Off the tier a record stream exists only to carry changes, so a changeless
  # tasklet writes none. Under the tier the record carries the OUTPUTS and
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
      Counters.deferred_started(s.counters, identity(s.operator_id, s.node_ref, s.cell_key))
      {:noreply, %{s | waiters: :queue.in({from, value}, s.waiters)}}
    end
  end

  def handle_call({:restore, header, snapshot, log, hint}, _from, %State{} = s) do
    case Records.restore(s, header, snapshot, log, hint) do
      {:ok, s, terminal, migrated} ->
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
    do: {:reply, {:ok, s.task_state, s.change_seq}, s}

  # What a sleeping bounded operator wakes at: the earliest processing-time timer.
  def handle_call(:earliest_timer, _from, %State{} = s),
    do: {:reply, List.first(Timers.all(s.timers.pending, :processing_time)), s}

  # Firings whose stage the journal records committed, restored before their
  # removal was written: they must not fire again.
  def handle_call({:drop_firings, fired}, _from, %State{} = s) do
    ops =
      for {kind, at} <- fired,
          Timers.contains?(s.timers.pending, kind, at),
          do: Timers.cancel_op(kind, at)

    pending = Timers.apply_all(s.timers.pending, ops)
    s = %{s | timers: %{s.timers | pending: pending}}
    {:reply, :ok, if(ops == [], do: s, else: ship_removals(s, ops))}
  end

  def handle_call({:idle?, ttl_ms}, _from, %State{} = s),
    do: {:reply, holds_nothing?(s) and now_ms() - s.last_input_ms > ttl_ms, s}

  def handle_call(:quiet_since, _from, %State{} = s) do
    quiet? = holds_nothing?(s) and s.stream.n == 0 and Timers.empty?(s.timers.pending)
    {:reply, if(quiet?, do: {:quiet, s.last_input_ms}, else: :busy), s}
  end

  def handle_call(:evict, _from, %State{} = s) do
    Records.delete(s)
    {:stop, :normal, :evicted, s}
  end

  def handle_call({:rebind, secondaries}, _from, %State{} = s),
    do: {:reply, :ok, s |> Records.rebind(secondaries) |> Records.release_settled()}

  def handle_call({:apply_state, json}, _from, %State{} = s) do
    {:reply, :ok, Records.write_slot(%{s | task_state: Tasklet.state_from_json(s.module, json)})}
  rescue
    e -> {:reply, {:error, "apply_state: #{Exception.message(e)}"}, s}
  end

  @impl true
  def handle_cast({:enqueue_drop, value, policy}, %State{} = s) do
    cond do
      s.size < s.capacity ->
        {:noreply, kick(enqueue(s, value))}

      policy == :drop_oldest ->
        {{:value, dropped}, q} = :queue.out(s.queue)
        record_drop(s, :drop_oldest, dropped)
        {:noreply, kick(enqueue(%{s | queue: q, size: s.size - 1}, value))}

      true ->
        record_drop(s, :drop_newest, value)
        {:noreply, s}
    end
  end

  @impl true
  def handle_info(:process, %State{} = s), do: {:noreply, process_next(s)}

  def handle_info({:park_released, park_id}, %State{park: %{id: park_id}} = s) do
    s = Records.release_settled(%{s | park: %{s.park | held?: false, id: nil}})
    {:noreply, process_next(s)}
  end

  def handle_info({:park_released, _stale}, s), do: {:noreply, s}

  # The BEAM's timer service is the wake list: a due message folds every
  # processing-time timer due by now onto the queue, ascending, so the order
  # never depends on the order two same-millisecond messages arrive in; one
  # that finds no room comes back later rather than dropping.
  # A bounded stage's Cell reports its firing to the driver instead: the
  # driver decides whether it opens a pass, joins the running stage or waits,
  # and hands it back through the queue.
  def handle_info({:timer_due, at}, %State{timers: %{gate: gate}} = s) when gate != nil do
    if Timers.contains?(s.timers.pending, :processing_time, at),
      do:
        Inbox.firing_due(s.operator, s.node_ref, s.cell_key, %Fire{
          kind: :processing_time,
          at: at
        })

    {:noreply, s}
  end

  def handle_info({:timer_due, at}, %State{size: size, capacity: capacity} = s)
      when size >= capacity do
    # A due message that finds no room comes back later, never drops.
    Process.send_after(
      self(),
      {:timer_due, at},
      Knob.arm(:cell_wake_retry, 1, s.operator_id)
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

  # The Operator is gone — stopped or crashed — so this Cell ends with it,
  # quietly, through `terminate/2`. Not a link: a link discards a `:normal`
  # exit signal, which is exactly how a removed operator's Cells outlived it.
  def handle_info({:DOWN, _ref, :process, operator, _reason}, %State{operator: operator} = s),
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

  # The stage that served this Cell's firings committed: their removals are
  # durable now.
  def handle_info(:release_firings, %State{timers: %{held: held}} = s) when held != [],
    do: {:noreply, ship_removals(%{s | timers: %{s.timers | held: []}}, held)}

  # Every inter-tasklet push of one invocation has been admitted: its record
  # may become durable now.
  def handle_info({:edges_delivered, ref}, %State{} = s),
    do: {:noreply, Records.release(s, ref)}

  def handle_info(_msg, s), do: {:noreply, s}

  # Evicted, or its Operator gone: the armed timers go with the process.
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
        s = s |> admit(item) |> Records.release_settled()
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

  # Release this Cell's deferred-push slot: nothing is held any more.
  # Idempotent, so the drain need not know whether one was ever taken.
  defp note_no_deferral(%State{} = s) do
    Counters.deferred_finished(s.counters, identity(s.operator_id, s.node_ref, s.cell_key))
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
    send(pid, {:stage_done, ref, self(), s.tier.written})
    %{s | tier: %{s.tier | written: nil}}
  end

  # Defence in depth, and no test distinguishes it: the slice's order is already
  # guaranteed by `Operator.push/6` pushing a member with `enqueue_block/2`, a
  # synchronous call, so the drain has buffered every member before the close
  # can arrive. Taking the close off the queue instead of flushing on the
  # message removes that dependency on another module's choice of call.
  defp admit(%State{} = s, %Tier.SliceEnd{}), do: Tier.flush_slice(s, &requeue/2)

  # An input at or below the floor this Cell already took is a COMPLETED
  # dispatch, never a re-run: a re-forward delivers it again.
  defp admit(%State{tier: %{floor: floor}} = s, %Input{offset: offset})
       when is_integer(offset) and offset <= floor do
    Counters.inc_dispatches(s.counters)
    Prefix.release(s.tier.prefix, offset)
    s
  end

  defp admit(%State{} = s, %Input{value: value} = input), do: admit_value(s, value, input)

  defp admit(%State{} = s, value), do: admit_value(s, value, nil)

  defp admit_value(%State{timers: %{clock: {:event_time, extract}} = timers} = s, value, input) do
    with {:ok, event_time} <- Clock.extract(extract, value),
         :advance <- Tier.admit(s, event_time, input && input.replay) do
      s = Clock.advance(s, event_time)

      case Timers.due(timers.pending, :event_time, s.timers.high_water) do
        [] ->
          dispatch(s, value, input)

        due ->
          fires = Enum.map(due, &%Fire{kind: :event_time, at: &1})
          requeue(s, fires ++ [input || value])
      end
    else
      # A correction was requested for it, or the beyond-window policy drops
      # it; a replayed slice brings a corrected one back under a hold of its own.
      :withhold ->
        Prefix.release(s.tier.prefix, input && input.offset)
        s

      {:buffer, at} ->
        Tier.buffer(s, %{(input || %Input{value: value}) | replay: :member}, at)

      # A raising extractor rides the ladder and parks as any bad input; a
      # beyond-window arrival dispatches without moving the high-water.
      _other ->
        dispatch(s, value, input)
    end
  end

  defp admit_value(%State{} = s, value, input), do: dispatch(s, value, input)

  # Taking an input: a forwarded one raises the floor a re-forward is dropped
  # at, and its hold waits for what it settles into.
  defp take(%State{tier: tier} = s, %Input{offset: offset} = input) when is_integer(offset) do
    floor = if input.forwarded?, do: max(tier.floor, offset), else: tier.floor
    unshipped = if tier.prefix, do: [offset | tier.unshipped], else: tier.unshipped
    %{s | tier: %{tier | floor: floor, unshipped: unshipped}}
  end

  defp take(%State{} = s, _input), do: s

  # Items at the head, in order; the size overshoots the capacity by internal
  # items only, bounded by the pending set.
  defp requeue(%State{} = s, items) do
    queue = items |> Enum.reverse() |> Enum.reduce(s.queue, &:queue.in_r/2)
    %{s | queue: queue, size: s.size + length(items)}
  end

  # `after` is the contract, not politeness: the in-flight report is what makes
  # ONE wedged Cell visible to the controller, so a dispatch that unwinds
  # (raise, throw, exit) must release its slot on the way out or the runner
  # would report a stall that healed long ago.
  defp dispatch(%State{} = s, item, input) do
    identity = identity(s.operator_id, s.node_ref, s.cell_key)
    Counters.dispatch_started(s.counters, identity)
    s = take(s, input)
    offset = input && input.offset

    try do
      case run_ladder(s, item, ctx_opts(s, item, input), 1) do
        {:ok, new_task_state, facts} ->
          {s, facts} = hold_removal(s, item, facts)
          s |> complete(new_task_state, facts, offset) |> request_completion(facts.completion)

        {:park, error, attempts} ->
          s |> park_firing(item) |> exhausted(item, error, attempts, offset)
      end
    after
      Counters.dispatch_finished(s.counters, identity)
    end
  end

  # Under the tier, and off it for a Cell that records and took an input-log
  # offset, a dispatch settles into ONE record, which keeps the offset beside
  # the state it produced.
  defp one_record?(%State{tier: %{on?: true}}, _offset), do: true

  defp one_record?(%State{persistent?: persistent?}, offset),
    do: persistent? and is_integer(offset)

  defp complete(%State{} = s, new_task_state, facts, offset) do
    if one_record?(s, offset),
      do: settle_tier(s, new_task_state, facts, offset),
      else: ship_changes(s, new_task_state, facts, offset)
  end

  # The records leave first (flush-before-emit, and the outputs are stamped
  # with the seq the dispatch started at), then the timers are armed, then the
  # outputs leave: an output on the wire means its dispatch's timers are armed
  # and counted, for every reader.
  defp ship_changes(%State{} = s, new_task_state, facts, offset) do
    {pending, ops, tally} = facts.timers
    Counters.inc_dispatches(s.counters)
    handout_seq = s.change_seq
    s = %{s | task_state: new_task_state, timers: %{s.timers | pending: pending}}
    s = s |> Records.ship(facts.changes, ops) |> Clock.reflect(ops, tally)
    flush_emits(s, facts.emits, handout_seq, offset)
  end

  # After the outputs, so the driver has them all when this stage settles. A
  # stream's Cell never gets here with one: `succeeded/4` refused it.
  defp request_completion(%State{} = s, nil), do: s

  defp request_completion(%State{} = s, result) do
    Inbox.completed(s.operator, s.node_ref, s.cell_key, result)
    s
  end

  # Under the tier the invocation's whole effect is ONE record: its changes,
  # the outputs it captured itself, the offset it consumed and its timer ops,
  # so state, outputs and the offset can never tear. Only an inter-tasklet
  # emit still leaves through the router; a bounded stage's Cell hands every
  # emit to its driver, which holds the release for the stage's commit.
  defp settle_tier(%State{} = s, new_task_state, facts, offset) do
    {pending, ops, tally} = facts.timers
    Counters.inc_dispatches(s.counters)
    handout_seq = s.change_seq
    s = %{s | task_state: new_task_state, timers: %{s.timers | pending: pending}}

    {s, report} = Tier.settle(s, facts, ops, offset)
    s = Clock.reflect(s, ops, tally)

    if s.tier.on? and not (s.tier.edges? or s.tier.stage?),
      do: s,
      else: flush_emits(s, facts.emits, handout_seq, offset, report)
  end

  # A bounded operator firing's removal is durable when the stage that served it
  # commits, as a message leaves the input log at the commit that consumed it:
  # it stays out of this dispatch's record until the driver's release.
  defp hold_removal(%State{timers: %{gate: :held}} = s, %Fire{kind: kind, at: at}, facts) do
    {pending, ops, tally} = facts.timers
    {held, ops} = Enum.split_with(ops, &(&1 == Timers.cancel_op(kind, at)))
    s = %{s | timers: %{s.timers | held: s.timers.held ++ held}}
    {s, %{facts | timers: {pending, ops, tally}}}
  end

  defp hold_removal(%State{} = s, _item, facts), do: {s, facts}

  # Removals written outside a dispatch: shipped as one record and disarmed.
  defp ship_removals(%State{} = s, ops, tally \\ @no_tally),
    do: s |> Records.ship([], ops) |> Clock.reflect(ops, tally)

  # A firing that exhausted its ladder still commits its removal, so a poison
  # timer parks once instead of firing for ever. Under the tier the removal
  # rides the park's own record instead, which is what makes it atomic.
  defp park_firing(%State{tier: %{on?: true}} = s, _item), do: s

  defp park_firing(%State{} = s, item) do
    {s, ops, tally} = firing_removal(s, item)
    if ops == [], do: s, else: ship_removals(s, ops, tally)
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
             guarded(fn -> Ctx.new(s.module, s.cell_key, clock_now(s, item), opts) end),
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
          checkpointed?: s.tier.checkpoint != nil and Ctx.checkpointed?(ctx),
          completion: Ctx.completion(ctx)
        }

        # The takes clear what they take; the slots an invocation writes only
        # when it uses them (the checkpoint flag, the stable-id ordinal, the
        # completion) are cleared here, so nothing of this ref outlives the
        # invocation.
        Ctx.discard(ctx)
        succeeded(s, new_state, facts, attempt)

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

  # An unbounded operator never completes, so an unbounded tasklet asking to fails its input,
  # and a retry would only ask again. A bounded stage's Cell is the one that
  # does not park, and its request goes on to the driver.
  defp succeeded(%State{park: %{enabled?: true}} = s, _new_state, %{completion: result}, attempt)
       when is_binary(result) do
    error =
      "CompleteOutsideBounded: tasklet `#{s.module.name()}` called complete/2, " <>
        "but an unbounded operator never completes"

    {:park, error, attempt}
  end

  defp succeeded(_s, new_state, facts, _attempt), do: {:ok, new_state, facts}

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

  # `on_timer/4` on a firing, `run/3` on an input; a firing on a tasklet that
  # exports no handler parks by name, without the retries a raise would climb.
  defp call(%State{} = s, %Undo{outputs: outputs}, ctx),
    do: s.module.compensate(outputs, s.task_state, ctx)

  defp call(%State{timers: %{on_timer?: true}} = s, %Fire{kind: kind, at: at}, ctx),
    do: s.module.on_timer(at, kind, s.task_state, ctx)

  defp call(%State{module: module}, %Fire{}, _ctx) do
    {:error,
     {:non_retryable,
      "tasklet `#{module.name()}` registered a timer but does not export on_timer/4"}}
  end

  defp call(%State{} = s, value, ctx), do: s.module.run(value, s.task_state, ctx)

  # The declared clock, evaluated once per attempt: the epoch millisecond, the
  # durable seq this dispatch starts at, or the input's own event time — and,
  # inside a firing, the scheduled timestamp under every clock.
  defp clock_now(_s, %Fire{at: at}), do: at
  defp clock_now(%State{} = s, value), do: Clock.now(s, value)
  # The invocation's timer session opens over the live pending set; a firing's
  # opens on its own removal. A replayed input's spawn requests issue nothing.
  # A `compensate` hook's context carries no timers: a rollback is no place to
  # arm a deadline.
  defp ctx_opts(%State{} = s, %Undo{}, _input),
    do: [timers: :unavailable] ++ s.ctx_opts ++ [replay: false]

  defp ctx_opts(%State{} = s, item, input) do
    s.ctx_opts ++
      [
        timers: s.timers.pending,
        firing: firing(item),
        replay: input != nil and input.replay != nil,
        origin: origin(s, input)
      ]
  end

  # What names a spawn this invocation asks for across a re-run of its input.
  defp origin(%State{} = s, %Input{offset: offset}) when is_integer(offset),
    do: {s.identity, offset}

  defp origin(%State{}, _input), do: nil

  defp firing(%Fire{kind: kind, at: at}), do: {kind, at}
  defp firing(_value), do: nil

  defp retry_or_park(%State{retry_max: max}, _value, _opts, attempt, error) when attempt >= max,
    do: {:park, error, attempt}

  defp retry_or_park(%State{} = s, value, opts, attempt, error) do
    Logger.debug(
      "[Flezha.SDK] #{State.label(s)} attempt #{attempt}/#{s.retry_max} failed: #{error}; retrying"
    )

    :telemetry.execute([:flezha, :sdk, :retry], %{count: 1}, %{
      operator_id: s.operator_id,
      attempt: attempt
    })

    Knob.wait(:sdk_retry, attempt, s.operator_id, base: s.retry_backoff_ms)
    run_ladder(s, value, opts, attempt + 1)
  end

  # Where a dispatch settles into one record a park IS a record: the entry, the
  # offset it consumed and a parked firing's removal ride one frame, and
  # nothing is held — the record is the durability, so there is no ack to wait
  # for.
  # A bounded stage's Cell does not park: its exhausted ladder fails the
  # tasklet, and the driver unwinds the run. An unbounded operator's Cell parks to the
  # DLQ, which is the only behaviour that existed before bounded operators.
  defp exhausted(%State{park: %{enabled?: false}} = s, _value, error, _attempts, _offset) do
    GenServer.cast(s.operator, {:stage_failed, s.node_ref, format_error(error)})
    s
  end

  defp exhausted(%State{} = s, value, error, attempts, offset),
    do: park(s, value, error, attempts, offset)

  defp park(%State{} = s, value, error, attempts, offset) do
    if one_record?(s, offset),
      do: park_in_record(s, value, error, attempts, offset),
      else: park_held(s, value, error, attempts)
  end

  defp park_held(%State{} = s, value, error, attempts) do
    seq = s.park.seq
    park_id = "#{State.label(s)}##{seq}"
    body = Park.body(park_id, park_channel(s), error, attempts, value)

    Park.ship(
      s.operator,
      self(),
      Park.identity(s.operator_id, s.module.name(), s.cell_key),
      body
    )

    parked_event(%{s | park: %{held?: true, id: park_id, seq: seq + 1}})
  end

  defp park_in_record(%State{} = s, value, error, attempts, offset) do
    {s, ops, tally} = firing_removal(s, value)
    dlq = Park.dlq_entry(park_channel(s), error, attempts, value)
    s = Tier.park(s, dlq, ops, offset, clock_now(s, value))
    s = if ops == [], do: s, else: Clock.reflect(s, ops, tally)
    parked_event(s)
  end

  defp park_channel(%State{} = s), do: s.input_channel || s.node_name || s.operator_id

  defp parked_event(%State{} = s) do
    :telemetry.execute([:flezha, :sdk, :parked], %{count: 1}, %{operator_id: s.operator_id})
    s
  end

  defp flush_emits(s, emits, handout_seq, offset, report \\ nil)
  defp flush_emits(%State{} = s, [], _handout_seq, _offset, _report), do: s

  defp flush_emits(%State{} = s, emits, handout_seq, offset, report) do
    {s, report} = routing_report(s, report)
    Inbox.emitted(s.operator, s.node_ref, s.cell_key, emits, handout_seq, offset, report)
    s
  end

  # The inputs this Cell took stay held until the router has routed their
  # emits: a push into a Cell of the instance takes a hold of its own.
  defp routing_report(%State{tier: %{prefix: nil}} = s, report), do: {s, report}

  defp routing_report(%State{} = s, nil) do
    ref = make_ref()
    {Records.owe(s, ref, nil), {self(), ref}}
  end

  defp routing_report(%State{} = s, report), do: {s, report}

  # Two readers, one event: telemetry is the in-node handle, the counter is what
  # leaves the runner on the heartbeat — a drop nobody can see from outside the
  # process is the defect this closes.
  defp record_drop(%State{} = s, policy, dropped) do
    Counters.inc_drops(s.counters, policy)
    if is_struct(dropped, Input), do: Prefix.release(s.tier.prefix, dropped.offset)

    :telemetry.execute([:flezha, :sdk, :mailbox], %{count: 1}, %{
      operator_id: s.operator_id,
      policy: policy
    })
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  # No queued item, no dispatch in flight, no park held, no deferred push.
  defp holds_nothing?(%State{} = s),
    do: s.size == 0 and not s.processing? and not s.park.held? and :queue.is_empty(s.waiters)

  defp format_error(reason) when is_binary(reason), do: reason

  # The ctx's timer refusal in a `compensate` hook, under the name every SDK gives it.
  defp format_error(:timers_unavailable),
    do: "TimersUnavailable: a compensate hook cannot register or cancel a timer"

  defp format_error(reason), do: inspect(reason)
end
