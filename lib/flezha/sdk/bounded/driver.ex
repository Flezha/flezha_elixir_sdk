defmodule Flezha.SDK.Bounded.Driver do
  @moduledoc """
  One process per bounded instance: the stage driver. A pass walks the chain
  of the entry channel its message arrived on, or of the tasklet whose timer
  fired: a tasklet becomes runnable once every producer feeding it in that
  chain has committed, so a fan-in is dispatched exactly once with the
  accumulated batch of this pass's producers. Each stage commits durably as
  soon as its Cells have drained, whatever its siblings are still doing, and
  the run ends by shipping `BoundedCompleted`.

  It presents the same contract to an `Flezha.SDK.Cell` that
  `Flezha.SDK.Operator` does: a Cell casts its emits to its `operator`
  pid and knows nothing else about it. So every Cell mechanism — the state,
  the records, the retry ladder — is reused as it stands, and what is written
  here is only what a *bounded* run adds: the stages, the barrier and the
  journal.

  The reference drives a chain on a pool thread that blocks on each barrier.
  This one is event-driven, because a blocked BEAM process cannot answer the
  stop, the inbox push or the restore that must reach it while a stage is in
  flight. Every wait is therefore a message: `{:stage_done, ref, pid, written}` from a
  Cell's queue barrier, `{:barrier_ack, ref, result, error}` from the runner,
  and for a message-fed pass with nothing queued, its next message or the idle
  window's `{:park_idle, ref}`.

  A message enters through the same `Flezha.SDK.Operator.Inbox.enter/5` the
  unbounded operator's frames take, and runs only the chain of the entry channel
  it arrived on; a chained loop's later passes re-enter the chain its first
  message picked.

  The driver is the gate a stage Cell's due timer passes through: a firing
  while the driver waits opens a pass at that tasklet; one whose stage is in
  flight joins it; any other waits in the queue with the messages and is
  served at the next boundary before the loop's own continuation. A served
  firing's removal is written when its stage commits.
  """

  use GenServer

  require Logger

  alias Flezha.SDK.{Cell, Knob}
  alias Flezha.SDK.Cell.Records
  alias Flezha.SDK.Operator.{Cells, Inbox, Routing}
  alias Flezha.SDK.Bounded.{Commit, Compensation, Instance, Resume, Spec, Stage}

  defmodule State do
    @moduledoc false
    defstruct [
      :instance,
      :spec,
      :operator_id,
      :runner,
      # the unbounded operator's Cell set, minted per mode and swept between passes
      :cells,
      # the stage barrier refs the running stages still owe, each with its
      # node and Cell, and how many each forward stage owes, so its last one
      # is seen without a scan
      awaiting: %{},
      owed: %{},
      # node_ref => what its cells released this iteration
      collected: %{},
      # node_ref => the keys of the Cells its stage dispatched to
      ran: %{},
      # node_ref => cell identity => the effect records that Cell wrote for the
      # stage, which its commit (or undo) names
      written: %{},
      # node_ref => producer task_idx => what it handed that node, until
      # every producer committed
      pending: %{},
      # node_ref => producers still owed
      waiting: %{},
      # stages completed this iteration, parents before children
      completed: [],
      # committed iterations, each a list of stage records
      committed: [],
      # stages whose Cells have drained, in the order they did, each owing
      # its commit; the head's is the one in flight
      finished: [],
      # the stage whose barrier is in flight, and the ladder it is on
      barrier: nil,
      carry: nil,
      # the entry channel whose chain the pass runs; a carry-fed pass keeps
      # the one the loop's first message picked
      pass_channel: nil,
      # the firing a timer-fed pass runs the tasklet chain of, with the carry
      # it leaves as it stood; `nil` for a message- or carry-fed pass
      origin: nil,
      # the input-log offset of the input this pass consumed, `nil` for none
      consumed_offset: nil,
      # the outside call that input carried, which every `response()` output
      # of the pass answers; `nil` for a timer- or carry-fed pass
      pass_call: nil,
      iter: 0,
      cancel: nil,
      # task_idx => {cell_key, result}: each tasklet's completion this
      # iteration, the one of its Cell whose key sorts last
      completions: %{},
      resume_owed: 0,
      # set while a `:per_input` loop waits for an input; its idle timer names it
      idle: nil,
      # The rollback phase, in one field so the struct stays a flat map (credo
      # refuses a 32nd): the groups still to undo, the one in flight, and the
      # terminal the unwind is heading for — `nil` while running forward.
      unwind: %{plan: [], current: [], status: nil},
      # {status, error} once the run has shipped its completion
      terminal: nil
    ]
  end

  # --- API ---

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc "Cooperative external stop: the run unwinds and completes as `cancelled`."
  def stop_bounded(pid), do: send(pid, :bounded_stop)

  @doc "Send one message on the first declared entry channel."
  def push_inbox(pid, payload), do: GenServer.cast(pid, {:input, nil, payload, %{}})

  # --- server ---

  @impl true
  def init(opts) do
    spec = Keyword.fetch!(opts, :spec)
    operator_id = Keyword.fetch!(opts, :operator_id)
    journal = journal(opts)

    # Cells are minted before anything runs, so a `RestoreInitState` frame
    # arriving right after the `AddOperator` ack always finds a declared one
    # registered; a keyed one the frame names is minted by its lookup.
    cells =
      opts
      |> Keyword.take([:operator_id, :range, :counters, :runner, :secondaries, :credit])
      |> Keyword.merge(topology: spec.topology, kind: :bounded)
      |> Cells.new()
      |> Cells.schedule_reap()

    state = %State{
      instance:
        Instance.new(operator_id, spec,
          journal: journal,
          owed_inbox: Keyword.get(opts, :owed_inbox, 0),
          park_idle_ms: Keyword.get(opts, :park_idle_ms),
          dead_letter_turns: Keyword.get(opts, :dead_letter_turns, false)
        ),
      spec: spec,
      operator_id: operator_id,
      pass_channel: journaled_channel(spec, journal),
      runner: Keyword.fetch!(opts, :runner),
      cells: cells,
      resume_owed: Keyword.get(opts, :resume_cell_count, 0)
    }

    arm_duration(spec)
    {:ok, %{state | iter: state.instance.iter}, {:continue, :bootstrap}}
  end

  # The wall-clock backstop a class declares. It trips the same cooperative
  # cancel an external stop does, so a capped run unwinds rather than being
  # killed mid-stage.
  defp arm_duration(%Spec{knobs: %{max_duration_ms: nil}}), do: :ok

  defp arm_duration(%Spec{knobs: %{max_duration_ms: ms}}),
    do: Process.send_after(self(), :max_duration, ms)

  @impl true
  def handle_continue(:bootstrap, %State{} = s) do
    if s.resume_owed > 0, do: settle(s), else: settle(resume_bound(s))
  end

  # The calls the controller's persistence commands make on an operator,
  # answered exactly as `Flezha.SDK.Operator` answers them, through the same
  # Cell set: a restore, a compaction and a live read reach a bounded operator
  # instance through the same `Flezha.SDK.Commands` path an unbounded one does.
  @impl true
  def handle_call({:lookup, tasklet, key, materialise?}, _from, %State{} = s) do
    {reply, cells} = Cells.lookup(s.cells, tasklet, key, materialise?)
    {:reply, reply, %{s | cells: cells}}
  end

  def handle_call(:cells, _from, %State{} = s), do: {:reply, Cells.listing(s.cells), s}

  # One restore frame the bind owed has landed — folded, refused by the cap,
  # or naming a Cell the class no longer declares — with the saga markers it
  # carried. An unwind the crash interrupted resumes from these: a Cell whose
  # undo already ran is skipped, and the rest of the plan still runs.
  def handle_call({:restore_landed, identity, intent, done}, _from, %State{} = s) do
    s = %{s | instance: Instance.note_compensation(s.instance, identity, intent, done)}

    case s.resume_owed do
      0 ->
        {:reply, :ok, s}

      1 ->
        case settle(resume_bound(s)) do
          {:noreply, s} -> {:reply, :ok, s}
          {:stop, reason, s} -> {:stop, reason, :ok, s}
        end

      owed ->
        {:reply, :ok, %{s | resume_owed: owed - 1}}
    end
  end

  # A Cell finished one invocation. Its outputs are collected for the stage
  # rather than routed: a bounded operator hands a batch on at the barrier, not per
  # value, so the whole stage's release is what the journal records.
  @impl true
  def handle_cast({:emitted, node_ref, cell_key, emits, seq, _input, report}, %State{} = s) do
    origin = %{node_ref: node_ref, cell_key: cell_key, seq: seq}
    values = Enum.map(emits, fn {value, _meta, _key} -> value end)

    s =
      emits
      |> Enum.with_index()
      |> Enum.reduce(s, fn {{value, meta, key}, ordinal}, acc ->
        sink_outputs(
          acc,
          node_ref,
          value,
          %{meta: meta, key: key, origin: origin, call: acc.pass_call},
          ordinal
        )
      end)

    s = %{s | collected: Map.update(s.collected, node_ref, values, &(&1 ++ values))}

    Inbox.delivered(report)

    {:noreply, s}
  end

  # A Cell exhausted its ladder: the tasklet failed. A turn of a `per_input`
  # keep-progress loop is dead-lettered and the loop goes on; any other run
  # unwinds.
  def handle_cast({:stage_failed, node_ref, error, attempts, cell_key}, %State{} = s) do
    if fails_turn?(s),
      do: settle(dead_letter(s, node_ref, error, attempts, cell_key)),
      else: settle(fail(s, node_ref, error))
  end

  # A message off the bound data stream, through the same entrance a stream
  # operator's frame takes: an undeclared name or a parser refusal parks it,
  # consumed without a pass, and a driver waiting for a message waits a fresh
  # idle window. A readable one waits in the inbox for the next message-fed
  # pass; a driver waiting for one starts that pass now.
  def handle_cast({:input, channel, message, metadata}, %State{} = s) do
    case Instance.take(s.instance, metadata) do
      {:ok, instance} -> enter(%{s | instance: instance}, channel, message, metadata)
      {:repeat, instance} -> {:noreply, %{s | instance: instance}}
    end
  end

  # A tasklet asked to end the instance. Each tasklet keeps the request of
  # its Cell whose key sorts last, so which Cell answered first never
  # decides the result; the iteration ends with the tasklet latest in DAG
  # order, the highest `task_idx` (`completion/1`).
  def handle_cast({:complete, node_ref, cell_key, result}, %State{} = s) do
    task_idx = Map.fetch!(s.spec.dag.pos, node_ref)

    case s.completions do
      %{^task_idx => {kept, _result}} when kept > cell_key ->
        {:noreply, s}

      completions ->
        {:noreply, %{s | completions: Map.put(completions, task_idx, {cell_key, result})}}
    end
  end

  # A stage Cell's processing-time timer is due. It opens a pass at that
  # tasklet when the driver waits with nothing queued, joins the tasklet's
  # stage while it runs, and otherwise waits its turn behind what arrived
  # first.
  # The entrance for a message this instance has not taken before.
  defp enter(%State{} = s, channel, message, metadata) do
    case Inbox.enter(
           s.runner,
           s.operator_id,
           s.spec.topology,
           channel,
           message,
           Inbox.input_call(metadata)
         ) do
      {:ok, channel, value} ->
        s = %{s | instance: Instance.push_inbox(s.instance, channel, value, metadata)}
        if s.idle, do: settle(begin_iteration(%{s | idle: nil})), else: {:noreply, s}

      :parked ->
        s = %{s | instance: Instance.entry_parked(s.instance, metadata)}
        if s.idle, do: {:noreply, await_input(s)}, else: {:noreply, s}
    end
  end

  @impl true
  def handle_info({:firing_due, node_ref, cell_key, fire}, %State{} = s) do
    firing = %Instance.Firing{node_ref: node_ref, cell_key: cell_key, fire: fire}

    cond do
      s.terminal != nil or Instance.queued?(s.instance, firing) ->
        {:noreply, s}

      s.idle != nil and Instance.idle?(s.instance) ->
        settle(open_timer_pass(%{s | idle: nil}, firing))

      joinable?(s, node_ref) ->
        {:noreply, dispatch_firing(s, firing)}

      true ->
        {:noreply, %{s | instance: Instance.push_firing(s.instance, firing)}}
    end
  end

  def handle_info({:stage_done, ref, _pid, written}, %State{} = s),
    do: settle(drained(s, ref, written))

  def handle_info({:barrier_ack, ref, _result, error}, %State{barrier: %{ref: ref}} = s),
    do: settle(barrier_settled(s, error))

  def handle_info({:barrier_ack, _stale, _result, _error}, %State{} = s), do: {:noreply, s}

  # A stop and the duration cap are cooperative: the run unwinds at its next
  # step. A loop waiting for input has no next step and nothing to unwind.
  def handle_info(stop, %State{} = s) when stop in [:bounded_stop, :max_duration] do
    s = %{s | cancel: :cancelled}
    if s.idle, do: settle(complete(%{s | idle: nil}, "cancelled", "")), else: {:noreply, s}
  end

  def handle_info({:park_idle, ref}, %State{idle: ref} = s), do: settle(park(%{s | idle: nil}))

  def handle_info({:secondaries, secondaries}, %State{} = s),
    do: {:noreply, %{s | cells: Cells.rebind(s.cells, secondaries)}}

  # A Cell that exits never answers a barrier it holds: that stage fails as
  # an exhausted ladder fails it, and the barrier counts as drained.
  def handle_info({:DOWN, _ref, :process, pid, reason}, %State{} = s) do
    s = %{s | cells: Cells.down(s.cells, pid, reason)}
    lost = for {ref, {_node_ref, ^pid}} <- s.awaiting, do: ref
    settle(Enum.reduce(lost, s, &lose_barrier(&2, &1, reason)))
  end

  # The sweep runs only while the loop waits for its next message, and keeps
  # every Cell a rollback of the class's mode could still reach.
  def handle_info(:reap, %State{} = s) do
    cells = if s.idle, do: Cells.reap(s.cells, Compensation.held(s)), else: s.cells
    {:noreply, %{s | cells: Cells.schedule_reap(cells)}}
  end

  def handle_info(_msg, %State{} = s), do: {:noreply, s}

  # A class that journals has its stage Cells capture their plugin output into
  # their own effect records, released once the stage commits; one that
  # journals nothing sends it straight to the wire, as an unbounded operator's is.
  defp sink_outputs(%State{} = s, node_ref, value, emit, ordinal) do
    if journals?(s) do
      s
    else
      s.spec.topology.routes
      |> Map.get({:node, node_ref}, [])
      |> Enum.filter(&match?(%{to: {:sink, _}}, &1))
      |> Enum.reduce(s, fn route, acc ->
        stamp_with = if route.keyed, do: {acc.operator_id, acc.spec.topology.node_modules}
        {metadata, bytes} = Routing.sink_frame(route, value, emit, ordinal, stamp_with)
        %{acc | cells: Cells.emit_output(acc.cells, metadata, bytes)}
      end)
    end
  end

  # One Cell's records for a stage, as its barrier reported them. A stage can
  # queue more than one barrier behind a Cell (a firing that joins it), so the
  # ranges widen.
  defp note_written(%State{} = s, _node_ref, nil), do: s

  defp note_written(%State{} = s, node_ref, %{cell_id: id} = written) do
    stage =
      s.written
      |> Map.get(node_ref, %{})
      |> Map.update(id, written, &Records.widen(&1, written))

    %{s | written: Map.put(s.written, node_ref, stage)}
  end

  # The last committed iteration ended the instance and the crash landed before
  # its terminal frame left, so it ends now with the journaled result rather
  # than waiting for input the finished run will never read.
  defp resume_bound(%State{instance: %Instance{journal: %{completion: result}}} = s)
       when is_binary(result),
       do: complete(%{s | resume_owed: 0}, "ok", "", result)

  # Every restore the bind owed has landed. A compensation-DONE marker is the
  # only proof an undo was in flight when the crash landed — intent markers
  # accumulate during ordinary forward progress too — so it is what decides
  # between finishing the unwind and running forward.
  defp resume_bound(%State{} = s) do
    s = drop_committed_firings(%{s | resume_owed: 0})

    cond do
      Instance.any_compensation_done?(s.instance) -> unwind(s, "error", "resumed mid-rollback")
      (stages = Resume.rewind_stages(s)) != [] -> rewind(s, stages)
      true -> begin_iteration(s)
    end
  end

  # A rewind unwinds the iterations above its target, then the loop goes on
  # from it: the journal has already folded the forward state back.
  defp rewind(%State{} = s, stages) do
    s = %{s | unwind: %{s.unwind | status: :rewind}}
    unwind_step(s, &Compensation.rewind(&1, stages))
  end

  # --- the iteration loop ---

  defp begin_iteration(%State{cancel: reason} = s) when reason != nil,
    do: complete(s, "cancelled", "")

  defp begin_iteration(%State{} = s) do
    if Resume.mid_iteration?(s.instance),
      do: resume_mid_iteration(s),
      else: fresh_seed(s)
  end

  # The pass the crash interrupted resumes as the pass it was: a message-fed
  # one re-takes the message the controller re-forwards (waiting for it if it
  # has not landed yet), a carry-fed one re-enters the journaled chain.
  defp resume_mid_iteration(%State{} = s) do
    case Resume.timer_root(s.instance) do
      nil ->
        resume_message_or_carry(s)

      origin ->
        start = Resume.start(s, origin, nil, &through_edge(s, &1, &2, &3))
        resume_iteration(%{s | origin: origin}, start)
    end
  end

  defp resume_message_or_carry(%State{} = s) do
    if message_fed?(s) do
      case pop_message(s) do
        {:message, s, item} ->
          resume_iteration(s, Resume.start(s, item.channel, item, &through_edge(s, &1, &2, &3)))

        :wait ->
          await_input(s)
      end
    else
      channel = carried_channel(s)
      resume_iteration(s, Resume.start(s, channel, nil, &through_edge(s, &1, &2, &3)))
    end
  end

  # A mid-iteration resume skips what the journal says committed and re-opens
  # the frontier its uncommitted children are owed. The journal is consumed
  # once: the iterations after it seed fresh.
  defp resume_iteration(%State{} = s, start) do
    {_journal, instance} = Instance.take_journal(s.instance)

    s = %{
      fresh_iteration(s)
      | instance: instance,
        pass_channel: start.channel || s.pass_channel,
        pending: start.pending,
        waiting: start.waiting,
        completed: start.completed,
        completions: start.completions
    }

    open_stages(s, start.ready)
  end

  defp fresh_seed(%State{} = s) do
    {journal, instance} = Instance.take_journal(s.instance)
    s = %{s | instance: instance, carry: s.carry || journaled_carry(s, journal)}

    case seed(s) do
      {:message, s, item} ->
        enter_pass(s, item)

      {:firing, s, firing} ->
        open_timer_pass(s, firing)

      {:carry, s, batch} ->
        channel = carried_channel(s)
        start_pass(s, channel, frontier(s, channel, fn _root -> unkeyed(batch) end))

      {:done, s} ->
        complete(s, "ok", "")

      :wait ->
        await_input(s)
    end
  end

  # A message's pass: every root of its chain, each through its own entry
  # edge. An edge whose transform rejects the message, or a key this partition
  # member does not own, parks it under the channel, consumed without a pass,
  # and the instance goes back to waiting.
  defp enter_pass(%State{} = s, item) do
    with {:ok, frontier} <- entry_frontier(s, item),
         :ok <- own_keys(s, frontier) do
      start_pass(s, item.channel, frontier)
    else
      {:rejected, reason} ->
        Inbox.park_pre_dispatch(s.runner, s.operator_id, item.channel, item.channel, reason, nil)
        await_input(%{s | instance: Instance.note_parked(s.instance, item.offset)})
    end
  end

  defp entry_frontier(%State{} = s, item) do
    {:ok, frontier(s, item.channel, &through_edge(s, {:entry, item.channel}, &1, [item.payload]))}
  rescue
    e -> {:rejected, "entry edge failed: " <> Exception.message(e)}
  end

  # Before any Cell is minted, as the unbounded operator's router refuses a key.
  defp own_keys(%State{cells: cells}, frontier) do
    Enum.find_value(frontier, :ok, fn {root, batch} ->
      Enum.find_value(batch, fn {_value, key} ->
        case Cells.admit(cells, root, key) do
          :ok -> nil
          {:error, reason} -> {:rejected, reason}
        end
      end)
    end)
  end

  # A pass's first frontier: every root of the chain that has a batch.
  defp frontier(%State{} = s, channel, batch_for) do
    for root <- Map.fetch!(s.spec.dag.chains, channel).roots,
        batch = batch_for.(root),
        batch != [],
        do: {root, batch}
  end

  defp start_pass(%State{} = s, channel, frontier),
    do: open_stages(%{fresh_iteration(s) | pass_channel: channel, origin: nil}, frontier)

  # A firing's pass: the one Cell whose timer fired, at its tasklet, whose
  # chain the pass runs. The entry channel and consumed offset stay the last
  # message-fed pass's, which its iteration commit names, and so does the
  # carry a chained loop feeds on when this pass reaches no leaf.
  defp open_timer_pass(%State{cancel: reason} = s, _firing) when reason != nil,
    do: complete(s, "cancelled", "")

  defp open_timer_pass(%State{} = s, %Instance.Firing{} = firing) do
    s = %{
      fresh_iteration(s)
      | origin: %{firing: firing, carry: s.carry},
        carry: nil,
        pass_call: nil,
        awaiting: %{},
        owed: %{}
    }

    s |> dispatch_firing(firing) |> finish_if_drained(firing.node_ref) |> pass_step()
  end

  # The firing on its Cell with a barrier behind it, counted with its stage.
  defp dispatch_firing(%State{} = s, %Instance.Firing{node_ref: node_ref} = firing) do
    {keys, awaiting, cells} =
      Stage.fire(s.cells, node_ref, firing.cell_key, firing.fire, self())

    %{
      s
      | cells: cells,
        collected: Map.put_new(s.collected, node_ref, []),
        ran: Map.update(s.ran, node_ref, keys, &Enum.uniq(&1 ++ keys))
    }
    |> await(node_ref, awaiting)
  end

  # A stage still waiting for its Cells can take a firing of its own tasklet.
  defp joinable?(%State{unwind: %{status: nil}, cancel: nil} = s, node_ref),
    do: running?(s, node_ref)

  defp joinable?(%State{}, _node_ref), do: false

  defp running?(%State{owed: owed}, node_ref), do: Map.has_key?(owed, node_ref)

  defp await(%State{} = s, _node_ref, []), do: s

  defp await(%State{} = s, node_ref, awaiting) do
    count = length(awaiting)
    owed = Map.update(s.owed, node_ref, count, &(&1 + count))
    %{s | awaiting: Enum.into(awaiting, s.awaiting), owed: owed}
  end

  # An undo's barrier names no forward stage, so it owes nothing here.
  defp owe_less(owed, node_ref) do
    case Map.get(owed, node_ref) do
      n when is_integer(n) and n > 1 -> Map.put(owed, node_ref, n - 1)
      _last_or_none -> Map.delete(owed, node_ref)
    end
  end

  # An empty inbox is waited on as the reference waits: for the tail a
  # rehydrating bind still owes, else for the idle window, and the loop parks
  # when the wait ends. A class that does not sleep waits for an input or a
  # stop.
  defp await_input(%State{} = s) do
    ref = make_ref()

    if s.spec.knobs.sleep_when_idle,
      do: Process.send_after(self(), {:park_idle, ref}, Instance.idle_ms(s.instance))

    %{s | idle: ref}
  end

  # Where iteration `iter` starts: the next message for a message-fed pass
  # (every pass of a `:per_input` loop, the first of any other), with nothing
  # queued meaning wait; done for a `:once` class after its one pass; the
  # previous terminal batch for a handoff-chained loop, which ends on an
  # empty one.
  defp seed(%State{spec: %Spec{knobs: %{loop_mode: :once}}, iter: iter} = s) when iter > 0,
    do: {:done, s}

  defp seed(%State{} = s) do
    case Instance.pop_trigger(s.instance, message_fed?(s)) do
      {:ok, %Instance.Firing{} = firing, instance} ->
        {:firing, %{s | instance: instance}, firing}

      {:ok, item, instance} ->
        {:message, message_pass(s, instance, item), item}

      {:empty, _instance} ->
        cond do
          message_fed?(s) -> :wait
          s.carry in [nil, []] -> {:done, s}
          true -> {:carry, %{s | carry: nil, consumed_offset: nil, pass_call: nil}, s.carry}
        end
    end
  end

  defp pop_message(%State{} = s) do
    case Instance.pop_inbox(s.instance) do
      {:ok, item, instance} ->
        {:message, message_pass(s, instance, item), item}

      {:empty, _instance} ->
        :wait
    end
  end

  defp message_pass(%State{} = s, instance, item),
    do: %{s | instance: instance, consumed_offset: item.offset, pass_call: item.call}

  # Every pass of a `:per_input` loop takes a message; any loop's first pass
  # does; a chained loop's later passes are carry-fed.
  defp message_fed?(%State{spec: %Spec{knobs: %{loop_mode: :per_input}}}), do: true
  defp message_fed?(%State{iter: 0}), do: true
  defp message_fed?(%State{}), do: false

  # The chain a carry-fed pass re-enters: the one the loop's first message
  # picked, or the journaled one after a resume.
  defp carried_channel(%State{pass_channel: channel}) when is_binary(channel), do: channel
  defp carried_channel(%State{spec: spec}), do: hd(spec.topology.entry_order)

  # The entry channel the journal names, when the class still declares it.
  defp journaled_channel(_spec, nil), do: nil

  defp journaled_channel(%Spec{topology: topo}, journal) do
    if journal.entry_channel in topo.entry_order, do: journal.entry_channel
  end

  # A chained loop resuming past its first pass carries the journaled
  # terminal batch into its next pass.
  defp journaled_carry(%State{spec: %Spec{knobs: %{loop_mode: :chained}}, iter: iter}, journal)
       when iter > 0 and journal != nil,
       do: Resume.carry(journal)

  defp journaled_carry(_s, _journal), do: nil

  defp fresh_iteration(%State{} = s),
    do: %{
      s
      | collected: %{},
        ran: %{},
        written: %{},
        pending: %{},
        waiting: %{},
        completed: [],
        finished: [],
        completions: %{}
    }

  # --- the stages of one pass ---

  defp open_stages(%State{cancel: reason} = s, _ready) when reason != nil,
    do: unwind(s, "cancelled", "")

  defp open_stages(%State{} = s, ready) do
    ready
    |> Enum.reduce(%{s | awaiting: %{}, owed: %{}}, fn {node, batch}, acc ->
      dispatch_stage(acc, node, batch)
    end)
    |> pass_step()
  end

  # The stage ran on exactly the Cells it handed a value to: that is what its
  # commit and its undo name, whether or not they emit. One that handed
  # nothing to any Cell has no barrier to wait for, so it is finished at once.
  defp dispatch_stage(%State{} = s, node_ref, batch) do
    {keys, awaiting, cells} = Stage.dispatch(s.cells, node_ref, batch, s.pass_call, self())

    %{
      s
      | cells: cells,
        collected: Map.put_new(s.collected, node_ref, []),
        ran: Map.put(s.ran, node_ref, keys)
    }
    |> await(node_ref, awaiting)
    |> finish_if_drained(node_ref)
  end

  defp finish_if_drained(%State{} = s, node_ref) do
    if running?(s, node_ref), do: s, else: %{s | finished: s.finished ++ [node_ref]}
  end

  # One of a stage's Cells drained past its barrier. The unwind waits for its
  # whole group; a forward stage is finished once its last Cell has drained.
  defp stage_settled(%State{unwind: %{status: st}, awaiting: awaiting} = s, _node_ref)
       when st != nil do
    if map_size(awaiting) == 0, do: unwind_step(s, &Compensation.commit/1), else: s
  end

  defp stage_settled(%State{} = s, node_ref), do: s |> finish_if_drained(node_ref) |> pass_step()

  # One `TaskCommit` at a time, in the order the stages finished: a child is
  # dispatched only on its producers' successful replies, so a crash re-runs
  # exactly the stages that had not committed. A cancelled pass commits
  # nothing more and unwinds once its running stages have drained; one with
  # nothing running and nothing owed ends its iteration.
  defp pass_step(%State{barrier: barrier} = s) when barrier != nil, do: s

  defp pass_step(%State{cancel: reason, awaiting: awaiting} = s) when reason != nil,
    do: if(map_size(awaiting) == 0, do: unwind(s, "cancelled", ""), else: s)

  defp pass_step(%State{finished: [node_ref | _]} = s), do: commit_stage(s, node_ref)
  defp pass_step(%State{awaiting: awaiting} = s) when map_size(awaiting) > 0, do: s
  defp pass_step(%State{} = s), do: seal_iteration(s)

  defp commit_stage(%State{} = s, node_ref) do
    task_idx = Map.fetch!(s.spec.dag.pos, node_ref)
    batch = Map.get(s.collected, node_ref, [])
    ran = s.ran |> Map.get(node_ref, []) |> Enum.map(&Cells.wire_identity(s.cells, node_ref, &1))

    s = record_completed(s, node_ref, task_idx, ran)
    Compensation.mark_intent(s, node_ref, ran)

    if journals?(s) do
      ref = make_ref()
      asked = if s.unwind.status == nil, do: Map.get(s.completions, task_idx)

      command =
        Commit.task(s.runner, self(), ref,
          operator_id: s.operator_id,
          task_idx: task_idx,
          iter: s.iter,
          cells_ran: Commit.cells_ran(s.written, node_ref, ran),
          handoff: handoff(s, node_ref, batch),
          compensating: s.unwind.status != nil,
          completes: asked != nil,
          result: result_of(asked),
          trigger: trigger(s, node_ref)
        )

      arm(s, ref, :task, command)
    else
      advance(s)
    end
  end

  # A barrier in flight: its reference, what it is committing, the rung of the
  # re-send ladder it is on, and the bytes a re-send repeats.
  defp arm(%State{} = s, ref, kind, command),
    do: %{s | barrier: %{ref: ref, kind: kind, attempt: 1, command: command}}

  defp record_completed(%State{} = s, node_ref, task_idx, ran),
    do: %{
      s
      | completed: s.completed ++ [%{node_ref: node_ref, task_idx: task_idx, cells: ran}]
    }

  # The committed stage's release accumulates into each child, which is
  # dispatched once every producer feeding it in this pass's chain has
  # committed, over their releases in DAG order whichever committed first,
  # as a resume rebuilds it; a producer of another chain is not running and
  # is never waited for.
  defp advance(%State{finished: [node_ref | rest]} = s) do
    batch = Map.get(s.collected, node_ref, [])
    children = Map.fetch!(s.spec.dag.children, node_ref)
    idx = Map.fetch!(s.spec.dag.pos, node_ref)

    s =
      Enum.reduce(children, %{s | finished: rest}, fn child, acc ->
        owed = Map.get(acc.waiting, child, chain_parents(acc, child)) - 1
        mapped = through_edge(acc, node_ref, child, batch)
        bank = acc.pending |> Map.get(child, %{}) |> Map.put(idx, mapped)

        cond do
          owed > 0 ->
            %{
              acc
              | waiting: Map.put(acc.waiting, child, owed),
                pending: Map.put(acc.pending, child, bank)
            }

          acc.cancel ->
            acc

          true ->
            acc = %{
              acc
              | waiting: Map.delete(acc.waiting, child),
                pending: Map.delete(acc.pending, child)
            }

            dispatch_stage(acc, child, Resume.in_producer_order(bank))
        end
      end)

    s = if children == [], do: %{s | carry: (s.carry || []) ++ batch}, else: s
    pass_step(s)
  end

  # --- the iteration boundary ---

  defp seal_iteration(%State{spec: %Spec{knobs: %{loop_mode: :once}}} = s),
    do: complete(s, "ok", "", result_of(completion(s)))

  defp seal_iteration(%State{} = s) do
    s = keep_carry(s)

    if journals?(s) do
      ref = make_ref()

      command =
        Commit.iter(s.runner, self(), ref,
          operator_id: s.operator_id,
          iter: s.iter,
          consumed_offset: s.consumed_offset,
          entry_channel: s.pass_channel,
          handoff: terminal_handoff(s),
          completes: completion(s) != nil,
          result: result_of(completion(s))
        )

      arm(s, ref, :iter, command)
    else
      next_iteration(s)
    end
  end

  # The iteration is committed. If a tasklet in it asked to end the instance,
  # the run ends here: no next input, no re-seed, no park.
  defp next_iteration(%State{} = s) do
    case completion(s) do
      nil -> begin_iteration(%{s | committed: s.committed ++ [s.completed], iter: s.iter + 1})
      asked -> complete(s, "ok", "", result_of(asked))
    end
  end

  # The completion the iteration ends with: the tasklet latest in DAG order,
  # whichever committed first.
  defp completion(%State{completions: completions}) when completions == %{}, do: nil

  defp completion(%State{completions: completions}),
    do: completions |> Enum.max_by(&elem(&1, 0)) |> elem(1)

  defp result_of(nil), do: <<>>
  defp result_of({_cell_key, result}), do: result

  # --- barriers ---

  defp barrier_settled(%State{barrier: %{kind: kind}} = s, nil) do
    s = %{s | barrier: nil}

    case kind do
      :task -> s |> release_firings() |> advance()
      :iter -> next_iteration(s)
      :dead_letter -> undo_turn(s)
      :comp -> unwind_step(s, &Compensation.commit/1)
    end
  end

  defp barrier_settled(%State{barrier: barrier} = s, error) do
    attempts = Knob.barrier_resend_attempts()

    if barrier.attempt < attempts do
      Logger.warning(
        "[Flezha.SDK] #{s.operator_id} barrier attempt #{barrier.attempt}/#{attempts} " <>
          "refused: #{error.message} (#{error.code}); re-sending"
      )

      # The same bytes, not a rebuild: the controller journals a barrier once
      # by its identity, so a transient stall delays the chain rather than
      # committing it twice or ending it.
      Commit.resend(s.runner, self(), barrier.ref, barrier.command)
      %{s | barrier: %{barrier | attempt: barrier.attempt + 1}}
    else
      Logger.warning(
        "[Flezha.SDK] #{s.operator_id} barrier gave up: #{error.message} (#{error.code})"
      )

      give_up(%{s | barrier: nil}, "#{error.message} (#{error.code})")
    end
  end

  # A barrier that gave up during the unwind ends the run on the status the
  # unwind was already heading for; one that gave up going forward starts it.
  defp give_up(%State{unwind: %{status: {status, _}}} = s, _error), do: complete(s, status, "")
  defp give_up(%State{unwind: %{status: :rewind}} = s, error), do: complete(s, "error", error)

  defp give_up(%State{unwind: %{status: :dead_letter}} = s, error),
    do: unwind(%{s | unwind: %{s.unwind | status: nil}}, "error", error)

  defp give_up(%State{} = s, error), do: unwind(s, "error", error)

  # --- a failed turn ---

  # Whether a tasklet's failure ends only its turn: a message-fed pass of a
  # `per_input` keep-progress loop, running forward with no stop asked, bound
  # by a controller that parks the turn.
  defp fails_turn?(%State{
         spec: %Spec{knobs: %{loop_mode: :per_input, rollback_mode: :keep_progress}},
         instance: %Instance{dead_letter_turns: true},
         unwind: %{status: nil},
         cancel: nil,
         origin: nil
       }),
       do: true

  defp fails_turn?(%State{}), do: false

  # The pass stops as an unwind stops it and its iteration commits as
  # dead-lettered, the controller parking its message first; on the ack the
  # turn is undone. A run that journals nothing keeps no input log, so it
  # parks the turn itself, without bytes, and undoes it at once.
  defp dead_letter(%State{} = s, node_ref, error, attempts, cell_key) do
    tasklet = Spec.tasklet_name(s.spec, node_ref)
    s = stop_pass(s, :dead_letter)

    if journals?(s) do
      ref = make_ref()

      command =
        Commit.iter(s.runner, self(), ref,
          operator_id: s.operator_id,
          iter: s.iter,
          consumed_offset: s.consumed_offset,
          entry_channel: s.pass_channel,
          handoff: [],
          dead_letter: %{tasklet: tasklet, attempts: attempts, error: error, cell_key: cell_key}
        )

      arm(s, ref, :dead_letter, command)
    else
      Inbox.park_pre_dispatch(
        s.runner,
        s.operator_id,
        "_turn",
        s.pass_channel,
        "tasklet `#{tasklet}` failed after #{attempts} attempt(s): #{error}",
        nil,
        cell_key: "_turn",
        attempts: attempts,
        reply_to: s.pass_call
      )

      undo_turn(s)
    end
  end

  # The failed turn's iteration is committed: what it ran is undone as a
  # rewind undoes an iteration, and the loop takes its next message.
  defp undo_turn(%State{} = s) do
    stages = Enum.map(s.completed, &Map.put(&1, :iter, s.iter))
    rewind(%{s | iter: s.iter + 1}, stages)
  end

  # --- terminal paths ---

  # A later Cell of a pass whose turn already failed has nothing to add.
  defp fail(%State{unwind: %{status: :dead_letter}} = s, _node_ref, _error), do: s

  defp fail(%State{unwind: %{status: st}} = s, node_ref, error) when st != nil do
    Logger.warning(
      "[Flezha.SDK] #{s.operator_id} compensate hook on #{Spec.tasklet_name(s.spec, node_ref)} " <>
        "failed: #{error}; the rest of the plan still runs"
    )

    s
  end

  defp fail(%State{} = s, node_ref, error),
    do: unwind(s, "error", "#{Spec.tasklet_name(s.spec, node_ref)}: #{error}")

  # The saga unwind is a phase, not a call: each stage's undo drains through
  # the same barrier a forward stage does, and the terminal frame is shipped
  # only once the whole plan has run. With no hook declared on anything that
  # ran there is nothing to undo and the run ends at once.
  defp unwind(%State{unwind: %{status: nil}, completed: []} = s, status, error) do
    # A resumed unwind has run no stage of its own, so what it undoes comes
    # from the journal rather than from this bind's progress.
    do_unwind(%{s | completed: Resume.completed_stages(s)}, status, error)
  end

  defp unwind(%State{unwind: %{status: nil}} = s, status, error), do: do_unwind(s, status, error)

  # A failure during the unwind does not restart it.
  defp unwind(%State{} = s, _status, _error), do: s

  defp do_unwind(%State{} = s, status, error) do
    s = stop_pass(s, {status, error})

    case Compensation.begin(s) do
      :nothing_to_undo -> complete(s, status, error)
      outcome -> unwind_step(s, fn _ -> outcome end)
    end
  end

  # No stage of the pass commits or is waited on any more; the run is now
  # heading for `status`.
  defp stop_pass(%State{} = s, status),
    do: %{
      s
      | finished: [],
        awaiting: %{},
        owed: %{},
        barrier: nil,
        unwind: %{s.unwind | status: status}
    }

  # Each step of the unwind is `Compensation`'s to decide; the driver only
  # carries the terminal it was already heading for.
  defp unwind_step(%State{unwind: %{status: :rewind}} = s, fun) do
    case fun.(s) do
      {:done, s} -> begin_iteration(%{s | unwind: %{s.unwind | status: nil}})
      {_unwinding, s} -> s
    end
  end

  defp unwind_step(%State{unwind: %{status: {status, error}}} = s, fun) do
    case fun.(s) do
      {:unwinding, s} -> s
      {:awaiting, s} -> s
      {:done, s} -> complete(s, status, error)
    end
  end

  # A park is not a terminal outcome: the instance tears its run down WITHOUT
  # a `BoundedCompleted`, and the controller keeps it suspended until an
  # inbox append or the wake deadline rehydrates it. The park record names the
  # last COMMITTED iteration, so a resume enters the one after it either way,
  # and the last message the run parked unread, which the inbox releases.
  defp park(%State{} = s) do
    wake = earliest_timer(s)
    Commit.park(s.runner, s.operator_id, max(s.iter - 1, 0), wake, s.instance.parked_offset)
    %{s | terminal: :parked}
  end

  # What a sleeper wakes at: the earliest processing-time timer any of its
  # Cells holds, `0` for none. An event-time timer waits for an input, which
  # wakes a sleeper anyway.
  defp earliest_timer(%State{cells: cells}) do
    cells
    |> Cells.pids()
    |> Enum.map(&Cell.earliest_timer/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.min(fn -> 0 end)
  end

  # The stage that served firings committed: each Cell it ran writes the
  # removals it held.
  defp release_firings(%State{finished: [node_ref | _]} = s) do
    for key <- Map.get(s.ran, node_ref, []),
        pid = Cells.pid(s.cells, node_ref, key),
        pid != nil,
        do: Cell.release_firings(pid)

    s
  end

  # The firing a timer-fed pass's root commit names; no other stage names one.
  defp trigger(%State{origin: %{firing: %{node_ref: node_ref} = firing}}, node_ref),
    do: Instance.Firing.trigger(firing)

  defp trigger(%State{}, _node_ref), do: nil

  # A timer-fed pass that released nothing at a leaf leaves a chained loop's
  # carry as it stood, so it does not end the loop as an empty batch would.
  defp keep_carry(%State{origin: %{carry: kept}, carry: carry} = s) when carry in [nil, []],
    do: %{s | carry: kept}

  defp keep_carry(%State{} = s), do: s

  # A restored timer whose firing's stage the journal records committed fired
  # before the crash: it is dropped from its Cell and from the queue.
  defp drop_committed_firings(%State{} = s) do
    fired = for {_iter, firing} <- Resume.committed_firings(s.instance), do: firing

    for %Instance.Firing{fire: fire} = firing <- fired,
        pid = Cells.pid(s.cells, firing.node_ref, firing.cell_key),
        pid != nil,
        do: Cell.drop_firings(pid, [{fire.kind, fire.at}])

    %{s | instance: Instance.filter_firings(s.instance, &(&1 not in fired))}
  end

  # Only a run that reached its end with a completion carries a result: one that
  # fails or is stopped discards what its iteration asked for.
  defp complete(s, status, error, result \\ <<>>)

  defp complete(%State{terminal: nil} = s, status, error, result) do
    Commit.completed(s.runner, s.operator_id, status, error, result)
    %{s | terminal: {status, error}}
  end

  defp complete(%State{} = s, _status, _error, _result), do: s

  # One exit for every flow function: a run that shipped its terminal frame
  # ends the process, and nothing else does.
  defp settle(%State{terminal: nil} = s), do: {:noreply, s}
  defp settle(%State{} = s), do: {:stop, :normal, s}

  defp drained(%State{} = s, ref, written) do
    case Map.pop(s.awaiting, ref) do
      {nil, _} ->
        s

      {{node_ref, _pid}, awaiting} ->
        s = %{s | awaiting: awaiting, owed: owe_less(s.owed, node_ref)}
        s |> note_written(node_ref, written) |> stage_settled(node_ref)
    end
  end

  # A forward failure clears every barrier, so a later one of the same Cell
  # may be gone already.
  defp lose_barrier(%State{awaiting: awaiting} = s, ref, reason) do
    case awaiting do
      %{^ref => {node_ref, _pid}} ->
        s |> fail(node_ref, "cell exited: #{inspect(reason)}") |> drained(ref, nil)

      _cleared ->
        s
    end
  end

  # --- projections ---

  defp journals?(%State{spec: spec}), do: spec.knobs.persistence_enabled

  # A stage's release through its output channel, as the journal records it.
  # The channel a tasklet releases on is its node's — one per node — so the
  # id is the stage's own position, which is what re-seeds its children.
  defp handoff(%State{} = s, node_ref, batch) do
    id = Map.fetch!(s.spec.dag.pos, node_ref)
    Enum.map(batch, fn value -> {id, encode(value)} end)
  end

  defp terminal_handoff(%State{carry: nil}), do: []

  defp terminal_handoff(%State{carry: carry} = s) do
    leaves = Map.fetch!(s.spec.dag.chains, carried_channel(s)).leaves
    id = leaves |> List.first() |> then(&Map.get(s.spec.dag.pos, &1, 0))
    Enum.map(carry, fn value -> {id, encode(value)} end)
  end

  # The producers of `child` a join waits for in this pass: those inside the
  # pass's chain.
  defp chain_parents(%State{} = s, child),
    do: s |> pass_chain() |> Map.fetch!(:parents_in_chain) |> Map.get(child, 0)

  # The chain this pass runs: the fired tasklet's, or the entry channel's.
  defp pass_chain(%State{origin: %{firing: %{node_ref: node_ref}}, spec: spec}),
    do: Map.fetch!(spec.dag.from_node, node_ref)

  defp pass_chain(%State{spec: spec} = s), do: Map.fetch!(spec.dag.chains, carried_channel(s))

  defp encode(value), do: Resume.encode(value)

  # An edge's fused transform and key, applied once as the batch crosses it —
  # the same application a resumed re-seed makes, so a resumed chain sees what
  # the live one did. Each value comes out with the key its edge gives it, so
  # a join fed by two keyed edges keeps both keys. `from` is a producing node,
  # or `{:entry, channel}` for the edge a message enters on.
  defp through_edge(%State{} = s, from, to, batch) do
    case edge_route(s, from, to) do
      nil ->
        unkeyed(batch)

      %{transform: transform, key_by: key_by} ->
        for value <- batch, out <- transform.(value), do: {out, key_by && key_by.(out)}
    end
  end

  defp unkeyed(batch), do: Enum.map(batch, &{&1, nil})

  defp edge_route(%State{} = s, {:entry, _} = source, to), do: route_into(s, source, to)
  defp edge_route(%State{} = s, from, to), do: route_into(s, {:node, from}, to)

  defp route_into(%State{} = s, source, to) do
    s.spec.topology.routes
    |> Map.get(source, [])
    |> Enum.find(&match?(%{to: {:node, ^to}}, &1))
  end

  defp journal(opts) do
    case Keyword.get(opts, :journal) do
      nil -> nil
      facts -> struct(Instance.Resume, facts)
    end
  end
end
