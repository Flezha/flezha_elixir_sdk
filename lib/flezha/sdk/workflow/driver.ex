defmodule Flezha.SDK.Workflow.Driver do
  @moduledoc """
  One process per workflow instance: the stage driver. A pass walks the chain
  of the entry channel its message arrived on, frontier by frontier: a tasklet
  becomes runnable once every producer feeding it in that chain has committed,
  so a fan-in is dispatched exactly once with the accumulated batch of this
  pass's producers. It commits each stage durably and ends by shipping
  `WorkflowCompleted`.

  It presents the same contract to an `Flezha.SDK.Cell` that
  `Flezha.SDK.Operative` does: a Cell casts its emits to its `operative`
  pid and knows nothing else about it. So every Cell mechanism — the state,
  the records, the retry ladder — is reused as it stands, and what is written
  here is only what a *bounded* run adds: the frontier, the barrier and the
  journal.

  The reference drives a chain on a pool thread that blocks on each barrier.
  This one is event-driven, because a blocked BEAM process cannot answer the
  stop, the inbox push or the restore that must reach it while a stage is in
  flight. Every wait is therefore a message: `{:stage_done, ref, pid}` from a
  Cell's queue barrier, `{:barrier_ack, ref, result, error}` from the runner,
  and for a message-fed pass with nothing queued, its next message or the idle
  window's `{:park_idle, ref}`.

  A message enters through the same `Flezha.SDK.Operative.Inbox.enter/5` the
  stream operative's frames take, and runs only the chain of the entry channel
  it arrived on; a chained loop's later passes re-enter the chain its first
  message picked.
  """

  use GenServer

  require Logger

  alias Flezha.SDK.Knob
  alias Flezha.SDK.Operative.{Cells, Inbox, Routing}
  alias Flezha.SDK.Workflow.{Commit, Compensation, Instance, Resume, Spec, Stage}

  defmodule State do
    @moduledoc false
    defstruct [
      :instance,
      :spec,
      :operative_id,
      :runner,
      # the stream operative's Cell set, minted per mode and swept between passes
      :cells,
      # the stage barrier refs this frontier still owes
      awaiting: %{},
      # node_ref => what its cells released this iteration
      collected: %{},
      # node_ref => the keys of the Cells its stage dispatched to
      ran: %{},
      # node_ref => plugin output staged for this stage's commit, and the
      # per-{stage, cell} ordinal each item takes.
      outbox: %{items: %{}, seq: %{}},
      # node_ref => accumulating input batch until every producer committed
      pending: %{},
      # node_ref => producers still owed
      waiting: %{},
      # stages completed this iteration, parents before children
      completed: [],
      # committed iterations, each a list of stage records
      committed: [],
      # the frontier being run, and the one it opens
      frontier: [],
      next: [],
      # the stage whose barrier is in flight, and the ladder it is on
      barrier: nil,
      carry: nil,
      # the entry channel whose chain the pass runs; a carry-fed pass keeps
      # the one the loop's first message picked
      pass_channel: nil,
      # the input-log offset of the input this pass consumed, `nil` for none
      consumed_offset: nil,
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
  def stop_workflow(pid), do: send(pid, :workflow_stop)

  @doc "Send one message on the first declared entry channel."
  def push_inbox(pid, payload), do: GenServer.cast(pid, {:input, nil, payload, %{}})

  # --- server ---

  @impl true
  def init(opts) do
    spec = Keyword.fetch!(opts, :spec)
    operative_id = Keyword.fetch!(opts, :operative_id)
    journal = journal(opts)

    # Cells are minted before anything runs, so a `RestoreInitState` frame
    # arriving right after the `AddOperative` ack always finds a declared one
    # registered; a keyed one the frame names is minted by its lookup.
    cells =
      opts
      |> Keyword.take([:operative_id, :range, :counters, :runner, :secondaries, :credit])
      |> Keyword.merge(topology: spec.topology, kind: :workflow)
      |> Cells.new()
      |> Cells.schedule_reap()

    state = %State{
      instance:
        Instance.new(operative_id, spec,
          journal: journal,
          owed_inbox: Keyword.get(opts, :owed_inbox, 0),
          park_idle_ms: Keyword.get(opts, :park_idle_ms)
        ),
      spec: spec,
      operative_id: operative_id,
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

  # The calls the controller's persistence commands make on an operative,
  # answered exactly as `Flezha.SDK.Operative` answers them, through the same
  # Cell set: a restore, a compaction and a live read reach a workflow
  # instance through the same `Flezha.SDK.Commands` path a stream one does.
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
  # rather than routed: a workflow hands a batch on at the barrier, not per
  # value, so the whole stage's release is what the journal records.
  @impl true
  def handle_cast({:emitted, node_ref, cell_key, emits, seq, _offset, report}, %State{} = s) do
    origin = %{node_ref: node_ref, cell_key: cell_key, seq: seq}
    values = Enum.map(emits, fn {value, _meta, _key} -> value end)

    s =
      emits
      |> Enum.with_index()
      |> Enum.reduce(s, fn {{value, meta, key}, ordinal}, acc ->
        sink_outputs(
          acc,
          node_ref,
          cell_key,
          value,
          %{meta: meta, key: key, origin: origin},
          ordinal
        )
      end)

    s = %{s | collected: Map.update(s.collected, node_ref, values, &(&1 ++ values))}

    Inbox.delivered(report)

    {:noreply, s}
  end

  # A Cell exhausted its ladder: the tasklet failed, so the run unwinds.
  def handle_cast({:stage_failed, node_ref, error}, %State{} = s),
    do: settle(fail(s, node_ref, error))

  # A message off the bound data stream, through the same entrance a stream
  # operative's frame takes: an undeclared name or a parser refusal parks it,
  # consumed without a pass, and a driver waiting for a message waits a fresh
  # idle window. A readable one waits in the inbox for the next message-fed
  # pass; a driver waiting for one starts that pass now.
  def handle_cast({:input, channel, message, metadata}, %State{} = s) do
    case Inbox.enter(s.runner, s.operative_id, s.spec.topology, channel, message) do
      {:ok, channel, value} ->
        s = %{s | instance: Instance.push_inbox(s.instance, channel, value, metadata)}
        if s.idle, do: settle(begin_iteration(%{s | idle: nil})), else: {:noreply, s}

      :parked ->
        s = %{s | instance: Instance.entry_parked(s.instance, metadata)}
        if s.idle, do: {:noreply, await_input(s)}, else: {:noreply, s}
    end
  end

  # A tasklet asked to end the instance. Each tasklet keeps the request of
  # its Cell whose key sorts last, so which Cell answered first never
  # decides the result; the iteration ends with the tasklet latest in the
  # chain's commit order (`completion/1`).
  def handle_cast({:complete, node_ref, cell_key, result}, %State{} = s) do
    task_idx = Map.fetch!(s.spec.dag.pos, node_ref)

    case s.completions do
      %{^task_idx => {kept, _result}} when kept > cell_key ->
        {:noreply, s}

      completions ->
        {:noreply, %{s | completions: Map.put(completions, task_idx, {cell_key, result})}}
    end
  end

  @impl true
  def handle_info({:stage_done, ref, _pid}, %State{} = s) do
    case Map.pop(s.awaiting, ref) do
      {nil, _} -> {:noreply, s}
      {_node_ref, awaiting} -> settle(stage_settled(%{s | awaiting: awaiting}))
    end
  end

  def handle_info({:barrier_ack, ref, _result, error}, %State{barrier: %{ref: ref}} = s),
    do: settle(barrier_settled(s, error))

  def handle_info({:barrier_ack, _stale, _result, _error}, %State{} = s), do: {:noreply, s}

  # A stop and the duration cap are cooperative: the run unwinds at its next
  # step. A loop waiting for input has no next step and nothing to unwind.
  def handle_info(stop, %State{} = s) when stop in [:workflow_stop, :max_duration] do
    s = %{s | cancel: :cancelled}
    if s.idle, do: settle(complete(%{s | idle: nil}, "cancelled", "")), else: {:noreply, s}
  end

  def handle_info({:park_idle, ref}, %State{idle: ref} = s), do: settle(park(%{s | idle: nil}))

  def handle_info({:secondaries, secondaries}, %State{} = s),
    do: {:noreply, %{s | cells: Cells.rebind(s.cells, secondaries)}}

  def handle_info({:DOWN, _ref, :process, pid, reason}, %State{} = s),
    do: {:noreply, %{s | cells: Cells.down(s.cells, pid, reason)}}

  # The sweep runs only while the loop waits for its next message, and keeps
  # every Cell a rollback of the class's mode could still reach.
  def handle_info(:reap, %State{} = s) do
    cells = if s.idle, do: Cells.reap(s.cells, Compensation.held(s)), else: s.cells
    {:noreply, %{s | cells: Cells.schedule_reap(cells)}}
  end

  def handle_info(_msg, %State{} = s), do: {:noreply, s}

  # A plugin-bound output of a workflow tasklet is STAGED, never shipped:
  # nothing reaches a plugin until the controller has journalled the stage's
  # commit and flushed the outbox behind it. That is the whole transactional
  # guarantee, and it is why the driver holds what the stream router sends.
  defp sink_outputs(%State{} = s, node_ref, cell_key, value, emit, ordinal) do
    s.spec.topology.routes
    |> Map.get({:node, node_ref}, [])
    |> Enum.filter(&match?(%{to: {:sink, _}}, &1))
    |> Enum.reduce(s, fn route, acc ->
      stamp_with = if route.keyed, do: {acc.operative_id, acc.spec.topology.node_modules}
      {metadata, bytes} = Routing.sink_frame(route, value, emit, ordinal, stamp_with)

      # A class that journals nothing has no commit to stage into: its output
      # goes straight to the wire, as a stream's does.
      if journals?(acc),
        do: stage_item(acc, node_ref, cell_key, metadata, bytes),
        else: %{acc | cells: Cells.emit_output(acc.cells, metadata, bytes)}
    end)
  end

  defp stage_item(%State{} = s, node_ref, cell_key, metadata, bytes) do
    slot = {node_ref, cell_key}

    item = %{
      cell_id: Cells.wire_identity(s.cells, node_ref, cell_key),
      seq_in_task: get_in(s.outbox, [:seq, slot]) || 0,
      channel: Map.fetch!(metadata, "channel"),
      payload: bytes
    }

    %{
      s
      | outbox: %{
          items: Map.update(s.outbox.items, node_ref, [item], &(&1 ++ [item])),
          seq: Map.update(s.outbox.seq, slot, 1, &(&1 + 1))
        }
    }
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
    s = %{s | resume_owed: 0}

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
        pass_channel: start.channel,
        pending: start.pending,
        waiting: start.waiting,
        completed: start.completed,
        completions: start.completions
    }

    dispatch_frontier(s, start.ready)
  end

  defp fresh_seed(%State{} = s) do
    {journal, instance} = Instance.take_journal(s.instance)
    s = %{s | instance: instance, carry: s.carry || journaled_carry(s, journal)}

    case seed(s) do
      {:message, s, item} ->
        enter_pass(s, item)

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
  # edge. An edge whose transform rejects the message parks it under the
  # channel, consumed without a pass, and the instance goes back to waiting.
  defp enter_pass(%State{} = s, item) do
    case entry_frontier(s, item) do
      {:ok, frontier} ->
        start_pass(s, item.channel, frontier)

      {:rejected, reason} ->
        Inbox.park_pre_dispatch(
          s.runner,
          s.operative_id,
          item.channel,
          item.channel,
          "entry edge failed: " <> reason,
          nil
        )

        await_input(%{s | instance: Instance.note_parked(s.instance, item.offset)})
    end
  end

  defp entry_frontier(%State{} = s, item) do
    {:ok, frontier(s, item.channel, &through_edge(s, {:entry, item.channel}, &1, [item.payload]))}
  rescue
    e -> {:rejected, Exception.message(e)}
  end

  # A pass's first frontier: every root of the chain that has a batch.
  defp frontier(%State{} = s, channel, batch_for) do
    for root <- Map.fetch!(s.spec.dag.chains, channel).roots,
        batch = batch_for.(root),
        batch != [],
        do: {root, batch}
  end

  defp start_pass(%State{} = s, channel, frontier),
    do: dispatch_frontier(%{fresh_iteration(s) | pass_channel: channel}, frontier)

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
  defp seed(%State{} = s) do
    cond do
      message_fed?(s) -> pop_message(s)
      s.spec.knobs.loop_mode == :once -> {:done, s}
      s.carry in [nil, []] -> {:done, s}
      true -> {:carry, %{s | carry: nil, consumed_offset: nil}, s.carry}
    end
  end

  defp pop_message(%State{} = s) do
    case Instance.pop_inbox(s.instance) do
      {:ok, item, instance} ->
        {:message, %{s | instance: instance, consumed_offset: item.offset}, item}

      {:empty, _instance} ->
        :wait
    end
  end

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
        outbox: %{items: %{}, seq: %{}},
        pending: %{},
        waiting: %{},
        completed: [],
        next: [],
        completions: %{}
    }

  # --- one frontier ---

  defp dispatch_frontier(%State{} = s, []), do: seal_iteration(s)

  defp dispatch_frontier(%State{cancel: reason} = s, _frontier) when reason != nil,
    do: unwind(s, "cancelled", "")

  # A frontier whose stages handed nothing to any Cell has no barrier to wait
  # for, so it settles at once.
  defp dispatch_frontier(%State{} = s, frontier) do
    s = %{s | frontier: frontier, next: [], awaiting: %{}}

    s =
      Enum.reduce(frontier, s, fn {node_ref, batch}, acc ->
        dispatch_stage(acc, node_ref, batch)
      end)

    if map_size(s.awaiting) == 0, do: stage_settled(s), else: s
  end

  # The stage ran on exactly the Cells it handed a value to: that is what its
  # commit and its undo name, whether or not they emit.
  defp dispatch_stage(%State{} = s, node_ref, batch) do
    {keys, awaiting, cells} = Stage.dispatch(s.cells, node_ref, batch, self())

    %{
      s
      | cells: cells,
        collected: Map.put_new(s.collected, node_ref, []),
        ran: Map.put(s.ran, node_ref, keys),
        awaiting: Enum.into(awaiting, s.awaiting)
    }
  end

  # Every Cell of this frontier has drained past its barrier: commit the
  # stages in DAG order, then accumulate what each released into its children.
  defp stage_settled(%State{awaiting: awaiting} = s) when map_size(awaiting) > 0, do: s

  defp stage_settled(%State{unwind: %{status: st}} = s) when st != nil,
    do: unwind_step(s, &Compensation.commit/1)

  defp stage_settled(%State{cancel: reason} = s) when reason != nil,
    do: unwind(s, "cancelled", "")

  defp stage_settled(%State{} = s), do: commit_next_stage(s)

  # One `TaskCommit` at a time: the driver starts the next tasklet only on
  # the successful reply, so a crash between them re-runs the uncommitted one.
  defp commit_next_stage(%State{frontier: []} = s), do: dispatch_frontier(s, s.next)

  defp commit_next_stage(%State{frontier: [{node_ref, _batch} | _]} = s) do
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
          operative_id: s.operative_id,
          task_idx: task_idx,
          iter: s.iter,
          cells_ran: ran,
          handoff: handoff(s, node_ref, batch),
          outbox: Map.get(s.outbox.items, node_ref, []),
          compensating: s.unwind.status != nil,
          completes: asked != nil,
          result: result_of(asked)
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

  # The committed stage's release accumulates into each child, which becomes
  # runnable once every producer feeding it in this pass's chain has
  # committed; a producer of another chain is not running and is never
  # waited for.
  defp advance(%State{frontier: [{node_ref, _} | rest]} = s) do
    batch = Map.get(s.collected, node_ref, [])
    children = Map.fetch!(s.spec.dag.children, node_ref)

    s =
      Enum.reduce(children, s, fn child, acc ->
        owed = Map.get(acc.waiting, child, chain_parents(acc, child)) - 1
        mapped = through_edge(acc, node_ref, child, batch)
        pending = Map.update(acc.pending, child, mapped, &(&1 ++ mapped))

        if owed == 0 do
          %{
            acc
            | waiting: Map.delete(acc.waiting, child),
              pending: Map.delete(pending, child),
              next: acc.next ++ [{child, Map.get(pending, child, [])}]
          }
        else
          %{acc | waiting: Map.put(acc.waiting, child, owed), pending: pending}
        end
      end)

    s = if children == [], do: %{s | carry: (s.carry || []) ++ batch}, else: s
    commit_next_stage(%{s | frontier: rest})
  end

  # --- the iteration boundary ---

  defp seal_iteration(%State{spec: %Spec{knobs: %{loop_mode: :once}}} = s),
    do: complete(s, "ok", "", result_of(completion(s)))

  defp seal_iteration(%State{} = s) do
    if journals?(s) do
      ref = make_ref()

      command =
        Commit.iter(s.runner, self(), ref,
          operative_id: s.operative_id,
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

  # The completion the iteration ends with: the tasklet latest in the chain's
  # commit order.
  defp completion(%State{completions: completions}) when completions == %{}, do: nil

  defp completion(%State{completions: completions}),
    do: completions |> Enum.max_by(&elem(&1, 0)) |> elem(1)

  defp result_of(nil), do: <<>>
  defp result_of({_cell_key, result}), do: result

  # --- barriers ---

  defp barrier_settled(%State{barrier: %{kind: kind}} = s, "") do
    s = %{s | barrier: nil}

    case kind do
      :task -> advance(s)
      :iter -> next_iteration(s)
      :comp -> unwind_step(s, &Compensation.commit/1)
    end
  end

  defp barrier_settled(%State{barrier: barrier} = s, error) do
    attempts = Knob.barrier_resend_attempts()

    if barrier.attempt < attempts do
      Logger.warning(
        "[Flezha.SDK] #{s.operative_id} barrier attempt #{barrier.attempt}/#{attempts} " <>
          "refused: #{error}; re-sending"
      )

      # The same bytes, not a rebuild: the controller journals a barrier once
      # by its identity, so a transient stall delays the chain rather than
      # committing it twice or ending it.
      Commit.resend(s.runner, self(), barrier.ref, barrier.command)
      %{s | barrier: %{barrier | attempt: barrier.attempt + 1}}
    else
      Logger.warning("[Flezha.SDK] #{s.operative_id} barrier gave up: #{error}")
      give_up(%{s | barrier: nil}, error)
    end
  end

  # A barrier that gave up during the unwind ends the run on the status the
  # unwind was already heading for; one that gave up going forward starts it.
  defp give_up(%State{unwind: %{status: {status, _}}} = s, _error), do: complete(s, status, "")
  defp give_up(%State{unwind: %{status: :rewind}} = s, error), do: complete(s, "error", error)
  defp give_up(%State{} = s, error), do: unwind(s, "error", error)

  # --- terminal paths ---

  defp fail(%State{unwind: %{status: st}} = s, node_ref, error) when st != nil do
    Logger.warning(
      "[Flezha.SDK] #{s.operative_id} compensate hook on #{Spec.tasklet_name(s.spec, node_ref)} " <>
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
    s = %{
      s
      | frontier: [],
        next: [],
        awaiting: %{},
        barrier: nil,
        unwind: %{s.unwind | status: {status, error}}
    }

    case Compensation.begin(s) do
      :nothing_to_undo -> complete(s, status, error)
      outcome -> unwind_step(s, fn _ -> outcome end)
    end
  end

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
  # a `WorkflowCompleted`, and the controller keeps it suspended until an
  # inbox append or the wake deadline rehydrates it. The park record names the
  # last COMMITTED iteration, so a resume enters the one after it either way,
  # and the last message the run parked unread, which the inbox releases.
  defp park(%State{} = s) do
    wake = s.spec.knobs.wake_after_ms || 0
    Commit.park(s.runner, s.operative_id, max(s.iter - 1, 0), wake, s.instance.parked_offset)
    %{s | terminal: :parked}
  end

  # Only a run that reached its end with a completion carries a result: one that
  # fails or is stopped discards what its iteration asked for.
  defp complete(s, status, error, result \\ <<>>)

  defp complete(%State{terminal: nil} = s, status, error, result) do
    Commit.completed(s.runner, s.operative_id, status, error, result)
    %{s | terminal: {status, error}}
  end

  defp complete(%State{} = s, _status, _error, _result), do: s

  # One exit for every flow function: a run that shipped its terminal frame
  # ends the process, and nothing else does.
  defp settle(%State{terminal: nil} = s), do: {:noreply, s}
  defp settle(%State{} = s), do: {:stop, :normal, s}

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
  defp chain_parents(%State{spec: spec, pass_channel: channel}, child),
    do:
      spec.dag.chains |> Map.fetch!(channel) |> Map.fetch!(:parents_in_chain) |> Map.get(child, 0)

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
