defmodule Flezha.SDK.Workflow.Driver do
  @moduledoc """
  One process per workflow instance: the stage driver. It walks the class's
  DAG frontier by frontier — a procedure becomes runnable once every producer
  feeding it has committed, so a fan-in is dispatched exactly once with the
  accumulated batch of all its producers — commits each stage durably, and
  ends by shipping `WorkflowCompleted`.

  It presents the same contract to an `Flezha.SDK.Entity` that
  `Flezha.SDK.Operative` does: an Entity casts its emits to its `operative`
  pid and knows nothing else about it. So every Entity mechanism — the state,
  the records, the retry ladder, the timers, the tier — is reused as it
  stands, and what is written here is only what a *bounded* run adds: the
  frontier, the barrier and the journal.

  The reference drives a chain on a pool thread that blocks on each barrier.
  This one is event-driven, because a blocked BEAM process cannot answer the
  stop, the inbox push or the restore that must reach it while a stage is in
  flight. Every wait is therefore a message: `{:stage_done, ref, pid}` from an
  Entity's queue barrier, and `{:barrier_ack, ref, result, error}` from the runner.
  """

  use GenServer

  require Logger

  alias Flezha.SDK.{Channels, Entity, Topology}
  alias Flezha.SDK.Operative.{Inbox, Routing}
  alias Flezha.SDK.Workflow.{Commit, Compensation, Instance, Resume, Spec, Stage}
  alias Flezha.SDK.Workflow.Ctx, as: WorkflowCtx

  defmodule State do
    @moduledoc false
    defstruct [
      :instance,
      :spec,
      :operative_id,
      :args,
      :runner,
      :counters,
      :credit,
      :secondaries,
      :slots,
      # node_ref => [entity_pid], one per partition in `entities_for` order
      entities: %{},
      # the stage barrier refs this frontier still owes
      awaiting: %{},
      # node_ref => what its entities released this iteration
      collected: %{},
      # node_ref => the entity identities that ran
      ran: %{},
      # node_ref => plugin output staged for this stage's commit, and the
      # per-{stage, entity} ordinal each item takes.
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
      consumed_offset: 0,
      iter: 0,
      cancel: nil,
      # the process running a run-sugar body, and what it completes with
      sugar: nil,
      result: <<>>,
      resume_owed: 0,
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

  @doc "Append one post-start input for a `:per_input` loop."
  def push_inbox(pid, payload), do: GenServer.cast(pid, {:input, nil, payload, %{}})

  # --- server ---

  @impl true
  def init(opts) do
    spec = Keyword.fetch!(opts, :spec)
    operative_id = Keyword.fetch!(opts, :operative_id)

    state = %State{
      instance:
        Instance.new(operative_id, spec,
          journal: journal(opts),
          owed_inbox: Keyword.get(opts, :owed_inbox, 0)
        ),
      spec: spec,
      operative_id: operative_id,
      args: Keyword.get(opts, :args, ""),
      runner: Keyword.fetch!(opts, :runner),
      counters: Keyword.fetch!(opts, :counters),
      credit: Keyword.get(opts, :credit),
      secondaries: Keyword.get(opts, :secondaries, []),
      slots: Keyword.get(opts, :slots),
      resume_owed: Keyword.get(opts, :resume_entity_count, 0)
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

  # Entities are built before anything runs, so a `RestoreInitState` frame
  # arriving right after the `AddOperative` ack always finds its Entity
  # registered rather than racing the first stage into existence.
  @impl true
  def handle_continue(:bootstrap, %State{spec: %Spec{topology: nil}} = s),
    do: settle(run_sugar(s))

  def handle_continue(:bootstrap, %State{} = s) do
    s = build_stage_entities(s)
    if s.resume_owed > 0, do: settle(s), else: settle(resume_bound(s))
  end

  # The two calls the controller's persistence commands make on an operative,
  # answered exactly as `Flezha.SDK.Operative` answers them: a restore, a
  # compaction and a live read reach a workflow instance through the same
  # `Flezha.SDK.Commands` path a stream one does, and they name a procedure
  # rather than a node. A stage's Entities are built before anything runs, so
  # nothing here materialises — a lookup either finds one or names why not.
  @impl true
  def handle_call(
        {:lookup, _procedure, _key, _materialise?},
        _from,
        %State{spec: %Spec{topology: nil}} = s
      ),
      do: {:reply, {:error, :procedure_not_found}, s}

  def handle_call({:lookup, procedure, key, _materialise?}, _from, %State{} = s) do
    case Topology.node_of(s.spec.topology, procedure) do
      {:ok, node_ref} ->
        case Enum.at(Map.get(s.entities, node_ref, []), Stage.index_of(s.spec, node_ref, key)) do
          nil -> {:reply, {:error, :entity_not_found}, s}
          pid -> {:reply, {:ok, pid}, s}
        end

      {:error, _} = error ->
        {:reply, error, s}
    end
  end

  # The saga markers one restore frame carried. An unwind the crash
  # interrupted resumes from these: an Entity whose undo already ran is
  # skipped, and the rest of the plan still runs.
  def handle_call({:compensation_markers, identity, intent, done}, _from, %State{} = s),
    do:
      {:reply, :ok,
       %{s | instance: Instance.note_compensation(s.instance, identity, intent, done)}}

  def handle_call(:entities, _from, %State{spec: %Spec{topology: nil}} = s), do: {:reply, [], s}

  def handle_call(:entities, _from, %State{spec: spec} = s) do
    listing =
      Enum.map(spec.dag.order, fn node_ref ->
        keys = Spec.stage_keys(spec, node_ref)
        pids = s.entities |> Map.get(node_ref, []) |> then(&Enum.zip(keys, &1))
        {spec.topology.node_modules[node_ref].name(), spec.topology.node_mode[node_ref], pids}
      end)

    {:reply, listing, s}
  end

  # An Entity finished one invocation. Its outputs are collected for the stage
  # rather than routed: a workflow hands a batch on at the barrier, not per
  # value, so the whole stage's release is what the journal records.
  @impl true
  def handle_cast({:emitted, node_ref, entity_key, emits, seq, _offset, report}, %State{} = s) do
    origin = %{node_ref: node_ref, entity_key: entity_key, seq: seq}
    values = Enum.map(emits, fn {value, _meta, _key} -> value end)

    s =
      emits
      |> Enum.with_index()
      |> Enum.reduce(s, fn {{value, meta, key}, ordinal}, acc ->
        stage_outbox(
          acc,
          node_ref,
          entity_key,
          value,
          %{meta: meta, key: key, origin: origin},
          ordinal
        )
      end)

    s = %{
      s
      | collected: Map.update(s.collected, node_ref, values, &(&1 ++ values)),
        ran: Map.update(s.ran, node_ref, [entity_key], &[entity_key | &1])
    }

    Inbox.delivered(report)

    {:noreply, s}
  end

  # An Entity exhausted its ladder: the procedure failed, so the run unwinds.
  def handle_cast({:stage_failed, node_ref, error}, %State{} = s),
    do: settle(fail(s, node_ref, error))

  # A post-start input off the bound data stream. A `:per_input` loop consumes
  # it as its next iteration; any other loop shape has no inbox to put it in,
  # so it is counted and dropped rather than silently seeding a second run.
  def handle_cast({:input, _channel, message, _metadata}, %State{} = s) do
    if s.spec.knobs.loop_mode == :per_input do
      s = %{s | instance: Instance.push_inbox(s.instance, message, 0)}

      if s.frontier == [] and s.barrier == nil and s.unwind.status == nil,
        do: settle(begin_iteration(s)),
        else: {:noreply, s}
    else
      Logger.warning(
        "[Flezha.SDK] #{s.operative_id} discarded an input: a #{s.spec.knobs.loop_mode} " <>
          "workflow consumes no inbox"
      )

      {:noreply, s}
    end
  end

  # A run-sugar body's own output: the run has no stages to stage it at, so it
  # leaves on the runner's secondary stream as it is produced.
  def handle_cast({:sugar_emit, channel, value}, %State{} = s) do
    bytes = if is_binary(value), do: value, else: Channels.default_serialize(value)
    Commit.output(s.runner, s.operative_id, %{"channel" => channel}, bytes)
    {:noreply, s}
  end

  def handle_cast({:sugar_result, result}, %State{} = s), do: {:noreply, %{s | result: result}}

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

  def handle_info({:sugar_done, outcome}, %State{} = s), do: settle(sugar_finished(s, outcome))

  # The body's process died without answering — a raise inside `safely` is
  # already an outcome, so this is an exit or a kill, and it ends the run.
  def handle_info({:DOWN, _ref, :process, pid, reason}, %State{sugar: pid} = s)
      when reason != :normal,
      do: settle(complete(s, "error", format(reason)))

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, %State{} = s), do: {:noreply, s}

  def handle_info(:workflow_stop, %State{} = s), do: {:noreply, %{s | cancel: :cancelled}}

  def handle_info(:max_duration, %State{} = s), do: {:noreply, %{s | cancel: :cancelled}}

  def handle_info({:restored, _entity}, %State{resume_owed: owed} = s) when owed > 1,
    do: {:noreply, %{s | resume_owed: owed - 1}}

  def handle_info({:restored, _entity}, %State{} = s), do: settle(resume_bound(s))

  def handle_info({:secondaries, secondaries}, %State{} = s) do
    Enum.each(all_entities(s), &Entity.rebind(&1, secondaries))
    {:noreply, %{s | secondaries: secondaries}}
  end

  def handle_info(_msg, %State{} = s), do: {:noreply, s}

  # A plugin-bound output of a workflow procedure is STAGED, never shipped:
  # nothing reaches a plugin until the controller has journalled the stage's
  # commit and flushed the outbox behind it. That is the whole transactional
  # guarantee, and it is why the driver holds what the stream router sends.
  defp stage_outbox(%State{} = s, node_ref, entity_key, value, emit, ordinal) do
    s.spec.topology.routes
    |> Map.get({:node, node_ref}, [])
    |> Enum.filter(&match?(%{to: {:sink, _}}, &1))
    |> Enum.reduce(s, fn route, acc ->
      stamp_with = if route.keyed, do: {acc.operative_id, acc.spec.topology.node_modules}
      {metadata, bytes} = Routing.sink_frame(route, value, emit, ordinal, stamp_with)

      slot = {node_ref, entity_key}

      item = %{
        entity_id: identity(acc, node_ref, entity_key),
        seq_in_proc: get_in(acc.outbox, [:seq, slot]) || 0,
        channel: Map.get(metadata, "channel", route.channel),
        payload: bytes
      }

      %{
        acc
        | outbox: %{
            items: Map.update(acc.outbox.items, node_ref, [item], &(&1 ++ [item])),
            seq: Map.update(acc.outbox.seq, slot, 1, &(&1 + 1))
          }
      }
    end)
  end

  # --- the run-sugar body ---

  # The body runs in its own process, not here. It is author code of unknown
  # duration, and a driver executing it could answer neither the stop nor the
  # duration cap while it ran — and its own `emit` reaches this process as a
  # message, which a driver blocked inside the call could never receive.
  defp run_sugar(%State{spec: spec} = s) do
    ctx = %WorkflowCtx{driver: self(), operative_id: s.operative_id}
    driver = self()

    {pid, _ref} =
      spawn_monitor(fn ->
        send(driver, {:sugar_done, safely(fn -> spec.module.run(ctx, s.args) end)})
      end)

    %{s | sugar: pid}
  end

  defp sugar_finished(%State{} = s, :ok), do: complete(s, "ok", "")
  defp sugar_finished(%State{} = s, {:error, reason}), do: complete(s, "error", format(reason))
  defp sugar_finished(%State{} = s, other), do: complete(s, "error", format(other))

  # Every restore the bind owed has landed. A compensation-DONE marker is the
  # only proof an undo was in flight when the crash landed — intent markers
  # accumulate during ordinary forward progress too — so it is what decides
  # between finishing the unwind and running forward.
  defp resume_bound(%State{} = s) do
    s = %{s | resume_owed: 0}

    if Instance.any_compensation_done?(s.instance),
      do: unwind(s, "error", "resumed mid-rollback"),
      else: begin_iteration(s)
  end

  # --- the iteration loop ---

  defp begin_iteration(%State{cancel: reason} = s) when reason != nil,
    do: complete(s, "cancelled", "")

  defp begin_iteration(%State{} = s) do
    case Resume.start(s, &through_edge(s, &1, &2, &3)) do
      nil -> fresh_seed(s)
      start -> resume_iteration(s, start)
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
        pending: start.pending,
        waiting: start.waiting,
        completed: start.completed
    }

    dispatch_frontier(s, start.ready)
  end

  defp fresh_seed(%State{} = s) do
    {_journal, instance} = Instance.take_journal(s.instance)
    s = %{s | instance: instance}

    case seed(s) do
      {:seeded, s, batch} -> dispatch_frontier(fresh_iteration(s), [{s.spec.dag.root, batch}])
      {:done, s} -> complete(s, "ok", "")
      {:park, s} -> park(s)
    end
  end

  # Where iteration `iter` starts: the spawn args on the first pass, the
  # previous terminal batch for a handoff-chained loop, the next inbox item
  # for a per-input one.
  defp seed(%State{iter: 0, carry: nil} = s), do: {:seeded, s, [s.args]}

  defp seed(%State{spec: %Spec{knobs: %{loop_mode: :once}}} = s), do: {:done, s}

  # A chained loop ends on an empty terminal batch: there is nothing to seed
  # the next pass with.
  defp seed(%State{spec: %Spec{knobs: %{loop_mode: :chained}}, carry: carry} = s)
       when carry in [nil, []],
       do: {:done, s}

  defp seed(%State{spec: %Spec{knobs: %{loop_mode: :chained}}, carry: batch} = s),
    do: {:seeded, %{s | carry: nil}, batch}

  defp seed(%State{spec: %Spec{knobs: %{loop_mode: :per_input}}} = s) do
    case Instance.pop_inbox(s.instance) do
      {:ok, item, instance} ->
        {:seeded, %{s | instance: instance, consumed_offset: item.offset}, [item.payload]}

      {:empty, instance} ->
        if Instance.may_park?(instance), do: {:park, %{s | instance: instance}}, else: {:done, s}
    end
  end

  defp fresh_iteration(%State{} = s),
    do: %{
      s
      | collected: %{},
        ran: %{},
        outbox: %{items: %{}, seq: %{}},
        pending: %{},
        waiting: %{},
        completed: [],
        next: []
    }

  # --- one frontier ---

  defp dispatch_frontier(%State{} = s, []), do: seal_iteration(s)

  defp dispatch_frontier(%State{cancel: reason} = s, _frontier) when reason != nil,
    do: unwind(s, "cancelled", "")

  defp dispatch_frontier(%State{} = s, frontier) do
    s = %{s | frontier: frontier, next: [], awaiting: %{}}

    Enum.reduce(frontier, s, fn {node_ref, batch}, acc -> dispatch_stage(acc, node_ref, batch) end)
  end

  defp dispatch_stage(%State{} = s, node_ref, batch) do
    pids = Map.fetch!(s.entities, node_ref)
    awaiting = Stage.dispatch(s.spec, node_ref, pids, batch, self())

    %{
      s
      | collected: Map.put_new(s.collected, node_ref, []),
        awaiting: Enum.into(awaiting, s.awaiting)
    }
  end

  # Every Entity of this frontier has drained past its barrier: commit the
  # stages in DAG order, then accumulate what each released into its children.
  defp stage_settled(%State{awaiting: awaiting} = s) when map_size(awaiting) > 0, do: s

  defp stage_settled(%State{unwind: %{status: st}} = s) when st != nil,
    do: unwind_step(s, &Compensation.commit/1)

  defp stage_settled(%State{cancel: reason} = s) when reason != nil,
    do: unwind(s, "cancelled", "")

  defp stage_settled(%State{} = s), do: commit_next_stage(s)

  # One `ProcCommit` at a time: the driver starts the next procedure only on
  # the successful reply, so a crash between them re-runs the uncommitted one.
  defp commit_next_stage(%State{frontier: []} = s), do: dispatch_frontier(s, s.next)

  defp commit_next_stage(%State{frontier: [{node_ref, _batch} | _]} = s) do
    proc_idx = Map.fetch!(s.spec.dag.pos, node_ref)
    batch = Map.get(s.collected, node_ref, [])
    ran = s.ran |> Map.get(node_ref, []) |> Enum.reverse() |> Enum.map(&identity(s, node_ref, &1))

    s = record_completed(s, node_ref, proc_idx, ran)
    Compensation.mark_intent(s, node_ref, ran)

    if journals?(s) do
      ref = make_ref()

      command =
        Commit.proc(s.runner, self(), ref,
          operative_id: s.operative_id,
          proc_idx: proc_idx,
          iter: s.iter,
          entities_ran: ran,
          handoff: handoff(s, node_ref, batch),
          outbox: Map.get(s.outbox.items, node_ref, []),
          compensating: s.unwind.status != nil
        )

      arm(s, ref, :proc, command)
    else
      advance(s)
    end
  end

  # A barrier in flight: its reference, what it is committing, the rung of the
  # re-send ladder it is on, and the bytes a re-send repeats.
  defp arm(%State{} = s, ref, kind, command),
    do: %{s | barrier: %{ref: ref, kind: kind, attempt: 1, command: command}}

  defp record_completed(%State{} = s, node_ref, proc_idx, ran),
    do: %{
      s
      | completed: s.completed ++ [%{node_ref: node_ref, proc_idx: proc_idx, entities: ran}]
    }

  # The committed stage's release accumulates into each child, which becomes
  # runnable once every producer feeding it has committed.
  defp advance(%State{frontier: [{node_ref, _} | rest]} = s) do
    batch = Map.get(s.collected, node_ref, [])
    children = Map.fetch!(s.spec.dag.children, node_ref)

    s =
      Enum.reduce(children, s, fn child, acc ->
        owed = Map.get(acc.waiting, child, length(Map.fetch!(acc.spec.dag.parents, child))) - 1
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
    do: complete(s, "ok", "")

  defp seal_iteration(%State{} = s) do
    if journals?(s) do
      ref = make_ref()

      command =
        Commit.iter(s.runner, self(), ref,
          operative_id: s.operative_id,
          iter: s.iter,
          consumed_offset: s.consumed_offset,
          handoff: terminal_handoff(s)
        )

      arm(s, ref, :iter, command)
    else
      next_iteration(s)
    end
  end

  defp next_iteration(%State{} = s),
    do: begin_iteration(%{s | committed: s.committed ++ [s.completed], iter: s.iter + 1})

  # --- barriers ---

  defp barrier_settled(%State{barrier: %{kind: kind}} = s, "") do
    s = %{s | barrier: nil}

    case kind do
      :proc -> advance(s)
      :iter -> next_iteration(s)
      :comp -> unwind_step(s, &Compensation.commit/1)
    end
  end

  defp barrier_settled(%State{barrier: barrier} = s, error) do
    attempts = s.spec.knobs.barrier_retry_attempts

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
  defp give_up(%State{} = s, error), do: unwind(s, "error", error)

  # --- terminal paths ---

  defp fail(%State{unwind: %{status: st}} = s, node_ref, error) when st != nil do
    Logger.warning(
      "[Flezha.SDK] #{s.operative_id} compensate hook on #{Spec.procedure_name(s.spec, node_ref)} " <>
        "failed: #{error}; the rest of the plan still runs"
    )

    s
  end

  defp fail(%State{} = s, node_ref, error),
    do: unwind(s, "error", "#{Spec.procedure_name(s.spec, node_ref)}: #{error}")

  # The saga unwind is a phase, not a call: each stage's undo drains through
  # the same barrier a forward stage does, and the terminal frame is shipped
  # only once the whole plan has run. With no hook declared on anything that
  # ran there is nothing to undo and the run ends at once.
  defp unwind(%State{unwind: %{status: nil}, completed: []} = s, status, error) do
    # A resumed unwind has run no stage of its own, so what it undoes comes
    # from the journal rather than from this bind's progress.
    completed =
      case Resume.start(s, &through_edge(s, &1, &2, &3)) do
        nil -> []
        start -> start.completed
      end

    do_unwind(%{s | completed: completed}, status, error)
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
  # last COMMITTED iteration, so a resume enters the one after it either way.
  defp park(%State{} = s) do
    wake = s.spec.knobs.wake_after_ms || 0
    Commit.park(s.runner, s.operative_id, max(s.iter - 1, 0), wake)
    %{s | terminal: :parked}
  end

  defp complete(%State{terminal: nil} = s, status, error) do
    Commit.completed(s.runner, s.operative_id, status, error, s.result)
    %{s | terminal: {status, error}}
  end

  defp complete(%State{} = s, _status, _error), do: s

  # One exit for every flow function: a run that shipped its terminal frame
  # ends the process, and nothing else does.
  defp settle(%State{terminal: nil} = s), do: {:noreply, s}
  defp settle(%State{} = s), do: {:stop, :normal, s}

  # --- entities ---

  defp build_stage_entities(%State{} = s) do
    entities =
      Stage.build_all(s.spec,
        operative: self(),
        operative_id: s.operative_id,
        counters: s.counters,
        secondaries: s.secondaries,
        slots: s.slots,
        credit: s.credit
      )

    %{s | entities: entities}
  end

  defp all_entities(%State{entities: entities}),
    do: entities |> Map.values() |> List.flatten()

  defp identity(%State{} = s, node_ref, key),
    do: Spec.entity_identity(s.spec, s.operative_id, node_ref, key)

  # --- projections ---

  defp journals?(%State{spec: spec}), do: spec.knobs.persistence_enabled

  # A stage's release through its output channel, as the journal records it.
  # The channel a procedure releases on is its node's — one per node — so the
  # id is the stage's own position, which is what re-seeds its children.
  defp handoff(%State{} = s, node_ref, batch) do
    id = Map.fetch!(s.spec.dag.pos, node_ref)
    Enum.map(batch, fn value -> {id, encode(value)} end)
  end

  defp terminal_handoff(%State{carry: nil}), do: []

  defp terminal_handoff(%State{carry: carry} = s) do
    id = s.spec.dag.leaves |> List.first() |> then(&Map.get(s.spec.dag.pos, &1, 0))
    Enum.map(carry, fn value -> {id, encode(value)} end)
  end

  defp encode(value), do: Resume.encode(value)

  # An edge's fused transform, applied once as the batch crosses it — the same
  # application a resumed re-seed makes, so a resumed chain sees what the live
  # one did.
  defp through_edge(%State{} = s, from, to, batch) do
    case edge_route(s, from, to) do
      nil -> batch
      %{transform: transform} -> Enum.flat_map(batch, transform)
    end
  end

  defp edge_route(%State{} = s, from, to) do
    s.spec.topology.routes
    |> Map.get({:node, from}, [])
    |> Enum.find(&match?(%{to: {:node, ^to}}, &1))
  end

  defp journal(opts) do
    case Keyword.get(opts, :journal) do
      nil -> nil
      facts -> struct(Instance.Resume, facts)
    end
  end

  defp safely(fun) do
    fun.()
  rescue
    e -> {:error, Exception.message(e)}
  catch
    :throw, reason -> {:error, format(reason)}
  end

  defp format(reason) when is_binary(reason), do: reason
  defp format(reason), do: inspect(reason)
end
