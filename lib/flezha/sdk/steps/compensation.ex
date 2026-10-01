defmodule Flezha.SDK.Steps.Compensation do
  @moduledoc """
  The saga unwind: when a run ends abnormally after partial progress, every
  Cell whose tasklet completed forward runs its `compensate/3` hook, and
  they run in **reverse completion order** — the stage that finished last is
  undone first, because a later stage's undo may depend on the state an
  earlier one still holds.

  One stage at a time. Its Cells undo together, but the next stage does not
  start until the whole of this one has drained past its barrier: the order is
  the guarantee, and running two stages at once would not keep it.

  `on_failure` decides the scope. `:undo_all` undoes every committed
  iteration as well as the in-flight one; `:keep_progress` keeps each finished
  iteration and undoes only the in-flight one. The mode is irrelevant to a
  single-pass chain, where both mean the same thing, and to a class with no
  hooks, where the plan is empty and the run ends at once.

  A hook is best-effort: a failure is logged and the plan moves on rather than
  stranding the rest of the undo. That is why a stage's Cell is asked to
  compensate through the ordinary dispatch — it climbs no ladder that could
  park, and the driver ignores a stage failure while it is already unwinding.
  """

  require Logger

  alias Flezha.SDK.{Cell, Record, Tasklet, Wire}
  alias Flezha.SDK.Operator.Cells
  alias Flezha.SDK.Steps.{Commit, Instance}

  @doc """
  Begin the unwind of `state`, or answer `:nothing_to_undo` when the class
  declares no hook on anything that ran.
  """
  @spec begin(struct()) :: {:unwinding, struct()} | {:done, struct()} | :nothing_to_undo
  def begin(state) do
    case plan(state) do
      [] -> :nothing_to_undo
      plan -> start(state, plan)
    end
  end

  @doc """
  Begin a rewind's unwind of `stages`, the committed tasklets above its
  target: latest first, each hook run once per Cell that ran and each stage
  journaled as undone, a hookless one included, so the controller's pending
  rewind shrinks to nothing. A rewind leaves no done marker: the journal's
  `compensated` entries are what a crash mid-rewind resumes from.
  """
  @spec rewind(struct(), [map()]) ::
          {:unwinding, struct()} | {:done, struct()} | {:awaiting, struct()}
  def rewind(state, stages), do: start(state, stages |> Enum.reverse() |> Enum.map(&List.wrap/1))

  defp start(state, plan), do: advance(%{state | unwind: %{state.unwind | plan: plan}})

  @doc """
  Start the next group, or answer `:done` when the plan is exhausted. A group
  whose every Cell already compensated before the crash dispatches nothing —
  and is stepped over rather than waited on, because no barrier would ever
  arrive for it.
  """
  @spec advance(struct()) :: {:unwinding, struct()} | {:done, struct()} | {:awaiting, struct()}
  def advance(%{unwind: %{plan: []}} = state), do: {:done, state}

  def advance(%{unwind: %{plan: [group | rest]}} = state) do
    state = %{state | unwind: %{state.unwind | plan: rest, current: []}}

    case dispatch_group(state, group) do
      %{unwind: %{current: []}} = state -> advance(state)
      # A rewound hookless stage runs nothing, so no barrier drains it.
      %{awaiting: awaiting} = state when map_size(awaiting) == 0 -> commit(state)
      state -> {:unwinding, state}
    end
  end

  @doc """
  The group in flight has drained: journal each stage's undo the way its
  forward run was journaled (a `TaskCommit` marked `compensating`, naming the
  records the hooks wrote), then move on. The done marker each one records is
  what makes a crash mid-rollback resume rather than repeat the undo.
  """
  @spec commit(struct()) :: {:unwinding, struct()} | {:done, struct()} | {:awaiting, struct()}
  def commit(%{unwind: %{current: []}} = state), do: advance(state)

  def commit(%{unwind: %{current: [stage | rest]}} = state) do
    if state.unwind.status != :rewind, do: mark(state, stage.node_ref, stage.cells, :done)
    state = %{state | unwind: %{state.unwind | current: rest}}

    if journals?(state) do
      ref = make_ref()

      command =
        Commit.task(state.runner, self(), ref,
          operator_id: Flezha.SDK.Scope.wire(state.operator_id, state.instance.scope),
          task_idx: stage.task_idx,
          iter: Map.get(stage, :iter, state.iter),
          cells_ran: Commit.cells_ran(state.written, stage.node_ref, stage.cells),
          handoff: [],
          compensating: true
        )

      {:awaiting, %{state | barrier: %{ref: ref, kind: :comp, attempt: 1, command: command}}}
    else
      commit(state)
    end
  end

  @doc """
  Record that `identities` completed forward and may need undo. Emitted before
  the stage's own commit, so a crash between marker and ack resolves as "not
  committed → re-run" rather than as an undo nobody knows about.
  """
  @spec mark_intent(struct(), reference(), [binary()]) :: :ok
  def mark_intent(state, node_ref, identities), do: mark(state, node_ref, identities, :intent)

  # The saga markers, on the same wire a Cell's records leave on. A class
  # that journals nothing has nowhere to persist them, and a tasklet with no
  # hook has nothing they could mean.
  defp mark(state, node_ref, identities, kind) do
    if journals?(state) and compensates?(state, %{node_ref: node_ref}) do
      payload_id =
        case kind do
          :intent -> Wire.payload_id_compensation_intent()
          :done -> Wire.payload_id_compensation_done()
        end

      Enum.each(identities, fn identity ->
        Commit.record(state.runner, payload_id, Record.compensation_frame(identity))
      end)
    end

    :ok
  end

  @doc """
  The Cells, by identity, a rollback of the class's mode could still reach
  between passes: every compensable Cell of a committed iteration under
  `:undo_all`, none under `:keep_progress`. The idle sweep keeps them.
  """
  @spec held(struct()) :: MapSet.t(binary())
  def held(state) do
    for group <- plan(%{state | completed: []}),
        stage <- group,
        identity <- stage.cells,
        into: MapSet.new(),
        do: identity
  end

  defp journals?(state), do: state.spec.settings.durable != :memory

  # The stages to undo, latest first: the in-flight iteration's completed
  # stages, then — under `:undo_all` — every committed iteration's, also
  # latest first. A stage whose tasklet declares no hook is left out, so a
  # class with no saga has an empty plan.
  defp plan(state) do
    iterations =
      case state.spec.settings.on_failure do
        :keep_progress -> [state.completed]
        :undo_all -> [state.completed | Enum.reverse(state.committed)]
      end

    iterations
    |> Enum.flat_map(&Enum.reverse/1)
    |> Enum.filter(&compensates?(state, &1))
    |> Enum.map(&List.wrap/1)
  end

  defp compensates?(state, %{node_ref: node_ref}),
    do: state.spec.topology.node_modules |> Map.fetch!(node_ref) |> Tasklet.compensates?()

  # One stage's Cells undo together, each followed by its barrier, so the
  # group is finished exactly when every one of them has drained. A Cell
  # whose undo already ran before the crash is skipped: the marker the restore
  # carried is what makes a resumed rollback finish rather than repeat.
  defp dispatch_group(state, group),
    do: Enum.reduce(group, state, &dispatch_stage(&2, &1))

  defp dispatch_stage(state, stage) do
    if compensates?(state, stage) do
      case outstanding(state, stage) do
        {[], state} -> state
        {pids, state} -> Enum.reduce(pids, arm(state, stage), &undo(&2, stage, &1))
      end
    else
      arm(state, stage)
    end
  end

  # The stage's forward release and its forward records are cleared first: the
  # compensating commit that follows names the UNDO's records, never the
  # forward run's again.
  defp arm(state, stage) do
    %{
      state
      | collected: Map.put(state.collected, stage.node_ref, []),
        written: Map.delete(state.written, stage.node_ref),
        unwind: %{state.unwind | current: state.unwind.current ++ [stage]}
    }
  end

  # The Cells `stage` recorded whose undo has not already run, each looked up
  # by its key as a restore looks it up, a keyed one minted. A marker the
  # restore carried is what makes a resumed rollback finish rather than repeat.
  defp outstanding(state, stage) do
    stage.cells
    |> Enum.reject(&Instance.compensation_done?(state.instance, &1))
    |> Enum.reduce({[], state}, fn identity, {pids, state} ->
      {:ok, {_operator, tasklet, key}} = Wire.decode_cell_id(identity)

      case Cells.lookup(state.cells, tasklet, key, true) do
        {{:ok, pid}, cells} -> {pids ++ [pid], %{state | cells: cells}}
        {{:error, _absent}, cells} -> {pids, %{state | cells: cells}}
      end
    end)
  end

  defp undo(state, stage, pid) do
    # A crash-resumed unwind reconstructs completed stages with no outputs —
    # a tasklet's collected batch was never durable — so a hook that records
    # an undo has to do so unconditionally, which is the contract the
    # `Tasklet` docs state.
    outputs = Map.get(state.collected, stage.node_ref, [])
    Cell.compensate(pid, outputs)
    ref = make_ref()
    Cell.barrier(pid, ref, self())
    Logger.debug("[Flezha.SDK] #{state.operator_id} compensating task #{stage.task_idx}")
    %{state | awaiting: Map.put(state.awaiting, ref, {stage.node_ref, pid})}
  end
end
