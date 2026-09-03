defmodule Flezha.SDK.Workflow.Compensation do
  @moduledoc """
  The saga unwind: when a run ends abnormally after partial progress, every
  Entity whose procedure completed forward runs its `compensate/3` hook, and
  they run in **reverse completion order** — the stage that finished last is
  undone first, because a later stage's undo may depend on the state an
  earlier one still holds.

  One stage at a time. Its Entities undo together, but the next stage does not
  start until the whole of this one has drained past its barrier: the order is
  the guarantee, and running two stages at once would not keep it.

  `rollback_mode` decides the scope. `:all_or_nothing` undoes every committed
  iteration as well as the in-flight one; `:keep_progress` keeps each finished
  iteration and undoes only the in-flight one. The mode is irrelevant to a
  single-pass chain, where both mean the same thing, and to a class with no
  hooks, where the plan is empty and the run ends at once.

  A hook is best-effort: a failure is logged and the plan moves on rather than
  stranding the rest of the undo. That is why a stage's Entity is asked to
  compensate through the ordinary dispatch — it climbs no ladder that could
  park, and the driver ignores a stage failure while it is already unwinding.
  """

  require Logger

  alias Flezha.SDK.{Entity, Procedure, Record}
  alias Flezha.SDK.Workflow.{Commit, Instance, Spec}
  alias Flezha.State.Wire

  @doc """
  Begin the unwind of `state`, or answer `:nothing_to_undo` when the class
  declares no hook on anything that ran.
  """
  @spec begin(struct()) :: {:unwinding, struct()} | {:done, struct()} | :nothing_to_undo
  def begin(state) do
    case plan(state) do
      [] -> :nothing_to_undo
      plan -> advance(%{state | unwind: %{state.unwind | plan: plan}})
    end
  end

  @doc """
  Start the next group, or answer `:done` when the plan is exhausted. A group
  whose every Entity already compensated before the crash dispatches nothing —
  and is stepped over rather than waited on, because no barrier would ever
  arrive for it.
  """
  @spec advance(struct()) :: {:unwinding, struct()} | {:done, struct()}
  def advance(%{unwind: %{plan: []}} = state), do: {:done, state}

  def advance(%{unwind: %{plan: [group | rest]}} = state) do
    state = %{state | unwind: %{state.unwind | plan: rest, current: []}}

    case dispatch_group(state, group) do
      %{unwind: %{current: []}} = state -> advance(state)
      state -> {:unwinding, state}
    end
  end

  @doc """
  The group in flight has drained: journal each stage's undo the way its
  forward run was journaled — a `ProcCommit` marked `compensating`, carrying
  what the hook staged — then move on. The done marker each one records is
  what makes a crash mid-rollback resume rather than repeat the undo.
  """
  @spec commit(struct()) :: {:unwinding, struct()} | {:done, struct()} | {:awaiting, struct()}
  def commit(%{unwind: %{current: []}} = state), do: advance(state)

  def commit(%{unwind: %{current: [stage | rest]}} = state) do
    mark(state, stage.node_ref, stage.entities, :done)
    state = %{state | unwind: %{state.unwind | current: rest}}

    if journals?(state) do
      ref = make_ref()

      command =
        Commit.proc(state.runner, self(), ref,
          operative_id: state.operative_id,
          proc_idx: stage.proc_idx,
          iter: state.iter,
          entities_ran: stage.entities,
          handoff: [],
          outbox: Map.get(state.outbox.items, stage.node_ref, []),
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

  # The saga markers, on the same wire an Entity's records leave on. A class
  # that journals nothing has nowhere to persist them, and a procedure with no
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

  defp journals?(state), do: state.spec.knobs.persistence_enabled

  # The stages to undo, latest first: the in-flight iteration's completed
  # stages, then — under `:all_or_nothing` — every committed iteration's, also
  # latest first. A stage whose procedure declares no hook is left out, so a
  # class with no saga has an empty plan.
  defp plan(state) do
    iterations =
      case state.spec.knobs.rollback_mode do
        :keep_progress -> [state.completed]
        :all_or_nothing -> [state.completed | Enum.reverse(state.committed)]
      end

    iterations
    |> Enum.flat_map(&Enum.reverse/1)
    |> Enum.filter(&compensates?(state, &1))
    |> Enum.map(&List.wrap/1)
  end

  defp compensates?(state, %{node_ref: node_ref}),
    do: state.spec.topology.node_modules |> Map.fetch!(node_ref) |> Procedure.compensates?()

  # One stage's Entities undo together, each followed by its barrier, so the
  # group is finished exactly when every one of them has drained. An Entity
  # whose undo already ran before the crash is skipped: the marker the restore
  # carried is what makes a resumed rollback finish rather than repeat.
  defp dispatch_group(state, group),
    do: Enum.reduce(group, state, &dispatch_stage(&2, &1))

  defp dispatch_stage(state, stage) do
    case outstanding(state, stage) do
      [] -> state
      pids -> Enum.reduce(pids, arm(state, stage), fn pid, acc -> undo(acc, stage, pid) end)
    end
  end

  # The stage's forward release and its staged plugin output are cleared
  # first: the compensating commit that follows carries the UNDO's output,
  # never a second copy of what the forward run already journaled.
  defp arm(state, stage) do
    %{
      state
      | collected: Map.put(state.collected, stage.node_ref, []),
        outbox: put_in(state.outbox, [:items, stage.node_ref], []),
        unwind: %{state.unwind | current: state.unwind.current ++ [stage]}
    }
  end

  # The Entities of `stage` whose undo has not already run. A marker the
  # restore carried is what makes a resumed rollback finish rather than repeat.
  defp outstanding(state, stage) do
    state.spec
    |> Spec.entity_identities(state.operative_id, stage.node_ref)
    |> Enum.zip(Map.get(state.entities, stage.node_ref, []))
    |> Enum.reject(fn {identity, _pid} ->
      Instance.compensation_done?(state.instance, identity)
    end)
    |> Enum.map(fn {_identity, pid} -> pid end)
  end

  defp undo(state, stage, pid) do
    # A crash-resumed unwind reconstructs completed stages with no outputs —
    # a procedure's collected batch was never durable — so a hook that records
    # an undo has to do so unconditionally, which is the contract the
    # `Procedure` docs state.
    outputs = Map.get(state.collected, stage.node_ref, [])
    Entity.compensate(pid, outputs)
    ref = make_ref()
    Entity.barrier(pid, ref, self())
    Logger.debug("[Flezha.SDK] #{state.operative_id} compensating proc #{stage.proc_idx}")
    %{state | awaiting: Map.put(state.awaiting, ref, stage.node_ref)}
  end
end
