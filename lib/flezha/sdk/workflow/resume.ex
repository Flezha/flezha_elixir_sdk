defmodule Flezha.SDK.Workflow.Resume do
  @moduledoc """
  Where a resumed iteration starts. The invariant is that a resumed chain sees
  exactly what the live one did: every uncommitted node re-seeds from each
  committed producer's journaled handoff, through the same edge transform the
  forward pass applied, and a join whose producers have not all committed
  banks what it has and waits for the rest.

  A committed tasklet is skipped rather than re-run, and its stage record is
  reconstructed so the compensation planner can undo it — with EMPTY outputs,
  because a tasklet's collected batch was never durable. That is the
  contract `Flezha.SDK.Tasklet`'s `compensate/3` documentation states.

  The journal codec is `:erlang.term_to_binary/1`, read back with `:safe`.
  It has to round-trip faithfully — a resumed batch that came back as a
  different term would hand the next tasklet something the live run never
  produced — and the class's own sink encoders, not this, decide the bytes
  that reach a plugin.
  """

  alias Flezha.SDK.Workflow.Instance

  @doc "Serialize one inter-tasklet value for the journal."
  @spec encode(term()) :: binary()
  def encode(value), do: :erlang.term_to_binary(value)

  @doc "Read one journaled inter-tasklet value back."
  @spec decode(binary()) :: term()
  def decode(payload), do: :erlang.binary_to_term(payload, [:safe])

  @doc "The terminal batch the last committed iteration journaled, read back."
  def carry(journal), do: Enum.map(journal.handoff, fn {_id, payload} -> decode(payload) end)

  @doc """
  Whether the journal the bind carried says a pass was in flight: a root of
  some chain committed inside the resume iteration.
  """
  @spec mid_iteration?(Instance.t()) :: boolean()
  def mid_iteration?(%Instance{journal: nil}), do: false

  def mid_iteration?(%Instance{journal: journal, spec: spec}) do
    roots =
      for {_channel, chain} <- spec.dag.chains,
          root <- chain.roots,
          do: Map.fetch!(spec.dag.pos, root)

    Enum.any?(journal.committed_task_idxs, &(&1 in roots))
  end

  @doc """
  The frontier a journal re-opens for the pass on `channel`: `%{channel:,
  ready: [{node_ref, batch}], pending:, waiting:, completed:, completions:}`.
  An uncommitted root re-runs over `message`, the one the controller
  re-forwarded, through its entry edge, or over the journaled carry when the
  pass is carry-fed; a join counts only the producers in this chain.
  """
  @spec start(
          struct(),
          String.t(),
          Instance.Item.t() | nil,
          (term(), reference(), [term()] -> [{term(), String.t() | nil}])
        ) :: map()
  def start(%{instance: %Instance{journal: journal}} = state, channel, message, through_edge) do
    dag = state.spec.dag
    chain = Map.fetch!(dag.chains, channel)
    committed = MapSet.new(journal.committed_task_idxs)

    dag.order
    |> Enum.filter(&MapSet.member?(chain.members, &1))
    |> Enum.reject(&MapSet.member?(committed, Map.fetch!(dag.pos, &1)))
    |> Enum.reduce(%{channel: channel, ready: [], pending: %{}, waiting: %{}}, fn node, acc ->
      reseed(acc, state, chain, node, committed, journal, message, through_edge)
    end)
    |> Map.put(:completed, completed_stages(state, committed))
    |> Map.put(:completions, journaled_completions(journal, committed))
  end

  # One uncommitted node's share of the journal: what each of its committed
  # producers in the chain released, through that edge. A root re-runs over
  # the pass's message or carry; a node with no committed producer is left
  # alone: it becomes runnable through the ordinary accumulate once its
  # producer runs.
  defp reseed(acc, state, chain, node, committed, journal, message, through_edge) do
    dag = state.spec.dag

    if node in chain.roots do
      seeded(acc, node, root_batch(acc.channel, node, journal, message, through_edge), [])
    else
      parents = Map.fetch!(dag.parents, node) |> Enum.filter(&MapSet.member?(chain.members, &1))

      {done, owed} =
        Enum.split_with(parents, &MapSet.member?(committed, Map.fetch!(dag.pos, &1)))

      if done == [],
        do: acc,
        else: seeded(acc, node, banked(journal, dag, done, node, through_edge), owed)
    end
  end

  # What an uncommitted root runs over again: the re-forwarded message through
  # the entry edge, or the previous terminal batch a chained pass carried.
  defp root_batch(channel, root, _journal, %Instance.Item{payload: payload}, through_edge),
    do: through_edge.({:entry, channel}, root, [payload])

  defp root_batch(_channel, _root, journal, nil, _through_edge),
    do: Enum.map(carry(journal), &{&1, nil})

  # What every committed producer of `node` released, each through its own
  # edge — the same application the live pass made.
  defp banked(journal, dag, done, node, through_edge) do
    Enum.flat_map(done, fn parent ->
      journal.task_handoffs
      |> Map.get(Map.fetch!(dag.pos, parent), [])
      |> Enum.map(fn {_id, payload} -> decode(payload) end)
      |> then(&through_edge.(parent, node, &1))
    end)
  end

  defp seeded(acc, _node, [], []), do: acc
  defp seeded(acc, node, batch, []), do: %{acc | ready: acc.ready ++ [{node, batch}]}

  defp seeded(acc, node, batch, owed),
    do: %{
      acc
      | pending: Map.put(acc.pending, node, batch),
        waiting: Map.put(acc.waiting, node, length(owed))
    }

  @doc """
  The journal's committed tasklets as stage records, in DAG order so an
  unwind runs them back to front; the outputs are empty by construction.
  """
  @spec completed_stages(struct()) :: [map()]
  def completed_stages(%{instance: %Instance{journal: nil}}), do: []

  def completed_stages(%{instance: %Instance{journal: journal}} = state),
    do: completed_stages(state, MapSet.new(journal.committed_task_idxs))

  defp completed_stages(state, committed) do
    dag = state.spec.dag

    for node <- dag.order,
        task_idx = Map.fetch!(dag.pos, node),
        MapSet.member?(committed, task_idx),
        do: %{node_ref: node, task_idx: task_idx, cells: cells_ran(state, task_idx)}
  end

  @doc """
  The tasklets a pending rewind unwinds, as stage records in the order they
  committed, each carrying the iteration it committed in.
  """
  @spec rewind_stages(struct()) :: [map()]
  def rewind_stages(%{instance: %Instance{journal: nil}}), do: []

  def rewind_stages(%{instance: %Instance{journal: journal}} = state) do
    node_at = Map.new(state.spec.dag.pos, fn {node, task_idx} -> {task_idx, node} end)

    for %{task_idx: task_idx, iter: iter, cells_ran: cells} <- journal.rewind_undo,
        node = node_at[task_idx],
        node != nil,
        do: %{node_ref: node, task_idx: task_idx, iter: iter, cells: cells}
  end

  # What the skipped committed tasklets of the resumed iteration asked for:
  # they never run again to ask twice. Keyed as the driver keeps a live one.
  defp journaled_completions(journal, committed) do
    for %{task_idx: task_idx, iter: iter} = task <- journal.committed_tasks,
        iter == journal.iter,
        MapSet.member?(committed, task_idx),
        result = Map.get(task, :completion),
        result != nil,
        into: %{},
        do: {task_idx, {nil, result}}
  end

  defp cells_ran(%{instance: %Instance{journal: journal}}, task_idx) do
    journal.committed_tasks
    |> Enum.find(%{}, &(Map.get(&1, :task_idx) == task_idx))
    |> Map.get(:cells_ran, [])
  end
end
