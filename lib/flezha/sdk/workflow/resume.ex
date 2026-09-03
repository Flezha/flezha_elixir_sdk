defmodule Flezha.SDK.Workflow.Resume do
  @moduledoc """
  Where a resumed iteration starts. The invariant is that a resumed chain sees
  exactly what the live one did: every uncommitted node re-seeds from each
  committed producer's journaled handoff, through the same edge transform the
  forward pass applied, and a join whose producers have not all committed
  banks what it has and waits for the rest.

  A committed procedure is skipped rather than re-run, and its stage record is
  reconstructed so the compensation planner can undo it — with EMPTY outputs,
  because a procedure's collected batch was never durable. That is the
  contract `Flezha.SDK.Procedure`'s `compensate/3` documentation states.

  The journal codec is `:erlang.term_to_binary/1`, read back with `:safe`.
  It has to round-trip faithfully — a resumed batch that came back as a
  different term would hand the next procedure something the live run never
  produced — and the class's own sink encoders, not this, decide the bytes
  that reach a plugin.
  """

  alias Flezha.SDK.Workflow.Instance

  @doc "Serialize one inter-procedure value for the journal."
  @spec encode(term()) :: binary()
  def encode(value), do: :erlang.term_to_binary(value)

  @doc "Read one journaled inter-procedure value back."
  @spec decode(binary()) :: term()
  def decode(payload), do: :erlang.binary_to_term(payload, [:safe])

  @doc """
  The frontier a journal re-opens: `%{ready: [{node_ref, batch}], pending:,
  waiting:, completed:}`, or `nil` when the bind carried no journal and the
  iteration seeds fresh.
  """
  @spec start(struct(), (reference(), reference(), [term()] -> [term()])) :: map() | nil
  def start(%{instance: %Instance{journal: nil}}, _through_edge), do: nil

  def start(%{instance: %Instance{journal: journal}} = state, through_edge) do
    dag = state.spec.dag
    committed = MapSet.new(journal.committed_proc_idxs)

    if MapSet.size(committed) == 0 do
      nil
    else
      dag.order
      |> Enum.reject(&MapSet.member?(committed, Map.fetch!(dag.pos, &1)))
      |> Enum.reduce(%{ready: [], pending: %{}, waiting: %{}}, fn node, acc ->
        reseed(acc, state, node, committed, journal, through_edge)
      end)
      |> Map.put(:completed, completed_stages(state, committed))
    end
  end

  # One uncommitted node's share of the journal: what each of its committed
  # producers released, through that edge. A node with no committed producer
  # is left alone — it becomes runnable through the ordinary accumulate once
  # its producer runs.
  defp reseed(acc, state, node, committed, journal, through_edge) do
    dag = state.spec.dag
    parents = Map.fetch!(dag.parents, node)
    {done, owed} = Enum.split_with(parents, &MapSet.member?(committed, Map.fetch!(dag.pos, &1)))

    if done == [],
      do: acc,
      else: seeded(acc, node, banked(journal, dag, done, node, through_edge), owed)
  end

  # What every committed producer of `node` released, each through its own
  # edge — the same application the live pass made.
  defp banked(journal, dag, done, node, through_edge) do
    Enum.flat_map(done, fn parent ->
      journal.proc_handoffs
      |> Map.get(Map.fetch!(dag.pos, parent), [])
      |> Enum.map(fn {_id, payload} -> decode(payload) end)
      |> then(&through_edge.(parent, node, &1))
    end)
  end

  defp seeded(acc, node, batch, []), do: %{acc | ready: acc.ready ++ [{node, batch}]}

  defp seeded(acc, node, batch, owed),
    do: %{
      acc
      | pending: Map.put(acc.pending, node, batch),
        waiting: Map.put(acc.waiting, node, length(owed))
    }

  # The committed procedures as stage records, in DAG order so an unwind runs
  # them back to front. The outputs are empty by construction.
  defp completed_stages(state, committed) do
    dag = state.spec.dag

    for node <- dag.order,
        proc_idx = Map.fetch!(dag.pos, node),
        MapSet.member?(committed, proc_idx),
        do: %{node_ref: node, proc_idx: proc_idx, entities: entities_ran(state, proc_idx)}
  end

  defp entities_ran(%{instance: %Instance{journal: journal}}, proc_idx) do
    journal.committed_procs
    |> Enum.find(%{}, &(Map.get(&1, :proc_idx) == proc_idx))
    |> Map.get(:entities_ran, [])
  end
end
