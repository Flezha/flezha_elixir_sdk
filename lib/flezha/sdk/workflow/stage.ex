defmodule Flezha.SDK.Workflow.Stage do
  @moduledoc """
  One stage of a workflow chain: how its batch reaches its Cells, and how the
  driver learns the stage has finished.

  Every Cell here is one of the stream operative's, held through
  `Flezha.SDK.Operative.Cells`, with the same state, records and ladder,
  bounded by the consuming edge's `retry_max`/`retry_backoff_ms` as a stream's
  Cell is. A value reaches its Cells by the same per-mode target resolution
  the stream operative routes with: the one Cell of a Single node, the keys
  `cells_for/1` names on a Static one, the Cell of its edge's key on a Dynamic
  one, minted on first sight up to the node's cap.

  A stage is finished when every Cell it handed a value to has drained past a
  barrier queued behind its values. An invocation that emits nothing reports
  nothing, so counting outputs could never tell the driver a stage was done.
  """

  alias Flezha.SDK.Cell
  alias Flezha.SDK.Operative.Cells

  @doc """
  Hand `batch` — `{value, key}` pairs, the key the edge gave each value — to
  the stage's Cells and queue one barrier behind each Cell that received one.
  Answers the keys of those Cells in the order they were first reached, the
  `{ref, node_ref}` pairs the driver waits on, and the Cell set.
  """
  @spec dispatch(Cells.t(), reference(), [{term(), String.t() | nil}], pid()) ::
          {[String.t() | nil], [{reference(), reference()}], Cells.t()}
  def dispatch(%Cells{} = cells, node_ref, batch, reply_to) do
    {reached, cells} =
      Enum.reduce(batch, {[], cells}, fn {value, key}, {reached, cells} ->
        case Cells.deliver(cells, node_ref, value, key, value) do
          {{:misroute, _reason}, cells} -> {reached, cells}
          {keys, cells} -> {reached ++ keys, cells}
        end
      end)

    # The Cell a key holds now, which a push that re-minted a dead one changed.
    keyed = Map.get(cells.cells, node_ref, %{})
    reached = for key <- Enum.uniq(reached), Map.has_key?(keyed, key), do: key

    awaiting =
      Enum.map(reached, fn key ->
        ref = make_ref()
        Cell.barrier(Map.fetch!(keyed, key), ref, reply_to)
        {ref, node_ref}
      end)

    {reached, awaiting, cells}
  end
end
