defmodule Flezha.SDK.Steps.Stage do
  @moduledoc """
  One stage of a steps chain: how its batch reaches its Cells, and how the
  driver learns the stage has finished.

  Every Cell here is one of the flow operator's, held through
  `Flezha.SDK.Operator.Cells`, with the same state, records and ladder,
  bounded by the consuming edge's `retry_max`/`retry_backoff_ms` as a flow operator's
  Cell is. A value reaches its Cells by the same per-mode target resolution
  the flow operator routes with: the one Cell of a Single node, the keys
  `cells_for/1` names on a Static one, the Cell of its edge's key on a Dynamic
  one, minted on first sight up to the node's cap.

  A stage is finished when every Cell it handed a value to has drained past a
  barrier queued behind its values. An invocation that emits nothing reports
  nothing, so counting outputs could never tell the driver a stage was done.
  """

  alias Flezha.SDK.Cell
  alias Flezha.SDK.Operator.Cells

  @doc """
  Hand `batch` — `{value, key}` pairs, the key the edge gave each value — to
  the stage's Cells and queue one barrier behind each Cell that received one.
  Each item carries `call`, the outside call the pass answers, so a Cell that
  records its own outputs stamps a `response()` one. Answers the keys of those
  Cells in the order they were first reached, the `{ref, {node_ref, cell_pid}}`
  pairs the driver waits on, and the Cell set.
  """
  @spec dispatch(Cells.t(), reference(), [{term(), String.t() | nil}], binary() | nil, pid()) ::
          {[String.t() | nil], [{reference(), {reference(), pid()}}], Cells.t()}
  def dispatch(%Cells{} = cells, node_ref, batch, call, reply_to) do
    {reached, cells} =
      Enum.reduce(batch, {[], cells}, fn {value, key}, {reached, cells} ->
        item = if call, do: %Cell.Input{value: value, call: call}, else: value

        case Cells.deliver(cells, node_ref, value, key, item) do
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
        pid = Map.fetch!(keyed, key)
        Cell.barrier(pid, ref, reply_to)
        {ref, {node_ref, pid}}
      end)

    {reached, awaiting, cells}
  end

  @doc """
  Hand one due timer firing to the Cell of `node_ref` under `key` and queue a
  barrier behind it, answering as `dispatch/4` does; nothing when that Cell is
  gone, and its timers with it.
  """
  @spec fire(Cells.t(), reference(), String.t() | nil, struct(), pid()) ::
          {[String.t() | nil], [{reference(), {reference(), pid()}}], Cells.t()}
  def fire(%Cells{} = cells, node_ref, key, fire, reply_to) do
    case Cells.pid(cells, node_ref, key) do
      nil ->
        {[], [], cells}

      pid ->
        ref = make_ref()
        Cell.fire(pid, fire)
        Cell.barrier(pid, ref, reply_to)
        {[key], [{ref, {node_ref, pid}}], cells}
    end
  end
end
