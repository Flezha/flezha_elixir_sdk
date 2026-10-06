defmodule Starter.Count do
  @moduledoc """
  Counts the visits of one page and answers the new count.

  The relay keys the channel by page, so each page has a cell of its own, and
  the `state` block is that cell's state. The state changes only through the
  `counted` change, which is recorded as it happens, so a runner that restarts
  finds the count it had.
  """

  # state is saved under this name, so choose it once
  use Flezha.SDK.Tasklet, name: "count"

  state do
    # What each page's cell remembers.
    field(:total, :integer, default: 0)
  end

  # How a visit changes it.
  defchange counted(seen) do
    %{seen | total: seen.total + 1}
  end

  @impl true
  def run(%{"page" => page}, seen, ctx) do
    seen = counted(seen, ctx)
    emit(ctx, %{page: page, total: seen.total})
    {:ok, seen}
  end
end
