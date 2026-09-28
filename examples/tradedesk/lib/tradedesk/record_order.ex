defmodule Tradedesk.RecordOrder do
  @moduledoc """
  Writes the placed order into the desk's book, on its own arm of the fan-out,
  once `Tradedesk.ConfirmFill` has run.

  The plainest shape a tasklet has: a value in, nothing kept, nothing out.

  It carries no `compensate/3` on purpose. A rollback undoes what the desk did
  to the world, not what it wrote down about it: the attempt happened, and the
  book says so even when the order was cancelled again.
  """
  use Flezha.SDK.Tasklet, name: "record_order"

  @impl true
  def run(%{order: order, order_id: order_id}, state, _ctx) do
    IO.puts("[book] #{order.symbol} #{order.quantity} as #{order_id}")
    {:ok, state}
  end
end
