defmodule Tradedesk.PlaceOrder do
  @moduledoc """
  Step two: places the order with the paper exchange, keeping the id it answers
  with, and cancels the order again if it is undone. It has the same shape as
  step one.
  """
  use Flezha.SDK.Tasklet, name: "place_order"

  # What this step remembers.
  state do
    field(:order_id, :string, default: "")
  end

  # How the id is written down.
  defchange placing(placement, order_id) do
    %{placement | order_id: order_id}
  end

  # The step: place the order under an id, remember the id, and hand the order
  # on.
  @impl true
  def run(%{order: order}, placement, ctx) do
    order_id = "ord-" <> stable_id(ctx)
    emit(ctx, %{order: order, order_id: order_id})
    {:ok, placing(placement, ctx, order_id)}
  end

  # The undo: cancel the order, by the id the state kept.
  @impl true
  def compensate(_outputs, placement, _ctx) do
    IO.puts("[undo] cancelled #{placement.order_id}")
    {:ok, placement}
  end
end
