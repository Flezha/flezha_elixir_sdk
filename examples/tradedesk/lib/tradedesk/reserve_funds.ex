defmodule Tradedesk.ReserveFunds do
  @moduledoc """
  Step one: puts the money for the order aside, and releases it again if the
  order is undone.

  The reservation id comes from `stable_id/1`, and the step both remembers it
  and hands it on.
  """
  use Flezha.SDK.Tasklet, name: "reserve_funds"

  # What this step remembers.
  state do
    field(:id, :string, default: "")
  end

  # How the id is written down.
  defchange reserving(reservation, id) do
    %{reservation | id: id}
  end

  # The step: reserve the money under an id, remember the id, and hand the
  # order on.
  @impl true
  def run(order, reservation, ctx) do
    # The same id when the step runs again.
    id = "res-" <> stable_id(ctx)
    emit(ctx, %{order: order, reservation_id: id})
    {:ok, reserving(reservation, ctx, id)}
  end

  # The undo: release the money, by the id the state kept.
  @impl true
  def compensate(_outputs, reservation, _ctx) do
    IO.puts("[undo] released #{reservation.id}")
    {:ok, reservation}
  end
end
