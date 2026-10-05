defmodule Tradedesk.TrackPrices do
  @moduledoc """
  Keeps a symbol's last ten prices, and compares each new trade with their
  average.

  One cell per symbol, because the relay keys the channel by symbol, and the
  window is that cell's state. Every change to it is recorded as it happens, so
  a runner that is killed and started again finds the window it had rather than
  an empty one.
  """
  use Flezha.SDK.Tasklet, name: "track_prices"

  @window 10

  state do
    # What each symbol's cell remembers.
    field(:prices, :list, default: [])
  end

  # How a price joins the window.
  defchange price_seen(window, price) do
    %{window | prices: Enum.take(window.prices ++ [price], -@window)}
  end

  @impl true
  def run(%{"symbol" => symbol, "price" => price}, window, ctx) when is_number(price) do
    # A symbol's first trade has nothing to compare with.
    if window.prices != [] do
      average = Enum.sum(window.prices) / length(window.prices)
      emit(ctx, %{symbol: symbol, price: price, average: average})
    end

    {:ok, price_seen(window, ctx, price)}
  end

  # JSON checks no types.
  def run(%{"price" => price}, _window, _ctx),
    do: {:error, "the price is not a number: #{inspect(price)}"}
end
