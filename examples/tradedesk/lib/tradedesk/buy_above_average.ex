defmodule Tradedesk.BuyAboveAverage do
  @moduledoc """
  Buys when a price is far enough above its symbol's average.

  It remembers nothing: the move it is handed already carries the average, so
  the decision is a function of one value, and a restart costs this stage
  nothing because there is nothing to bring back. That is why it has no `state`
  block, beside `Tradedesk.TrackPrices`, which keeps the window.

  It does two things per input, both through the context: it starts a steps
  relay and it emits.
  """
  use Flezha.SDK.Tasklet, name: "buy_above_average"

  @buy_above_pct 2.0
  @units_per_percent 10

  @impl true
  def run(%{symbol: symbol, price: price, average: average}, state, ctx) do
    percent = (price - average) / average * 100.0

    if percent >= @buy_above_pct do
      # A stronger move buys more.
      quantity = round(percent * @units_per_percent)

      # One steps relay per order.
      spawn_steps(ctx, "order_executor", %{symbol: symbol, quantity: quantity})

      emit(
        ctx,
        "#{symbol} #{decimals(price, 4)} is #{decimals(percent, 2)}% above its average " <>
          "of #{decimals(average, 4)}, buying #{quantity}"
      )
    end

    {:ok, state}
  end

  defp decimals(number, places), do: :erlang.float_to_binary(number / 1, decimals: places)
end
