defmodule Tradedesk.ConfirmFill do
  @moduledoc """
  Asks the exchange whether the order filled, on one arm of the fan-out. The
  paper exchange rejects anything above its own size, which is what makes this
  the step that fails, and takes ten seconds to answer, which leaves time to
  stop the runner in the middle of an order.

  It remembers nothing and has nothing to undo, so it writes only `run/3`. It
  ends the instance with a result rather than emitting one: the order's
  outcome is the instance's own answer, which `flz relay info` reads back.

  A failure here is retried first, and once the retries are spent the instance
  ends abnormally. Everything that finished is then undone in reverse, across
  both arms: the order is cancelled and the money released, while the desk's
  book is skipped because it declares no `compensate/3`.
  """
  use Flezha.SDK.Tasklet, name: "confirm_fill"

  @exchange_max 100

  @impl true
  def run(%{order: order, order_id: order_id}, state, ctx) do
    # The paper exchange takes ten seconds to answer. Only the worker running
    # this order waits; the runner's other workers keep serving every other cell.
    Process.sleep(10_000)

    if order.quantity > @exchange_max do
      # Tried again, then the order is undone.
      {:error,
       "the exchange fills at most #{@exchange_max} a time, and this is #{order.quantity}"}
    else
      complete(ctx, "filled #{order.symbol} #{order.quantity} as #{order_id}")
      {:ok, state}
    end
  end
end
