defmodule Tradedesk.OrderExecutor do
  @moduledoc """
  The desk's steps relay: one instance per order, which runs its steps
  once and then ends.

  A steps relay declares its steps the way a flow relay declares
  its graph, on the same builder, and the controller schedules, restarts and
  relocates it in the same way. What differs is the lifecycle: each step's
  output is written down as it commits, so an instance that is interrupted
  resumes at the step it reached rather than from the beginning, and the
  instance ends by itself once the chain has run.

  When a step fails and its retries are spent, the instance ends abnormally
  instead, and the default rollback undoes every step that finished, in
  reverse, through their `compensate/3`: the order is cancelled, then the money
  is released. A step with no `compensate/3` is skipped, which is why the
  desk's book is left alone.
  """
  use Flezha.SDK.Relay,
    factory_id: "order_executor",
    # one instance per order; each step runs once
    # a failed step undoes the finished ones
    run: {:steps, each: :once, on_failure: :undo_all}

  defmodule OrderTicket do
    @moduledoc """
    The order an instance is started with, decoded from its first message:
    `--text '{"symbol": "EUR/USD", "quantity": 10}' --channel order`.
    """
    defstruct [:symbol, :quantity]

    def decode(json) do
      case Jason.decode(json) do
        {:ok, %{"symbol" => symbol, "quantity" => quantity}}
        when is_binary(symbol) and is_integer(quantity) ->
          {:ok, %__MODULE__{symbol: symbol, quantity: quantity}}

        _other ->
          {:error, "an order is {\"symbol\": <text>, \"quantity\": <whole number>}"}
      end
    end
  end

  @doc """
  Two steps in order, then a fan-out. The placement is where the chain stops
  being a line: confirming the fill and booking the order are two
  consequences of it, so `branch` gives each of them the placed channel.

  A step's children run only once it has committed, so the desk's order of
  events is fixed rather than a race. The two arms run side by side, and each
  commits as soon as it finishes, so a crash while the fill is confirmed does
  not book the order again. A step that fails ends the order, which is why a
  refusal of the fill unwinds the steps before it.

  Nothing ends on an output: the last step ends the instance with its result,
  which `flz relay info` shows.
  """
  @impl true
  def channels(c) do
    c
    # the order to carry out
    |> input_with("order", &OrderTicket.decode/1)
    # step one
    |> into(Tradedesk.ReserveFunds)
    # step two, once step one is done
    |> into(Tradedesk.PlaceOrder)
    # two arms, side by side
    |> branch([&into(&1, Tradedesk.ConfirmFill), &into(&1, Tradedesk.RecordOrder)])
  end
end
