defmodule Tradedesk.OrderExecutor do
  @moduledoc """
  The desk's bounded operator: one instance per order, which runs its steps
  once and then ends.

  A bounded operator declares its steps the way an unbounded operator declares
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
  use Flezha.SDK.BoundedOperator, factory_id: "order_executor"

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
  consequences of it, so the placed channel is used twice, once for each.

  The driver runs the chain level by level: every step of a level commits
  before any of its children run, so the desk's order of events is fixed
  rather than a race. The two steps of the last level run one after the
  other, the fill first. A step that fails poisons its whole level, which is
  why a refusal of the fill unwinds the steps before it.

  Nothing ends on a sink: the last step ends the instance with its result,
  which `flz operator info` shows.
  """
  @impl true
  def channels(c) do
    placed =
      c
      |> input_with("order", &OrderTicket.decode/1)
      |> into(Tradedesk.ReserveFunds)
      |> into(Tradedesk.PlaceOrder)

    # Two arms on the one placed channel.
    placed |> into(Tradedesk.ConfirmFill)
    placed |> into(Tradedesk.RecordOrder)
  end
end
