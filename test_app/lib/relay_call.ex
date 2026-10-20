defmodule ElixirRunner.RelayCall do
  @moduledoc """
  Relay call fixtures (`relay_call`): relays that call another
  relay from their graph and read the outcome as a `Flezha.SDK.Response`.

    * `call_pricing`: a flow callee answering `price:<input>` on its output
      `quotes`; an input starting `silent` is never answered.
    * `call_orders`: a flow caller calling the instance `pricing`, answered
      from `quotes`.
    * `call_agent`: a durable `:per_input` `:keep_progress` session answering
      `answer:<turn>`; `boom` fails the turn, `slow` answers after 2 s.
    * `call_gateway`: an effectively-once caller calling `call_agent` by the
      session key of each `<session>:<text>` turn, and without creating it for
      a `resume:<session>:<text>` turn, both Response channels wired back into
      its calling tasklet.
    * `call_tool_session`: a durable `:per_input` session calling `pricing`
      from a turn and reading the outcome as its next turn.
    * `call_orders_steps`: `call_orders` as a durable `:per_input` session, so
      `order_ask`'s channel ends only in the call.
    * `orders`: the `relay_api` fixture, a described JSON entry `place`
      answering through `placed` and a text entry `audit` no output answers.

  Every outcome is written to an output as one line,
  `<request>=ok:<answer>@<target>/<created>` or
  `<request>=error:<code>@<target>/<created>`.
  """

  alias Flezha.SDK.Response

  @deadline_ms 30_000
  # The prefix a caller's tasklet puts on an outcome line, which no request
  # carries, so one output channel can feed both the call and the output.
  @outcome "response "
  # The prefix of a turn resuming a session that must already run: its call
  # may not create the callee.
  @resume "resume:"

  @doc "One outcome as the line every SDK's fixture writes."
  def render(%Response{request: request, target: target, created: created} = response) do
    outcome =
      case response.result do
        {:ok, answer} -> "ok:" <> answer
        {:error, error} -> "error:" <> error.code
      end

    "#{request}=#{outcome}@#{target}/#{created}"
  end

  @doc "What a caller's tasklet emits: a turn as it came, an outcome as its prefixed line."
  def pass(%Response{} = response), do: @outcome <> render(response)
  def pass(turn), do: turn

  def turn?(line), do: not outcome?(line) and not resume?(line)
  def resume?(line), do: String.starts_with?(line, @resume)
  def outcome?(line), do: String.starts_with?(line, @outcome)

  @doc "An outcome or a resume line without its prefix."
  def unprefixed(@outcome <> line), do: line
  def unprefixed(@resume <> line), do: line

  @doc "The session a turn or an outcome line belongs to: what precedes its `:`."
  def session(line) do
    line |> String.replace_prefix(@outcome, "") |> String.split(":", parts: 2) |> hd()
  end

  @doc false
  def deadline_ms, do: @deadline_ms

  defmodule PricingQuote do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "pricing_quote"

    @impl true
    def run("silent" <> _, state, _ctx), do: {:ok, state}

    def run(input, state, ctx) do
      emit(ctx, "price:" <> input)
      {:ok, state}
    end
  end

  defmodule OrderAsk do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "order_ask"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule OrderPriced do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "order_priced"

    @impl true
    def run(response, state, ctx) do
      emit(ctx, ElixirRunner.RelayCall.render(response))
      {:ok, state}
    end
  end

  defmodule AgentTurn do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "agent_turn"

    @impl true
    def run(input, state, ctx) do
      case input |> String.split(":", parts: 2) |> List.last() do
        "boom" -> {:error, {:non_retryable, "the turn failed"}}
        text -> answer(text, input, state, ctx)
      end
    end

    defp answer(text, input, state, ctx) do
      if text == "slow", do: Process.sleep(2_000)
      emit(ctx, "answer:" <> input)
      {:ok, state}
    end
  end

  defmodule GatewayAccount do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "gateway_account"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, ElixirRunner.RelayCall.pass(input))
      {:ok, state}
    end
  end

  defmodule GatewayResume do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "gateway_resume"

    @impl true
    def run(line, state, ctx) do
      emit(ctx, ElixirRunner.RelayCall.unprefixed(line))
      {:ok, state}
    end
  end

  defmodule GatewayLog do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "gateway_log"

    @impl true
    def run(line, state, ctx) do
      emit(ctx, ElixirRunner.RelayCall.unprefixed(line))
      {:ok, state}
    end
  end

  defmodule ToolTurn do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "tool_turn"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, ElixirRunner.RelayCall.pass(input))
      {:ok, state}
    end
  end

  defmodule ToolLog do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "tool_log"

    @impl true
    def run(line, state, ctx) do
      emit(ctx, ElixirRunner.RelayCall.unprefixed(line))
      {:ok, state}
    end
  end

  defmodule PricingOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    import Flezha.SDK.Channels
    def factory_id, do: "call_pricing"
    def channels(c), do: c |> input("in", codec: :text) |> into(PricingQuote) |> output("quotes")
  end

  defmodule OrdersOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    import Flezha.SDK.Channels
    alias ElixirRunner.RelayCall
    def factory_id, do: "call_orders"

    def channels(c) do
      c
      |> input("in", codec: :text)
      |> into(OrderAsk)
      |> named("order")
      |> call("pricing", deadline_ms: RelayCall.deadline_ms(), answer_channel: "quotes")
      |> into(OrderPriced)
      |> output("priced")
    end
  end

  defmodule OrdersSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "call_orders_steps"
    def run, do: {:steps, each: :per_input}
    def durable, do: ElixirRunner.fixture_durable()
    def max_duration_ms, do: 600_000
    defdelegate channels(c), to: OrdersOp
  end

  defmodule AgentSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    import Flezha.SDK.Channels
    def factory_id, do: "call_agent"
    def run, do: {:steps, each: :per_input, on_failure: :keep_progress}
    def durable, do: ElixirRunner.fixture_durable()
    def max_duration_ms, do: 600_000
    def channels(c), do: c |> input("turn", codec: :text) |> into(AgentTurn) |> output()
  end

  defmodule GatewayOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    import Flezha.SDK.Channels
    alias ElixirRunner.RelayCall
    def factory_id, do: "call_gateway"
    def run, do: {:flow, exactly: :effectively_once}

    def channels(c) do
      account = c |> input("in", codec: :text) |> into(GatewayAccount) |> named("gateway")

      account
      |> filter(&RelayCall.turn?/1)
      |> key_by(&RelayCall.session/1)
      |> call(factory: "call_agent", deadline_ms: RelayCall.deadline_ms())
      |> into(GatewayAccount)

      account
      |> filter(&RelayCall.resume?/1)
      |> into(GatewayResume)
      |> named("resume")
      |> key_by(&RelayCall.session/1)
      |> call(factory: "call_agent", deadline_ms: RelayCall.deadline_ms(), create: false)
      |> into(GatewayAccount)

      account
      |> filter(&RelayCall.outcome?/1)
      |> key_by(&RelayCall.session/1)
      |> into(GatewayLog)
      |> keyed()
      |> output("gateway_out")
    end
  end

  defmodule ToolSessionSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    import Flezha.SDK.Channels
    alias ElixirRunner.RelayCall
    def factory_id, do: "call_tool_session"
    def run, do: {:steps, each: :per_input}
    def durable, do: ElixirRunner.fixture_durable()
    def max_duration_ms, do: 600_000

    def channels(c) do
      turn = c |> input("turn", codec: :text) |> into(ToolTurn) |> named("tool")

      turn
      |> filter(&(not RelayCall.outcome?(&1)))
      |> call("pricing", deadline_ms: RelayCall.deadline_ms())
      |> into(ToolTurn)

      turn |> filter(&RelayCall.outcome?/1) |> into(ToolLog) |> output("tool_out")
    end
  end

  defmodule PlaceOrder do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "place_order"

    @impl true
    def run(%{"pair" => pair, "amount" => amount}, state, ctx) do
      emit(ctx, %{"pair" => pair, "amount" => amount, "status" => "placed"})
      {:ok, state}
    end
  end

  defmodule AuditOrder do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "audit_order"

    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule ApiOrders do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    import Flezha.SDK.Channels
    def factory_id, do: "orders"

    @order %{
      "type" => "object",
      "properties" => %{"pair" => %{"type" => "string"}, "amount" => %{"type" => "number"}},
      "required" => ["pair", "amount"]
    }

    def channels(c) do
      c
      |> input("place", description: "Places an order", schema: @order)
      |> into(PlaceOrder)
      |> output("placed")

      c |> input("audit", codec: :text) |> into(AuditOrder)
    end
  end

  def all,
    do: [PricingOp, OrdersOp, AgentSteps, GatewayOp, ToolSessionSteps, OrdersSteps, ApiOrders]
end
