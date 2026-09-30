defmodule ElixirRunner.OperatorCall do
  @moduledoc """
  Operator call fixtures (`operator_call`): operators that call another
  operator from their graph and read the outcome as a `Flezha.SDK.Response`.

    * `call_pricing`: an unbounded callee answering `price:<input>` through
      `response/1`; an input starting `silent` is never answered.
    * `call_orders`: an unbounded caller calling the instance `pricing`.
    * `call_agent`: a durable `:per_input` `:keep_progress` session answering
      `answer:<turn>`; `boom` fails the turn, `slow` answers after 2 s.
    * `call_gateway`: an effectively-once caller calling `call_agent` by the
      session key of each `<session>:<text>` turn, and without creating it for
      a `resume:<session>:<text>` turn, both Response channels wired back into
      its calling tasklet.
    * `call_tool_session`: a durable `:per_input` session calling `pricing`
      from a turn and reading the outcome as its next turn.

  Every outcome is written to a sink as one line,
  `<request>=ok:<answer>@<target>/<created>` or
  `<request>=error:<code>@<target>/<created>`.
  """

  alias Flezha.SDK.Response

  @deadline_ms 30_000
  # The prefix a caller's tasklet puts on an outcome line, which no request
  # carries, so one output channel can feed both the call and the sink.
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
  def relay(%Response{} = response), do: @outcome <> render(response)
  def relay(turn), do: turn

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
      emit(ctx, ElixirRunner.OperatorCall.render(response))
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
      emit(ctx, ElixirRunner.OperatorCall.relay(input))
      {:ok, state}
    end
  end

  defmodule GatewayResume do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "gateway_resume"

    @impl true
    def run(line, state, ctx) do
      emit(ctx, ElixirRunner.OperatorCall.unprefixed(line))
      {:ok, state}
    end
  end

  defmodule GatewayLog do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "gateway_log"

    @impl true
    def run(line, state, ctx) do
      emit(ctx, ElixirRunner.OperatorCall.unprefixed(line))
      {:ok, state}
    end
  end

  defmodule ToolTurn do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "tool_turn"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, ElixirRunner.OperatorCall.relay(input))
      {:ok, state}
    end
  end

  defmodule ToolLog do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "tool_log"

    @impl true
    def run(line, state, ctx) do
      emit(ctx, ElixirRunner.OperatorCall.unprefixed(line))
      {:ok, state}
    end
  end

  defmodule PricingOp do
    @moduledoc false
    @behaviour Flezha.SDK.UnboundedOperator
    import Flezha.SDK.Channels
    def factory_id, do: "call_pricing"
    def channels(c), do: c |> input("in") |> into(PricingQuote) |> response()
  end

  defmodule OrdersOp do
    @moduledoc false
    @behaviour Flezha.SDK.UnboundedOperator
    import Flezha.SDK.Channels
    alias ElixirRunner.OperatorCall
    def factory_id, do: "call_orders"

    def channels(c) do
      c
      |> input("in")
      |> into(OrderAsk)
      |> named("order")
      |> call("pricing", deadline_ms: OperatorCall.deadline_ms())
      |> into(OrderPriced)
      |> sink("priced")
    end
  end

  defmodule AgentBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    import Flezha.SDK.Channels
    def factory_id, do: "call_agent"
    def loop_mode, do: :per_input
    def rollback_mode, do: :keep_progress
    def self_terminating?, do: false
    def max_duration_ms, do: 600_000
    def channels(c), do: c |> input("turn") |> into(AgentTurn) |> response()
  end

  defmodule GatewayOp do
    @moduledoc false
    @behaviour Flezha.SDK.UnboundedOperator
    import Flezha.SDK.Channels
    alias ElixirRunner.OperatorCall
    def factory_id, do: "call_gateway"

    def channels(c) do
      account = c |> effectively_once() |> input("in") |> into(GatewayAccount) |> named("gateway")

      account
      |> filter(&OperatorCall.turn?/1)
      |> key_by(&OperatorCall.session/1)
      |> call(factory: "call_agent", deadline_ms: OperatorCall.deadline_ms())
      |> into(GatewayAccount)

      account
      |> filter(&OperatorCall.resume?/1)
      |> into(GatewayResume)
      |> named("resume")
      |> key_by(&OperatorCall.session/1)
      |> call(factory: "call_agent", deadline_ms: OperatorCall.deadline_ms(), create: false)
      |> into(GatewayAccount)

      account
      |> filter(&OperatorCall.outcome?/1)
      |> key_by(&OperatorCall.session/1)
      |> into(GatewayLog)
      |> keyed()
      |> sink("gateway_out")
    end
  end

  defmodule ToolSessionBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    import Flezha.SDK.Channels
    alias ElixirRunner.OperatorCall
    def factory_id, do: "call_tool_session"
    def loop_mode, do: :per_input
    def self_terminating?, do: false
    def max_duration_ms, do: 600_000

    def channels(c) do
      turn = c |> input("turn") |> into(ToolTurn) |> named("tool")

      turn
      |> filter(&(not OperatorCall.outcome?(&1)))
      |> call("pricing", deadline_ms: OperatorCall.deadline_ms())
      |> into(ToolTurn)

      turn |> filter(&OperatorCall.outcome?/1) |> into(ToolLog) |> sink("tool_out")
    end
  end

  def all, do: [PricingOp, OrdersOp, AgentBounded, GatewayOp, ToolSessionBounded]
end
