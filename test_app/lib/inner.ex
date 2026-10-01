# -- an operator inside an operator -------------------------------------------

# One stepped session per key inside a flow gateway: `key:text` inputs, each
# key's session counting its turns in its own state and answering
# `key#turns:text` on the gateway's `replies` sink. The session sleeps after
# `FLEZHA_FIXTURE_SLEEP_AFTER_MS` and wakes on its key's next message with its
# count intact, while the gateway and every other key go on. Its second stage
# fails on the text `fail` (the turn is undone) and holds `hold` until the
# release env (a crash lands between the two stages); the gateway's reply holds
# `stall` the same way (a crash lands after the session answered).
#
# `loop_gateway` embeds the chained `countdown`: `label:n` counts down one per
# pass, and only the converged `label:0` reaches the gateway's `replies`.
defmodule ElixirRunner.SessionTurnTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "session_turn"

  @impl true
  def init_state(_), do: %{value: 0}

  defchange turn(s, n) do
    %{s | value: s.value + n}
  end

  @impl true
  def state_from_json(%{"value" => value}), do: %{value: value}

  @impl true
  def run(input, s, ctx) do
    [key, text] = input |> to_string() |> String.split(":", parts: 2)
    s = turn(s, ctx, 1)
    emit(ctx, "#{key}##{s.value}:#{text}")
    {:ok, s}
  end

  @impl true
  def compensate(_outputs, s, ctx), do: {:ok, turn(s, ctx, -1)}
end

defmodule ElixirRunner.SessionCheckTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "session_check"

  @impl true
  def run(input, s, ctx) do
    case input |> to_string() |> String.split(":", parts: 2) do
      [_key, "fail"] ->
        {:error, {:non_retryable, "session_check refuses fail"}}

      [_key, "hold"] ->
        ElixirRunner.Steps.Gate.block_until_released()
        emit(ctx, input)
        {:ok, s}

      _turn ->
        emit(ctx, input)
        {:ok, s}
    end
  end
end

defmodule ElixirRunner.SessionOp do
  @moduledoc false
  use Flezha.SDK.Operator, factory_id: "session", run: {:steps, each: :per_input}

  @impl true
  def durable, do: ElixirRunner.fixture_durable()

  @impl true
  def channels(c) do
    c
    |> input("turns")
    |> into(ElixirRunner.SessionTurnTask)
    |> into(ElixirRunner.SessionCheckTask)
  end
end

defmodule ElixirRunner.GatewayReplyTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "gateway_reply"

  @impl true
  def run(input, s, ctx) do
    if String.ends_with?(to_string(input), ":stall"),
      do: ElixirRunner.Steps.Gate.block_until_released()

    emit(ctx, input)
    {:ok, s}
  end
end

defmodule ElixirRunner.GatewayOp do
  @moduledoc false
  use Flezha.SDK.Operator, factory_id: "session_gateway"

  # It journals its input (a kept instance that may sleep), so an answer a
  # session passed it is replayed after a crash.
  @impl true
  def durable, do: {:local, sleep_after: 3_600_000}

  @impl true
  def channels(c) do
    c
    |> input("requests")
    |> key_by(&ElixirRunner.Key.prefix/1)
    |> into(ElixirRunner.SessionOp)
    |> into(ElixirRunner.GatewayReplyTask)
    |> sink("replies")
  end
end

defmodule ElixirRunner.CountdownTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "countdown_step"

  @impl true
  def run(input, s, ctx) do
    [label, n] = input |> to_string() |> String.split(":", parts: 2)
    n = String.to_integer(n)
    if n > 0, do: emit(ctx, "#{label}:#{n - 1}")
    {:ok, s}
  end
end

defmodule ElixirRunner.CountdownOp do
  @moduledoc false
  use Flezha.SDK.Operator, factory_id: "countdown", run: {:steps, each: :chained}

  @impl true
  def channels(c), do: c |> input("in") |> into(ElixirRunner.CountdownTask) |> named("carry")
end

defmodule ElixirRunner.LoopGatewayOp do
  @moduledoc false
  use Flezha.SDK.Operator, factory_id: "loop_gateway"

  @impl true
  def channels(c) do
    c
    |> input("requests")
    |> into(ElixirRunner.CountdownOp)
    |> into(ElixirRunner.GatewayReplyTask)
    |> sink("replies")
  end
end

defmodule ElixirRunner.Inner do
  @moduledoc "The operators that embed another this standalone runner advertises."

  def all, do: [ElixirRunner.GatewayOp, ElixirRunner.LoopGatewayOp]
end
