defmodule ElixirRunner.Call do
  @moduledoc """
  Call fixtures (`call_response`): operators an outside caller calls and whose
  graph answers it through `response/1` — from the first tasklet, from a
  downstream one, from whichever of two filtered paths the input took, and
  from a durable steps turn.
  """

  defmodule Echo do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "call_echo"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule Parse do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "call_parse"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule Answer do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "call_answer"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, "re:" <> input)
      {:ok, state}
    end
  end

  defmodule Admit do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "call_admit"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule Accept do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "call_accept"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, "accepted:" <> input)
      {:ok, state}
    end
  end

  defmodule Refuse do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "call_refuse"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, "refused:" <> input)
      {:ok, state}
    end
  end

  defmodule Turn do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "call_turn"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule EchoOp do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def factory_id, do: "call_echo"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(ElixirRunner.Call.Echo) |> response()
    end
  end

  defmodule DownstreamOp do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def factory_id, do: "call_downstream"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Call.Parse)
      |> into(ElixirRunner.Call.Answer)
      |> response()
    end
  end

  # An input starting `no` takes the refused path, every other the accepted one.
  defmodule SplitOp do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def factory_id, do: "call_split"

    def channels(c) do
      import Flezha.SDK.Channels

      admitted = c |> input("in") |> into(ElixirRunner.Call.Admit)

      admitted
      |> filter(&(not String.starts_with?(&1, "no")))
      |> into(ElixirRunner.Call.Accept)
      |> response()

      admitted
      |> filter(&String.starts_with?(&1, "no"))
      |> into(ElixirRunner.Call.Refuse)
      |> response()
    end
  end

  defmodule TurnSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def factory_id, do: "call_turn"
    def run, do: {:steps, each: :per_input}
    def durable, do: ElixirRunner.fixture_durable()
    def max_duration_ms, do: 600_000

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(ElixirRunner.Call.Turn) |> response()
    end
  end

  def all, do: [EchoOp, DownstreamOp, SplitOp, TurnSteps]
end
