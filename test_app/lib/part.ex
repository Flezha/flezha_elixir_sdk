defmodule ElixirRunner.Part do
  @moduledoc """
  Part fixtures (`parts`): for an input `n` the tasklet sends the parts `1` to
  `n` and then emits `done:<n>` on its output `answer`; for `retry:<n>` it sends
  the same parts and fails its first attempt retryably, then sends them again
  and succeeds. `part_echo` runs as a flow, `part_echo_steps` a `per_input`
  steps relay doing the same per message.
  """

  @failed_once {__MODULE__, :failed_once}

  @doc false
  def echo(input, state, ctx) do
    {count, retry?} =
      case input do
        "retry:" <> n -> {String.to_integer(n), true}
        n -> {String.to_integer(n), false}
      end

    for i <- 1..count//1, do: :ok = Flezha.SDK.Ctx.part(ctx, Integer.to_string(i))

    if retry? and first_failure?(input),
      do: {:error, "the first attempt of #{input} fails"},
      else: emit_done(ctx, count, state)
  end

  defp emit_done(ctx, count, state) do
    Flezha.SDK.Ctx.emit(ctx, "done:#{count}")
    {:ok, state}
  end

  # Process-wide, so the second attempt of an input succeeds.
  defp first_failure?(input) do
    failed = :persistent_term.get(@failed_once, MapSet.new())

    if MapSet.member?(failed, input) do
      false
    else
      :persistent_term.put(@failed_once, MapSet.put(failed, input))
      true
    end
  end

  defmodule Echo do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "part_echo"

    @impl true
    def run(input, state, ctx), do: ElixirRunner.Part.echo(input, state, ctx)
  end

  defmodule EchoTurn do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "part_echo_turn"

    @impl true
    def run(input, state, ctx), do: ElixirRunner.Part.echo(input, state, ctx)
  end

  defmodule EchoOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "part_echo"

    def channels(c) do
      import Flezha.SDK.ChannelBuilder

      c
      |> input("in", codec: :text)
      |> retry_backoff_ms(25)
      |> into(ElixirRunner.Part.Echo)
      |> output("answer")
    end
  end

  defmodule EchoSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "part_echo_steps"
    def run, do: {:steps, each: :per_input}
    def durable, do: ElixirRunner.fixture_durable()
    def max_duration_ms, do: 600_000

    def channels(c) do
      import Flezha.SDK.ChannelBuilder

      c
      |> input("in", codec: :text)
      |> retry_backoff_ms(25)
      |> into(ElixirRunner.Part.EchoTurn)
      |> output("answer")
    end
  end

  def all, do: [EchoOp, EchoSteps]
end
