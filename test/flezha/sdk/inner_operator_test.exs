defmodule Flezha.SDK.InnerOperatorTest do
  # async: false: a local run reads the process-wide notice counters.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO, only: [with_io: 2]

  alias Flezha.SDK.{Channels, Local, Topology}

  defmodule Turn do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "turn"

    @impl true
    def init_state(_), do: %{turns: 0}

    @impl true
    def run(%{user: user, text: text}, state, ctx) do
      state = %{state | turns: state.turns + 1}
      emit(ctx, "#{user}:#{state.turns}:#{text}")
      {:ok, state}
    end
  end

  defmodule Audit do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "audit"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule Reply do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "reply"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule Upcase do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "upcase"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, String.upcase(input))
      {:ok, state}
    end
  end

  defmodule Session do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def run, do: {:steps, each: :per_input}
    def durable, do: false
    def factory_id, do: "session"
    def channels(c), do: c |> Channels.input("turns") |> Channels.into(Turn)
  end

  defmodule TwoEntries do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def run, do: {:steps, each: :per_input}
    def factory_id, do: "two_entries"

    def channels(c) do
      c |> Channels.input("a") |> Channels.into(Turn)
      c |> Channels.input("b") |> Channels.into(Turn)
    end
  end

  defmodule TwoExits do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def run, do: {:steps, each: :per_input}
    def factory_id, do: "two_exits"

    def channels(c) do
      turns = Channels.input(c, "turns")
      Channels.into(turns, Turn)
      Channels.into(turns, Audit)
    end
  end

  defmodule Cleanup do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def factory_id, do: "cleanup"
    def channels(c), do: c |> Channels.input("in") |> Channels.into(Upcase)
  end

  defmodule Gateway do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def factory_id, do: "gateway"
    def durable, do: false

    def channels(c) do
      c
      |> Channels.input_with("requests", &{:ok, decode(&1)})
      |> Channels.key_by(& &1.user)
      |> Channels.into(Session, max_cells: 2)
      |> Channels.into(Reply)
      |> Channels.sink("out")
    end

    defp decode(line) do
      [user, text] = String.split(line, ":", parts: 2)
      %{user: user, text: text}
    end
  end

  defmodule StepsOuter do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def run, do: {:steps, each: :per_input}
    def factory_id, do: "steps_outer"
    def channels(c), do: c |> Channels.input("in") |> Channels.into(Session)
  end

  defmodule Embeds do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def factory_id, do: "embeds"
    def channels(c), do: c |> Channels.input("in") |> Channels.into(TwoEntries)
  end

  defmodule EmbedsChosen do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def factory_id, do: "embeds_chosen"
    def channels(c), do: c |> Channels.input("in") |> Channels.into(TwoEntries, input: "b")
  end

  defmodule EmbedsTwoExits do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def factory_id, do: "embeds_two_exits"
    def channels(c), do: c |> Channels.input("in") |> Channels.into(TwoExits)
  end

  defmodule Inlines do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def factory_id, do: "inlines"
    def durable, do: false

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.into(Cleanup)
        |> Channels.into(Reply)
        |> Channels.sink("out")
  end

  defp codes(module) do
    assert {:error, error} = Topology.build(module)
    Enum.map(error.details["issues"], & &1["code"])
  end

  test "a keyed edge into a steps class builds a scope node and no Cells of its own" do
    assert {:ok, topology} = Topology.build(Gateway)
    assert [{ref, facts}] = Map.to_list(topology.node_scope)
    assert topology.node_name[ref] == "session"
    assert facts.factory_id == "session" and facts.entry == "turns" and facts.max_cells == 2
    refute Map.has_key?(topology.node_modules, ref)
    refute Map.has_key?(topology.node_mode, ref)
  end

  test "a steps class embedding a steps class is refused naming both" do
    assert ["topology.nested_steps"] = codes(StepsOuter)
    {:error, error} = Topology.build(StepsOuter)
    assert error.details["outer"] == "steps_outer" and error.details["inner"] == "session"
  end

  test "several entry channels need the one the edge feeds named" do
    assert ["topology.embedded_entry_ambiguous"] = codes(Embeds)
    assert {:ok, _} = Topology.build(EmbedsChosen)
  end

  test "an embedded graph with two exits is refused naming both" do
    assert ["topology.embedded_exit_ambiguous"] = codes(EmbedsTwoExits)
    {:error, error} = Topology.build(EmbedsTwoExits)
    assert error.details["channels"] == "audit, turn"
  end

  test "a flow class is inlined under prefixed names" do
    assert {:ok, topology} = Topology.build(Inlines)
    assert topology.node_scope == %{}
    assert "cleanup.upcase" in Map.values(topology.node_name)
  end

  test "every key runs its own scope, with its own state, and exits continue the outer graph" do
    outputs = run(Gateway, "requests", ["alice:hi", "bob:yo", "alice:again"], 3)
    assert Enum.sort(outputs) == ["alice:1:hi", "alice:2:again", "bob:1:yo"]
  end

  test "a key past max_cells opens no scope, and the drop is counted" do
    test = self()
    handler = "inner-operator-capped-#{inspect(test)}"

    :telemetry.attach(handler, [:flezha, :sdk, :dynamic], &__MODULE__.dynamic_event/4, test)

    outputs = run(Gateway, "requests", ["a:1", "b:1", "c:1"], 2)
    :telemetry.detach(handler)

    assert Enum.sort(outputs) == ["a:1:1", "b:1:1"]
    assert_received {:dynamic, :capped}
  end

  test "an inlined flow class runs as part of the outer graph" do
    assert run(Inlines, "in", ["quiet"], 1) == ["QUIET"]
  end

  def dynamic_event(_event, _measure, %{event: event}, test), do: send(test, {:dynamic, event})

  # Feed `lines` to a local run of `module` and answer the first `count`
  # outputs on its sink `out`.
  defp run(module, channel, lines, count) do
    {outputs, _io} =
      with_io(:stderr, fn ->
        {:ok, run} = Local.start(module)
        test = self()
        :ok = Local.on_sink(run, "out", &send(test, {:out, &1.payload}))
        for line <- lines, do: :ok = Local.feed(run, channel, line)
        outputs = for _ <- 1..count, do: assert_receive({:out, value}, 5_000) && value
        :ok = Local.run_until_idle(run, 5_000)
        Local.stop(run)
        outputs
      end)

    outputs
  end
end
