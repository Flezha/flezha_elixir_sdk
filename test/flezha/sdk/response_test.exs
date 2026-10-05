defmodule Flezha.SDK.ResponseTest do
  @moduledoc """
  `response/1`: a plugin end whose outputs answer the outside call their input
  carried. Declared, it derives as a sink does and never beside one; live,
  every output carries `flz.reply_to` — the call a downstream tasklet's input
  inherited from the input upstream of it, or an empty value — and a park names
  the call waiting on the parked input.
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Counters, LocalStream, Relay, Topology, Wire}
  alias Flezha.SDK.Steps.{Driver, Spec}
  alias Flezha.SDK.Relay.{Inbox, Routing}
  alias Flezha.SDK.Test.StubRunner

  defmodule Parse do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "parse"

    @impl true
    def run("boom", _state, _ctx), do: {:error, {:non_retryable, "boom"}}

    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule Answer do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "answer"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, "re:" <> input)
      {:ok, state}
    end
  end

  defmodule Log do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "log"

    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule DownstreamOp do
    use Flezha.SDK.Relay, factory_id: "response_downstream"
    def channels(c), do: c |> input("in") |> into(Parse) |> into(Answer) |> response()
  end

  defmodule HybridOp do
    use Flezha.SDK.Relay, factory_id: "response_hybrid"
    def channels(c), do: c |> input("in") |> into(Parse) |> response() |> into(Log)
  end

  defmodule BesideSinkOp do
    use Flezha.SDK.Relay, factory_id: "response_beside_sink"
    def channels(c), do: c |> input("in") |> into(Parse) |> sink("out") |> response()
  end

  defmodule TwiceOp do
    use Flezha.SDK.Relay, factory_id: "response_twice"
    def channels(c), do: c |> input("in") |> into(Parse) |> response() |> response()
  end

  defmodule UnkeyedTierOp do
    use Flezha.SDK.Relay, factory_id: "response_unkeyed_tier"
    def run, do: {:flow, exactly: :effectively_once}

    def channels(c),
      do: c |> input("in") |> into(Parse) |> response()
  end

  # A two-stage turn: `parse` hands on, `answer` answers the pass's call.
  defmodule TurnLoop do
    @behaviour Flezha.SDK.Relay
    import Flezha.SDK.Channels
    def factory_id, do: "response_turn"
    def run, do: {:steps, each: :per_input}
    def max_duration_ms, do: 600_000
    def channels(c), do: c |> input("in") |> into(Parse) |> into(Answer) |> response()
  end

  defmodule QuietTurnLoop do
    @behaviour Flezha.SDK.Relay
    import Flezha.SDK.Channels
    def factory_id, do: "response_quiet_turn"
    def run, do: {:steps, each: :per_input}
    def durable, do: false
    def max_duration_ms, do: 600_000
    def channels(c), do: c |> input("in") |> into(Parse) |> into(Answer) |> response()
  end

  # This process as the controller end of a native stream the process `to`
  # writes its records and outputs on.
  defp wire_to(to) do
    {sdk_side, controller_side} = LocalStream.open()
    :ok = LocalStream.controlling_process(controller_side, self())
    :ok = LocalStream.set_active(controller_side)
    send(to, {:secondaries, [sdk_side]})
  end

  defp start_turns(module) do
    {:ok, spec} = Spec.build(module)
    {:ok, runner} = StubRunner.start_link(test: self())

    {:ok, driver} =
      Driver.start_link(
        relay_id: "turn-1",
        counters: Counters.new(),
        spec: spec,
        runner: runner
      )

    _state = :sys.get_state(driver)
    wire_to(driver)
    driver
  end

  # The outputs of the effect records written on the stream, with their call.
  defp recorded_answer do
    receive do
      {:flz_data, _, "cell_effect_record", frame, _meta} ->
        {:ok, %{body: body}} = Wire.decode_frame(frame)
        {:ok, %{outputs: outputs}} = Wire.decode_effect_body(body)

        case outputs do
          [] -> recorded_answer()
          [output] -> {output.payload, Map.new(output.metadata)["flz.reply_to"]}
        end
    after
      5_000 -> flunk("no effect record carried an output")
    end
  end

  defp ends(topology, module) do
    node = Enum.find_value(topology.node_modules, fn {ref, m} -> m == module && ref end)
    for %{to: {:sink, _}} = route <- Map.get(topology.routes, {:node, node}, []), do: route
  end

  describe "the declaration" do
    test "a response is a plugin end with no name, alone or with a consumer behind it" do
      {:ok, topology} = Topology.build(DownstreamOp)
      assert [%{response: true, channel: "answer"}] = ends(topology, Answer)
      assert ends(topology, Parse) == []

      {:ok, hybrid} = Topology.build(HybridOp)
      assert [%{response: true}] = ends(hybrid, Parse)
    end

    test "a sink beside a response, or two responses, end the channel twice" do
      for module <- [BesideSinkOp, TwiceOp] do
        assert {:error, %Flezha.SDK.Error{code: "topology.channel_ends_twice"}} =
                 Topology.build(module)
      end
    end

    test "an unkeyed response needs no key under the tier" do
      assert {:ok, _topology} = Topology.build(UnkeyedTierOp)
    end
  end

  test "only a response output carries the call, empty when nobody waits" do
    route = %{channel: "ans", response: true, key_by: nil, serializer: & &1}
    emit = %{meta: %{}, key: nil, origin: nil, call: "w:t"}

    assert {%{"flz.reply_to" => "w:t"}, "v"} = Routing.sink_frame(route, "v", emit, 0, nil)

    assert {%{"flz.reply_to" => ""}, "v"} =
             Routing.sink_frame(route, "v", %{emit | call: nil}, 0, nil)

    {metadata, "v"} = Routing.sink_frame(%{route | response: false}, "v", emit, 0, nil)
    refute Map.has_key?(metadata, "flz.reply_to")
  end

  describe "a live relay" do
    setup do
      {:ok, topology} = Topology.build(DownstreamOp)
      {:ok, runner} = StubRunner.start_link(test: self())

      opts = [
        relay_id: "answer-1",
        topology: topology,
        counters: Counters.new(),
        runner: runner
      ]

      {:ok, op} = Relay.Server.start_link(opts)
      {sdk_side, controller_side} = LocalStream.open()
      :ok = LocalStream.controlling_process(controller_side, self())
      :ok = LocalStream.set_active(controller_side)
      send(op, {:secondaries, [sdk_side]})
      %{op: op}
    end

    test "the downstream answer carries the call of the input upstream of it", %{op: op} do
      Inbox.input(op, "in", "hello", %{"flz.reply_to" => "w:tok"})
      assert_receive {:flz_data, _, "answer-1", "re:hello", %{"flz.reply_to" => "w:tok"}}, 5_000

      Inbox.input(op, "in", "plain", %{})
      assert_receive {:flz_data, _, "answer-1", "re:plain", %{"flz.reply_to" => ""}}, 5_000
    end

    test "a park names the call waiting on the parked input", %{op: op} do
      Inbox.input(op, "in", "boom", %{"flz.reply_to" => "w:boom"})
      assert_receive {:shipped, {:park_payload, %{reply_to: "w:boom", error: error}}}, 5_000
      assert error =~ "boom"
    end
  end

  describe "a steps pass" do
    test "answers the call its message carried from its last stage, off the journal" do
      driver = start_turns(QuietTurnLoop)
      Inbox.input(driver, "in", "hello", %{"flz.reply_to" => "w:one"})
      assert_receive {:flz_data, _, "turn-1", "re:hello", %{"flz.reply_to" => "w:one"}}, 5_000

      Inbox.input(driver, "in", "plain", %{})
      assert_receive {:flz_data, _, "turn-1", "re:plain", %{"flz.reply_to" => ""}}, 5_000
    end

    test "records the call on the last stage's output when it journals" do
      driver = start_turns(TurnLoop)
      Inbox.input(driver, "in", "hello", %{"flz.reply_to" => "w:one", "flz.offset" => "1"})
      assert recorded_answer() == {"re:hello", "w:one"}
    end
  end
end
