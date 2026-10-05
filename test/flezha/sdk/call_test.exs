defmodule Flezha.SDK.CallTest do
  @moduledoc """
  `call/3`: a plugin end whose outputs are requests to another relay, and
  whose outcomes come back on the Response channel as `%Flezha.SDK.Response{}`.
  Declared, it closes only its own chain, stands alone on its channel and is
  refused where it cannot work; live, each output carries the call's stamps on
  the wire and in an effect record, and no other output carries them.
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Contract, Counters, Error, LocalStream, Relay, Response, Topology, Wire}
  alias Flezha.SDK.Steps.{Driver, Spec}
  alias Flezha.SDK.Relay.{Inbox, Routing}
  alias Flezha.SDK.Test.StubRunner

  defmodule Ask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "ask"

    @impl true
    def run(%Response{} = response, state, ctx) do
      emit(ctx, "outcome:" <> response.target)
      {:ok, state}
    end

    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule Log do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "log"

    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule BackIntoCallerOp do
    use Flezha.SDK.Relay, factory_id: "call_back"

    def channels(c) do
      asked = c |> input("in") |> into(Ask) |> named("quote")
      asked |> call("pricing", deadline_ms: 500) |> into(Ask)
      asked |> filter(&String.starts_with?(&1, "outcome:")) |> into(Log)
    end
  end

  defmodule FactoryOp do
    use Flezha.SDK.Relay, factory_id: "call_factory"

    def channels(c) do
      c
      |> input("in")
      |> into(Ask)
      |> filter(&(&1 != "drop"))
      |> map(&("req:" <> &1))
      |> key_by(fn "req:" <> rest -> rest |> String.split(":") |> hd() end)
      |> call(factory: "agent", deadline_ms: 60_000, create: false, channel: "turn")
      |> into(Log)
    end
  end

  defmodule TierOp do
    use Flezha.SDK.Relay, factory_id: "call_tier"
    def run, do: {:flow, exactly: :effectively_once}

    def channels(c),
      do:
        c
        |> input("in")
        |> into(Ask)
        |> call("p", deadline_ms: 1)
        |> into(Log)
  end

  defmodule DecodedOp do
    use Flezha.SDK.Relay, factory_id: "call_decoded"

    def channels(c) do
      c
      |> input("in")
      |> into(Ask)
      |> call("p", deadline_ms: 5, decode: &{:ok, String.upcase(&1)})
      |> into(Log)
    end
  end

  defmodule SinkThenCallOp do
    use Flezha.SDK.Relay, factory_id: "call_beside_sink"

    def channels(c) do
      asked = c |> input("in") |> into(Ask) |> sink("out")
      asked |> call("pricing", deadline_ms: 500) |> into(Log)
    end
  end

  defmodule CallThenResponseOp do
    use Flezha.SDK.Relay, factory_id: "call_beside_response"

    def channels(c) do
      asked = c |> input("in") |> into(Ask)
      asked |> call("pricing", deadline_ms: 500) |> into(Log)
      asked |> response()
    end
  end

  defmodule DeadlineZeroOp do
    use Flezha.SDK.Relay, factory_id: "call_zero"
    def channels(c), do: c |> input("in") |> into(Ask) |> call("p", deadline_ms: 0) |> into(Log)
  end

  defmodule UnkeyedNoCreateOp do
    use Flezha.SDK.Relay, factory_id: "call_unkeyed"

    def channels(c) do
      c
      |> input("in")
      |> into(Ask)
      |> call(factory: "agent", deadline_ms: 5, create: false)
      |> into(Log)
    end
  end

  defmodule UnreadOp do
    use Flezha.SDK.Relay, factory_id: "call_unread"

    def channels(c) do
      _response = c |> input("in") |> into(Ask) |> call("p", deadline_ms: 5)
    end
  end

  defmodule OnEntryOp do
    use Flezha.SDK.Relay, factory_id: "call_on_entry"

    def channels(c) do
      entry = c |> input("in")
      _ = entry |> call("p", deadline_ms: 5)
      entry |> into(Log)
    end
  end

  defmodule OnceLoop do
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    import Flezha.SDK.Channels
    def factory_id, do: "call_once"
    def channels(c), do: c |> input("in") |> into(Ask) |> call("p", deadline_ms: 5) |> into(Ask)
  end

  defmodule ChainedLoop do
    @behaviour Flezha.SDK.Relay
    import Flezha.SDK.Channels
    def factory_id, do: "call_chained"
    def run, do: {:steps, each: :chained}
    def channels(c), do: c |> input("in") |> into(Ask) |> call("p", deadline_ms: 5) |> into(Ask)
  end

  defmodule TurnLoop do
    @behaviour Flezha.SDK.Relay
    import Flezha.SDK.Channels
    def factory_id, do: "call_turn"
    def run, do: {:steps, each: :per_input}
    def max_duration_ms, do: 600_000

    def channels(c),
      do: c |> input("in") |> into(Ask) |> call("pricing", deadline_ms: 500) |> into(Ask)
  end

  defp ends(topology, module) do
    node = Enum.find_value(topology.node_modules, fn {ref, m} -> m == module && ref end)
    for %{to: {:sink, _}} = route <- Map.get(topology.routes, {:node, node}, []), do: route
  end

  defp codes(%Error{details: %{"issues" => issues}}), do: Enum.map(issues, & &1["code"])

  defp start(module, id) do
    {:ok, topology} = Topology.build(module)
    {:ok, runner} = StubRunner.start_link(test: self())
    opts = [relay_id: id, topology: topology, counters: Counters.new(), runner: runner]
    {:ok, op} = Relay.Server.start_link(opts)
    wire_to(op)
    op
  end

  defp wire_to(to) do
    {sdk_side, controller_side} = LocalStream.open()
    :ok = LocalStream.controlling_process(controller_side, self())
    :ok = LocalStream.set_active(controller_side)
    send(to, {:secondaries, [sdk_side]})
  end

  defp envelope(target, value),
    do: Jason.encode!(%{call: "c1", target: target, created: true, request: "", value: value})

  describe "the declaration" do
    test "a call is a plugin end with no sink, its Response an entry fed back into the caller" do
      {:ok, topology} = Topology.build(BackIntoCallerOp)
      assert [%{call: call, channel: "quote", response: false}] = ends(topology, Ask)

      assert call == %{
               target: "id:pricing",
               factory?: false,
               create: true,
               deadline_ms: 500,
               channel: nil,
               response: "quote.response"
             }

      assert topology.entry_order == ["in", "quote.response"]
      assert [%{to: {:node, ask}}] = topology.routes[{:entry, "quote.response"}]
      assert topology.node_modules[ask] == Ask
    end

    test "a factory call carries the chain's transforms, key, create and channel" do
      {:ok, topology} = Topology.build(FactoryOp)
      assert [%{call: call} = route] = ends(topology, Ask)
      assert %{target: "factory:agent", create: false, channel: "turn"} = call
      assert route.transform.("drop") == []
      assert route.transform.("s1:hi") == ["req:s1:hi"]
      assert route.key_by.("req:s1:hi") == "s1"
    end

    test "a call end needs no key under the tier" do
      assert {:ok, _topology} = Topology.build(TierOp)
    end

    test "the Response channel decodes the envelope with the call's decode" do
      {:ok, topology} = Topology.build(DecodedOp)
      parser = topology.entries["ask.response"].parser
      value = Base.encode64("fine")

      assert {:ok, %Response{result: {:ok, "FINE"}, target: "p-1", created: true}} =
               parser.(envelope("p-1", value))

      assert {:error, _} = parser.("not an envelope")
    end
  end

  describe "the refusals" do
    test "a call beside a sink or a response ends the channel twice" do
      for module <- [SinkThenCallOp, CallThenResponseOp] do
        assert {:error, %Error{code: "topology.channel_ends_twice"}} = Topology.build(module)
      end
    end

    test "a zero deadline, an unkeyed factory call that may not create, an unread Response" do
      assert {:error, %Error{code: "topology.call_deadline_zero"}} =
               Topology.build(DeadlineZeroOp)

      assert {:error, %Error{code: "topology.call_without_key"}} =
               Topology.build(UnkeyedNoCreateOp)

      assert {:error, %Error{code: "topology.call_response_unread"} = error} =
               Topology.build(UnreadOp)

      assert error.details["response"] == "ask.response"
    end

    test "a call on an entry channel is refused as a sink on it would be" do
      assert {:error, %Error{code: "topology.sink_on_entry"}} = Topology.build(OnEntryOp)
    end

    test "only a per-input loop may call" do
      for module <- [OnceLoop, ChainedLoop] do
        assert {:error, %Error{} = error} = Spec.build(module)
        assert "topology.call_outside_per_input_loop" in codes(error)
      end

      assert {:ok, _spec} = Spec.build(TurnLoop)
    end
  end

  describe "the stamps" do
    @call %{
      target: "factory:agent",
      factory?: true,
      create: false,
      deadline_ms: 1_000,
      channel: "turn",
      response: "ask.response"
    }
    @route %{
      channel: "ask",
      response: false,
      call: @call,
      key_by: &String.upcase/1,
      serializer: &Function.identity/1
    }

    test "a call output carries its callee, Response channel, deadline and key" do
      emit = %{
        meta: %{"flz.reply_to" => "w:t", "user" => "u"},
        key: nil,
        origin: nil,
        call: "w:t"
      }

      before = System.system_time(:millisecond)
      {metadata, "v"} = Routing.sink_frame(@route, "v", emit, 0, nil)

      assert %{
               "channel" => "ask",
               "user" => "u",
               "flz.call" => "factory:agent",
               "flz.call_create" => "false",
               "flz.call_response" => "ask.response",
               "flz.call_channel" => "turn",
               "flz.key" => "V"
             } = metadata

      refute Map.has_key?(metadata, "flz.reply_to")
      deadline = String.to_integer(metadata["flz.deadline_at"])
      assert deadline >= before + 1_000 and deadline <= System.system_time(:millisecond) + 1_000
    end

    test "a creating, unkeyed call on the callee's first channel stamps only what it has" do
      call = %{@call | target: "id:pricing", factory?: false, create: true, channel: nil}
      route = %{@route | call: call, key_by: nil}
      {metadata, _} = Routing.sink_frame(route, "v", %{meta: %{}, key: nil, origin: nil}, 0, nil)

      assert Map.keys(metadata) |> Enum.sort() ==
               ~w(channel flz.call flz.call_response flz.deadline_at)
    end

    test "every other output drops the call's keys" do
      stamped =
        Map.new(
          ~w(flz.call flz.call_create flz.call_response flz.deadline_at
                           flz.call_channel flz.key flz.reply_to),
          &{&1, "x"}
        )

      emit = %{meta: Map.put(stamped, "user", "u"), key: nil, origin: nil}
      route = %{channel: "out", response: false, key_by: nil, serializer: & &1}

      assert {%{"channel" => "out", "user" => "u"} = metadata, "v"} =
               Routing.sink_frame(route, "v", emit, 0, nil)

      assert map_size(metadata) == 2
      {answer, _} = Routing.sink_frame(%{route | response: true}, "v", emit, 0, nil)
      assert answer == %{"channel" => "out", "user" => "u", "flz.reply_to" => ""}
    end
  end

  describe "a live relay" do
    test "emits each request on the calling channel with the call's stamps" do
      op = start(FactoryOp, "caller-1")
      Inbox.input(op, "in", "drop", %{})
      Inbox.input(op, "in", "s1:hi", %{"flz.reply_to" => "w:tok"})

      assert_receive {:flz_data, _, "caller-1", "req:s1:hi", metadata}, 5_000
      assert %{"channel" => "ask", "flz.call" => "factory:agent", "flz.key" => "s1"} = metadata
      refute Map.has_key?(metadata, "flz.reply_to")
      refute_received {:flz_data, _, "caller-1", "req:drop", _}
    end

    test "hands an outcome to the tasklet the Response channel feeds, as a Response" do
      op = start(BackIntoCallerOp, "caller-2")
      Inbox.input(op, "quote.response", envelope("pricing-1", Base.encode64("9")), %{})
      assert_receive {:flz_data, _, "caller-2", "outcome:pricing-1", metadata}, 5_000
      assert metadata["flz.call"] == "id:pricing"
    end

    test "records the stamps on an effect record's output under a journaling turn" do
      {:ok, spec} = Spec.build(TurnLoop)
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
      Inbox.input(driver, "in", "hello", %{"flz.offset" => "1"})

      assert {"hello", metadata} = recorded_output()
      assert %{"flz.call" => "id:pricing", "flz.call_response" => "ask.response"} = metadata
    end
  end

  defp recorded_output do
    receive do
      {:flz_data, _, "cell_effect_record", frame, _meta} ->
        {:ok, %{body: body}} = Wire.decode_frame(frame)
        {:ok, %{outputs: outputs}} = Wire.decode_effect_body(body)

        case outputs do
          [] -> recorded_output()
          [output] -> {output.payload, Map.new(output.metadata)}
        end
    after
      5_000 -> flunk("no effect record carried an output")
    end
  end

  describe "the envelope" do
    @cases "test/fixtures/call_response_envelope.json"
           |> Contract.path!()
           |> File.read!()
           |> Jason.decode!()
           |> Map.fetch!("cases")

    for %{"name" => name} = fixture <- @cases do
      @fixture fixture
      test "decodes #{name}" do
        %{"payload" => payload, "expect" => expect} = @fixture
        assert {:ok, %Response{} = response} = Response.decode(payload, &{:ok, &1})

        assert response.target == expect["target"]
        assert response.created == expect["created"]
        assert response.request == Base.decode64!(expect["request"])

        case response.result do
          {:ok, value} ->
            assert expect["ok"]
            assert value == Base.decode64!(expect["value"])

          {:error, %Error{} = error} ->
            refute expect["ok"]
            assert error.code == expect["error_code"]
            assert Atom.to_string(error.class) == expect["error_class"]
            assert error.message == expect["error_message"]
        end
      end
    end
  end
end
