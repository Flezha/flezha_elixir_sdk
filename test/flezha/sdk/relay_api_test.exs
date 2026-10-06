defmodule Flezha.SDK.RelayApiTest do
  @moduledoc """
  A relay's API: the entries a class announces (format, schema, description and
  the answer's format), the schemas registration refuses, and
  `Flezha.SDK.call/4`, which calls a channel by value through the controller.
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Channels, Codec, Topology}

  @order %{
    "type" => "object",
    "properties" => %{"pair" => %{"type" => "string"}, "amount" => %{"type" => "number"}},
    "required" => ["pair", "amount"]
  }

  defmodule Echo do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "echo"
    @impl true
    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule Sink do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "sink"
    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule Orders do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "orders"

    def channels(c) do
      c
      |> Channels.input("place",
        description: "Places an order",
        schema: Flezha.SDK.RelayApiTest.order()
      )
      |> Channels.into(Echo)
      |> Channels.output("placed")

      c |> Channels.input("audit", codec: :text) |> Channels.into(Sink)
      c |> Channels.input_with("raw", &{:ok, &1}) |> Channels.into(Echo)
    end
  end

  defmodule Ask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "ask"
    @impl true
    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule Mixed do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "mixed"

    def channels(c) do
      echo = c |> Channels.input("in") |> Channels.into(Echo)
      echo |> Channels.output("json")
      echo |> Channels.into(Sink) |> Channels.output_with("own", & &1)

      c
      |> Channels.input("question")
      |> Channels.into(Ask)
      |> Channels.call("pricing", deadline_ms: 100)
      |> Channels.into(Sink)
    end
  end

  # The schema and codec its one entry declares are the calling test's.
  defmodule Broken do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "broken"

    def channels(c) do
      {schema, codec} = Process.get(:relay_api_schema)
      c |> Channels.input("in", schema: schema, codec: codec) |> Channels.into(Echo)
    end
  end

  def order, do: @order

  defp refused(schema, codec \\ :json) do
    Process.put(:relay_api_schema, {schema, codec})
    assert {:error, error} = Topology.build(Broken)
    error
  end

  test "each entry announces its format, schema, description and answer" do
    {:ok, topology} = Topology.build(Orders)

    assert Codec.api(topology) == [
             %{
               channel: "place",
               codec: "json",
               schema_json: Jason.encode!(@order),
               description: "Places an order",
               answer_codec: "json",
               answer_schema_json: ""
             },
             %{
               channel: "audit",
               codec: "text",
               schema_json: "",
               description: "",
               answer_codec: "",
               answer_schema_json: ""
             },
             %{
               channel: "raw",
               codec: "custom",
               schema_json: "",
               description: "",
               answer_codec: "json",
               answer_schema_json: ""
             }
           ]
  end

  test "outputs that disagree answer bytes, and a Response channel is no entry of the API" do
    {:ok, topology} = Topology.build(Mixed)

    assert [%{channel: "in", answer_codec: "bytes"}, %{channel: "question", answer_codec: ""}] =
             Codec.api(topology)
  end

  test "the UpdateFactories announcement carries the entries" do
    {:ok, topology} = Topology.build(Orders)
    run = Map.put(Codec.run(topology), :api, Codec.api(topology))

    {:update_factories, %{factories: [decl]}} =
      Codec.update_factories_command([{"orders", nil, run}])

    assert [%{channel: "place", description: "Places an order"}, %{codec: "text"}, _raw] =
             decl.api
  end

  test "a schema that is not an object with a type is refused at registration" do
    for schema <- ["object", %{"properties" => %{}}] do
      error = refused(schema)
      assert error.code == "topology.schema_invalid"

      assert %{"channel" => "in", "reason" => "a schema is a JSON object with a type"} =
               error.details
    end
  end

  test "a schema on a channel that is not json is refused at registration" do
    error = refused(@order, :text)
    assert error.code == "topology.schema_invalid"
    assert error.details["reason"] == "only a json channel carries a schema"
  end

  # A runner server answering each control call with the next reply.
  defp runner_server(replies) do
    test = self()
    spawn_link(fn -> serve(test, replies) end)
  end

  defp serve(_test, []), do: :ok

  defp serve(test, [reply | replies]) do
    receive do
      {:"$gen_cast", {:admin, from, ref, verb, args}} ->
        send(test, {:called, verb, Jason.decode!(args)})

        case reply do
          %Flezha.SDK.Error{} -> send(from, {:reply_ack, ref, nil, reply})
          json -> send(from, {:reply_ack, ref, json, nil})
        end
    end

    serve(test, replies)
  end

  test "a call sends the value as an output would and decodes each answer" do
    server =
      runner_server([
        ~s({"answer":{"status":"placed"},"answer_codec":"json"}),
        ~s({"answer":"ok","answer_codec":"text"}),
        ~s({"answer":"AQI=","answer_codec":"bytes"}),
        ~s({"accepted":true})
      ])

    assert {:ok, %{"status" => "placed"}} =
             Flezha.SDK.call(server, "orders", "place", %{"pair" => "EUR/USD"})

    assert_receive {:called, "Relay.call_channel",
                    ["orders", "place", payload, %{"payload" => "base64"}]}

    assert Base.decode64!(payload) == ~s({"pair":"EUR/USD"})

    assert {:ok, "ok"} = Flezha.SDK.call(server, "orders", "note", "hé")
    assert_receive {:called, _, ["orders", "note", payload, _]}
    assert Base.decode64!(payload) == "hé"

    assert {:ok, <<1, 2>>} = Flezha.SDK.call(server, "orders", "raw", <<7>>)
    assert {:ok, %{"accepted" => true}} = Flezha.SDK.call(server, "orders", "audit", "x")
  end

  test "a refused call answers the controller's error" do
    refusal = Flezha.SDK.Error.new("api.channel_unknown", channel: "plce", channels: "place")
    server = runner_server([refusal])

    assert {:error, %{code: "api.channel_unknown"}} =
             Flezha.SDK.call(server, "orders", "plce", %{})
  end
end
