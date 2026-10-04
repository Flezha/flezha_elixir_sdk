defmodule Flezha.SDK.SendTest do
  @moduledoc """
  `Flezha.SDK.send/5`: the controller request it makes, and the local send,
  which enters an operator this runner holds where a data-stream frame enters.
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Channels, Topology}

  defmodule Echo do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "echo"
    @impl true
    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule Relay do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "relay"
    @impl true
    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule EchoOp do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def factory_id, do: "send_echo"

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.into(Echo)
        |> Channels.into(Relay)
        |> Channels.sink("out")
  end

  # A runner server that records each control call and holds one operator,
  # `orders`, whose inbox is the test process.
  defp runner_server do
    test = self()
    {:ok, topology} = Topology.build(EchoOp)
    spawn_link(fn -> serve(test, topology) end)
  end

  defp serve(test, topology) do
    receive do
      {:"$gen_cast", {:admin, from, ref, verb, args}} ->
        send(test, {:called, verb, Jason.decode!(args)})
        send(from, {:reply_ack, ref, ~s({"to":"orders","verdict":"absorbed"}), nil})

      {:"$gen_call", from, {:local_target, "orders"}} ->
        GenServer.reply(from, {:ok, test, topology})

      {:"$gen_call", from, {:local_target, other}} ->
        GenServer.reply(
          from,
          {:error, Flezha.SDK.Error.new("runtime.operator_not_local", operator: other)}
        )

      {:"$gen_call", from, :operators} ->
        GenServer.reply(from, {:ok, ["orders"]})
    end

    serve(test, topology)
  end

  test "a send names its options only when given, and answers the verdict" do
    server = runner_server()

    assert {:ok, %{"verdict" => "absorbed"}} = Flezha.SDK.send(server, "orders", "o-41", "in")
    assert_receive {:called, "Operator.send", ["orders", "o-41", "in"]}

    Flezha.SDK.send(server, "orders", "o-41", "in", msg_id: "orders/0/41", key: "EURUSD")

    assert_receive {:called, "Operator.send",
                    ["orders", "o-41", "in", %{"msg_id" => "orders/0/41", "key" => "EURUSD"}]}
  end

  test "a local send enters the operator's inbox and never reaches the controller" do
    server = runner_server()

    assert {:ok, nil} = Flezha.SDK.send(server, "orders", "hello", "in", local: true)
    assert_receive {:"$gen_cast", {:input, "in", "hello", %{"channel" => "in"}}}
    refute_received {:called, _, _}
    assert {:ok, ["orders"]} = Flezha.SDK.operators(server)
  end

  test "a local send is refused for what this runner cannot take" do
    server = runner_server()

    local = fn op, channel, opts ->
      Flezha.SDK.send(server, op, "x", channel, [local: true] ++ opts)
    end

    assert {:error, %{code: "runtime.operator_not_local"}} = local.("elsewhere", "in", [])
    assert {:error, %{code: "runtime.channel_undeclared"}} = local.("orders", "nope", [])
    assert {:error, %{code: "runtime.channel_wrong_kind"}} = local.("orders", "out", [])

    assert {:error, %{code: "runtime.channel_wrong_kind", details: %{"declared" => "inter_task"}}} =
             local.("orders", "echo", [])

    for other <- [msg_id: "m-1", key: "k"] do
      assert {:error, %{code: "usage.options_conflict"}} = local.("orders", "in", [other])
    end

    refute_received {:"$gen_cast", {:input, _, _, _}}
    refute_received {:called, _, _}
  end
end
