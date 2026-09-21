defmodule Flezha.SDK.RouteLatestTest do
  @moduledoc """
  The packaged latest-document router, run in local mode: each distinct
  document reaches a key once, on that key's own next traffic item.
  """
  # async: false: local mode's process-wide notice counter.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO, only: [with_io: 2]

  alias Flezha.SDK.{Channels, Cell, Local, Operative, Topology}

  defmodule RouterOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "router_op"

    def channels(c) do
      documents = Channels.input(c, "thresholds")
      trades = Channels.input(c, "trades")

      c
      |> Channels.route_latest(documents, trades, &(&1 |> String.split(":") |> hd()))
      |> Channels.sink_with("routed", fn {key, document} -> "#{key}=#{document}" end)
    end
  end

  defmodule NamedRouterOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "named_router_op"

    def channels(c) do
      documents = Channels.input(c, "thresholds")
      trades = Channels.input(c, "trades")

      c
      |> Channels.route_latest_named(documents, trades, & &1, "thresholds_router")
      |> Channels.sink()
    end
  end

  test "a document reaches each key once, on the key's own next trade" do
    {{:ok, run}, _notice} = with_io(:stderr, fn -> Local.start(RouterOp) end)
    on_exit(fn -> Local.stop(run) end)

    reader = self()
    :ok = Local.on_sink(run, "routed", &send(reader, {:routed, &1.payload}))

    feed(run, "thresholds", "v1")
    feed(run, "trades", "A:1")
    feed(run, "trades", "B:1")
    feed(run, "thresholds", "v1")
    feed(run, "trades", "A:2")

    assert routed() == ["A=v1", "B=v1"]

    assert [{"route_latest", :single, [{_key, router}]}] =
             Operative.cells(Local.operative(run))

    assert {:ok, %{version: 1, delivered: %{"A" => 1, "B" => 1}}, _seq} =
             Cell.read(router, 1_000)

    feed(run, "thresholds", "v2")
    feed(run, "trades", "A:3")

    assert routed() == ["A=v2"]
  end

  test "the router node is `route_latest`, and the named spelling names it" do
    assert {:ok, topology} = Topology.build(RouterOp)
    assert {:ok, node} = Topology.node_of(topology, "route_latest")
    assert topology.node_name[node] == nil

    assert {:ok, named} = Topology.build(NamedRouterOp)
    assert {:ok, node} = Topology.node_of(named, "route_latest")
    assert named.node_name[node] == "thresholds_router"
  end

  # Each item settles before the next, so the order across the two entries is
  # the order written here.
  defp feed(run, channel, payload) do
    :ok = Local.feed(run, channel, payload)
    :ok = Local.run_until_idle(run, 5_000)
  end

  defp routed(acc \\ []) do
    receive do
      {:routed, payload} -> routed([payload | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
