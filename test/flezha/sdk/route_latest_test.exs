defmodule Flezha.SDK.RouteLatestTest do
  @moduledoc """
  The packaged latest-document router, run in local mode: each distinct
  document reaches a key once, on that key's own next traffic item.
  """
  # async: false: one local run per process at a time.
  use ExUnit.Case, async: false

  alias Flezha.SDK.{Channels, Declaration, Local}

  defmodule RouterOp do
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "router_op"

    def channels(c) do
      documents = Channels.input(c, "thresholds", codec: :text)
      trades = Channels.input(c, "trades", codec: :text)

      c
      |> Channels.route_latest(documents, trades, &(&1 |> String.split(":") |> hd()))
      |> Channels.output_with("routed", fn {key, document} -> "#{key}=#{document}" end)
    end
  end

  defmodule NamedRouterOp do
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "named_router_op"

    def channels(c) do
      documents = Channels.input(c, "thresholds")
      trades = Channels.input(c, "trades")

      c
      |> Channels.route_latest_named(documents, trades, & &1, "thresholds_router")
      |> Channels.output()
    end
  end

  test "a document reaches each key once, on the key's own next trade" do
    {:ok, run} = Local.start(RouterOp)
    on_exit(fn -> Local.stop(run) end)

    reader = self()
    :ok = Local.on_output(run, "routed", &send(reader, {:routed, &1.payload}))

    feed(run, "thresholds", "v1")
    feed(run, "trades", "A:1")
    feed(run, "trades", "B:1")
    feed(run, "thresholds", "v1")
    feed(run, "trades", "A:2")

    assert routed() == ["A=v1", "B=v1"]

    feed(run, "thresholds", "v2")
    feed(run, "trades", "A:3")

    assert routed() == ["A=v2"]
  end

  test "the router node is `route_latest`, and the named spelling names it" do
    assert tasklet_names(RouterOp) == ["route_latest"]
    assert tasklet_names(NamedRouterOp) == ["thresholds_router"]
  end

  defp tasklet_names(module) do
    {:ok, class} = Declaration.build(module)
    for tasklet <- Jason.decode!(class.json)["tasklets"], do: tasklet["name"]
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
