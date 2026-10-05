defmodule Flezha.SDK.RelaySleepTest do
  @moduledoc """
  A flow instance whose class sleeps ships `IterPark` at iteration `0` once
  every Cell has been quiet for its `sleep_after`, and not while one still
  holds work or was active inside it; a class that does not sleep never does.
  A flow instance ends from inside, on a tasklet's `complete/2` or its class's
  `max_duration_ms`, once its Cells hold nothing.
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Channels, Counters, Relay, Topology}
  alias Flezha.SDK.Relay.Inbox
  alias Flezha.SDK.Test.StubRunner

  @window_ms 150

  # Journals nothing: with no stream bound here, a record would be held, and a
  # Cell holding one is rightly not quiet.
  defmodule Seen do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "seen"

    @impl true
    def run(input, s, _ctx) do
      if input == "slow", do: Process.sleep(4 * 150)
      {:ok, s}
    end
  end

  defmodule Sleeping do
    use Flezha.SDK.Relay, factory_id: "sleeping", durable: {:local, sleep_after: 150}
    def channels(c), do: c |> input("in") |> key_by(& &1) |> into(Seen)
  end

  defmodule Awake do
    use Flezha.SDK.Relay, factory_id: "awake", durable: {:local, sleep_after: false}
    def channels(c), do: c |> Channels.input("in") |> Channels.into(Seen)
  end

  # Completes on "done", after taking as long as "slow" asks.
  defmodule Finisher do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "finisher"

    @impl true
    def run(input, s, ctx) do
      if input == "slow", do: Process.sleep(150)
      if input == "done", do: complete(ctx, %{"last" => s})
      {:ok, input}
    end
  end

  defmodule Completing do
    use Flezha.SDK.Relay, factory_id: "completing"
    def channels(c), do: c |> input("in") |> key_by(& &1) |> into(Finisher)
  end

  defmodule Capped do
    use Flezha.SDK.Relay, factory_id: "capped", max_duration_ms: 100
    def channels(c), do: c |> input("in") |> into(Finisher)
  end

  defp start(module, opts \\ []) do
    assert {:ok, topology} = Topology.build(module)
    {:ok, runner} = StubRunner.start_link(test: self())

    {:ok, pid} =
      Relay.Server.start_link(
        [relay_id: "op-#{inspect(module)}", topology: topology, counters: Counters.new()] ++
          [runner: runner] ++ opts
      )

    pid
  end

  test "an idle flow instance that sleeps ships IterPark at iteration 0, once" do
    started = System.monotonic_time(:millisecond)
    op = start(Sleeping)
    Inbox.input(op, "in", "a", %{})

    assert_receive {:shipped, {:iter_park, %{iter: 0, wake_deadline_ms: 0}}}, 5_000
    assert System.monotonic_time(:millisecond) - started >= @window_ms
    refute_receive {:shipped, {:iter_park, _}}, 4 * @window_ms
  end

  test "a Cell still running keeps the relay awake until it is done" do
    started = System.monotonic_time(:millisecond)
    op = start(Sleeping)
    Inbox.input(op, "in", "slow", %{})

    assert_receive {:shipped, {:iter_park, %{iter: 0}}}, 5_000
    assert System.monotonic_time(:millisecond) - started >= 4 * @window_ms + @window_ms
  end

  test "the owed input tail holds the sleep until it arrives" do
    op = start(Sleeping, owed_inbox: 1)
    refute_receive {:shipped, {:iter_park, _}}, 3 * @window_ms

    Inbox.input(op, "in", "a", %{"flz.offset" => "0"})
    assert_receive {:shipped, {:iter_park, %{iter: 0}}}, 5_000
  end

  test "a flow class whose sleep_after is false never ships IterPark" do
    op = start(Awake)
    Inbox.input(op, "in", "a", %{})
    refute_receive {:shipped, {:iter_park, _}}, 4 * @window_ms
    assert Process.alive?(op)
  end

  test "complete/2 ends a flow instance after the work its Cells hold" do
    started = System.monotonic_time(:millisecond)
    op = start(Completing)
    Inbox.input(op, "in", "slow", %{})
    Inbox.input(op, "in", "done", %{})

    assert_receive {:shipped, {:relay_completed, done}}, 5_000
    assert System.monotonic_time(:millisecond) - started >= 150, "the slow Cell drained first"
    assert done.status == "completed"
    assert Jason.decode!(done.result) == %{"last" => nil}
    refute_receive {:shipped, {:relay_completed, _}}, 3 * @window_ms
  end

  test "a flow class's max_duration_ms ends it as cancelled" do
    op = start(Capped)
    Inbox.input(op, "in", "a", %{})

    assert_receive {:shipped, {:relay_completed, %{status: "cancelled"}}}, 5_000
    assert Process.alive?(op)
  end
end
