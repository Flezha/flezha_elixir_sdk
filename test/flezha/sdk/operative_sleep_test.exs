defmodule Flezha.SDK.OperativeSleepTest do
  @moduledoc """
  A stream instance whose class sleeps ships `IterPark` at iteration `0` once
  every Cell has been quiet for the idle window, and not while one still holds
  work or was active inside it; a class that does not sleep never does.
  """
  # Sets FLEZHA_PARK_IDLE_MS for the instances it starts.
  use ExUnit.Case, async: false

  alias Flezha.SDK.{Channels, Counters, Operative, Topology}
  alias Flezha.SDK.Operative.Inbox
  alias Flezha.SDK.Test.StubRunner

  @window_ms 150

  # Journals nothing: with no stream bound here, a record would be held, and a
  # Cell holding one is rightly not quiet.
  defmodule Seen do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "seen"
    def init_state(_), do: nil

    def run(input, s, _ctx) do
      if input == "slow", do: Process.sleep(4 * 150)
      {:ok, s}
    end
  end

  defmodule Sleeping do
    use Flezha.SDK.StreamOperative, factory_id: "sleeping", sleep_when_idle: true
    def channels(c), do: c |> input("in") |> key_by(& &1) |> into(Seen)
  end

  defmodule Awake do
    use Flezha.SDK.StreamOperative, factory_id: "awake"
    def channels(c), do: c |> Channels.input("in") |> Channels.into(Seen)
  end

  setup do
    previous = System.get_env("FLEZHA_PARK_IDLE_MS")
    System.put_env("FLEZHA_PARK_IDLE_MS", Integer.to_string(@window_ms))

    on_exit(fn ->
      if previous,
        do: System.put_env("FLEZHA_PARK_IDLE_MS", previous),
        else: System.delete_env("FLEZHA_PARK_IDLE_MS")
    end)
  end

  defp start(module, opts \\ []) do
    assert {:ok, topology} = Topology.build(module)
    {:ok, runner} = StubRunner.start_link(test: self())

    {:ok, pid} =
      Operative.start_link(
        [operative_id: "op-#{inspect(module)}", topology: topology, counters: Counters.new()] ++
          [runner: runner] ++ opts
      )

    pid
  end

  test "an idle stream that sleeps ships IterPark at iteration 0, once" do
    started = System.monotonic_time(:millisecond)
    op = start(Sleeping)
    Inbox.input(op, "in", "a", %{})

    assert_receive {:shipped, {:iter_park, %{iter: 0, wake_deadline_ms: 0}}}, 5_000
    assert System.monotonic_time(:millisecond) - started >= @window_ms
    refute_receive {:shipped, {:iter_park, _}}, 4 * @window_ms
  end

  test "a Cell still running keeps the stream awake until it is done" do
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

  test "a stream that does not declare sleeping never ships IterPark" do
    op = start(Awake)
    Inbox.input(op, "in", "a", %{})
    refute_receive {:shipped, {:iter_park, _}}, 4 * @window_ms
    assert Process.alive?(op)
  end
end
