defmodule Flezha.SDK.TaskletClockTest do
  @moduledoc """
  The idle TTL in whole seconds, the clock a tasklet declares by what it
  writes, and the refusal of the names both replaced: at compile time in the
  `use` form, at registration in the hand-written one.
  """
  use ExUnit.Case, async: true

  import Flezha.SDK.Test.Refused

  alias Flezha.SDK.{Declaration, Tasklet}

  defmodule Session do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "session", idle_ttl_seconds: 1

    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule Nochange do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "nochange", logical_clock: true

    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule NochangeRelay do
    @moduledoc false
    use Flezha.SDK.Relay, factory_id: "nochange_relay"

    @impl true
    def channels(c), do: c |> input("in") |> into(Nochange)
  end

  test "the idle TTL is a whole number of seconds" do
    assert Session.idle_ttl_seconds() == 1
    assert Tasklet.idle_ttl_ms(Session) == 1_000
    assert Tasklet.idle_ttl_ms(Nochange) == nil

    for value <- ["0.2", "0", "-1", ~s|"1"|] do
      error =
        refused("""
        use Flezha.SDK.Tasklet, name: "p", idle_ttl_seconds: #{value}
        def run(_i, s, _c), do: {:ok, s}
        """)

      assert error.description =~ "idle_ttl_seconds #{value}; it takes a positive integer"
    end

    error =
      refused("""
      use Flezha.SDK.Tasklet, name: "p", logical_clock: :yes
      def run(_i, s, _c), do: {:ok, s}
      """)

    assert error.description =~ "logical_clock :yes; it takes true or false"
  end

  test "two clocks do not compile, in a tasklet or in a short form" do
    for line <- [
          ~s|Tasklet, name: "quiet", logical_clock: true|,
          ~s|Relay, factory_id: "quiet", logical_clock: true|
        ] do
      error =
        refused("""
        use Flezha.SDK.#{line}
        def event_time(_input), do: 0
        def run(_i, s, _c), do: {:ok, s}
        """)

      assert error.description =~ "event_time/1" and error.description =~ "logical_clock: true"

      assert error.description =~ ~r/^Flezha\.SDK\.Refused\d+ defines/,
             "names the module the author wrote, read: #{error.description}"
    end
  end

  test "a logical clock on a changeless tasklet compiles and is refused at registration" do
    assert Nochange.logical_clock() == true
    assert Tasklet.clock(Nochange) == :logical

    assert {:error, %Flezha.SDK.Error{code: "topology.unservable_clock", details: details}} =
             Declaration.check(NochangeRelay)

    assert details["tasklet"] == Nochange.name()
    assert length(details["issues"]) == 1
  end

  test "a retired option or callback does not compile, naming what replaced it" do
    for {option, instead} <- [
          {"cell_idle_ttl_ms: 200", "`idle_ttl_seconds:`"},
          {"clock: :logical", "`logical_clock: true`"}
        ],
        macro <- [~s|Tasklet, name: "p"|, ~s|Relay, factory_id: "p"|] do
      error =
        refused("""
        use Flezha.SDK.#{macro}, #{option}
        def run(_i, s, _c), do: {:ok, s}
        """)

      assert error.description =~ "no option" and error.description =~ instead
    end

    for {definition, instead} <- [
          {"def clock, do: :logical", "`logical_clock/0`"},
          {"defp clock, do: :event_time", "`logical_clock/0`"},
          {"def event_time_of(_input), do: 0", "`event_time/1`"},
          {"defp event_time_of(_input), do: 0", "`event_time/1`"},
          {"def cell_idle_ttl_ms, do: 20", "`idle_ttl_seconds/0`"}
        ],
        macro <- [~s|Tasklet, name: "p"|, ~s|Relay, factory_id: "p"|] do
      error =
        refused("""
        use Flezha.SDK.#{macro}
        #{definition}
        def run(_i, s, _c), do: {:ok, s}
        """)

      assert error.description =~ "no longer a tasklet callback" and
               error.description =~ instead,
             "read: #{error.description}"
    end
  end

  # A hand-written tasklet is the one form that can write both clocks, or a
  # callback that no longer exists, so registration refuses each by name.
  defmodule TwoClocksByHand do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "two_clocks"
    def init_state(_key), do: 0
    def logical_clock, do: true
    def event_time(_input), do: 0
    def apply_change(count, n), do: count + n
    def run(_input, count, _ctx), do: {:ok, count}
  end

  defmodule RetiredByHand do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "retired"
    def init_state(_key), do: nil
    def clock, do: :logical
    def event_time_of(_input), do: 0
    def cell_idle_ttl_ms, do: 20
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule TwoClocksRelay do
    @moduledoc false
    use Flezha.SDK.Relay, factory_id: "two_clocks_relay"

    @impl true
    def channels(c), do: c |> input("in") |> into(TwoClocksByHand)
  end

  defmodule RetiredRelay do
    @moduledoc false
    use Flezha.SDK.Relay, factory_id: "retired_relay"

    @impl true
    def channels(c), do: c |> input("in") |> into(RetiredByHand)
  end

  test "a hand-written tasklet with two clocks, or a retired callback, is refused at registration" do
    assert {:error, %Flezha.SDK.Error{code: "topology.two_clocks", details: two}} =
             Declaration.check(TwoClocksRelay)

    assert two["tasklet"] == TwoClocksByHand.name()

    assert {:error, %Flezha.SDK.Error{details: %{"issues" => issues}}} =
             Declaration.check(RetiredRelay)

    assert for(
             %{"code" => "topology.retired_callback", "details" => details} <- issues,
             do: {details["callback"], details["instead"]}
           ) ==
             [
               {"cell_idle_ttl_ms/0", "`idle_ttl_seconds/0`, in whole seconds"},
               {"clock/0", "`logical_clock/0` answering true, or `event_time/1` for event time"},
               {"event_time_of/1", "`event_time/1`"}
             ]
  end
end
