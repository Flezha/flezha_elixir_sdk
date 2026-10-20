defmodule Flezha.SDK.TaskletTestHarnessTest do
  use ExUnit.Case, async: true

  alias Flezha.SDK.{TaskletTestHarness, TimerFiring}
  alias Flezha.SDK.TaskletTestHarness.Outcome

  defmodule Counter do
    use Flezha.SDK.Tasklet, name: "harness_counter"

    defchange(add(count, n), do: count + n)

    @impl true
    def init_state(_key), do: 0

    @impl true
    def run(n, count, ctx) do
      count = add(count, ctx, n)
      emit(ctx, count)
      {:ok, count}
    end
  end

  # Its fold reads the process dictionary, so a replay folds to another state.
  defmodule Impure do
    @behaviour Flezha.SDK.Tasklet
    def name, do: "harness_impure"
    def init_state(_key), do: 0

    def apply_change(count, n) do
      bump = Process.get(:harness_impure_bump, 0)
      Process.put(:harness_impure_bump, bump + 1)
      count + n + bump
    end

    def run(n, count, ctx), do: {:ok, Flezha.SDK.Ctx.apply(ctx, count, n)}
  end

  defmodule Session do
    use Flezha.SDK.Tasklet, name: "harness_session"

    @impl true
    def init_state(_key), do: nil

    @impl true
    def event_time(at), do: at

    @impl true
    def run(at, _last, ctx) do
      {:ok, _at} = timer_at_event_time(ctx, at + 100)
      {:ok, at}
    end

    @impl true
    def on_timer(%TimerFiring{} = firing, last, ctx) do
      emit(ctx, {firing, now(ctx)})
      {:ok, last}
    end
  end

  defmodule Calls do
    use Flezha.SDK.Tasklet, name: "harness_calls"

    @impl true
    def init_state(_key), do: nil

    @impl true
    def run(name, state, ctx) do
      case effect(ctx, name, fn -> :called end) do
        {:ok, _} -> {:ok, state}
        error -> error
      end
    end
  end

  test "a counter fed three inputs holds their sum and replays to it" do
    h = TaskletTestHarness.new(Counter)

    {outcomes, h} =
      Enum.map_reduce([1, 2, 3], h, fn n, h -> TaskletTestHarness.feed(h, n) end)

    assert Enum.map(outcomes, & &1.outputs) == [[1], [3], [6]]
    assert Enum.map(outcomes, & &1.changes) == [[{:add, 1}], [{:add, 2}], [{:add, 3}]]
    assert TaskletTestHarness.state(h) == 6
    assert TaskletTestHarness.replay_equals(h) == :ok
  end

  test "an apply_change that is not a pure fold is caught by replay_equals" do
    h = TaskletTestHarness.new(Impure)
    {%Outcome{result: :ok}, h} = TaskletTestHarness.feed(h, 1)
    {%Outcome{result: :ok}, h} = TaskletTestHarness.feed(h, 1)

    assert TaskletTestHarness.state(h) == 3
    assert TaskletTestHarness.replay_equals(h) == {:mismatch, 3, 7}
  end

  test "an event-time timer fires through on_timer/3 at its scheduled time, once" do
    {%Outcome{timers: [op]}, h} = TaskletTestHarness.feed(TaskletTestHarness.new(Session), 900)
    assert op == %{kind: :event_time, scheduled_at: 1_000, register: true}

    assert {[], h} = TaskletTestHarness.advance_event_time(h, 999)
    {[fired], h} = TaskletTestHarness.advance_event_time(h, 1_000)
    firing = %TimerFiring{kind: :event_time, scheduled_at: 1_000}
    assert fired.outputs == [{firing, 1_000}]
    assert {[], _h} = TaskletTestHarness.advance_event_time(h, 5_000)
  end

  test "a timer fired by hand on a tasklet without on_timer/3 answers the park's error" do
    {outcome, _h} = TaskletTestHarness.fire_timer(TaskletTestHarness.new(Counter), :event_time, 5)
    assert {:error, {:non_retryable, reason}} = outcome.result
    assert reason =~ "on_timer/3"
  end

  test "an effect name over the core's bound is refused with the core's effect.bad_name" do
    h = TaskletTestHarness.new(Calls)

    {outcome, h} = TaskletTestHarness.feed(h, String.duplicate("m", 256))
    assert {:error, {:non_retryable, %Flezha.SDK.Error{code: "effect.bad_name"}}} = outcome.result

    {outcome, _h} = TaskletTestHarness.feed(h, String.duplicate("m", 255))

    assert {:error, {:non_retryable, %Flezha.SDK.Error{code: "effect.outside_invocation"}}} =
             outcome.result
  end
end
