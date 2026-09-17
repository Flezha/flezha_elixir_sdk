defmodule Flezha.SDK.IntervalTest do
  use ExUnit.Case, async: true

  alias Flezha.SDK.Entity.Interval

  defp output(payload), do: %{channel: "out", key: "k", metadata: [], payload: payload}

  defp invocation(changes, outputs, offset) do
    %{
      changes: changes,
      timers: [],
      outputs: outputs,
      offset: offset,
      event_time: 7,
      checkpointed: false,
      parked: false
    }
  end

  defp at_checkpoint(every, max_delay_ms),
    do: %{every: every, outputs: :at_checkpoint, max_delay_ms: max_delay_ms}

  defp as_produced(every), do: %{every: every, outputs: :as_produced, max_delay_ms: nil}

  defp payloads(record), do: Enum.map(record.outputs, & &1.payload)

  test "per-input ships each effectful invocation and nothing else" do
    staged = %Interval{}

    assert {shipped, staged} = Interval.fold(staged, nil, invocation([1], [output("a")], 4), 0)
    assert shipped.changes == [1]
    assert shipped.offset == 4
    assert length(shipped.outputs) == 1

    assert {nil, _staged} = Interval.fold(staged, nil, invocation([], [], 5), 0)
  end

  test "as-produced flushes mid-interval outputs acknowledging nothing" do
    policy = as_produced(nil)

    assert {shipped, staged} =
             Interval.fold(%Interval{}, policy, invocation([1], [output("a")], 4), 0)

    assert shipped.changes == []
    assert shipped.offset == nil
    assert length(shipped.outputs) == 1
    assert Enum.reverse(staged.changes) == [1]
  end

  test "at-checkpoint holds every output until the interval closes" do
    policy = at_checkpoint(3, 50)

    staged =
      Enum.reduce([{1, 10}, {2, 11}], %Interval{}, fn {n, offset}, staged ->
        assert {nil, staged} =
                 Interval.fold(staged, policy, invocation([n], [output("held")], offset), 0)

        staged
      end)

    assert {closing, staged} =
             Interval.fold(staged, policy, invocation([3], [output("last")], 12), 0)

    assert closing.changes == [1, 2, 3]
    assert payloads(closing) == ["held", "held", "last"]
    assert closing.offset == 12
    refute Interval.open?(staged)
  end

  test "an explicit checkpoint closes early" do
    policy = at_checkpoint(500, 50)

    assert {nil, staged} =
             Interval.fold(%Interval{}, policy, invocation([1], [output("a")], 1), 0)

    closing = %{invocation([2], [output("b")], 2) | checkpointed: true}
    assert {shipped, _staged} = Interval.fold(staged, policy, closing, 0)
    assert shipped.changes == [1, 2]
    assert length(shipped.outputs) == 2
  end

  test "the delay closes a quiet Entity's interval, and a spent deadline does nothing" do
    policy = at_checkpoint(nil, 50)

    assert {nil, staged} =
             Interval.fold(%Interval{}, policy, invocation([1], [output("a")], 9), 1_000)

    assert Interval.deadline_ms(staged, policy) == 1_050
    refute Interval.due?(staged, policy, 1_049)
    assert Interval.due?(staged, policy, 1_050)

    assert {shipped, staged} = Interval.close_on_deadline(staged)
    assert length(shipped.outputs) == 1
    assert shipped.offset == 9
    assert shipped.event_time == 7

    assert {nil, _staged} = Interval.close_on_deadline(staged)
    assert Interval.deadline_ms(staged, policy) == nil
  end

  test "a park closes the interval so the poison input never replays" do
    policy = at_checkpoint(500, 50)

    assert {nil, staged} =
             Interval.fold(%Interval{}, policy, invocation([1], [output("a")], 1), 0)

    parking = %{invocation([], [], 2) | parked: true}
    assert {shipped, _staged} = Interval.fold(staged, policy, parking, 0)
    assert shipped.offset == 2
    assert shipped.changes == [1]
  end

  test "a restore discards the open interval entirely" do
    policy = at_checkpoint(500, 50)

    assert {nil, staged} =
             Interval.fold(%Interval{}, policy, invocation([1], [output("a")], 1), 0)

    staged = Interval.clear(staged)

    assert staged.changes == []
    assert staged.outputs == []
    assert staged.offset == nil
    refute Interval.open?(staged)
  end
end
