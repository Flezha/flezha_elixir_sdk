defmodule Flezha.SDK.TimersTest do
  @moduledoc """
  The pending-timer set and the session over a copy of it: the twin of
  `flz_core/src/timer.rs`'s own tests, minus the arming (the BEAM's) and the
  registry (telemetry's).
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.Timers

  @policy %{procedure: "counter_probe_procedure", cap: 10_000, clock: :wall}

  describe "the pending set" do
    test "registration is idempotent and cancellation removes" do
      pending = Timers.new()
      assert {true, pending} = Timers.apply(pending, Timers.register_op(:processing_time, 31_000))

      assert {false, pending} =
               Timers.apply(pending, Timers.register_op(:processing_time, 31_000))

      assert Timers.size(pending) == 1
      assert {true, pending} = Timers.apply(pending, Timers.cancel_op(:processing_time, 31_000))
      assert Timers.empty?(pending)
    end

    test "the two kinds are separate sets" do
      pending =
        Timers.new()
        |> Timers.apply_all([
          Timers.register_op(:event_time, 100),
          Timers.register_op(:processing_time, 100)
        ])

      assert Timers.size(pending) == 2
      {true, pending} = Timers.apply(pending, Timers.cancel_op(:event_time, 100))
      assert Timers.contains?(pending, :processing_time, 100)
      refute Timers.contains?(pending, :event_time, 100)
    end

    test "due is ascending and inclusive of the bound" do
      pending =
        Timers.new()
        |> Timers.apply_all(Enum.map([300, 100, 200], &Timers.register_op(:event_time, &1)))

      assert Timers.due(pending, :event_time, 200) == [100, 200]
      assert Timers.due(pending, :event_time, 99) == []
      assert Timers.due(pending, :processing_time, 1_000) == []
    end

    # The compaction re-statement is the only record of the timers once the
    # ops below the snapshot are truncated.
    test "register_ops round-trips the whole set" do
      pending =
        Timers.new()
        |> Timers.apply_all([
          Timers.register_op(:event_time, 5),
          Timers.register_op(:processing_time, 9),
          Timers.register_op(:processing_time, 7)
        ])

      # Set equality, not tree equality: a balanced tree's shape follows its
      # insertion order.
      rebuilt = Timers.new() |> Timers.apply_all(Timers.register_ops(pending))
      assert Timers.all(rebuilt, :event_time) == Timers.all(pending, :event_time)
      assert Timers.all(rebuilt, :processing_time) == Timers.all(pending, :processing_time)
      assert Enum.map(Timers.register_ops(pending), & &1.scheduled_at) == [5, 7, 9]
    end
  end

  describe "the session" do
    test "register_after anchors on now and answers the absolute timestamp; a duplicate is no op" do
      session = Timers.session(Timers.new(), @policy, 1_000)
      assert {:ok, 31_000, session} = Timers.register_after(session, 30_000)
      assert {:ok, 31_000, session} = Timers.register_after(session, 30_000)
      assert {pending, [op], tally} = Timers.commit(session)
      assert op == Timers.register_op(:processing_time, 31_000)
      assert Timers.contains?(pending, :processing_time, 31_000)
      assert tally == %{registered: 1, fired: 0, cancelled: 0, refused: 0}
    end

    test "past the cap a registration is refused naming the procedure and the bound; the set is unchanged" do
      session = Timers.session(Timers.new(), %{@policy | cap: 1}, 1_000)
      {:ok, 31_000, session} = Timers.register_after(session, 30_000)

      assert {:error, {:timer_cap_exceeded, "counter_probe_procedure", 1}, session} =
               Timers.register_after(session, 60_000)

      # Re-registering a pending timestamp is not an allocation.
      assert {:ok, 31_000, session} = Timers.register_after(session, 30_000)
      assert {pending, [_], %{registered: 1, refused: 1}} = Timers.commit(session)
      assert Timers.size(pending) == 1
    end

    test "an event-time register under a wall clock is refused naming the procedure and the clock" do
      session = Timers.session(Timers.new(), @policy, 1_000)

      assert {:error,
              {:event_time_timer_without_event_time_clock, "counter_probe_procedure", :wall},
              session} = Timers.register(session, :event_time, 5_000)

      assert {pending, [], %{refused: 1}} = Timers.commit(session)
      assert Timers.empty?(pending)

      session = Timers.session(Timers.new(), %{@policy | clock: :event_time}, 1_000)
      assert {:ok, 5_000, session} = Timers.register(session, :event_time, 5_000)
      assert {pending, [_], _} = Timers.commit(session)
      assert Timers.contains?(pending, :event_time, 5_000)
    end

    test "a cancel produces an op only when something was pending" do
      {pending, _, _} =
        Timers.new() |> Timers.session(@policy, 1_000) |> elem_register(30_000) |> Timers.commit()

      session = Timers.session(pending, @policy, 2_000)
      assert {false, session} = Timers.cancel(session, :processing_time, 5)
      assert {true, session} = Timers.cancel(session, :processing_time, 31_000)
      assert {pending, [op], %{cancelled: 1}} = Timers.commit(session)
      assert op == Timers.cancel_op(:processing_time, 31_000)
      assert Timers.empty?(pending)
    end

    # The removal is op 0 so it rides the same record as what `on_timer`
    # applies: either both land or the whole firing repeats.
    test "a firing session opens on the removal, counted as fired, and now is the scheduled time" do
      {pending, _, _} =
        Timers.new() |> Timers.session(@policy, 1_000) |> elem_register(8_000) |> Timers.commit()

      session = Timers.session(pending, @policy, 9_000, {:processing_time, 9_000})
      assert Timers.now(session) == 9_000
      # A chained registration anchors on the firing that woke it.
      assert {:ok, 39_000, session} = Timers.register_after(session, 30_000)
      assert {pending, [removal, chained], tally} = Timers.commit(session)
      assert removal == Timers.cancel_op(:processing_time, 9_000)
      assert chained == Timers.register_op(:processing_time, 39_000)
      assert tally == %{registered: 1, fired: 1, cancelled: 0, refused: 0}
      refute Timers.contains?(pending, :processing_time, 9_000)
      assert Timers.contains?(pending, :processing_time, 39_000)
    end
  end

  defp elem_register(session, delay) do
    {:ok, _at, session} = Timers.register_after(session, delay)
    session
  end
end
