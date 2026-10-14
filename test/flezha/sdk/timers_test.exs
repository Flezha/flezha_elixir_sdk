defmodule Flezha.SDK.TimersTest do
  @moduledoc """
  The host's copy of a Cell's pending timers and the session over a copy of
  it, which the timer cap and a cancel's answer read. Then the ctx's four
  timer calls over it, and their refusal where timers are unavailable.
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Ctx, Timers}

  @policy %{tasklet: "counter_probe_tasklet", cap: 10_000, clock: :wall}

  describe "the session" do
    test "register_after anchors on now and answers the absolute timestamp; a duplicate is no op" do
      session = Timers.session(Timers.new(), @policy, 1_000)
      assert {:ok, 31_000, session} = Timers.register_after(session, 30_000)
      assert {:ok, 31_000, session} = Timers.register_after(session, 30_000)
      assert {pending, [op]} = Timers.commit(session)
      assert op == Timers.register_op(:processing_time, 31_000)
      assert Timers.contains?(pending, :processing_time, 31_000)
    end

    test "past the cap a registration is refused naming the tasklet and the bound; the set is unchanged" do
      session = Timers.session(Timers.new(), %{@policy | cap: 1}, 1_000)
      {:ok, 31_000, session} = Timers.register_after(session, 30_000)

      assert {:error, %Flezha.SDK.Error{code: "runtime.timer_cap_exceeded", details: details},
              session} = Timers.register_after(session, 60_000)

      assert details == %{"tasklet" => "counter_probe_tasklet", "max" => 1}

      # Re-registering a pending timestamp is not an allocation.
      assert {:ok, 31_000, session} = Timers.register_after(session, 30_000)
      assert {pending, [_]} = Timers.commit(session)
      assert Timers.size(pending) == 1
    end

    test "an event-time register under a wall clock is refused naming the tasklet and the clock" do
      session = Timers.session(Timers.new(), @policy, 1_000)

      assert {:error,
              %Flezha.SDK.Error{
                code: "runtime.event_time_timer_without_event_time_clock",
                details: %{"tasklet" => "counter_probe_tasklet", "clock" => "wall"}
              }, session} = Timers.register(session, :event_time, 5_000)

      assert {pending, []} = Timers.commit(session)
      assert Timers.size(pending) == 0

      session = Timers.session(Timers.new(), %{@policy | clock: :event_time}, 1_000)
      assert {:ok, 5_000, session} = Timers.register(session, :event_time, 5_000)
      assert {pending, [_]} = Timers.commit(session)
      assert Timers.contains?(pending, :event_time, 5_000)
    end

    # A Cell the core restored holds timers this host copy never saw, so a
    # cancel is recorded whether or not the copy held it.
    test "a cancel answers whether it was pending here, and is recorded either way" do
      {pending, _} =
        Timers.new() |> Timers.session(@policy, 1_000) |> elem_register(30_000) |> Timers.commit()

      session = Timers.session(pending, @policy, 2_000)
      assert {false, session} = Timers.cancel(session, :processing_time, 5)
      assert {true, session} = Timers.cancel(session, :processing_time, 31_000)
      assert {pending, ops} = Timers.commit(session)

      assert ops == [
               Timers.cancel_op(:processing_time, 5),
               Timers.cancel_op(:processing_time, 31_000)
             ]

      assert Timers.size(pending) == 0
    end

    # The removal is op 0 so it rides the same record as what `on_timer`
    # applies: either both land or the whole firing repeats.
    test "a firing session opens on the removal, and now is the scheduled time" do
      {pending, _} =
        Timers.new() |> Timers.session(@policy, 1_000) |> elem_register(8_000) |> Timers.commit()

      session = Timers.session(pending, @policy, 9_000, {:processing_time, 9_000})
      assert Timers.now(session) == 9_000
      # A chained registration anchors on the firing that woke it.
      assert {:ok, 39_000, session} = Timers.register_after(session, 30_000)
      assert {pending, [removal, chained]} = Timers.commit(session)
      assert removal == Timers.cancel_op(:processing_time, 9_000)
      assert chained == Timers.register_op(:processing_time, 39_000)
      refute Timers.contains?(pending, :processing_time, 9_000)
      assert Timers.contains?(pending, :processing_time, 39_000)
    end
  end

  describe "the ctx" do
    # A compensate hook's ctx hands `timers: :unavailable`; a run's hands the
    # Cell's pending set.
    test "with timers unavailable every timer call is refused and nothing is armed" do
      ctx = Ctx.new(nil, nil, 1_000, timer_policy: @policy, timers: :unavailable)

      unavailable = {:error, Flezha.SDK.Error.new("runtime.timers_unavailable")}
      assert Ctx.timer_after(ctx, 1_000) == unavailable
      assert Ctx.timer_at_event_time(ctx, 5) == unavailable
      assert Ctx.cancel_processing_time_timer(ctx, 2_000) == unavailable
      assert Ctx.cancel_event_time_timer(ctx, 5) == unavailable
      assert {pending, []} = Ctx.take_timers(ctx)
      assert Timers.size(pending) == 0
    end

    test "a run's ctx arms the same call" do
      ctx = Ctx.new(nil, nil, 1_000, timer_policy: @policy)

      assert Ctx.timer_after(ctx, 1_000) == {:ok, 2_000}
      assert {pending, [op]} = Ctx.take_timers(ctx)
      assert op == Timers.register_op(:processing_time, 2_000)
      assert Timers.contains?(pending, :processing_time, 2_000)
    end
  end

  defp elem_register(session, delay) do
    {:ok, _at, session} = Timers.register_after(session, delay)
    session
  end
end
