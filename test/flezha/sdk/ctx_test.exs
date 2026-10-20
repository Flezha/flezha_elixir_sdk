defmodule Flezha.SDK.CtxTest do
  @moduledoc """
  The ctx's four timer calls: what each answers, and the ops an item hands
  the core, which judges them when it settles. Then their refusal where
  timers are unavailable, and the stable id's cross-language vector.
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.Ctx

  defp op(kind, at, register), do: %{kind: kind, scheduled_at: at, register: register}

  test "stable_id matches the cross-language vector" do
    assert Ctx.derive_stable_id("instance/op/key", 42, 0) == "bbf81fd4955afd1724203700a240e822"
    assert Ctx.derive_stable_id("instance/op/key", 42, 1) == "bbf81fd4965afd1724203700a240e95d"
    assert Ctx.derive_stable_id("", 0, 0) == "15915375576d2f389a938dc3a9940c9d"
  end

  test "a registration answers its absolute timestamp, anchored on now, and each call is one op" do
    ctx = Ctx.new(nil, nil, 1_000)

    assert Ctx.timer_after(ctx, 30_000) == {:ok, 31_000}
    assert Ctx.timer_after(ctx, 30_000) == {:ok, 31_000}
    assert Ctx.timer_at_event_time(ctx, 5_000) == {:ok, 5_000}

    assert Ctx.take_timers(ctx) == [
             op(:processing_time, 31_000, true),
             op(:processing_time, 31_000, true),
             op(:event_time, 5_000, true)
           ]

    assert Ctx.take_timers(ctx) == [], "the ops are taken once"
  end

  test "a cancel answers :ok and hands its op, pending or not" do
    ctx = Ctx.new(nil, nil, 1_000)

    assert Ctx.cancel_processing_time_timer(ctx, 5) == :ok
    assert Ctx.cancel_event_time_timer(ctx, 7) == :ok
    assert Ctx.take_timers(ctx) == [op(:processing_time, 5, false), op(:event_time, 7, false)]
  end

  # The core adds a firing's own removal; `now` is the scheduled time.
  test "a firing's ctx hands only what on_timer registered, anchored on the firing" do
    ctx = Ctx.new(nil, nil, 9_000)

    assert Ctx.timer_after(ctx, 30_000) == {:ok, 39_000}
    assert Ctx.take_timers(ctx) == [op(:processing_time, 39_000, true)]
  end

  # A compensate hook's ctx hands `timers?: false`.
  test "with timers unavailable every timer call is refused and nothing is armed" do
    ctx = Ctx.new(nil, nil, 1_000, timers?: false)

    unavailable = {:error, Flezha.SDK.Error.new("runtime.timers_unavailable")}
    assert Ctx.timer_after(ctx, 1_000) == unavailable
    assert Ctx.timer_at_event_time(ctx, 5) == unavailable
    assert Ctx.cancel_processing_time_timer(ctx, 2_000) == unavailable
    assert Ctx.cancel_event_time_timer(ctx, 5) == unavailable
    assert Ctx.take_timers(ctx) == []
  end
end
