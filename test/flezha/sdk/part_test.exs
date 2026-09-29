defmodule Flezha.SDK.PartTest do
  @moduledoc """
  `part/2`: a preview of an invocation's output, sent from inside `run/3` on
  the node's sink route. Parts are numbered per attempt from `0`, carry the
  attempt, the Cell and its key, leave before `run/3` returns, and are refused
  on a channel with no sink (a `response()` end included) and through a ctx
  whose invocation has returned. The frames match the shared fixture the core
  produces.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Flezha.SDK.{Contract, Counters, LocalStream, Operator, Topology}
  alias Flezha.SDK.Bounded.{Driver, Spec}
  alias Flezha.SDK.Operator.Inbox
  alias Flezha.SDK.Test.StubRunner

  @probe {__MODULE__, :probe}
  @failed {__MODULE__, :failed}
  @kept {__MODULE__, :kept}

  # `n` sends the parts `1..n` and emits `done:<n>`; `retry:<n>` fails its
  # first attempt after its parts; `hold` sends a part and waits for `:go`.
  defmodule P0 do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "p0"

    @impl true
    def run("hold", state, ctx) do
      :ok = part(ctx, "early")
      send(:persistent_term.get({Flezha.SDK.PartTest, :probe}), {:holding, self()})

      receive do
        :go -> :ok
      end

      emit(ctx, "late")
      {:ok, state}
    end

    def run("retry:" <> n = input, state, ctx) do
      parts(ctx, String.to_integer(n))
      failed = :persistent_term.get({Flezha.SDK.PartTest, :failed}, MapSet.new())

      if MapSet.member?(failed, input) do
        emit(ctx, "done:" <> n)
        {:ok, state}
      else
        :persistent_term.put({Flezha.SDK.PartTest, :failed}, MapSet.put(failed, input))
        {:error, "first attempt"}
      end
    end

    def run(n, state, ctx) do
      parts(ctx, String.to_integer(n))
      emit(ctx, "done:" <> n)
      {:ok, state}
    end

    defp parts(ctx, n), do: for(i <- 1..n//1, do: :ok = part(ctx, Integer.to_string(i)))
  end

  # Answers what `part/2` said, to the test.
  defmodule Refused do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "refused"

    @impl true
    def run("keep", state, ctx) do
      :persistent_term.put({Flezha.SDK.PartTest, :kept}, ctx)
      {:ok, state}
    end

    def run("late", state, _ctx) do
      kept = :persistent_term.get({Flezha.SDK.PartTest, :kept})
      send(:persistent_term.get({Flezha.SDK.PartTest, :probe}), {:part, part(kept, "x")})
      {:ok, state}
    end

    def run(_input, state, ctx) do
      send(:persistent_term.get({Flezha.SDK.PartTest, :probe}), {:part, part(ctx, "x")})
      {:ok, state}
    end
  end

  defmodule Log do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "log"

    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule SinkOp do
    use Flezha.SDK.UnboundedOperator, factory_id: "part_sink"

    def channels(c),
      do: c |> input("in") |> retry_backoff_ms(1) |> into(P0) |> sink("answer")
  end

  defmodule KeyedOp do
    use Flezha.SDK.UnboundedOperator, factory_id: "part_keyed"

    def channels(c),
      do: c |> input("in") |> key_by(fn _ -> "s-7" end) |> into(P0) |> sink("answer")
  end

  defmodule EdgeOp do
    use Flezha.SDK.UnboundedOperator, factory_id: "part_edge"
    def channels(c), do: c |> input("in") |> into(Refused) |> into(Log)
  end

  defmodule ResponseOp do
    use Flezha.SDK.UnboundedOperator, factory_id: "part_response"
    def channels(c), do: c |> input("in") |> into(Refused) |> response()
  end

  defmodule KeepOp do
    use Flezha.SDK.UnboundedOperator, factory_id: "part_keep"
    def channels(c), do: c |> input("in") |> into(Refused) |> sink("answer")
  end

  defmodule TurnLoop do
    @behaviour Flezha.SDK.BoundedOperator
    import Flezha.SDK.Channels
    def factory_id, do: "part_turn"
    def loop_mode, do: :per_input
    def max_duration_ms, do: 600_000
    def persistence_enabled, do: false
    def channels(c), do: c |> input("in") |> into(P0) |> sink("answer")
  end

  setup do
    :persistent_term.put(@probe, self())

    on_exit(fn ->
      for key <- [@probe, @failed, @kept], do: :persistent_term.erase(key)
    end)
  end

  # An operator of `module` under `id`, whose data stream is this process.
  defp start(module, id) do
    {:ok, topology} = Topology.build(module)
    {:ok, runner} = StubRunner.start_link(test: self())
    opts = [operator_id: id, topology: topology, counters: Counters.new(), runner: runner]
    {:ok, op} = Operator.start_link(opts)
    wire_to(op)
    op
  end

  defp wire_to(to) do
    {sdk_side, controller_side} = LocalStream.open()
    :ok = LocalStream.controlling_process(controller_side, self())
    :ok = LocalStream.set_active(controller_side)
    send(to, {:secondaries, [sdk_side]})
  end

  defp next_part do
    assert_receive {:flz_data, _, "output_part", payload, metadata}, 5_000
    {payload, metadata}
  end

  test "the frames are the shared fixture's, parts ahead of their output" do
    fixture =
      "test/fixtures/part_frames.json" |> Contract.path!() |> File.read!() |> Jason.decode!()

    log =
      capture_log([level: :debug], fn ->
        op = start(SinkOp, "part_fixture")
        Inbox.input(op, "in", "3", %{})
        Inbox.input(op, "in", "retry:2", %{})

        for %{"payload" => payload, "metadata" => metadata} <- fixture["frames"] do
          assert next_part() == {payload, metadata}
        end

        assert_receive {:flz_data, _, "part_fixture", "done:3", _}, 5_000
        assert_receive {:flz_data, _, "part_fixture", "done:2", _}, 5_000
      end)

    assert log =~ "attempt 1/3 failed: first attempt; retrying"
  end

  test "a keyed Cell names its Cell and key" do
    op = start(KeyedOp, "part_keyed_op")
    Inbox.input(op, "in", "1", %{})
    {"1", metadata} = next_part()
    assert metadata["flz.key"] == "s-7"
    assert metadata["flz.cell"] == "part_keyed_op/p0/s-7"
    assert_receive {:flz_data, _, "part_keyed_op", "done:1", _}, 5_000
  end

  test "a part leaves while run/3 is still blocked" do
    op = start(SinkOp, "part_hold")
    Inbox.input(op, "in", "hold", %{})
    assert_receive {:holding, cell}, 5_000
    assert {"early", %{"flz.part" => "0"}} = next_part()
    refute_received {:flz_data, _, "part_hold", _, _}
    send(cell, :go)
    assert_receive {:flz_data, _, "part_hold", "late", _}, 5_000
  end

  test "a channel that ends in no sink, or in a response, refuses a part" do
    for {module, id} <- [{EdgeOp, "part_edge_op"}, {ResponseOp, "part_resp"}] do
      op = start(module, id)
      Inbox.input(op, "in", "x", %{})
      assert_receive {:part, {:error, %Flezha.SDK.Error{code: "part.no_sink"} = error}}, 5_000
      assert error.details["tasklet"] == "refused"
      assert error.details["operator"] == id
      assert error.details["channel"] not in [nil, ""]
      refute_received {:flz_data, _, "output_part", _, _}
    end
  end

  test "a ctx kept past its invocation is refused" do
    op = start(KeepOp, "part_kept")
    Inbox.input(op, "in", "keep", %{})
    Inbox.input(op, "in", "late", %{})

    assert_receive {:part, {:error, %Flezha.SDK.Error{code: "part.outside_invocation"}}},
                   5_000
  end

  test "a bounded stage's parts lead its answer" do
    {:ok, spec} = Spec.build(TurnLoop)
    {:ok, runner} = StubRunner.start_link(test: self())

    {:ok, driver} =
      Driver.start_link(
        operator_id: "part_turn_1",
        counters: Counters.new(),
        park_idle_ms: 60_000,
        spec: spec,
        runner: runner
      )

    _state = :sys.get_state(driver)
    wire_to(driver)
    Inbox.input(driver, "in", "2", %{})
    assert {"1", %{"flz.part" => "0", "flz.part_attempt" => "1"}} = next_part()
    assert {"2", %{"flz.part" => "1"}} = next_part()
    assert_receive {:flz_data, _, "part_turn_1", "done:2", _}, 5_000
  end
end
