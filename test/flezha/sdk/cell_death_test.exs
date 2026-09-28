defmodule Flezha.SDK.CellDeathTest do
  @moduledoc """
  A bounded stage whose Cell dies with the barrier still queued behind its
  work: the barrier is never answered, so the driver fails that stage the way
  an exhausted ladder does and the run ends instead of waiting for ever.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog, only: [with_log: 1]

  alias Flezha.SDK.{Channels, Counters}
  alias Flezha.SDK.Operator.Inbox
  alias Flezha.SDK.Test.StubRunner
  alias Flezha.SDK.Bounded.{Driver, Spec}

  @collector :cell_death_test

  # Blocks inside `run/3` until the test ends its Cell.
  defmodule Blocker do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "blocker"

    @impl true
    def run(input, s, ctx) do
      send(:cell_death_test, {:running, self()})

      receive do
        :release -> :ok
      end

      emit(ctx, input)
      {:ok, s}
    end
  end

  defmodule Tail do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "tail"

    @impl true
    def run(input, s, _ctx) do
      send(:cell_death_test, {:ran, "tail", input})
      {:ok, s}
    end
  end

  defmodule Undoable do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "undoable"

    @impl true
    def run(input, s, ctx) do
      emit(ctx, input)
      {:ok, s}
    end

    @impl true
    def compensate(_outputs, s, _ctx) do
      send(:cell_death_test, {:undone, "undoable"})
      {:ok, s}
    end
  end

  # Its undo blocks until the test ends its Cell.
  defmodule HeldUndo do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "held_undo"

    @impl true
    def run(input, s, ctx) do
      emit(ctx, input)
      {:ok, s}
    end

    @impl true
    def compensate(_outputs, s, _ctx) do
      send(:cell_death_test, {:compensating, self()})

      receive do
        :release -> {:ok, s}
      end
    end
  end

  defmodule Boom do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "boom"

    @impl true
    def run(_input, _s, _ctx), do: {:error, {:non_retryable, "boom"}}
  end

  defmodule BlockerFlow do
    def factory_id, do: "cell_death_blocker_flow"

    def channels(c),
      do: c |> Channels.input("in") |> Channels.into(Blocker) |> Channels.into(Tail)
  end

  defmodule HeldUndoFlow do
    def factory_id, do: "cell_death_held_undo_flow"

    def channels(c) do
      c
      |> Channels.input("in")
      |> Channels.into(Undoable)
      |> Channels.into(HeldUndo)
      |> Channels.into(Boom)
    end
  end

  setup do
    Process.register(self(), @collector)
    :ok
  end

  defp start_driver(flow) do
    assert {:ok, spec} = Spec.build(flow)
    {:ok, runner} = StubRunner.start_link(test: self())

    {:ok, driver} =
      Driver.start_link(
        operator_id: "cell-death",
        counters: Counters.new(),
        spec: spec,
        runner: runner
      )

    Inbox.input(driver, nil, "go", %{})
    driver
  end

  # Start `flow`, kill the Cell `started` answers mid-run, and collect the
  # run's terminal frame with every line it logged.
  defp kill_mid_run(flow, started) do
    with_log(fn ->
      driver = start_driver(flow)
      cell = started.()
      ref = Process.monitor(driver)
      Process.exit(cell, :kill)
      assert_receive {:DOWN, ^ref, :process, ^driver, :normal}, 5_000
      assert_receive {:shipped, {:bounded_completed, done}}, 5_000
      done
    end)
  end

  test "a stage whose Cell dies before its barrier fails, and the run ends" do
    {done, log} =
      kill_mid_run(BlockerFlow, fn ->
        assert_receive {:running, cell}, 5_000
        cell
      end)

    assert %{status: "error", error: "blocker: cell exited: :killed"} = done
    assert log =~ "cell crashed: operator=cell-death"
    assert log =~ "reason=:killed"
    refute_received {:ran, "tail", _}
  end

  test "an undo whose Cell dies counts as failed, and the rest of the plan still runs" do
    {done, log} =
      kill_mid_run(HeldUndoFlow, fn ->
        assert_receive {:compensating, cell}, 5_000
        cell
      end)

    assert %{status: "error", error: "boom: boom"} = done
    assert_received {:undone, "undoable"}
    assert log =~ "cell-death compensating task 1"
    assert log =~ "cell-death compensating task 0"

    assert log =~
             "compensate hook on held_undo failed: cell exited: :killed; the rest of the plan still runs"

    assert log =~ "cell crashed: operator=cell-death"
  end
end
