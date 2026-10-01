defmodule Flezha.SDK.CellDeathTest do
  @moduledoc """
  A Cell that dies outside its ladder. In a steps stage, with the barrier
  still queued behind its work, the barrier is never answered, so the driver
  fails that stage the way an exhausted ladder does and the run ends instead
  of waiting for ever. In a flow instance holding unfinished input, the
  Operator ends so the controller rebinds it and re-forwards that input; with
  nothing held, the Cell is minted again.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog, only: [with_log: 1]

  alias Flezha.SDK.{Channels, Counters, Operator, Topology}
  alias Flezha.SDK.Operator.Inbox
  alias Flezha.SDK.Test.StubRunner
  alias Flezha.SDK.Steps.{Driver, Spec}

  @collector :cell_death_test

  # "go" and "a" block inside `run/3` until the test ends their Cell, "park"
  # exhausts the ladder, and anything else runs at once.
  defmodule Blocker do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "blocker"

    @impl true
    def run("park", _s, _ctx), do: {:error, {:non_retryable, "park"}}

    def run(input, s, ctx) when input in ["go", "a"] do
      send(:cell_death_test, {:running, self()})

      receive do
        :release ->
          :ok

        # An exit signal from a process the tasklet linked: the ladder's
        # rescue cannot catch it, so it takes the Cell down.
        :linked_exit ->
          spawn_link(fn -> exit(:boom) end)
          Process.sleep(:infinity)
      end

      emit(ctx, input)
      {:ok, s}
    end

    def run(input, s, _ctx) do
      send(:cell_death_test, {:took, input, self()})
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

  defmodule BlockerOp do
    use Flezha.SDK.Operator, factory_id: "cell_death_blocker_op"
    def channels(c), do: c |> input("in") |> into(Blocker)
  end

  defmodule KeyedOp do
    use Flezha.SDK.Operator, factory_id: "cell_death_keyed_op"
    def channels(c), do: c |> input("in") |> key_by(& &1) |> into(Blocker)
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
      assert_receive {:shipped, {:operator_completed, done}}, 5_000
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

  describe "a flow instance" do
    # Unlinked, so its exit reaches the test as a `:DOWN` rather than an exit
    # signal.
    defp start_operator(module \\ BlockerOp) do
      {:ok, topology} = Topology.build(module)
      {:ok, runner} = StubRunner.start_link(test: self())

      {:ok, op} =
        Operator.Server.start_link(
          operator_id: "cell-death-op",
          topology: topology,
          counters: Counters.new(),
          runner: runner
        )

      Process.unlink(op)
      {op, Process.monitor(op)}
    end

    defp at(offset), do: %{"flz.offset" => Integer.to_string(offset)}

    defp cell_of(op),
      do: op |> Operator.Server.cells() |> then(fn [{_, _, [{nil, pid}]}] -> pid end)

    defp end_cell(cell, :kill), do: Process.exit(cell, :kill)
    defp end_cell(cell, :linked_exit), do: send(cell, :linked_exit)

    for how <- [:kill, :linked_exit] do
      test "#{how}: a Cell that dies with unfinished input ends its Operator" do
        {op, ref} = start_operator()

        {_, log} =
          with_log(fn ->
            Inbox.input(op, "in", "a", at(10))
            assert_receive {:running, cell}, 5_000
            Inbox.input(op, "in", "b", at(11))
            Inbox.input(op, "in", "c", at(12))

            end_cell(cell, unquote(how))
            Inbox.input(op, "in", "d", at(13))
            assert_receive {:DOWN, ^ref, :process, ^op, {:cell_exited, _}}, 5_000
          end)

        refute_receive {:took, _, _}, 100
        assert log =~ "cell crashed: operator=cell-death-op node=#Reference"
        assert log =~ "key=nil"
      end
    end

    test "inputs queued behind a parked input end the Operator with their Cell" do
      {op, ref} = start_operator()
      Inbox.input(op, "in", "park", at(10))
      assert_receive {:shipped, {:park_payload, _body}}, 5_000
      Inbox.input(op, "in", "b", at(11))
      Inbox.input(op, "in", "c", at(12))
      cell = cell_of(op)

      with_log(fn ->
        Process.exit(cell, :kill)
        Inbox.input(op, "in", "d", at(13))
        assert_receive {:DOWN, ^ref, :process, ^op, {:cell_exited, _}}, 5_000
      end)

      refute_receive {:took, _, _}, 100
    end

    # The push is queued ahead of the Operator's `:DOWN`, so the push is what
    # finds the Cell dead: its exit names the push, not the kill.
    test "a push to a dead Cell does not mint a replacement over held input" do
      {op, ref} = start_operator()
      Inbox.input(op, "in", "a", at(10))
      assert_receive {:running, cell}, 5_000

      :sys.suspend(op)
      Inbox.input(op, "in", "b", at(11))
      cell_ref = Process.monitor(cell)
      Process.exit(cell, :kill)
      assert_receive {:DOWN, ^cell_ref, :process, ^cell, :killed}, 5_000

      with_log(fn ->
        :sys.resume(op)
        assert_receive {:DOWN, ^ref, :process, ^op, {:cell_exited, {:noproc, _}}}, 5_000
      end)

      refute_receive {:took, _, _}, 100
    end

    test "a Cell death with no offset held re-mints as today" do
      {op, ref} = start_operator()
      Inbox.input(op, "in", "a", %{})
      assert_receive {:running, cell}, 5_000

      {fresh, _log} =
        with_log(fn ->
          Process.exit(cell, :kill)
          replacement(op, cell)
        end)

      Inbox.input(op, "in", "d", %{})
      assert_receive {:took, "d", ^fresh}, 5_000
      refute_received {:DOWN, ^ref, :process, ^op, _}
    end

    test "an Operator that stops abnormally ends every Cell before its own exit is seen" do
      {op, ref} = start_operator(KeyedOp)
      for key <- ["x", "y", "z"], do: Inbox.input(op, "in", key, %{})
      for _ <- 1..3, do: assert_receive({:took, _, _}, 5_000)
      cells = for {_, _, keyed} <- Operator.Server.cells(op), {_key, pid} <- keyed, do: pid
      cell_refs = Enum.map(cells, &Process.monitor/1)
      assert length(cell_refs) == 3

      with_log(fn -> GenServer.stop(op, :boom) end)

      downs =
        for _ <- 1..4 do
          assert_receive {:DOWN, down_ref, :process, _pid, _reason}, 5_000
          down_ref
        end

      assert List.last(downs) == ref
      assert Enum.sort(Enum.drop(downs, -1)) == Enum.sort(cell_refs)
    end

    defp replacement(op, dead) do
      case cell_of(op) do
        ^dead -> Process.sleep(5) && replacement(op, dead)
        pid -> pid
      end
    end
  end
end
