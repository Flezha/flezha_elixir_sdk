defmodule Flezha.SDK.OperatorPushTest do
  @moduledoc """
  An unbounded operator pushing into a Cell that is mid-run: with room in the
  Cell's queue the push lands at once, so the operator keeps answering; with
  none the pusher waits, and keeps its place in line.
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Cell, Counters, Operator, Topology}
  alias Flezha.SDK.Operator.Inbox
  alias Flezha.SDK.Test.StubRunner

  # Reports each run and holds it until the test releases it. The give-up
  # outlasts the operator's own control-call bound, so a router stuck behind
  # a run cannot answer in time by accident.
  defmodule Held do
    @moduledoc false
    def run(input) do
      send(:operator_push_test, {:running, input, self()})

      receive do
        :release -> :ok
      after
        15_000 -> :ok
      end
    end
  end

  defmodule Gate do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "gate"

    @impl true
    def run(input, s, _ctx) do
      Held.run(input)
      {:ok, s}
    end
  end

  defmodule NarrowGate do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "narrow_gate", mailbox_capacity: 1

    @impl true
    def run(input, s, _ctx) do
      Held.run(input)
      {:ok, s}
    end
  end

  defmodule SheddingGateOp do
    use Flezha.SDK.UnboundedOperator, factory_id: "shedding_gate_op"
    def channels(c), do: c |> input("in") |> on_full(:drop_oldest) |> into(NarrowGate)
  end

  defmodule GateOp do
    use Flezha.SDK.UnboundedOperator, factory_id: "gate_op"
    def channels(c), do: c |> input("in") |> into(Gate)
  end

  defmodule NarrowGateOp do
    use Flezha.SDK.UnboundedOperator, factory_id: "narrow_gate_op"
    def channels(c), do: c |> input("in") |> into(NarrowGate)
  end

  setup do
    Process.register(self(), :operator_push_test)
    :ok
  end

  defp start(module) do
    {:ok, topology} = Topology.build(module)
    {:ok, runner} = StubRunner.start_link(test: self())
    opts = [operator_id: "gate-1", topology: topology, counters: Counters.new(), runner: runner]
    {:ok, op} = Operator.start_link(opts)
    op
  end

  test "a push into a Cell that is mid-run with room leaves the operator answering" do
    op = start(GateOp)
    Inbox.input(op, "in", "first", %{})
    assert_receive {:running, "first", cell}, 5_000

    Inbox.input(op, "in", "second", %{})
    assert [{"gate", :single, [{nil, ^cell}]}] = Operator.cells(op)

    send(cell, :release)
    assert_receive {:running, "second", ^cell}, 5_000
    send(cell, :release)
  end

  test "a slot freed while a pusher waits is that pusher's, never a later one's" do
    op = start(NarrowGateOp)
    Inbox.input(op, "in", "a", %{})
    assert_receive {:running, "a", cell}, 5_000
    room = :sys.get_state(op).cells.rooms[cell]

    # "b" takes the one slot; "c" finds none and waits in a call.
    :ok = Cell.push(cell, room, "b")
    Task.start(fn -> Cell.push(cell, room, "c") end)
    until(fn -> Enum.any?(messages(cell), &match?({:"$gen_call", _, {:enqueue, "c"}}, &1)) end)

    send(cell, :release)
    assert_receive {:running, "b", ^cell}, 5_000

    # Dequeuing "b" handed its slot to "c", so a pusher arriving now finds
    # none and waits behind "c" rather than casting ahead of it.
    assert Cell.Room.free(room) == 0
    Task.start(fn -> Cell.push(cell, room, "d") end)

    for input <- ["c", "d"] do
      send(cell, :release)
      assert_receive {:running, ^input, ^cell}, 5_000
    end

    send(cell, :release)
  end

  # A bounded stage sends its value, then the barrier behind it, the same
  # way: an internal item must not take the slot the value was sent for.
  for policy <- [:drop_oldest, :drop_newest] do
    test "#{policy}: a barrier sent behind a value never costs it the one slot" do
      op = start(SheddingGateOp)
      Inbox.input(op, "in", "a", %{})
      assert_receive {:running, "a", cell}, 5_000
      Cell.enqueue_drop(cell, "v1", unquote(policy))
      ref = make_ref()
      :ok = Cell.barrier(cell, ref, self())

      send(cell, :release)
      assert_receive {:running, "v1", ^cell}, 5_000
      send(cell, :release)
      assert_receive {:stage_done, ^ref, ^cell, _written}, 5_000
    end
  end

  test "drop_oldest sheds the oldest input, never a barrier queued ahead of it" do
    op = start(SheddingGateOp)
    Inbox.input(op, "in", "a", %{})
    assert_receive {:running, "a", cell}, 5_000

    ref = make_ref()
    :ok = Cell.barrier(cell, ref, self())
    Cell.enqueue_drop(cell, "v1", :drop_oldest)
    Cell.enqueue_drop(cell, "v2", :drop_oldest)

    send(cell, :release)
    assert_receive {:stage_done, ^ref, ^cell, _written}, 5_000
    assert_receive {:running, "v2", ^cell}, 5_000
    send(cell, :release)
  end

  defp messages(pid), do: pid |> Process.info(:messages) |> elem(1)

  defp until(ready?) do
    unless ready?.() do
      Process.sleep(5)
      until(ready?)
    end
  end
end
