defmodule Flezha.SDK.TaskletActivityTest do
  @moduledoc """
  The activity read: one row per tasklet with its live Cells and queued
  items, every total absent, and a relay the runner does not host refused.
  """
  # The deadline case sets an application-wide knob.
  use ExUnit.Case, async: false

  alias Flezha.SDK.{Commands, Counters, Relay, Topology}
  alias Flezha.SDK.Proto.RunnerCommands
  alias Flezha.SDK.Relay.Inbox
  alias Flezha.SDK.Test.StubRunner

  defmodule Gate do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "gate"

    @impl true
    def run(input, s, _ctx) do
      send(:tasklet_activity_test, {:running, input, self()})

      receive do
        :release -> {:ok, s}
      after
        15_000 -> {:ok, s}
      end
    end
  end

  defmodule Quiet do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "quiet"

    @impl true
    def run(_input, s, _ctx), do: {:ok, s}
  end

  defmodule GateOp do
    use Flezha.SDK.Relay, factory_id: "activity_gate_op"
    def channels(c), do: c |> input("in") |> into(Gate) |> into(Quiet)
  end

  setup do
    Process.register(self(), :tasklet_activity_test)
    {:ok, topology} = Topology.build(GateOp)
    {:ok, runner} = StubRunner.start_link(test: self())
    opts = [relay_id: "gate-1", topology: topology, counters: Counters.new(), runner: runner]
    {:ok, op} = Relay.Server.start_link(opts)
    on_exit(fn -> Application.delete_env(:flezha_sdk, :sdk_activity_read_ms) end)
    %{op: op}
  end

  test "a backlog behind a running input shows in its tasklet's row", %{op: op} do
    cell = backlog(op, 3)
    me = self()
    Task.start(fn -> send(me, {:answer, read(op, "gate-1")}) end)
    until(fn -> Enum.any?(messages(cell), &match?({:"$gen_call", _, :queued}, &1)) end)
    send(cell, :release)

    assert_receive {:answer, {"success", [], raw}}, 5_000
    answer = RunnerCommands.TaskletActivityResponse.decode!(raw)
    assert answer.instance_seq == :erlang.phash2(op, 4_294_967_296)

    assert Enum.sort_by(answer.rows, & &1.tasklet) == [
             %RunnerCommands.TaskletActivityRow{
               tasklet: "gate",
               cells: 1,
               queued: 3,
               queued_max: 3
             },
             %RunnerCommands.TaskletActivityRow{
               tasklet: "quiet",
               cells: 1,
               queued: 0,
               queued_max: 0
             }
           ]

    assert Enum.all?(answer.rows, &is_nil(&1.admitted_total)), "this runtime keeps no totals"
    release_all(cell)
  end

  test "a Cell that does not answer by the deadline leaves its queue absent", %{op: op} do
    Application.put_env(:flezha_sdk, :sdk_activity_read_ms, 50)
    cell = backlog(op, 1)

    assert {"success", [], raw} = read(op, "gate-1")
    rows = RunnerCommands.TaskletActivityResponse.decode!(raw).rows
    assert %{cells: 1, queued: nil, queued_max: nil} = Enum.find(rows, &(&1.tasklet == "gate"))

    assert %{cells: 1, queued: 0, queued_max: 0} = Enum.find(rows, &(&1.tasklet == "quiet")),
           "a busy Cell costs only its own tasklet the deadline"

    release_all(cell)
  end

  test "a relay the runner does not host is refused by name", %{op: op} do
    assert {"error", [%{code: "runtime.relay_not_found"} = refusal], ""} = read(op, "elsewhere")
    assert "elsewhere" in Map.values(refusal.details)
  end

  # One input running and held, `n` more queued behind it in its Cell.
  defp backlog(op, n) do
    Inbox.input(op, "in", "first", %{})
    assert_receive {:running, "first", cell}, 5_000
    for i <- 1..n, do: Inbox.input(op, "in", "next-#{i}", %{})
    until(fn -> Enum.count(messages(cell), &match?({:"$gen_cast", {:enqueue, _}}, &1)) == n end)
    cell
  end

  defp read(op, relay_id) do
    me = self()
    respond = fn status, errors, raw, _meta -> send(me, {:read, {status, errors, raw}}) end

    Commands.dispatch(:tasklet_activity, %{relay_id: relay_id}, respond, %{
      relays: %{"gate-1" => {op, "f"}}
    })

    receive do
      {:read, answer} -> answer
    after
      5_000 -> flunk("the read did not answer")
    end
  end

  defp release_all(cell) do
    send(cell, :release)

    receive do
      {:running, _input, ^cell} -> release_all(cell)
    after
      200 -> :ok
    end
  end

  defp messages(pid), do: pid |> Process.info(:messages) |> elem(1)

  defp until(ready?) do
    unless ready?.() do
      Process.sleep(5)
      until(ready?)
    end
  end
end
