defmodule Flezha.SDK.Steps.InstancesTest do
  @moduledoc """
  Two instances of one steps operator running at once: each keeps its own
  state, because a Cell is a process per operator instance, node and key, and
  each derives `stable_id/1` from its own Cell's identity and change seq.
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Cell, Counters, Ctx, Operator, Park}
  alias Flezha.SDK.Steps.{Driver, Spec}
  alias Flezha.SDK.Operator.Inbox
  alias Flezha.SDK.Test.StubRunner

  defmodule Tally do
    use Flezha.SDK.Tasklet, name: "tally"

    @impl true
    def init_state(_key), do: 0

    defchange add(count, n) do
      count + n
    end

    @impl true
    def run(_input, count, ctx) do
      send(:steps_instances_test, {:stable_id, stable_id(ctx)})
      {:ok, add(count, ctx, 1)}
    end
  end

  defmodule TallyLoop do
    use Flezha.SDK.Operator,
      factory_id: "tally_loop",
      run: {:steps, each: :per_input},
      durable: false

    @impl true
    def channels(c), do: c |> input("in") |> into(Tally)
  end

  test "two instances of one steps operator keep their own state and their own stable ids" do
    Process.register(self(), :steps_instances_test)
    {:ok, spec} = Spec.build(TallyLoop)
    [a, b] = for id <- ["tally-a", "tally-b"], do: start(spec, id)

    Inbox.input(a, "in", "x", %{"flz.offset" => "0"})
    Inbox.input(b, "in", "x", %{"flz.offset" => "0"})
    Inbox.input(a, "in", "y", %{"flz.offset" => "1"})

    ids =
      for _pass <- 1..3 do
        assert_receive {:stable_id, id}, 5_000
        id
      end

    assert Enum.sort(ids) ==
             Enum.sort([
               Ctx.derive_stable_id(Park.identity("tally-a", "tally", ""), 0, 0),
               Ctx.derive_stable_id(Park.identity("tally-a", "tally", ""), 1, 0),
               Ctx.derive_stable_id(Park.identity("tally-b", "tally", ""), 0, 0)
             ])

    assert length(Enum.uniq(ids)) == 3

    assert {:ok, 2, _seq} = read(a)
    assert {:ok, 1, _seq} = read(b)
  end

  defp start(spec, operator_id) do
    {:ok, runner} = StubRunner.start_link(test: self())

    {:ok, driver} =
      Driver.start_link(
        spec: spec,
        runner: runner,
        operator_id: operator_id,
        counters: Counters.new()
      )

    on_exit(fn -> if Process.alive?(driver), do: Driver.stop_steps(driver) end)
    driver
  end

  defp read(driver) do
    {:ok, cell} = Operator.Server.lookup(driver, "tally", nil)
    Cell.read(cell, 1_000)
  end
end
