defmodule Flezha.SDK.Steps.SpecTest do
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Channels, Declarations, Topology}
  alias Flezha.SDK.Steps.Spec

  # --- fixtures ---

  defmodule PassTask do
    use Flezha.SDK.Tasklet, name: "pass"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule TailTask do
    use Flezha.SDK.Tasklet, name: "tail"
    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule OtherTailTask do
    use Flezha.SDK.Tasklet, name: "other_tail"
    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule JoinTask do
    use Flezha.SDK.Tasklet, name: "join"
    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule StaticTask do
    use Flezha.SDK.Tasklet, name: "static", cell_keys: ["1m"]
    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule LinearSteps do
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    def factory_id, do: "linear_steps"

    def channels(c) do
      c
      |> Channels.input("in")
      |> Channels.into(PassTask)
      |> Channels.into(TailTask)
    end
  end

  defp issues(module), do: Enum.map(listed(module), & &1["code"])

  defp listed(module) do
    assert {:error, %Flezha.SDK.Error{details: %{"issues" => issues}}} = Spec.build(module)
    issues
  end

  # --- the tasklet DAG ---

  test "a class silent on every setting reads each default" do
    assert {:ok, spec} = Spec.build(LinearSteps)
    assert spec.factory_id == "linear_steps"
    assert spec.settings.each == :once
    assert spec.settings.on_failure == :undo_all
    assert spec.settings.durable == :local
    assert spec.settings.sleep_after_ms == 6 * 60 * 60 * 1000
  end

  test "a structured class builds a Kahn-ordered DAG rooted at the entry's consumer" do
    assert {:ok, spec} = Spec.build(LinearSteps)
    assert [root, tail] = spec.dag.order
    assert %{"in" => chain} = spec.dag.chains
    assert chain.roots == [root]
    assert chain.leaves == [tail]
    assert MapSet.equal?(chain.members, MapSet.new([root, tail]))
    assert chain.parents_in_chain == %{root => 0, tail => 1}
    assert spec.dag.leaves == [tail]
    assert spec.dag.pos[root] == 0
    assert spec.dag.pos[tail] == 1
    assert spec.dag.parents[tail] == [root]
    assert spec.dag.children[root] == [tail]
  end

  test "task_idx numbering is the declaration's, not the ref map's" do
    orders =
      for _ <- 1..8 do
        assert {:ok, spec} = Spec.build(LinearSteps)
        Enum.map(spec.dag.order, &spec.topology.node_modules[&1])
      end

    assert Enum.uniq(orders) == [[PassTask, TailTask]]
  end

  # --- class-fact refusals ---

  test "NoStepsBody: no channels/1" do
    defmodule Bodyless do
      def factory_id, do: "bodyless"
    end

    # The one-function form is no body: a class carrying only it declares none.
    defmodule OneFunction do
      def factory_id, do: "one_function"
      def run(_ctx, _args), do: :ok
    end

    assert "topology.no_steps_body" in issues(Bodyless)
    assert "topology.no_steps_body" in issues(OneFunction)
  end

  test "a steps class that never ends is accepted" do
    defmodule StopOnly do
      def factory_id, do: "stop_only"
      def run, do: {:steps, each: :per_input}
      def channels(c), do: c |> Channels.input("in") |> Channels.into(TailTask)
    end

    assert {:ok, %Spec{settings: %{max_duration_ms: nil}}} = Spec.build(StopOnly)
  end

  # --- the settings tree, written by hand ---

  defp refused_setting(module) do
    assert {:error, %Flezha.SDK.Error{code: code, details: details}} = Topology.build(module)
    {code, Map.drop(details, ["issues"])}
  end

  test "a value outside its choices is refused naming the setting and the value" do
    defmodule ForeverLoop do
      def factory_id, do: "forever_loop"
      def run, do: {:steps, each: :forever}
      def channels(c), do: c |> Channels.input("in") |> Channels.into(TailTask)
    end

    defmodule KindLiteral do
      def factory_id, do: "kind_literal"
      def run, do: :bounded
      def channels(c), do: c |> Channels.input("in") |> Channels.into(TailTask)
    end

    assert {"topology.invalid_setting",
            %{
              "declaration" => "relay 'forever_loop'",
              "setting" => "each",
              "value" => "forever"
            }} =
             refused_setting(ForeverLoop)

    assert {"topology.invalid_setting", %{"setting" => "run", "value" => "bounded"}} =
             refused_setting(KindLiteral)
  end

  defmodule Misplaced do
    def factory_id, do: "misplaced"
    def run, do: Process.get(:steps_spec_test_run)
    def durable, do: :local
    def channels(c), do: c |> Channels.input("in") |> Channels.into(TailTask)
  end

  test "a setting under a parent that does not carry it is refused naming both" do
    for {run, setting, parent} <- [
          {{:flow, each: :per_input}, "each", "flow"},
          {{:steps, exactly: :effectively_once}, "exactly", "steps"},
          {{:steps, correction_ms: 5_000}, "correction_ms", "steps"},
          {{:flow, exactly: {:at_least_once, ack: :checkpoint}}, "ack", "at_least_once"}
        ] do
      Process.put(:steps_spec_test_run, run)

      assert {"topology.unknown_field",
              %{"declaration" => "'#{parent}' of relay 'misplaced'", "fields" => setting}} ==
               refused_setting(Misplaced)
    end
  end

  test "a sleep interval on a class kept in memory is refused naming both" do
    defmodule SleeplessFlow do
      def factory_id, do: "sleepless_flow"
      def durable, do: {false, sleep_after: 1_000}
      def channels(c), do: c |> Channels.input("in") |> Channels.into(TailTask)
    end

    assert {"topology.unknown_field",
            %{"declaration" => "'false' of relay 'sleepless_flow'", "fields" => "sleep_after"}} ==
             refused_setting(SleeplessFlow)
  end

  test "the one cross rule: the tier needs a disk" do
    defmodule TierInMemory do
      def factory_id, do: "tier_in_memory"
      def run, do: {:flow, exactly: :effectively_once}
      def durable, do: false
      def channels(c), do: c |> Channels.input("in") |> Channels.into(TailTask)
    end

    assert {"topology.effectively_once_without_persistence", %{}} = refused_setting(TierInMemory)
  end

  test "a flow class and a steps class may both keep their work remotely" do
    defmodule RemoteFlow do
      def factory_id, do: "remote_flow"
      def durable, do: :remote
      def channels(c), do: c |> Channels.input("in") |> Channels.into(TailTask)
    end

    defmodule RemoteSteps do
      def factory_id, do: "remote_steps"
      def run, do: :steps
      def durable, do: {:remote, sleep_after: false}
      def channels(c), do: c |> Channels.input("in") |> Channels.into(TailTask)
    end

    for class <- [RemoteFlow, RemoteSteps] do
      assert {:ok, spec} = Spec.build(class)
      assert %{"durable" => "remote"} = declared = Flezha.SDK.Declarations.for(spec)
      refute Map.has_key?(declared, "sleep_after_ms")
    end
  end

  test "each class declares where it keeps its work and when it sleeps" do
    defmodule SleepingFlow do
      use Flezha.SDK.Relay, factory_id: "sleeping_flow", durable: {:local, sleep_after: 150}
      def channels(c), do: c |> input("in") |> into(TailTask)
    end

    defmodule OptedOutSteps do
      use Flezha.SDK.Relay, factory_id: "opted_out_steps", run: :steps, durable: false
      def channels(c), do: c |> input("in") |> into(TailTask)
    end

    defmodule CappedSteps do
      use Flezha.SDK.Relay,
        factory_id: "capped_steps",
        run: {:steps, each: :chained, on_failure: :keep_progress},
        max_duration_ms: 5_000

      def channels(c), do: c |> input("in") |> into(TailTask)
    end

    assert {:ok, sleeping} = Topology.build(SleepingFlow)

    assert %{"run" => "flow", "durable" => "local", "sleep_after_ms" => "150", "keyed" => "false"} ==
             Flezha.SDK.Declarations.for(sleeping)

    assert {:ok, opted_out} = Spec.build(OptedOutSteps)
    declared = Flezha.SDK.Declarations.for(opted_out)
    assert %{"run" => "steps", "durable" => "memory", "each" => "once"} = declared
    refute Map.has_key?(declared, "sleep_after_ms")

    assert {:ok, capped} = Spec.build(CappedSteps)

    assert %{
             "each" => "chained",
             "on_failure" => "keep_progress",
             "max_duration_ms" => "5000",
             "sleep_after_ms" => "21600000"
           } = Flezha.SDK.Declarations.for(capped)
  end

  test "a class that says nothing sleeps by its run: a flow never, a steps class after six hours" do
    defmodule SilentFlow do
      use Flezha.SDK.Relay, factory_id: "silent_flow"
      def channels(c), do: c |> input("in") |> into(TailTask)
    end

    defmodule LocalFlow do
      use Flezha.SDK.Relay, factory_id: "local_flow", durable: :local
      def channels(c), do: c |> input("in") |> into(TailTask)
    end

    for module <- [SilentFlow, LocalFlow] do
      assert {:ok, topology} = Topology.build(module)
      assert %{"run" => "flow", "durable" => "local"} = declared = Declarations.for(topology)
      refute Map.has_key?(declared, "sleep_after_ms"), "#{inspect(module)} sleeps"
    end

    assert {:ok, spec} = Spec.build(LinearSteps)
    assert %{"sleep_after_ms" => "21600000"} = Declarations.for(spec)
  end

  # --- structural refusals ---

  test "NoEntryChannel — tasklets with nothing to seed them" do
    defmodule NoEntry do
      def factory_id, do: "no_entry"

      def channels(c) do
        c |> Channels.input("in") |> Channels.into(PassTask)
        :ok
      end
    end

    # A class whose `channels/1` declares no channel at all.
    defmodule Empty do
      def factory_id, do: "empty"
      def channels(_c), do: :ok
    end

    assert "topology.no_entry_channel" in issues(Empty)
    assert {:ok, _} = Spec.build(NoEntry)
  end

  test "an entry channel may feed several tasklets, all roots of its chain" do
    defmodule TwoRoots do
      def factory_id, do: "two_roots"

      def channels(c) do
        s = Channels.input(c, "in")
        Channels.into(s, PassTask)
        Channels.into(s, TailTask)
      end
    end

    assert {:ok, spec} = Spec.build(TwoRoots)
    assert %{"in" => %{roots: roots}} = spec.dag.chains
    assert Enum.map(roots, &spec.topology.node_modules[&1]) == [PassTask, TailTask]
  end

  test "an entry channel is a merge source: the merged tasklet is a root of its chain" do
    defmodule EntryMerge do
      def factory_id, do: "entry_merge"

      def channels(c) do
        entry = Channels.input(c, "in")
        entry |> Channels.into(PassTask) |> Channels.into(TailTask)
        Channels.into(entry, TailTask)
      end
    end

    assert {:ok, spec} = Spec.build(EntryMerge)
    [pass, tail] = spec.dag.order
    assert %{"in" => chain} = spec.dag.chains
    assert chain.roots == [pass, tail]
    # The join waits for its one producer inside the chain, not for the entry.
    assert chain.parents_in_chain[tail] == 1
  end

  test "each entry channel roots its own chain, and a join counts only the producers in it" do
    defmodule TwoChains do
      def factory_id, do: "two_chains"

      def channels(c) do
        a = c |> Channels.input("a") |> Channels.into(PassTask)
        b = c |> Channels.input("b") |> Channels.into(TailTask)
        Channels.into(a, JoinTask)
        Channels.into(b, JoinTask)
      end
    end

    assert {:ok, spec} = Spec.build(TwoChains)
    modules = &Enum.map(&1, fn ref -> spec.topology.node_modules[ref] end)
    assert %{"a" => a, "b" => b} = spec.dag.chains
    assert modules.(a.roots) == [PassTask]
    assert modules.(b.roots) == [TailTask]
    join = Enum.find(spec.dag.order, &(spec.topology.node_modules[&1] == JoinTask))
    assert MapSet.member?(a.members, join) and MapSet.member?(b.members, join)
    refute MapSet.member?(a.members, hd(b.roots))
    assert {a.parents_in_chain[join], b.parents_in_chain[join]} == {1, 1}
    assert length(spec.dag.parents[join]) == 2
  end

  test "a keyed edge into a keyless tasklet makes it dynamic, as a flow relay's" do
    defmodule DynamicSteps do
      def factory_id, do: "dynamic_steps"

      def channels(c) do
        c
        |> Channels.input("in")
        |> Channels.into(PassTask)
        |> Channels.key_by(& &1)
        |> Channels.into(TailTask)
      end
    end

    assert {:ok, spec} = Spec.build(DynamicSteps)
    tail = Enum.find(spec.dag.order, &(spec.topology.node_modules[&1] == TailTask))
    assert spec.topology.node_mode[tail] == :dynamic
  end

  test "a static-mode tasklet is not a dynamic one" do
    defmodule StaticSteps do
      def factory_id, do: "static_steps"

      def channels(c) do
        c |> Channels.input("in") |> Channels.into(StaticTask)
      end
    end

    assert {:ok, spec} = Spec.build(StaticSteps)
    assert Map.values(spec.topology.node_mode) == [:static]
  end

  test "BranchingChainedLoop — a chained loop over several terminals" do
    defmodule BranchingChain do
      def factory_id, do: "branching_chain"
      def run, do: {:steps, each: :chained}

      def channels(c) do
        mid = c |> Channels.input("in") |> Channels.into(PassTask)
        Channels.into(mid, TailTask)
        Channels.into(mid, OtherTailTask)
      end
    end

    defmodule ConvergingChain do
      def factory_id, do: "converging_chain"
      def run, do: {:steps, each: :chained}

      def channels(c) do
        mid = c |> Channels.input("in") |> Channels.into(PassTask)
        Channels.into(mid, TailTask)
      end
    end

    assert "topology.branching_chained_loop" in issues(BranchingChain)
    assert {:ok, _} = Spec.build(ConvergingChain)
  end

  test "a per_input loop over a branching topology is legal" do
    defmodule BranchingPerInput do
      def factory_id, do: "branching_per_input"
      def run, do: {:steps, each: :per_input}

      def channels(c) do
        mid = c |> Channels.input("in") |> Channels.into(PassTask)
        Channels.into(mid, TailTask)
        Channels.into(mid, OtherTailTask)
      end
    end

    assert {:ok, spec} = Spec.build(BranchingPerInput)
    assert length(spec.dag.leaves) == 2
  end

  # --- the underlying topology's refusals still apply ---

  test "a topology refusal is carried through, named as one" do
    defmodule BadTopology do
      def factory_id, do: "bad_topology"

      def channels(c) do
        c |> Channels.input("in") |> Channels.key_by(& &1) |> Channels.into(StaticTask)
      end
    end

    assert issues(BadTopology) == ["topology.key_by_on_unkeyable_target"]
  end

  # --- accumulation ---

  test "every issue is reported at once, not just the first" do
    defmodule ManyProblems do
      def factory_id, do: "many_problems"
      def run, do: :steps

      def channels(c) do
        c |> Channels.input("in") |> Channels.key_by(& &1) |> Channels.into(StaticTask)
        c |> Channels.input("other") |> Channels.map(& &1)
      end
    end

    found = issues(ManyProblems)
    assert "topology.key_by_on_unkeyable_target" in found
    assert "topology.dangling_transform_chain" in found
  end
end
