defmodule Flezha.SDK.Bounded.SpecTest do
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Channels, Ctx, Topology}
  alias Flezha.SDK.Bounded.Spec

  # --- fixtures ---

  defmodule PassTask do
    @behaviour Flezha.SDK.Tasklet
    def name, do: "pass"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      Ctx.emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule TailTask do
    @behaviour Flezha.SDK.Tasklet
    def name, do: "tail"
    def init_state(_), do: nil
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule OtherTailTask do
    @behaviour Flezha.SDK.Tasklet
    def name, do: "other_tail"
    def init_state(_), do: nil
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule JoinTask do
    @behaviour Flezha.SDK.Tasklet
    def name, do: "join"
    def init_state(_), do: nil
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule StaticTask do
    @behaviour Flezha.SDK.Tasklet
    def name, do: "static"
    def cell_keys, do: ["1m"]
    def init_state(_), do: nil
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule LinearBounded do
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "linear_bounded"

    def channels(c) do
      c
      |> Channels.input("in")
      |> Channels.into(PassTask)
      |> Channels.into(TailTask)
    end
  end

  defp issues(module) do
    assert {:error, {:invalid_bounded, issues}} = Spec.build(module)
    Enum.map(issues, &elem(&1, 0))
  end

  # --- the tasklet DAG ---

  test "a class silent on every knob reads each default" do
    assert {:ok, spec} = Spec.build(LinearBounded)
    assert spec.factory_id == "linear_bounded"
    assert spec.knobs.loop_mode == :once
    assert spec.knobs.rollback_mode == :all_or_nothing
    assert spec.knobs.persistence_enabled
  end

  test "a structured class builds a Kahn-ordered DAG rooted at the entry's consumer" do
    assert {:ok, spec} = Spec.build(LinearBounded)
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
        assert {:ok, spec} = Spec.build(LinearBounded)
        Enum.map(spec.dag.order, &spec.topology.node_modules[&1])
      end

    assert Enum.uniq(orders) == [[PassTask, TailTask]]
  end

  # --- class-fact refusals ---

  test "NoBoundedBody: no channels/1" do
    defmodule Bodyless do
      def factory_id, do: "bodyless"
    end

    # The one-function form is no body: a class carrying only it declares none.
    defmodule OneFunction do
      def factory_id, do: "one_function"
      def run(_ctx, _args), do: :ok
    end

    assert :NoBoundedBody in issues(Bodyless)
    assert :NoBoundedBody in issues(OneFunction)
  end

  test "ExternalStopOnly — not self-terminating and no duration cap" do
    defmodule StopOnly do
      def factory_id, do: "stop_only"
      def self_terminating?, do: false
      def channels(c), do: c |> Channels.input("in") |> Channels.into(TailTask)
    end

    defmodule StopOnlyCapped do
      def factory_id, do: "stop_only_capped"
      def self_terminating?, do: false
      def max_duration_ms, do: 5_000
      def channels(c), do: c |> Channels.input("in") |> Channels.into(TailTask)
    end

    assert :ExternalStopOnly in issues(StopOnly)
    assert {:ok, _} = Spec.build(StopOnlyCapped)
  end

  test "InvalidMode — a hand-written mode outside its set is refused naming the value" do
    defmodule ForeverLoop do
      def factory_id, do: "forever_loop"
      def loop_mode, do: :forever
      def rollback_mode, do: :best_effort
      def channels(c), do: c |> Channels.input("in") |> Channels.into(TailTask)
    end

    assert {:error, {:invalid_bounded, issues}} = Spec.build(ForeverLoop)
    assert {:InvalidMode, "forever_loop", :loop_mode, :forever} in issues
    assert {:InvalidMode, "forever_loop", :rollback_mode, :best_effort} in issues
  end

  # --- a setting of the other kind ---

  # A bounded class writes the same builder an unbounded operator does, so what only a
  # stream carries is refused by its own name, carried as the topology's.
  defp kind_refusals(module) do
    assert {:error, {:invalid_bounded, issues}} = Spec.build(module)
    for {:Topology, issue} <- issues, do: elem(issue, 0)
  end

  test "the tier, a checkpoint setting and a correction window on a bounded operator are refused by name" do
    defmodule TierBounded do
      def factory_id, do: "tier_bounded"

      def channels(c),
        do: c |> Channels.effectively_once() |> Channels.input("in") |> Channels.into(TailTask)
    end

    defmodule CheckpointBounded do
      def factory_id, do: "checkpoint_bounded"

      def channels(c),
        do: c |> Channels.checkpoint_every(500) |> Channels.input("in") |> Channels.into(TailTask)
    end

    defmodule CorrectionBounded do
      def factory_id, do: "correction_bounded"

      def channels(c),
        do:
          c
          |> Channels.correction_window(60_000)
          |> Channels.input("in")
          |> Channels.into(TailTask)
    end

    assert kind_refusals(TierBounded) == [:EffectivelyOnceOnBounded]
    # Judged as the bounded operator's own setting, not by the stream rule that needs the tier.
    assert kind_refusals(CheckpointBounded) == [:CheckpointOnBounded]
    assert kind_refusals(CorrectionBounded) == [:CorrectionWindowOnBounded]
  end

  test "synchronous durability on an unbounded operator is refused, and it keeps its tier" do
    defmodule SyncUnbounded do
      @behaviour Flezha.SDK.UnboundedOperator
      def factory_id, do: "sync_unbounded"
      def sync_durability, do: true
      def channels(c), do: c |> Channels.input("in") |> Channels.into(TailTask)
    end

    defmodule TierUnbounded do
      @behaviour Flezha.SDK.UnboundedOperator
      def factory_id, do: "tier_unbounded"

      def channels(c),
        do: c |> Channels.effectively_once() |> Channels.input("in") |> Channels.into(TailTask)
    end

    assert {:error, {:invalid_topology, [{:SyncDurabilityOnUnbounded, _}]}} =
             Topology.build(SyncUnbounded)

    assert {:ok, %Topology{effectively_once?: true}} = Topology.build(TierUnbounded)
  end

  test "sleeping without persistence is refused for either kind, and both switches are declared" do
    defmodule SleeplessUnbounded do
      use Flezha.SDK.UnboundedOperator,
        factory_id: "sleepless_unbounded",
        persistence_enabled: false,
        sleep_when_idle: true

      def channels(c), do: c |> input("in") |> into(TailTask)
    end

    defmodule SleeplessBounded do
      use Flezha.SDK.BoundedOperator,
        factory_id: "sleepless_bounded",
        persistence_enabled: false,
        sleep_when_idle: true

      def channels(c), do: c |> input("in") |> into(TailTask)
    end

    defmodule SleepingUnbounded do
      use Flezha.SDK.UnboundedOperator, factory_id: "sleeping_unbounded", sleep_when_idle: true
      def channels(c), do: c |> input("in") |> into(TailTask)
    end

    defmodule OptedOutBounded do
      use Flezha.SDK.BoundedOperator,
        factory_id: "opted_out_bounded",
        persistence_enabled: false

      def channels(c), do: c |> input("in") |> into(TailTask)
    end

    assert {:error, {:invalid_topology, [{:SleepWithoutPersistence, _}]}} =
             Topology.build(SleeplessUnbounded)

    assert kind_refusals(SleeplessBounded) == [:SleepWithoutPersistence]

    assert {:ok, sleeping} = Topology.build(SleepingUnbounded)
    assert %{"sleep_when_idle" => "true"} = declared = Flezha.SDK.Declarations.for(sleeping)
    refute Map.has_key?(declared, "persistent")

    assert {:ok, durable} = Spec.build(LinearBounded)
    assert %{"sleep_when_idle" => "true"} = Flezha.SDK.Declarations.for(durable)

    assert {:ok, opted_out} = Spec.build(OptedOutBounded)
    declared = Flezha.SDK.Declarations.for(opted_out)
    assert declared["persistent"] == "false"
    refute Map.has_key?(declared, "sleep_when_idle")
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

    assert :NoEntryChannel in issues(Empty)
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

  test "a keyed edge into a keyless tasklet makes it dynamic, as an unbounded operator's" do
    defmodule DynamicBounded do
      def factory_id, do: "dynamic_bounded"

      def channels(c) do
        c
        |> Channels.input("in")
        |> Channels.into(PassTask)
        |> Channels.key_by(& &1)
        |> Channels.into(TailTask)
      end
    end

    assert {:ok, spec} = Spec.build(DynamicBounded)
    tail = Enum.find(spec.dag.order, &(spec.topology.node_modules[&1] == TailTask))
    assert spec.topology.node_mode[tail] == :dynamic
  end

  test "a static-mode tasklet is not a dynamic one" do
    defmodule StaticBounded do
      def factory_id, do: "static_bounded"

      def channels(c) do
        c |> Channels.input("in") |> Channels.into(StaticTask)
      end
    end

    assert {:ok, spec} = Spec.build(StaticBounded)
    assert Map.values(spec.topology.node_mode) == [:static]
  end

  test "BranchingChainedLoop — a chained loop over several terminals" do
    defmodule BranchingChain do
      def factory_id, do: "branching_chain"
      def loop_mode, do: :chained

      def channels(c) do
        mid = c |> Channels.input("in") |> Channels.into(PassTask)
        Channels.into(mid, TailTask)
        Channels.into(mid, OtherTailTask)
      end
    end

    defmodule ConvergingChain do
      def factory_id, do: "converging_chain"
      def loop_mode, do: :chained

      def channels(c) do
        mid = c |> Channels.input("in") |> Channels.into(PassTask)
        Channels.into(mid, TailTask)
      end
    end

    assert :BranchingChainedLoop in issues(BranchingChain)
    assert {:ok, _} = Spec.build(ConvergingChain)
  end

  test "a per_input loop over a branching topology is legal" do
    defmodule BranchingPerInput do
      def factory_id, do: "branching_per_input"
      def loop_mode, do: :per_input

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

    assert {:error, {:invalid_bounded, issues}} = Spec.build(BadTopology)
    assert [{:Topology, {:KeyByOnUnkeyableTarget, _}}] = issues
  end

  # --- accumulation ---

  test "every issue is reported at once, not just the first" do
    defmodule ManyProblems do
      def factory_id, do: "many_problems"
      def self_terminating?, do: false
    end

    defmodule TieredStopOnly do
      def factory_id, do: "tiered_stop_only"
      def self_terminating?, do: false

      def channels(c),
        do: c |> Channels.effectively_once() |> Channels.input("in") |> Channels.into(TailTask)
    end

    found = issues(ManyProblems)
    assert :NoBoundedBody in found
    assert :ExternalStopOnly in found

    assert {:error, {:invalid_bounded, issues}} = Spec.build(TieredStopOnly)
    assert {:ExternalStopOnly, "tiered_stop_only"} in issues
    assert {:Topology, {:EffectivelyOnceOnBounded, inspect(TieredStopOnly)}} in issues
  end
end
