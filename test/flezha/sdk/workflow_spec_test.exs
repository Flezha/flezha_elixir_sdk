defmodule Flezha.SDK.Workflow.SpecTest do
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Channels, Ctx}
  alias Flezha.SDK.Workflow.Spec

  # --- fixtures ---

  defmodule PassProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "pass"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      Ctx.emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule TailProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "tail"
    def init_state(_), do: nil
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule OtherTailProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "other_tail"
    def init_state(_), do: nil
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule StaticProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "static"
    def entity_keys, do: ["1m"]
    def init_state(_), do: nil
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule EchoWorkflow do
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "echo_workflow"

    def run(ctx, args) do
      Ctx.emit(ctx, "echo:" <> args)
      :ok
    end
  end

  defmodule LinearWorkflow do
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "linear_workflow"

    def procedures(c) do
      c
      |> Channels.input("in")
      |> Channels.into(PassProc)
      |> Channels.into(TailProc)
    end
  end

  defp issues(module) do
    assert {:error, {:invalid_workflow, issues}} = Spec.build(module)
    Enum.map(issues, &elem(&1, 0))
  end

  # --- the two bodies ---

  test "a run-sugar class builds with no topology and an empty DAG" do
    assert {:ok, spec} = Spec.build(EchoWorkflow)
    assert spec.factory_id == "echo_workflow"
    assert spec.topology == nil
    assert spec.dag.order == []
    assert spec.knobs.loop_mode == :once
    assert spec.knobs.rollback_mode == :all_or_nothing
    assert spec.knobs.retry_attempts == 3
    assert spec.knobs.persistence_enabled
  end

  test "a structured class builds a Kahn-ordered DAG rooted at the entry's consumer" do
    assert {:ok, spec} = Spec.build(LinearWorkflow)
    assert [root, tail] = spec.dag.order
    assert spec.dag.root == root
    assert spec.dag.leaves == [tail]
    assert spec.dag.pos[root] == 0
    assert spec.dag.pos[tail] == 1
    assert spec.dag.parents[tail] == [root]
    assert spec.dag.children[root] == [tail]
  end

  test "proc_idx numbering is the declaration's, not the ref map's" do
    orders =
      for _ <- 1..8 do
        assert {:ok, spec} = Spec.build(LinearWorkflow)
        Enum.map(spec.dag.order, &spec.topology.node_modules[&1])
      end

    assert Enum.uniq(orders) == [[PassProc, TailProc]]
  end

  # --- class-fact refusals ---

  test "NoWorkflowBody — neither run/2 nor procedures/1" do
    defmodule Bodyless do
      def factory_id, do: "bodyless"
    end

    assert :NoWorkflowBody in issues(Bodyless)
  end

  test "ExternalStopOnly — not self-terminating and no duration cap" do
    defmodule StopOnly do
      def factory_id, do: "stop_only"
      def self_terminating?, do: false
      def run(_ctx, _args), do: :ok
    end

    defmodule StopOnlyCapped do
      def factory_id, do: "stop_only_capped"
      def self_terminating?, do: false
      def max_duration_ms, do: 5_000
      def run(_ctx, _args), do: :ok
    end

    assert :ExternalStopOnly in issues(StopOnly)
    assert {:ok, _} = Spec.build(StopOnlyCapped)
  end

  test "ZeroRetryBackoff — a ladder with a zero base" do
    defmodule HotLadder do
      def factory_id, do: "hot_ladder"
      def retry_backoff_ms, do: 0
      def run(_ctx, _args), do: :ok
    end

    defmodule FailFast do
      def factory_id, do: "fail_fast"
      def retry_backoff_ms, do: 0
      def retry_attempts, do: 1
      def run(_ctx, _args), do: :ok
    end

    assert :ZeroRetryBackoff in issues(HotLadder)
    assert {:ok, _} = Spec.build(FailFast), "a zero base is legal for fail-fast"
  end

  test "LoopWithoutChain — a loop mode on a run-sugar body" do
    defmodule LoopingSugar do
      def factory_id, do: "looping_sugar"
      def loop_mode, do: :chained
      def run(_ctx, _args), do: :ok
    end

    assert :LoopWithoutChain in issues(LoopingSugar)
  end

  # --- structural refusals ---

  test "NoEntryChannel — procedures with nothing to seed them" do
    defmodule NoEntry do
      def factory_id, do: "no_entry"

      def procedures(c) do
        c |> Channels.input("in") |> Channels.into(PassProc)
        :ok
      end
    end

    # A class whose `procedures/1` declares no channel at all.
    defmodule Empty do
      def factory_id, do: "empty"
      def procedures(_c), do: :ok
    end

    assert :NoEntryChannel in issues(Empty)
    assert {:ok, _} = Spec.build(NoEntry)
  end

  test "EntryRoot — the entry channel feeds more than one procedure" do
    defmodule TwoRoots do
      def factory_id, do: "two_roots"

      def procedures(c) do
        s = Channels.input(c, "in")
        Channels.into(s, PassProc)
        Channels.into(s, TailProc)
      end
    end

    assert :EntryRoot in issues(TwoRoots)
  end

  test "MergeFromEntryChannel — the entry is one merge source among several" do
    defmodule EntryMerge do
      def factory_id, do: "entry_merge"

      def procedures(c) do
        entry = Channels.input(c, "in")
        entry |> Channels.into(PassProc) |> Channels.into(TailProc)
        Channels.into(entry, TailProc)
      end
    end

    assert :MergeFromEntryChannel in issues(EntryMerge)
  end

  test "a second entry channel is refused as two roots, which is the only shape that could orphan one" do
    defmodule Orphan do
      def factory_id, do: "orphan"

      def procedures(c) do
        c |> Channels.input("in") |> Channels.into(PassProc)
        c |> Channels.input("other") |> Channels.into(TailProc)
      end
    end

    assert :EntryRoot in issues(Orphan)
  end

  test "DynamicEntities — a keyed edge into a keyless procedure" do
    defmodule DynamicWorkflow do
      def factory_id, do: "dynamic_workflow"

      def procedures(c) do
        c
        |> Channels.input("in")
        |> Channels.into(PassProc)
        |> Channels.key_by(& &1)
        |> Channels.into(TailProc)
      end
    end

    assert :DynamicEntities in issues(DynamicWorkflow)
  end

  test "a static-mode procedure is not a dynamic one" do
    defmodule StaticWorkflow do
      def factory_id, do: "static_workflow"

      def procedures(c) do
        c |> Channels.input("in") |> Channels.into(StaticProc)
      end
    end

    assert {:ok, spec} = Spec.build(StaticWorkflow)
    assert Map.values(spec.topology.node_mode) == [:static]
  end

  test "BranchingChainedLoop — a chained loop over several terminals" do
    defmodule BranchingChain do
      def factory_id, do: "branching_chain"
      def loop_mode, do: :chained

      def procedures(c) do
        mid = c |> Channels.input("in") |> Channels.into(PassProc)
        Channels.into(mid, TailProc)
        Channels.into(mid, OtherTailProc)
      end
    end

    defmodule ConvergingChain do
      def factory_id, do: "converging_chain"
      def loop_mode, do: :chained

      def procedures(c) do
        mid = c |> Channels.input("in") |> Channels.into(PassProc)
        Channels.into(mid, TailProc)
      end
    end

    assert :BranchingChainedLoop in issues(BranchingChain)
    assert {:ok, _} = Spec.build(ConvergingChain)
  end

  test "a per_input loop over a branching topology is legal" do
    defmodule BranchingPerInput do
      def factory_id, do: "branching_per_input"
      def loop_mode, do: :per_input

      def procedures(c) do
        mid = c |> Channels.input("in") |> Channels.into(PassProc)
        Channels.into(mid, TailProc)
        Channels.into(mid, OtherTailProc)
      end
    end

    assert {:ok, spec} = Spec.build(BranchingPerInput)
    assert length(spec.dag.leaves) == 2
  end

  # --- the underlying topology's refusals still apply ---

  test "a topology refusal is carried through, named as one" do
    defmodule BadTopology do
      def factory_id, do: "bad_topology"

      def procedures(c) do
        c |> Channels.input("in") |> Channels.key_by(& &1) |> Channels.into(StaticProc)
      end
    end

    assert {:error, {:invalid_workflow, issues}} = Spec.build(BadTopology)
    assert [{:Topology, {:KeyByOnUnkeyableTarget, _}}] = issues
  end

  # --- accumulation ---

  test "every issue is reported at once, not just the first" do
    defmodule ManyProblems do
      def factory_id, do: "many_problems"
      def self_terminating?, do: false
      def retry_backoff_ms, do: 0
      def loop_mode, do: :chained
      def run(_ctx, _args), do: :ok
    end

    found = issues(ManyProblems)
    assert :ExternalStopOnly in found
    assert :ZeroRetryBackoff in found
    assert :LoopWithoutChain in found
  end
end
