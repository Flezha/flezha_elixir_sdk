# Twins of the rust_runner durable structured workflow fixtures
# (test/apps/rust_runner/src/workflow.rs) under the same factory ids,
# procedure names, channel names and output bytes; the loop and bench family
# is in workflows_loop.ex.
#
# Elixir SDK notes, each the same for every fixture here:
#
#   * the release gate is a `receive` with no `after`. It parks the Entity's
#     own process, which is what makes the crash window deterministic without
#     a timer — and `Process.sleep` would trip the `sleeps` ratchet as well as
#     freezing a scheduler for nothing.
#   * a bare `sink(name)` already ships a binary as its own bytes here, so the
#     fixtures that name an encoder do it to REWRITE the value, never to
#     correct a default.

defmodule ElixirRunner.Workflows.Gate do
  @moduledoc false

  @release_env "FLEZHA_WORKFLOW_E2E_RELEASE"
  @branch_fail_env "FLEZHA_BRANCH_FAIL"
  @retry_fails_env "FLEZHA_WORKFLOW_RETRY_FAILS"

  @doc """
  Park this Entity until the release env is set. The harness kills the first
  runner and launches the replacement WITH the variable, so the gate is a
  boot-time decision: never releasing on the first runner, returning at once
  on the second.
  """
  def block_until_released do
    if System.get_env(@release_env) do
      :ok
    else
      receive do
        :released -> :ok
      end
    end
  end

  def branch_fail?, do: System.get_env(@branch_fail_env) != nil

  def armed_failures do
    case System.get_env(@retry_fails_env) do
      nil -> 0
      value -> String.to_integer(value)
    end
  end

  @doc "Attempts the flaky procedure has made in this runner."
  def note_attempt do
    count = (:persistent_term.get({__MODULE__, :attempts}, 0) || 0) + 1
    :persistent_term.put({__MODULE__, :attempts}, count)
    count
  end
end

defmodule ElixirRunner.Workflows.Seen do
  @moduledoc false

  @doc """
  The reference's `SeenState`: a set kept sorted so snapshot bytes are
  deterministic, and idempotent on insert so re-running a procedure after a
  resume changes nothing. Every durable fixture here folds its changes with it.
  """
  def empty, do: %{items: []}

  def insert(state, item),
    do: %{state | items: state.items |> List.insert_at(0, item) |> Enum.uniq() |> Enum.sort()}
end

defmodule ElixirRunner.Workflows.Step do
  @moduledoc false

  @doc """
  The reference's `durable_step!`: a durable `String -> String` procedure that
  records one idempotent change keyed by its own name — so a resume re-running
  it is a no-op over the restored state — and passes the input on. `compensate`
  names the undo body when the step declares one.
  """
  defmacro defstep(module, name, opts \\ []) do
    body = durable_body(name, Keyword.get(opts, :compensate))

    quote do
      defmodule unquote(module) do
        @moduledoc false
        unquote(body)
      end
    end
  end

  @doc """
  The step's body as it stands inside its module. It is a function rather than
  only a macro because the sixteen bench chain positions are generated in a
  comprehension, where the step's name is a value the macro could not see, and
  generating them from a second copy of this body is how the two would drift.
  """
  def durable_body(name, compensate \\ nil) do
    quote do
      @behaviour Flezha.SDK.Procedure

      alias Flezha.SDK.Ctx

      def name, do: unquote(name)
      def init_state(_), do: ElixirRunner.Workflows.Seen.empty()
      def apply_change(state, item), do: ElixirRunner.Workflows.Seen.insert(state, item)

      def run(input, state, ctx) do
        state = Ctx.apply(ctx, state, unquote(name))
        Ctx.emit(ctx, input)
        {:ok, state}
      end

      unquote(compensate_body(compensate))
    end
  end

  defp compensate_body(nil), do: nil

  defp compensate_body(body) do
    quote do
      # A crash-resumed compensation reconstructs completed stages with EMPTY
      # outputs, so a hook that records an undo does so unconditionally.
      def compensate(_outputs, state, var!(ctx)) do
        _ = var!(ctx)
        unquote(body)
        {:ok, state}
      end
    end
  end
end

defmodule ElixirRunner.Workflows do
  @moduledoc false
  import ElixirRunner.Workflows.Step

  alias ElixirRunner.Workflows.Gate
  alias Flezha.SDK.Ctx

  # -- shared steps ---------------------------------------------------------

  defstep(CommitA, "commit_a")
  defstep(SagaA, "saga_a", compensate: Gate.block_until_released())
  # An undo that does nothing but record that it ran: the done marker it
  # leaves is what a crash mid-rollback reads to skip this step on resume.
  defstep(SagaB, "saga_b", compensate: :ok)
  defstep(BranchA, "branch_a", compensate: Ctx.emit(ctx, "undo-a"))
  defstep(BranchB, "branch_b", compensate: Ctx.emit(ctx, "undo-b"))
  defstep(DiamA, "diam_a")
  defstep(DiamB1, "diam_b1")
  defstep(RetryA, "retry_a", compensate: Ctx.emit(ctx, "undone"))

  defstep(UndoOutboxA, "undo_outbox_a",
    compensate:
      (
        Gate.block_until_released()
        Ctx.emit(ctx, "undone")
      )
  )

  defmodule BlockB do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "block_b"
    def init_state(_), do: nil

    def run(_input, state, _ctx) do
      Gate.block_until_released()
      {:ok, state}
    end
  end

  defmodule SagaC do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "saga_c"
    def init_state(_), do: nil
    def run(_input, _state, _ctx), do: {:error, {:non_retryable, "saga_c always fails"}}
  end

  defmodule UndoC do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "undo_outbox_c"
    def init_state(_), do: nil
    def run(_input, _state, _ctx), do: {:error, {:non_retryable, "undo_outbox_c always fails"}}
  end

  defmodule TierCommitA do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "tier_commit_a"
    def init_state(_), do: ElixirRunner.Workflows.Seen.empty()
    def apply_change(state, item), do: ElixirRunner.Workflows.Seen.insert(state, item)

    def run(input, state, ctx) do
      state = Ctx.apply(ctx, state, "tier_commit_a")
      Ctx.emit_keyed(ctx, "job", "committed:" <> input)
      {:ok, state}
    end
  end

  # The sibling leaf of the branch tree: emits straight to the sink, and its
  # undo does too.
  defmodule BranchC do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "branch_c"
    def init_state(_), do: ElixirRunner.Workflows.Seen.empty()
    def apply_change(state, item), do: ElixirRunner.Workflows.Seen.insert(state, item)

    def run(input, state, ctx) do
      state = Ctx.apply(ctx, state, "branch_c")
      Ctx.emit(ctx, "c:" <> input)
      {:ok, state}
    end

    def compensate(_outputs, state, ctx) do
      Ctx.emit(ctx, "undo-c")
      {:ok, state}
    end
  end

  # The deep leaf under BranchB: blocks (crash-resume), or fails (compensation).
  defmodule BranchD do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "branch_d"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      if Gate.branch_fail?() do
        {:error, {:non_retryable, "branch_d failed"}}
      else
        Gate.block_until_released()
        Ctx.emit(ctx, "d:" <> input)
        {:ok, state}
      end
    end
  end

  # Blocks BEFORE its durable change and emit, so the kill window is
  # deterministic: B2 is always the uncommitted producer while the sibling arm
  # committed.
  defmodule DiamB2 do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "diam_b2"
    def init_state(_), do: ElixirRunner.Workflows.Seen.empty()
    def apply_change(state, item), do: ElixirRunner.Workflows.Seen.insert(state, item)

    def run(input, state, ctx) do
      Gate.block_until_released()
      state = Ctx.apply(ctx, state, "diam_b")
      Ctx.emit(ctx, "b:" <> input)
      {:ok, state}
    end
  end

  defmodule DiamC do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "diam_c"
    def init_state(_), do: ElixirRunner.Workflows.Seen.empty()
    def apply_change(state, item), do: ElixirRunner.Workflows.Seen.insert(state, item)

    def run(input, state, ctx) do
      state = Ctx.apply(ctx, state, "diam_c")
      Ctx.emit(ctx, "c:" <> input)
      {:ok, state}
    end
  end

  defmodule DiamJoin do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "diam_join"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      Ctx.emit(ctx, "j:" <> input)
      {:ok, state}
    end
  end

  # Fails its first `FLEZHA_WORKFLOW_RETRY_FAILS` attempts, then emits the
  # attempt it succeeded on. A retry re-runs it in place, so a per-runner
  # counter is the whole ladder's view.
  defmodule RetryFlaky do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "retry_flaky"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      attempt = Gate.note_attempt()

      if attempt <= Gate.armed_failures() do
        {:error, "flaky attempt #{attempt}"}
      else
        Ctx.emit(ctx, "attempt:#{attempt}:#{input}")
        {:ok, state}
      end
    end
  end

  # -- classes --------------------------------------------------------------

  defmodule EchoWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "echo_workflow"

    def run(ctx, args) do
      Flezha.SDK.Workflow.Ctx.emit(ctx, "echo:" <> args)
      :ok
    end
  end

  defmodule CommitBlockWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "commit_block_workflow"

    def procedures(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Workflows.CommitA)
      |> named("mid")
      |> into(ElixirRunner.Workflows.BlockB)
    end
  end

  defmodule SyncDurableWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "sync_durable_workflow"
    def sync_durability, do: true

    def procedures(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Workflows.CommitA)
      |> named("mid")
      |> into(ElixirRunner.Workflows.BlockB)
    end
  end

  defmodule OptOutCommitBlockWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "optout_commit_block"
    def persistence_enabled, do: false

    def procedures(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Workflows.CommitA)
      |> named("mid")
      |> sink_with("optout_sink", fn s -> "a-ran:" <> s end)
      |> into(ElixirRunner.Workflows.BlockB)
    end
  end

  defmodule TierCommitBlockWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "tier_commit_block_workflow"

    def procedures(c) do
      import Flezha.SDK.Channels

      c
      |> effectively_once()
      |> input("in")
      |> into(ElixirRunner.Workflows.TierCommitA)
      |> named("mid")
      |> keyed()
      |> sink_with("tier_sink", & &1)
      |> into(ElixirRunner.Workflows.BlockB)
    end
  end

  defmodule SagaBlockWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "saga_block_workflow"

    def procedures(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Workflows.SagaA)
      |> named("s1")
      |> into(ElixirRunner.Workflows.SagaB)
      |> named("s2")
      |> into(ElixirRunner.Workflows.SagaC)
    end
  end

  defmodule OutboxBlockWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "outbox_block_workflow"

    def procedures(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Workflows.CommitA)
      |> named("mid")
      |> sink_with("outbox_sink", fn s -> "a-out:" <> s end)
      |> into(ElixirRunner.Workflows.BlockB)
    end
  end

  defmodule UndoOutboxWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "undo_outbox_workflow"

    def procedures(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Workflows.UndoOutboxA)
      |> named("mid")
      |> sink_with("undo_sink", fn s -> "out:" <> s end)
      |> into(ElixirRunner.Workflows.UndoC)
    end
  end

  # The procedure TREE. One channel, two consumers, is THE fan-out: both
  # branches read exactly what BranchA released on it.
  defmodule BranchWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "branch_workflow"

    # Fail fast: the compensation scenario is about the rollback order, not
    # the ladder before it.
    def retry_attempts, do: 0

    def procedures(c) do
      import Flezha.SDK.Channels

      mid =
        c
        |> input("in")
        |> into(ElixirRunner.Workflows.BranchA)
        |> named("mid")
        |> sink_with("branch_sink", fn s -> "a:" <> s end)

      mid
      |> into(ElixirRunner.Workflows.BranchB)
      |> named("bmid")
      |> sink_with("branch_sink", fn s -> "b:" <> s end)
      |> into(ElixirRunner.Workflows.BranchD)
      |> named("d_out")
      |> sink("branch_sink")

      mid
      |> into(ElixirRunner.Workflows.BranchC)
      |> named("c_out")
      |> sink("branch_sink")
    end
  end

  # The fan-in DAG. A held handle wired twice is the fan-out; two unnamed
  # mentions of DiamJoin are one node, dispatched once after both arms commit.
  defmodule DiamondWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "diamond_workflow"

    def procedures(c) do
      import Flezha.SDK.Channels

      fan =
        c
        |> input("in")
        |> into(ElixirRunner.Workflows.DiamA)
        |> named("dfan")
        |> sink_with("diamond_sink", fn s -> "a:" <> s end)

      fan
      |> into(ElixirRunner.Workflows.DiamB1)
      |> named("db1")
      |> into(ElixirRunner.Workflows.DiamB2)
      |> named("db2")
      |> sink_with("diamond_sink", & &1)
      |> into(ElixirRunner.Workflows.DiamJoin)

      fan
      |> into(ElixirRunner.Workflows.DiamC)
      |> named("dc")
      |> sink_with("diamond_sink", & &1)
      |> into(ElixirRunner.Workflows.DiamJoin)
      |> named("j_out")
      |> sink("diamond_sink")
    end
  end

  defmodule RetryWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "retry_workflow"

    def procedures(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Workflows.RetryA)
      |> named("mid")
      |> sink_with("retry_sink", fn s -> "res:" <> s end)
      |> into(ElixirRunner.Workflows.RetryFlaky)
      |> named("out")
      |> sink("retry_sink")
    end
  end

  @doc "The durable structured workflow classes this runner advertises."
  def all do
    [
      EchoWorkflow,
      CommitBlockWorkflow,
      SyncDurableWorkflow,
      OptOutCommitBlockWorkflow,
      TierCommitBlockWorkflow,
      SagaBlockWorkflow,
      OutboxBlockWorkflow,
      UndoOutboxWorkflow,
      BranchWorkflow,
      DiamondWorkflow,
      RetryWorkflow
    ]
  end
end
