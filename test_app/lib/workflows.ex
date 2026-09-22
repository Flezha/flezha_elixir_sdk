# Twins of the rust_runner durable structured workflow fixtures
# (test/apps/rust_runner/src/workflow.rs) under the same factory ids,
# tasklet names, channel names and output bytes; the loop and bench family
# is in workflows_loop.ex.
#
# Elixir SDK notes, each the same for every fixture here:
#
#   * the release gate is a `receive` with no `after`. It parks the Cell's
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
  Park this Cell until the release env is set. The harness kills the first
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

  @doc "Attempts the flaky tasklet has made in this runner."
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
  deterministic, and idempotent on insert so re-running a tasklet after a
  resume changes nothing. Every durable fixture here folds its changes with it.
  """
  def empty, do: %{items: []}

  def insert(state, item),
    do: %{state | items: state.items |> List.insert_at(0, item) |> Enum.uniq() |> Enum.sort()}
end

defmodule ElixirRunner.Workflows.Step do
  @moduledoc false

  @doc """
  The reference's `durable_step!`: a durable `String -> String` tasklet that
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
      @behaviour Flezha.SDK.Tasklet

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
  defstep(KeyedCommit, "keyed_commit", compensate: :ok)

  defstep(UndoOutboxA, "undo_outbox_a",
    compensate:
      (
        Gate.block_until_released()
        Ctx.emit(ctx, "undone")
      )
  )

  defmodule BlockB do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "block_b"
    def init_state(_), do: nil

    def run(_input, state, _ctx) do
      Gate.block_until_released()
      {:ok, state}
    end
  end

  defmodule KeySplit do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "key_split"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      input |> String.split(",") |> Enum.each(&Ctx.emit(ctx, &1))
      {:ok, state}
    end
  end

  defmodule KeyedFail do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "keyed_fail"
    def init_state(_), do: nil
    def run(_input, _state, _ctx), do: {:error, {:non_retryable, "keyed_fail always fails"}}
  end

  defmodule SagaC do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "saga_c"
    def init_state(_), do: nil
    def run(_input, _state, _ctx), do: {:error, {:non_retryable, "saga_c always fails"}}
  end

  defmodule UndoC do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "undo_outbox_c"
    def init_state(_), do: nil
    def run(_input, _state, _ctx), do: {:error, {:non_retryable, "undo_outbox_c always fails"}}
  end

  defmodule TierCommitA do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "tier_commit_a"
    def init_state(_), do: ElixirRunner.Workflows.Seen.empty()
    def apply_change(state, item), do: ElixirRunner.Workflows.Seen.insert(state, item)

    def run(input, state, ctx) do
      state = Ctx.apply(ctx, state, "tier_commit_a")
      Ctx.emit(ctx, "committed:" <> input)
      {:ok, state}
    end
  end

  # The sibling leaf of the branch tree: emits straight to the sink, and its
  # undo does too.
  defmodule BranchC do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
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
    @behaviour Flezha.SDK.Tasklet
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
    @behaviour Flezha.SDK.Tasklet
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
    @behaviour Flezha.SDK.Tasklet
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
    @behaviour Flezha.SDK.Tasklet
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
    @behaviour Flezha.SDK.Tasklet
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

  # Echoes the first message, which a `:once` workflow's one pass runs on.
  defmodule Echo do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "echo"
    def init_state(_), do: nil

    def run(args, state, ctx) do
      Ctx.emit(ctx, "echo:" <> args)
      {:ok, state}
    end
  end

  # -- classes --------------------------------------------------------------

  defmodule EchoWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "echo_workflow"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Workflows.Echo)
      |> sink("out")
    end
  end

  # `typed_once_workflow`: a `once` class whose entry channel `order` decodes
  # a JSON ticket; its one tasklet prints the text, emits it to its sink and
  # completes with it, so the pass runs on the payload it was spawned with.
  defmodule OncePlaced do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "once_placed"
    def init_state(_), do: nil

    def run(%{"text" => text}, state, ctx) do
      IO.puts("once_ran #{text}")
      Flezha.SDK.Ctx.complete(ctx, text)
      Flezha.SDK.Ctx.emit(ctx, "placed:" <> text)
      {:ok, state}
    end
  end

  defmodule TypedOnceWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "typed_once_workflow"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input_with("order", &Jason.decode/1)
      |> into(ElixirRunner.Workflows.OncePlaced)
      |> sink("placed")
    end
  end

  defmodule CommitBlockWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "commit_block_workflow"

    def channels(c) do
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

    def channels(c) do
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

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Workflows.CommitA)
      |> sink_with("mid", fn s -> "a-ran:" <> s end)
      |> into(ElixirRunner.Workflows.BlockB)
    end
  end

  # The hybrid mid-chain shape the conformance fixture's `commit_block_chain`
  # twin is transcribed from: A's channel reaches a plugin sink and feeds B.
  defmodule TierCommitBlockWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "tier_commit_block_workflow"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Workflows.TierCommitA)
      |> sink_with("mid", & &1)
      |> into(ElixirRunner.Workflows.BlockB)
    end
  end

  defmodule SagaBlockWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "saga_block_workflow"

    def channels(c) do
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

  # The entry's comma-separated key list is split into one value per key, and
  # a `key_by` on that value mints one Cell per key of a durable compensable
  # tasklet that declares no keys; each Cell commits one idempotent change.
  # The third tasklet blocks until the release env, or fails.
  def keyed_commits(c) do
    import Flezha.SDK.Channels

    c
    |> input("in")
    |> into(ElixirRunner.Workflows.KeySplit)
    |> named("keys")
    |> key_by(& &1)
    |> into(ElixirRunner.Workflows.KeyedCommit)
    |> named("committed")
  end

  defmodule KeyedBlockWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "keyed_block_workflow"

    def channels(c),
      do:
        c
        |> ElixirRunner.Workflows.keyed_commits()
        |> Flezha.SDK.Channels.into(ElixirRunner.Workflows.BlockB)
  end

  defmodule KeyedSagaWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "keyed_saga_workflow"

    def channels(c),
      do:
        c
        |> ElixirRunner.Workflows.keyed_commits()
        |> Flezha.SDK.Channels.into(ElixirRunner.Workflows.KeyedFail)
  end

  defmodule OutboxBlockWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "outbox_block_workflow"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Workflows.CommitA)
      |> sink_with("mid", fn s -> "a-out:" <> s end)
      |> into(ElixirRunner.Workflows.BlockB)
    end
  end

  defmodule UndoOutboxWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "undo_outbox_workflow"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Workflows.UndoOutboxA)
      |> sink_with("mid", fn s -> "out:" <> s end)
      |> into(ElixirRunner.Workflows.UndoC)
    end
  end

  # The tasklet TREE. One channel, two consumers, is THE fan-out: both
  # branches read exactly what BranchA released on it.
  defmodule BranchWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "branch_workflow"

    def channels(c) do
      import Flezha.SDK.Channels

      mid =
        c
        |> input("in")
        |> into(ElixirRunner.Workflows.BranchA)
        |> sink_with("mid", fn s -> "a:" <> s end)

      # Fail fast: the compensation scenario is about the rollback order, not
      # the ladder before it.
      mid
      |> into(ElixirRunner.Workflows.BranchB)
      |> sink_with("bmid", fn s -> "b:" <> s end)
      |> retry_max(1)
      |> into(ElixirRunner.Workflows.BranchD)
      |> sink("d_out")

      mid
      |> into(ElixirRunner.Workflows.BranchC)
      |> sink("c_out")
    end
  end

  # The fan-in DAG. A held handle wired twice is the fan-out; two unnamed
  # mentions of DiamJoin are one node, dispatched once after both arms commit.
  defmodule DiamondWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "diamond_workflow"

    def channels(c) do
      import Flezha.SDK.Channels

      fan =
        c
        |> input("in")
        |> into(ElixirRunner.Workflows.DiamA)
        |> sink_with("dfan", fn s -> "a:" <> s end)

      fan
      |> into(ElixirRunner.Workflows.DiamB1)
      |> named("db1")
      |> into(ElixirRunner.Workflows.DiamB2)
      |> sink_with("db2", & &1)
      |> into(ElixirRunner.Workflows.DiamJoin)

      fan
      |> into(ElixirRunner.Workflows.DiamC)
      |> sink_with("dc", & &1)
      |> into(ElixirRunner.Workflows.DiamJoin)
      |> sink("j_out")
    end
  end

  defmodule RetryWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "retry_workflow"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Workflows.RetryA)
      |> sink_with("mid", fn s -> "res:" <> s end)
      |> into(ElixirRunner.Workflows.RetryFlaky)
      |> sink("out")
    end
  end

  # `workflow_timer_refused`: a tasklet that registers a timer inside a
  # workflow. Timers do not fire there yet, so the ctx refuses naming
  # `TimersUnavailable`, the tasklet fails with the refusal, and the instance
  # ends in error rather than losing the timer.
  defmodule TimerRefused do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "workflow_timer_refused"
    def init_state(_), do: nil

    def run(_input, state, ctx) do
      with {:ok, _at} <- Ctx.timer_after(ctx, 60_000), do: {:ok, state}
    end
  end

  defmodule WorkflowTimerRefused do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "workflow_timer_refused"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("input")
      |> into(ElixirRunner.Workflows.TimerRefused)
    end
  end

  # `static_cell_workflow`: a per-input loop whose one tasklet declares three
  # static cells and routes every message to two of them; each cell that runs
  # emits `<cell key>:<message>`, so one message yields exactly two outputs.
  defmodule StaticCellStep do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "static_cell_step"
    def init_state(_), do: nil
    def cell_keys, do: ["a", "b", "c"]
    def cells_for(_input), do: ["a", "b"]

    def run(input, state, ctx) do
      Ctx.emit(ctx, "#{Ctx.cell_key(ctx)}:#{input}")
      {:ok, state}
    end
  end

  defmodule StaticCellWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "static_cell_workflow"
    def loop_mode, do: :per_input
    def self_terminating?, do: false
    def max_duration_ms, do: 600_000

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Workflows.StaticCellStep)
      |> sink("out")
    end
  end

  @doc "The durable structured workflow classes this runner advertises."
  def all do
    [
      EchoWorkflow,
      WorkflowTimerRefused,
      StaticCellWorkflow,
      TypedOnceWorkflow,
      CommitBlockWorkflow,
      SyncDurableWorkflow,
      OptOutCommitBlockWorkflow,
      TierCommitBlockWorkflow,
      SagaBlockWorkflow,
      KeyedBlockWorkflow,
      KeyedSagaWorkflow,
      OutboxBlockWorkflow,
      UndoOutboxWorkflow,
      BranchWorkflow,
      DiamondWorkflow,
      RetryWorkflow
    ]
  end
end
