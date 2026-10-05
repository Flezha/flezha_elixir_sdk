# Twins of the rust_runner durable structured steps fixtures
# (test/apps/rust_runner/src/steps.rs) under the same factory ids,
# tasklet names, channel names and output bytes; the loop and bench family
# is in steps_loop.ex.
#
# Elixir SDK notes, each the same for every fixture here:
#
#   * the release gate is a `receive` with no `after`. It parks the Cell's
#     own process, which is what makes the crash window deterministic without
#     a timer — and `Process.sleep` would trip the `sleeps` ratchet as well as
#     freezing a scheduler for nothing.
#   * a bare `output(name)` already ships a binary as its own bytes here, so the
#     fixtures that name an encoder do it to REWRITE the value, never to
#     correct a default.

defmodule ElixirRunner.Steps.Gate do
  @moduledoc false

  @release_env "FLEZHA_STEPS_E2E_RELEASE"
  @branch_fail_env "FLEZHA_BRANCH_FAIL"
  @retry_fails_env "FLEZHA_STEPS_RETRY_FAILS"

  @doc """
  Park this Cell until the release env is set. The harness kills the first
  runner and launches the replacement WITH the variable, so the gate is a
  boot-time decision: never releasing on the first runner, returning at once
  on the second.
  """
  def block_until_released do
    if released?() do
      :ok
    else
      receive do
        :released -> :ok
      end
    end
  end

  @doc "Whether this runner was booted with the release env set."
  def released?, do: System.get_env(@release_env) != nil

  @doc "Park at `point` when this runner was booted to hold there, until the release."
  def hold_at(point) do
    if System.get_env("FLEZHA_STEPS_E2E_HOLD") == point, do: block_until_released()
    :ok
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

defmodule ElixirRunner.Steps.Seen do
  @moduledoc false

  @doc """
  The reference's `SeenState`: a set kept sorted so snapshot bytes are
  deterministic, and idempotent on insert so re-running a tasklet after a
  resume changes nothing. Every durable fixture here folds its changes with it:
  `use` gives a tasklet the empty set and the `insert` change.
  """
  defmacro __using__(_opts) do
    quote do
      require Flezha.SDK.State

      @impl true
      def init_state(_), do: ElixirRunner.Steps.Seen.empty()

      Flezha.SDK.State.defchange insert(state, item) do
        ElixirRunner.Steps.Seen.insert(state, item)
      end
    end
  end

  def empty, do: %{items: []}

  def insert(state, item),
    do: %{state | items: state.items |> List.insert_at(0, item) |> Enum.uniq() |> Enum.sort()}
end

defmodule ElixirRunner.Steps.Step do
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
      use Flezha.SDK.Tasklet, name: unquote(name)

      use ElixirRunner.Steps.Seen

      @impl true
      def run(input, state, ctx) do
        state = insert(state, ctx, unquote(name))
        emit(ctx, input)
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
      @impl true
      def compensate(_outputs, state, var!(ctx)) do
        _ = var!(ctx)
        unquote(body)
        {:ok, state}
      end
    end
  end
end

defmodule ElixirRunner.Steps do
  @moduledoc false
  import ElixirRunner.Steps.Step

  alias ElixirRunner.Steps.Gate

  # -- shared steps ---------------------------------------------------------

  defstep(CommitA, "commit_a")
  defstep(SagaA, "saga_a", compensate: Gate.block_until_released())
  # An undo that does nothing but record that it ran: the done marker it
  # leaves is what a crash mid-rollback reads to skip this step on resume.
  defstep(SagaB, "saga_b", compensate: :ok)
  defstep(BranchA, "branch_a", compensate: emit(ctx, "undo-a"))
  defstep(BranchB, "branch_b", compensate: emit(ctx, "undo-b"))
  defstep(DiamA, "diam_a")
  defstep(DiamB1, "diam_b1")
  defstep(RetryA, "retry_a", compensate: emit(ctx, "undone"))
  defstep(KeyedCommit, "keyed_commit", compensate: :ok)

  defstep(UndoOutboxA, "undo_outbox_a",
    compensate:
      (
        Gate.block_until_released()
        emit(ctx, "undone")
      )
  )

  defmodule BlockB do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "block_b"

    @impl true
    def run(_input, state, _ctx) do
      Gate.block_until_released()
      {:ok, state}
    end
  end

  defmodule KeySplit do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "key_split"

    @impl true
    def run(input, state, ctx) do
      input |> String.split(",") |> Enum.each(&emit(ctx, &1))
      {:ok, state}
    end
  end

  defmodule KeyedFail do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "keyed_fail"
    @impl true
    def run(_input, _state, _ctx), do: {:error, {:non_retryable, "keyed_fail always fails"}}
  end

  defmodule SagaC do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "saga_c"
    @impl true
    def run(_input, _state, _ctx), do: {:error, {:non_retryable, "saga_c always fails"}}
  end

  defmodule UndoC do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "undo_outbox_c"
    @impl true
    def run(_input, _state, _ctx), do: {:error, {:non_retryable, "undo_outbox_c always fails"}}
  end

  defmodule TierCommitA do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "tier_commit_a"
    use ElixirRunner.Steps.Seen

    @impl true
    def run(input, state, ctx) do
      state = insert(state, ctx, "tier_commit_a")
      emit(ctx, "committed:" <> input)
      {:ok, state}
    end
  end

  # The sibling leaf of the branch tree: emits straight to the output, and its
  # undo does too.
  defmodule BranchC do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "branch_c"
    use ElixirRunner.Steps.Seen

    @impl true
    def run(input, state, ctx) do
      state = insert(state, ctx, "branch_c")
      emit(ctx, "c:" <> input)
      {:ok, state}
    end

    @impl true
    def compensate(_outputs, state, ctx) do
      emit(ctx, "undo-c")
      {:ok, state}
    end
  end

  # The deep leaf under BranchB: blocks (crash-resume), or fails (compensation).
  defmodule BranchD do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "branch_d"

    @impl true
    def run(input, state, ctx) do
      if Gate.branch_fail?() do
        {:error, {:non_retryable, "branch_d failed"}}
      else
        Gate.block_until_released()
        emit(ctx, "d:" <> input)
        {:ok, state}
      end
    end
  end

  # Blocks BEFORE its durable change and emit, so the kill window is
  # deterministic: B2 is always the uncommitted producer while the sibling arm
  # committed.
  defmodule DiamB2 do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "diam_b2"
    use ElixirRunner.Steps.Seen

    @impl true
    def run(input, state, ctx) do
      Gate.block_until_released()
      state = insert(state, ctx, "diam_b")
      emit(ctx, "b:" <> input)
      {:ok, state}
    end
  end

  defmodule DiamC do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "diam_c"
    use ElixirRunner.Steps.Seen

    @impl true
    def run(input, state, ctx) do
      state = insert(state, ctx, "diam_c")
      emit(ctx, "c:" <> input)
      {:ok, state}
    end
  end

  defmodule DiamJoin do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "diam_join"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, "j:" <> input)
      {:ok, state}
    end
  end

  # Fails its first `FLEZHA_STEPS_RETRY_FAILS` attempts, then emits the
  # attempt it succeeded on. A retry re-runs it in place, so a per-runner
  # counter is the whole ladder's view.
  defmodule RetryFlaky do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "retry_flaky"

    @impl true
    def run(input, state, ctx) do
      attempt = Gate.note_attempt()

      if attempt <= Gate.armed_failures() do
        {:error, "flaky attempt #{attempt}"}
      else
        emit(ctx, "attempt:#{attempt}:#{input}")
        {:ok, state}
      end
    end
  end

  # Echoes the first message, which a `:once` steps relay's one pass runs on.
  defmodule Echo do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "echo"

    @impl true
    def run(args, state, ctx) do
      emit(ctx, "echo:" <> args)
      {:ok, state}
    end
  end

  # -- classes --------------------------------------------------------------

  defmodule EchoSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    def durable, do: ElixirRunner.fixture_durable()
    def factory_id, do: "echo_steps"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Steps.Echo)
      |> output("out")
    end
  end

  # `typed_once_steps`: a `once` class whose entry channel `order` decodes
  # a JSON ticket; its one tasklet prints the text, emits it to its output and
  # completes with it, so the pass runs on the payload it was spawned with.
  defmodule OncePlaced do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "once_placed"

    @impl true
    def run(%{"text" => text}, state, ctx) do
      IO.puts("once_ran #{text}")
      complete(ctx, text)
      emit(ctx, "placed:" <> text)
      {:ok, state}
    end
  end

  defmodule TypedOnceSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    def durable, do: ElixirRunner.fixture_durable()
    def factory_id, do: "typed_once_steps"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input_with("order", &Jason.decode/1)
      |> into(ElixirRunner.Steps.OncePlaced)
      |> output("placed")
    end
  end

  defmodule CommitBlockSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    def durable, do: ElixirRunner.fixture_durable()
    def factory_id, do: "commit_block_steps"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Steps.CommitA)
      |> named("mid")
      |> into(ElixirRunner.Steps.BlockB)
    end
  end

  defmodule SyncDurableSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "sync_durable_steps"
    def run, do: :steps
    def durable, do: :remote

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Steps.CommitA)
      |> named("mid")
      |> into(ElixirRunner.Steps.BlockB)
    end
  end

  defmodule OptOutCommitBlockSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "optout_commit_block"
    def run, do: :steps
    def durable, do: false

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Steps.CommitA)
      |> output_with("mid", fn s -> "a-ran:" <> s end)
      |> into(ElixirRunner.Steps.BlockB)
    end
  end

  # The hybrid mid-chain shape the conformance fixture's `commit_block_chain`
  # twin is transcribed from: A's channel reaches a plugin output and feeds B.
  defmodule TierCommitBlockSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    def durable, do: ElixirRunner.fixture_durable()
    def factory_id, do: "tier_commit_block_steps"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Steps.TierCommitA)
      |> output_with("mid", & &1)
      |> into(ElixirRunner.Steps.BlockB)
    end
  end

  defmodule SagaBlockSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    def durable, do: ElixirRunner.fixture_durable()
    def factory_id, do: "saga_block_steps"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Steps.SagaA)
      |> named("s1")
      |> into(ElixirRunner.Steps.SagaB)
      |> named("s2")
      |> into(ElixirRunner.Steps.SagaC)
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
    |> into(ElixirRunner.Steps.KeySplit)
    |> named("keys")
    |> key_by(& &1)
    |> into(ElixirRunner.Steps.KeyedCommit)
    |> named("committed")
  end

  defmodule KeyedBlockSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    def durable, do: ElixirRunner.fixture_durable()
    def factory_id, do: "keyed_block_steps"

    def channels(c),
      do:
        c
        |> ElixirRunner.Steps.keyed_commits()
        |> Flezha.SDK.Channels.into(ElixirRunner.Steps.BlockB)
  end

  defmodule KeyedSagaSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    def durable, do: ElixirRunner.fixture_durable()
    def factory_id, do: "keyed_saga_steps"

    def channels(c),
      do:
        c
        |> ElixirRunner.Steps.keyed_commits()
        |> Flezha.SDK.Channels.into(ElixirRunner.Steps.KeyedFail)
  end

  # A steps output's identity: `commit_a` commits durable state AND emits
  # once, keyed `k` with one metadata pair, on a keyed hybrid channel; the
  # output rides its Cell's record and reaches the output once the stage commits.
  defmodule OutboxA do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "commit_a"
    use ElixirRunner.Steps.Seen

    @impl true
    def run(input, state, ctx) do
      state = insert(state, ctx, "commit_a")
      :ok = emit_keyed_with_meta(ctx, "k", input, %{"trace" => "t1"})
      {:ok, state}
    end
  end

  defmodule OutboxBlockSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    def durable, do: ElixirRunner.fixture_durable()
    def factory_id, do: "outbox_block_steps"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Steps.OutboxA)
      |> keyed()
      |> output_with("mid", fn s -> "a-out:" <> s end)
      |> into(ElixirRunner.Steps.BlockB)
    end
  end

  defmodule UndoOutboxSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    def durable, do: ElixirRunner.fixture_durable()
    def factory_id, do: "undo_outbox_steps"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Steps.UndoOutboxA)
      |> output_with("mid", fn s -> "out:" <> s end)
      |> into(ElixirRunner.Steps.UndoC)
    end
  end

  # A failed stage delivers nothing. The entry's two values reach one
  # `failed_stage` Cell in order: the first emits on a plugin channel, so the
  # Cell's record carries an output; the second fails, so the stage never
  # commits and no marker ever names that record.
  defmodule FailedStage do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "failed_stage"
    @impl true
    def run("b", _state, _ctx), do: {:error, {:non_retryable, "failed_stage fails on b"}}

    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule FailedStageSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    def durable, do: ElixirRunner.fixture_durable()
    def factory_id, do: "failed_stage_steps"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Steps.KeySplit)
      |> into(ElixirRunner.Steps.FailedStage)
      |> output_with("mid", fn s -> "lost:" <> s end)
    end
  end

  # The tasklet TREE. One channel, two consumers, is THE fan-out: both
  # branches read exactly what BranchA released on it.
  defmodule BranchSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    def durable, do: ElixirRunner.fixture_durable()
    def factory_id, do: "branch_steps"

    def channels(c) do
      import Flezha.SDK.Channels

      mid =
        c
        |> input("in")
        |> into(ElixirRunner.Steps.BranchA)
        |> output_with("mid", fn s -> "a:" <> s end)

      # Fail fast: the compensation scenario is about the rollback order, not
      # the ladder before it.
      mid
      |> into(ElixirRunner.Steps.BranchB)
      |> output_with("bmid", fn s -> "b:" <> s end)
      |> retry_max(1)
      |> into(ElixirRunner.Steps.BranchD)
      |> output("d_out")

      mid
      |> into(ElixirRunner.Steps.BranchC)
      |> output("c_out")
    end
  end

  # The fan-in DAG. A held handle wired twice is the fan-out; two unnamed
  # mentions of DiamJoin are one node, dispatched once after both arms commit.
  defmodule DiamondSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    def durable, do: ElixirRunner.fixture_durable()
    def factory_id, do: "diamond_steps"

    def channels(c) do
      import Flezha.SDK.Channels

      fan =
        c
        |> input("in")
        |> into(ElixirRunner.Steps.DiamA)
        |> output_with("dfan", fn s -> "a:" <> s end)

      fan
      |> into(ElixirRunner.Steps.DiamB1)
      |> named("db1")
      |> into(ElixirRunner.Steps.DiamB2)
      |> output_with("db2", & &1)
      |> into(ElixirRunner.Steps.DiamJoin)

      fan
      |> into(ElixirRunner.Steps.DiamC)
      |> output_with("dc", & &1)
      |> into(ElixirRunner.Steps.DiamJoin)
      |> output("j_out")
    end
  end

  defmodule RetrySteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    def durable, do: ElixirRunner.fixture_durable()
    def factory_id, do: "retry_steps"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Steps.RetryA)
      |> output_with("mid", fn s -> "res:" <> s end)
      |> into(ElixirRunner.Steps.RetryFlaky)
      |> output("out")
    end
  end

  # `timer_loop_steps`: `in -> tick -> tock -> out`, a per-input loop whose
  # `tick` arms a processing-time timer of the delay `after:<ms>` names and
  # emits `armed:<at>`; its firing emits `fired:<at>` and opens a pass of its
  # own. `slow:<ms>` holds `tick` that long, for a firing due mid-pass. On a
  # runner booted with `FLEZHA_STEPS_E2E_HOLD` the firing parks at the gate:
  # `timer` inside `on_timer`, before `tick` commits; `tock` in `tock`, after.
  defmodule Tick do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "tick"
    use ElixirRunner.Steps.Seen

    @impl true
    def run("after:" <> delay, state, ctx) do
      {:ok, at} = timer_after(ctx, String.to_integer(delay))
      emit(ctx, "armed:#{at}")
      {:ok, insert(state, ctx, "armed:#{at}")}
    end

    def run("slow:" <> pause = input, state, ctx) do
      Process.sleep(String.to_integer(pause))
      emit(ctx, input)
      {:ok, state}
    end

    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end

    @impl true
    def on_timer(fired_at, _kind, state, ctx) do
      Gate.hold_at("timer")
      emit(ctx, "fired:#{fired_at}")
      {:ok, state}
    end
  end

  defmodule Tock do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "tock"

    @impl true
    def run(input, state, ctx) do
      if String.starts_with?(input, "fired:"), do: Gate.hold_at("tock")
      emit(ctx, "tock:" <> input)
      {:ok, state}
    end
  end

  defmodule TimerLoopSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "timer_loop_steps"
    def run, do: {:steps, each: :per_input}
    def durable, do: ElixirRunner.fixture_durable()
    def max_duration_ms, do: 600_000

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Steps.Tick)
      |> named("ticked")
      |> into(ElixirRunner.Steps.Tock)
      |> output("out")
    end
  end

  # `event_timer_steps`: an entry-fed tasklet whose clock is the message's
  # leading event time; each message arms an event-time timer 100 later and
  # emits `seen:<t>`, and the firing emits `fired:<at>`.
  defmodule EventTick do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "event_tick"
    use ElixirRunner.Steps.Seen

    @impl true
    def event_time(input),
      do: input |> String.split(":", parts: 2) |> hd() |> ElixirRunner.Numbers.int()

    @impl true
    def run(_input, state, ctx) do
      {:ok, at} = timer_at_event_time(ctx, now(ctx) + 100)
      emit(ctx, "seen:#{now(ctx)}")
      {:ok, insert(state, ctx, "armed:#{at}")}
    end

    @impl true
    def on_timer(fired_at, _kind, state, ctx) do
      emit(ctx, "fired:#{fired_at}")
      {:ok, state}
    end
  end

  defmodule EventTimerSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "event_timer_steps"
    def run, do: {:steps, each: :per_input}
    def durable, do: ElixirRunner.fixture_durable()
    def max_duration_ms, do: 600_000

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Steps.EventTick)
      |> output("out")
    end
  end

  # `static_cell_steps`: a per-input loop whose one tasklet declares three
  # static cells and routes every message to two of them; each cell that runs
  # emits `<cell key>:<message>`, so one message yields exactly two outputs.
  defmodule StaticCellStep do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "static_cell_step", cell_keys: ["a", "b", "c"]
    @impl true
    def cells_for(_input), do: ["a", "b"]

    @impl true
    def run(input, state, ctx) do
      emit(ctx, "#{cell_key(ctx)}:#{input}")
      {:ok, state}
    end
  end

  defmodule StaticCellSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "static_cell_steps"
    def run, do: {:steps, each: :per_input}
    def durable, do: ElixirRunner.fixture_durable()
    def max_duration_ms, do: 600_000

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Steps.StaticCellStep)
      |> output("out")
    end
  end

  @doc "The durable structured steps classes this runner advertises."
  def all do
    [
      EchoSteps,
      TimerLoopSteps,
      EventTimerSteps,
      StaticCellSteps,
      TypedOnceSteps,
      CommitBlockSteps,
      SyncDurableSteps,
      OptOutCommitBlockSteps,
      TierCommitBlockSteps,
      SagaBlockSteps,
      KeyedBlockSteps,
      KeyedSagaSteps,
      OutboxBlockSteps,
      UndoOutboxSteps,
      FailedStageSteps,
      BranchSteps,
      DiamondSteps,
      RetrySteps
    ]
  end
end
