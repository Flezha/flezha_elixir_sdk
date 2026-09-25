# Twins of the rust_runner durable structured bounded fixtures
# (test/apps/rust_runner/src/bounded.rs) under the same factory ids,
# tasklet names, channel names and output bytes; the loop and bench family
# is in bounded_loop.ex.
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

defmodule ElixirRunner.Bounded.Gate do
  @moduledoc false

  @release_env "FLEZHA_BOUNDED_E2E_RELEASE"
  @branch_fail_env "FLEZHA_BRANCH_FAIL"
  @retry_fails_env "FLEZHA_BOUNDED_RETRY_FAILS"

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

  @doc "Park at `point` when this runner was booted to hold there, until the release."
  def hold_at(point) do
    if System.get_env("FLEZHA_BOUNDED_E2E_HOLD") == point, do: block_until_released()
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

defmodule ElixirRunner.Bounded.Seen do
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

defmodule ElixirRunner.Bounded.Step do
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
      def init_state(_), do: ElixirRunner.Bounded.Seen.empty()
      def apply_change(state, item), do: ElixirRunner.Bounded.Seen.insert(state, item)

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

defmodule ElixirRunner.Bounded do
  @moduledoc false
  import ElixirRunner.Bounded.Step

  alias ElixirRunner.Bounded.Gate
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
    def init_state(_), do: ElixirRunner.Bounded.Seen.empty()
    def apply_change(state, item), do: ElixirRunner.Bounded.Seen.insert(state, item)

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
    def init_state(_), do: ElixirRunner.Bounded.Seen.empty()
    def apply_change(state, item), do: ElixirRunner.Bounded.Seen.insert(state, item)

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
    def init_state(_), do: ElixirRunner.Bounded.Seen.empty()
    def apply_change(state, item), do: ElixirRunner.Bounded.Seen.insert(state, item)

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
    def init_state(_), do: ElixirRunner.Bounded.Seen.empty()
    def apply_change(state, item), do: ElixirRunner.Bounded.Seen.insert(state, item)

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

  # Fails its first `FLEZHA_BOUNDED_RETRY_FAILS` attempts, then emits the
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

  # Echoes the first message, which a `:once` bounded operator's one pass runs on.
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

  defmodule EchoBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "echo_bounded"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Bounded.Echo)
      |> sink("out")
    end
  end

  # `typed_once_bounded`: a `once` class whose entry channel `order` decodes
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

  defmodule TypedOnceBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "typed_once_bounded"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input_with("order", &Jason.decode/1)
      |> into(ElixirRunner.Bounded.OncePlaced)
      |> sink("placed")
    end
  end

  defmodule CommitBlockBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "commit_block_bounded"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Bounded.CommitA)
      |> named("mid")
      |> into(ElixirRunner.Bounded.BlockB)
    end
  end

  defmodule SyncDurableBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "sync_durable_bounded"
    def sync_durability, do: true

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Bounded.CommitA)
      |> named("mid")
      |> into(ElixirRunner.Bounded.BlockB)
    end
  end

  defmodule OptOutCommitBlockBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "optout_commit_block"
    def persistence_enabled, do: false

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Bounded.CommitA)
      |> sink_with("mid", fn s -> "a-ran:" <> s end)
      |> into(ElixirRunner.Bounded.BlockB)
    end
  end

  # The hybrid mid-chain shape the conformance fixture's `commit_block_chain`
  # twin is transcribed from: A's channel reaches a plugin sink and feeds B.
  defmodule TierCommitBlockBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "tier_commit_block_bounded"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Bounded.TierCommitA)
      |> sink_with("mid", & &1)
      |> into(ElixirRunner.Bounded.BlockB)
    end
  end

  defmodule SagaBlockBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "saga_block_bounded"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Bounded.SagaA)
      |> named("s1")
      |> into(ElixirRunner.Bounded.SagaB)
      |> named("s2")
      |> into(ElixirRunner.Bounded.SagaC)
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
    |> into(ElixirRunner.Bounded.KeySplit)
    |> named("keys")
    |> key_by(& &1)
    |> into(ElixirRunner.Bounded.KeyedCommit)
    |> named("committed")
  end

  defmodule KeyedBlockBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "keyed_block_bounded"

    def channels(c),
      do:
        c
        |> ElixirRunner.Bounded.keyed_commits()
        |> Flezha.SDK.Channels.into(ElixirRunner.Bounded.BlockB)
  end

  defmodule KeyedSagaBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "keyed_saga_bounded"

    def channels(c),
      do:
        c
        |> ElixirRunner.Bounded.keyed_commits()
        |> Flezha.SDK.Channels.into(ElixirRunner.Bounded.KeyedFail)
  end

  # A bounded output's identity: `commit_a` commits durable state AND emits
  # once, keyed `k` with one metadata pair, on a keyed hybrid channel; the
  # output rides its Cell's record and reaches the sink once the stage commits.
  defmodule OutboxA do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "commit_a"
    def init_state(_), do: ElixirRunner.Bounded.Seen.empty()
    def apply_change(state, item), do: ElixirRunner.Bounded.Seen.insert(state, item)

    def run(input, state, ctx) do
      state = Ctx.apply(ctx, state, "commit_a")
      :ok = Ctx.emit_keyed_with_meta(ctx, "k", input, %{"trace" => "t1"})
      {:ok, state}
    end
  end

  defmodule OutboxBlockBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "outbox_block_bounded"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Bounded.OutboxA)
      |> keyed()
      |> sink_with("mid", fn s -> "a-out:" <> s end)
      |> into(ElixirRunner.Bounded.BlockB)
    end
  end

  defmodule UndoOutboxBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "undo_outbox_bounded"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Bounded.UndoOutboxA)
      |> sink_with("mid", fn s -> "out:" <> s end)
      |> into(ElixirRunner.Bounded.UndoC)
    end
  end

  # A failed stage delivers nothing. The entry's two values reach one
  # `failed_stage` Cell in order: the first emits on a plugin channel, so the
  # Cell's record carries an output; the second fails, so the stage never
  # commits and no marker ever names that record.
  defmodule FailedStage do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "failed_stage"
    def init_state(_), do: nil
    def run("b", _state, _ctx), do: {:error, {:non_retryable, "failed_stage fails on b"}}

    def run(input, state, ctx) do
      Ctx.emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule FailedStageBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "failed_stage_bounded"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Bounded.KeySplit)
      |> into(ElixirRunner.Bounded.FailedStage)
      |> sink_with("mid", fn s -> "lost:" <> s end)
    end
  end

  # The tasklet TREE. One channel, two consumers, is THE fan-out: both
  # branches read exactly what BranchA released on it.
  defmodule BranchBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "branch_bounded"

    def channels(c) do
      import Flezha.SDK.Channels

      mid =
        c
        |> input("in")
        |> into(ElixirRunner.Bounded.BranchA)
        |> sink_with("mid", fn s -> "a:" <> s end)

      # Fail fast: the compensation scenario is about the rollback order, not
      # the ladder before it.
      mid
      |> into(ElixirRunner.Bounded.BranchB)
      |> sink_with("bmid", fn s -> "b:" <> s end)
      |> retry_max(1)
      |> into(ElixirRunner.Bounded.BranchD)
      |> sink("d_out")

      mid
      |> into(ElixirRunner.Bounded.BranchC)
      |> sink("c_out")
    end
  end

  # The fan-in DAG. A held handle wired twice is the fan-out; two unnamed
  # mentions of DiamJoin are one node, dispatched once after both arms commit.
  defmodule DiamondBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "diamond_bounded"

    def channels(c) do
      import Flezha.SDK.Channels

      fan =
        c
        |> input("in")
        |> into(ElixirRunner.Bounded.DiamA)
        |> sink_with("dfan", fn s -> "a:" <> s end)

      fan
      |> into(ElixirRunner.Bounded.DiamB1)
      |> named("db1")
      |> into(ElixirRunner.Bounded.DiamB2)
      |> sink_with("db2", & &1)
      |> into(ElixirRunner.Bounded.DiamJoin)

      fan
      |> into(ElixirRunner.Bounded.DiamC)
      |> sink_with("dc", & &1)
      |> into(ElixirRunner.Bounded.DiamJoin)
      |> sink("j_out")
    end
  end

  defmodule RetryBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "retry_bounded"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Bounded.RetryA)
      |> sink_with("mid", fn s -> "res:" <> s end)
      |> into(ElixirRunner.Bounded.RetryFlaky)
      |> sink("out")
    end
  end

  # `timer_loop_bounded`: `in -> tick -> relay -> out`, a per-input loop whose
  # `tick` arms a processing-time timer of the delay `after:<ms>` names and
  # emits `armed:<at>`; its firing emits `fired:<at>` and opens a pass of its
  # own. `slow:<ms>` holds `tick` that long, for a firing due mid-pass. On a
  # runner booted with `FLEZHA_BOUNDED_E2E_HOLD` the firing parks at the gate:
  # `timer` inside `on_timer`, before `tick` commits; `relay` in `relay`, after.
  defmodule Tick do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "tick"
    def init_state(_), do: ElixirRunner.Bounded.Seen.empty()
    def apply_change(state, item), do: ElixirRunner.Bounded.Seen.insert(state, item)

    def run("after:" <> delay, state, ctx) do
      {:ok, at} = Ctx.timer_after(ctx, String.to_integer(delay))
      Ctx.emit(ctx, "armed:#{at}")
      {:ok, Ctx.apply(ctx, state, "armed:#{at}")}
    end

    def run("slow:" <> pause = input, state, ctx) do
      Process.sleep(String.to_integer(pause))
      Ctx.emit(ctx, input)
      {:ok, state}
    end

    def run(input, state, ctx) do
      Ctx.emit(ctx, input)
      {:ok, state}
    end

    def on_timer(fired_at, _kind, state, ctx) do
      Gate.hold_at("timer")
      Ctx.emit(ctx, "fired:#{fired_at}")
      {:ok, state}
    end
  end

  defmodule Relay do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "relay"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      if String.starts_with?(input, "fired:"), do: Gate.hold_at("relay")
      Ctx.emit(ctx, "relay:" <> input)
      {:ok, state}
    end
  end

  defmodule TimerLoopBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "timer_loop_bounded"
    def loop_mode, do: :per_input
    def max_duration_ms, do: 600_000

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Bounded.Tick)
      |> named("ticked")
      |> into(ElixirRunner.Bounded.Relay)
      |> sink("out")
    end
  end

  # `event_timer_bounded`: an entry-fed tasklet whose clock is the message's
  # leading event time; each message arms an event-time timer 100 later and
  # emits `seen:<t>`, and the firing emits `fired:<at>`.
  defmodule EventTick do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "event_tick"
    def clock, do: :event_time
    def init_state(_), do: ElixirRunner.Bounded.Seen.empty()
    def apply_change(state, item), do: ElixirRunner.Bounded.Seen.insert(state, item)

    def event_time_of(input),
      do: input |> String.split(":", parts: 2) |> hd() |> ElixirRunner.Numbers.int()

    def run(_input, state, ctx) do
      {:ok, at} = Ctx.timer_at_event_time(ctx, Ctx.now(ctx) + 100)
      Ctx.emit(ctx, "seen:#{Ctx.now(ctx)}")
      {:ok, Ctx.apply(ctx, state, "armed:#{at}")}
    end

    def on_timer(fired_at, _kind, state, ctx) do
      Ctx.emit(ctx, "fired:#{fired_at}")
      {:ok, state}
    end
  end

  defmodule EventTimerBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "event_timer_bounded"
    def loop_mode, do: :per_input
    def max_duration_ms, do: 600_000

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Bounded.EventTick)
      |> sink("out")
    end
  end

  # `static_cell_bounded`: a per-input loop whose one tasklet declares three
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

  defmodule StaticCellBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "static_cell_bounded"
    def loop_mode, do: :per_input
    def self_terminating?, do: false
    def max_duration_ms, do: 600_000

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.Bounded.StaticCellStep)
      |> sink("out")
    end
  end

  @doc "The durable structured bounded classes this runner advertises."
  def all do
    [
      EchoBounded,
      TimerLoopBounded,
      EventTimerBounded,
      StaticCellBounded,
      TypedOnceBounded,
      CommitBlockBounded,
      SyncDurableBounded,
      OptOutCommitBlockBounded,
      TierCommitBlockBounded,
      SagaBlockBounded,
      KeyedBlockBounded,
      KeyedSagaBounded,
      OutboxBlockBounded,
      UndoOutboxBounded,
      FailedStageBounded,
      BranchBounded,
      DiamondBounded,
      RetryBounded
    ]
  end
end
