# Twins of the rust_runner durable-loop and bench workflow fixtures
# (test/apps/rust_runner/src/workflow_loop.rs): the chat loop with suspension,
# its stateless twin, the rewindable variant, and the chains and loop
# `mix bench.workflow` drives. The crash-resume family is in workflows.ex,
# which also holds the release gate all of these share.

defmodule ElixirRunner.WorkflowsLoop do
  @moduledoc false
  alias ElixirRunner.Workflows.Gate
  alias Flezha.SDK.Ctx

  # The turn whose payload is this blocks while the release env is unset, so a
  # SIGKILL lands with the earlier iterations durable and that one in flight.
  # Keying the block on the PAYLOAD rather than an invocation count keeps it
  # deterministic across the park/rehydrate cycles, which re-run the driver but
  # not the turns.
  @crash_turn "mid-crash"

  defmodule ChatTurn do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure

    alias ElixirRunner.Workflows.Gate
    alias Flezha.SDK.Ctx

    def name, do: "chat_turn"
    def init_state(_), do: ElixirRunner.Workflows.Seen.empty()
    def apply_change(state, item), do: ElixirRunner.Workflows.Seen.insert(state, item)

    def run(input, state, ctx) do
      if input == "mid-crash", do: Gate.block_until_released()
      # Keyed by the turn, so the record count is the number of DISTINCT turns
      # that ran — a re-run of a committed iteration would show up.
      state = Ctx.apply(ctx, state, input)
      Ctx.emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule ChatReply do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure

    alias Flezha.SDK.Ctx

    def name, do: "chat_reply"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      Ctx.emit(ctx, "reply:" <> input)
      {:ok, state}
    end
  end

  # The rewindable reply: the undo handle lives in Entity state, because a
  # rewind's compensation is rebuilt from the journal and is handed NO retained
  # outputs. So the hook reads the last thing it said, retracts it, and pops it.
  defmodule ChatReplyUndoable do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure

    alias Flezha.SDK.Ctx

    def name, do: "chat_reply_undoable"
    def init_state(_), do: %{said: []}

    def apply_change(state, {:said, turn}), do: %{state | said: state.said ++ [turn]}
    def apply_change(state, :retracted), do: %{state | said: Enum.drop(state.said, -1)}

    def run(input, state, ctx) do
      state = Ctx.apply(ctx, state, {:said, input})
      Ctx.emit(ctx, "reply:" <> input)
      {:ok, state}
    end

    def compensate(_outputs, state, ctx) do
      case List.last(state.said) do
        nil ->
          {:ok, state}

        turn ->
          state = Ctx.apply(ctx, state, :retracted)
          Ctx.emit(ctx, "retract:" <> turn)
          {:ok, state}
      end
    end
  end

  # One turn's durable state for the bench: how many turns ran and the last
  # one — bounded, so a million-turn soak snapshots the same few bytes at every
  # iteration.
  defmodule BenchTurn do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure

    alias Flezha.SDK.Ctx

    def name, do: "bench_turn"
    def init_state(_), do: %{turns: 0, last: ""}
    def apply_change(state, turn), do: %{state | turns: state.turns + 1, last: turn}

    def run(input, state, ctx) do
      state = Ctx.apply(ctx, state, input)
      Ctx.emit(ctx, input)
      {:ok, state}
    end
  end

  # The sixteen chain positions the bench chains are built from. Each needs its
  # own procedure — a chain position is a node — so they are generated from the
  # same body `defstep` writes rather than written out sixteen times.
  for n <- 0..15 do
    Module.create(
      Module.concat(__MODULE__, "BenchS#{n}"),
      ElixirRunner.Workflows.Step.durable_body("bench_s#{n}"),
      Macro.Env.location(__ENV__)
    )
  end

  @bench_steps for n <- 0..15, do: Module.concat(__MODULE__, "BenchS#{n}")

  # -- classes --------------------------------------------------------------

  defmodule ChatLoopWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "chat_loop_workflow"
    def loop_mode, do: :per_input

    # A chat session ends on an external stop, not on its own; the duration
    # backstop keeps a stray e2e instance from outliving the test run.
    def self_terminating?, do: false
    def max_duration_ms, do: 600_000

    def procedures(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.WorkflowsLoop.ChatTurn)
      |> named("mid")
      |> into(ElixirRunner.WorkflowsLoop.ChatReply)
      |> named("out")
      |> sink("chat_sink")
    end
  end

  # The same durable per-input loop minus its stateful first procedure, so
  # nothing writes an Entity record and the controller ships no restore on a
  # rehydrating bind: the instance's proc-journal reaches the driver only if it
  # rides the bind itself.
  defmodule StatelessLoopWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "stateless_loop_workflow"
    def loop_mode, do: :per_input
    def self_terminating?, do: false
    def max_duration_ms, do: 600_000

    def procedures(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.WorkflowsLoop.ChatReply)
      |> named("out")
      |> sink("chat_sink")
    end
  end

  defmodule ChatRewindWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "chat_rewind_workflow"
    def loop_mode, do: :per_input
    def self_terminating?, do: false
    def max_duration_ms, do: 600_000

    def procedures(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.WorkflowsLoop.ChatTurn)
      |> named("mid")
      |> into(ElixirRunner.WorkflowsLoop.ChatReplyUndoable)
      |> named("out")
      |> sink("chat_sink")
    end
  end

  defmodule BenchLoopWorkflow do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "bench_loop_workflow"
    def loop_mode, do: :per_input
    def self_terminating?, do: false
    # A soak outlives the chat loop's 600 s.
    def max_duration_ms, do: 24 * 3600 * 1000

    def wake_after_ms do
      case System.get_env("FLEZHA_BENCH_WAKE_AFTER_MS") do
        nil -> nil
        value -> String.to_integer(value)
      end
    end

    def procedures(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.WorkflowsLoop.BenchTurn)
      |> named("mid")
      |> into(ElixirRunner.WorkflowsLoop.ChatReply)
      |> named("out")
      |> sink("bench_sink")
    end
  end

  @doc """
  A linear chain `in -> first -> rest… -> bench_sink`. The edge names follow
  the reference, which spells them from the step's own name.
  """
  def chain(c, steps) do
    import Flezha.SDK.Channels

    [first | rest] = steps

    rest
    |> Enum.reduce(c |> input("in") |> into(first), fn step, s ->
      s |> named("edge_" <> step.name()) |> into(step)
    end)
    |> named("out")
    |> sink("bench_sink")
  end

  @doc false
  def bench_steps(count), do: Enum.take(@bench_steps, count)

  defmodule BenchChain1 do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "bench_chain_1"

    def procedures(c),
      do: ElixirRunner.WorkflowsLoop.chain(c, ElixirRunner.WorkflowsLoop.bench_steps(1))
  end

  defmodule BenchChain4 do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "bench_chain_4"

    def procedures(c),
      do: ElixirRunner.WorkflowsLoop.chain(c, ElixirRunner.WorkflowsLoop.bench_steps(4))
  end

  defmodule BenchChain16 do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "bench_chain_16"

    def procedures(c),
      do: ElixirRunner.WorkflowsLoop.chain(c, ElixirRunner.WorkflowsLoop.bench_steps(16))
  end

  defmodule BenchChainOptOut4 do
    @moduledoc false
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "bench_chain_optout_4"
    def persistence_enabled, do: false

    def procedures(c),
      do: ElixirRunner.WorkflowsLoop.chain(c, ElixirRunner.WorkflowsLoop.bench_steps(4))
  end

  @doc "The durable-loop and bench workflow classes this runner advertises."
  def all do
    [
      ChatLoopWorkflow,
      StatelessLoopWorkflow,
      ChatRewindWorkflow,
      BenchChain1,
      BenchChain4,
      BenchChain16,
      BenchChainOptOut4,
      BenchLoopWorkflow
    ]
  end

  @doc false
  def crash_turn, do: @crash_turn
end
