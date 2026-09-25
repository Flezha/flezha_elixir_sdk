# Twins of the rust_runner durable-loop and bench bounded fixtures
# (test/apps/rust_runner/src/bounded_loop.rs): the chat loop with suspension,
# its stateless twin, the rewindable variant, and the chains and loop
# `mix bench.bounded` drives. The crash-resume family is in bounded.ex,
# which also holds the release gate all of these share.

defmodule ElixirRunner.BoundedLoop do
  @moduledoc false
  alias ElixirRunner.Bounded.Gate
  alias Flezha.SDK.Ctx

  # The turn whose payload is this blocks while the release env is unset, so a
  # SIGKILL lands with the earlier iterations durable and that one in flight.
  # Keying the block on the PAYLOAD rather than an invocation count keeps it
  # deterministic across the park/rehydrate cycles, which re-run the driver but
  # not the turns.
  @crash_turn "mid-crash"

  defmodule ChatTurn do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet

    alias ElixirRunner.Bounded.Gate
    alias Flezha.SDK.Ctx

    def name, do: "chat_turn"
    def init_state(_), do: ElixirRunner.Bounded.Seen.empty()
    def apply_change(state, item), do: ElixirRunner.Bounded.Seen.insert(state, item)

    # The closing turn is not a turn of the conversation: it is not recorded,
    # and it ends the session with the number of turns the state holds.
    def run("bye", state, ctx) do
      Ctx.emit(ctx, "bye")
      Ctx.complete(ctx, Integer.to_string(length(state.items)))
      {:ok, state}
    end

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
    @behaviour Flezha.SDK.Tasklet

    alias Flezha.SDK.Ctx

    def name, do: "chat_reply"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      Ctx.emit(ctx, "reply:" <> input)
      {:ok, state}
    end
  end

  # The rewindable reply: the undo handle lives in Cell state, because a
  # rewind's compensation is rebuilt from the journal and is handed NO retained
  # outputs. So the hook reads the last thing it said, retracts it, and pops it.
  defmodule ChatReplyUndoable do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet

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
    @behaviour Flezha.SDK.Tasklet

    alias Flezha.SDK.Ctx

    def name, do: "bench_turn"
    def init_state(_), do: %{turns: 0, last: ""}
    def apply_change(state, turn), do: %{state | turns: state.turns + 1, last: turn}

    # With `FLEZHA_BENCH_WAKE_AFTER_MS` set each turn arms a timer that far
    # out, whose firing sends the bench's wake marker, `wake:<fired_at>`.
    def run(input, state, ctx) do
      state = Ctx.apply(ctx, state, input)

      case System.get_env("FLEZHA_BENCH_WAKE_AFTER_MS") do
        nil -> :ok
        after_ms -> {:ok, _at} = Ctx.timer_after(ctx, String.to_integer(after_ms))
      end

      Ctx.emit(ctx, input)
      {:ok, state}
    end

    def on_timer(fired_at, _kind, state, ctx) do
      Ctx.emit(ctx, "wake:#{fired_at}")
      {:ok, state}
    end
  end

  # The sixteen chain positions the bench chains are built from. Each needs its
  # own tasklet — a chain position is a node — so they are generated from the
  # same body `defstep` writes rather than written out sixteen times.
  for n <- 0..15 do
    Module.create(
      Module.concat(__MODULE__, "BenchS#{n}"),
      ElixirRunner.Bounded.Step.durable_body("bench_s#{n}"),
      Macro.Env.location(__ENV__)
    )
  end

  @bench_steps for n <- 0..15, do: Module.concat(__MODULE__, "BenchS#{n}")

  # -- classes --------------------------------------------------------------

  defmodule ChatLoopBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "chat_loop_bounded"
    def loop_mode, do: :per_input

    # A chat session ends itself on the turn `bye`; the duration backstop keeps
    # a stray e2e instance from outliving the test run.
    def max_duration_ms, do: 600_000

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.BoundedLoop.ChatTurn)
      |> named("mid")
      |> into(ElixirRunner.BoundedLoop.ChatReply)
      |> sink("out")
    end
  end

  # The reply that blocks on the turn `reply-crash` while the release env is
  # unset, after the root has committed: what `reply_block_loop_bounded`
  # crashes in.
  defmodule BlockingReply do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet

    alias ElixirRunner.Bounded.Gate
    alias Flezha.SDK.Ctx

    def name, do: "blocking_reply"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      if input == "reply-crash", do: Gate.block_until_released()
      Ctx.emit(ctx, "reply:" <> input)
      {:ok, state}
    end
  end

  # The chat loop with the block moved AFTER the root's commit, so a SIGKILL
  # lands between a root's `task_committed` and the pass's `iter_committed`.
  defmodule ReplyBlockLoopBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "reply_block_loop_bounded"
    def loop_mode, do: :per_input
    def self_terminating?, do: false
    def max_duration_ms, do: 600_000

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.BoundedLoop.ChatTurn)
      |> named("mid")
      |> into(ElixirRunner.BoundedLoop.BlockingReply)
      |> sink("out")
    end
  end

  # A durable per-input loop keyed on its entry edge by the text before the
  # first `:`, so one Cell counts one symbol's messages and emits
  # `<symbol>:<count>`: the shape a partition group of bounded operator members takes.
  defmodule KeyedLoopCount do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet

    alias Flezha.SDK.Ctx

    def name, do: "keyed_loop_count"
    def init_state(_), do: 0
    def apply_change(count, delta), do: count + delta

    def run(input, count, ctx) do
      count = Ctx.apply(ctx, count, 1)
      Ctx.emit(ctx, ElixirRunner.Key.prefix(input) <> ":" <> Integer.to_string(count))
      {:ok, count}
    end
  end

  defmodule KeyedLoopBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "keyed_loop_bounded"
    def loop_mode, do: :per_input
    def self_terminating?, do: false
    def max_duration_ms, do: 600_000

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("trades")
      |> key_by(&ElixirRunner.Key.prefix/1)
      |> into(ElixirRunner.BoundedLoop.KeyedLoopCount)
      |> sink("keyed_loop_bounded_out")
    end
  end

  # The same durable per-input loop minus its stateful first tasklet, so
  # nothing writes a Cell record and the controller ships no restore on a
  # rehydrating bind: the instance's task-journal reaches the driver only if it
  # rides the bind itself.
  defmodule StatelessLoopBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "stateless_loop_bounded"
    def loop_mode, do: :per_input
    def self_terminating?, do: false
    def max_duration_ms, do: 600_000

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.BoundedLoop.ChatReply)
      |> sink("out")
    end
  end

  defmodule ChatRewindBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "chat_rewind_bounded"
    def loop_mode, do: :per_input
    def self_terminating?, do: false
    def max_duration_ms, do: 600_000

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.BoundedLoop.ChatTurn)
      |> named("mid")
      |> into(ElixirRunner.BoundedLoop.ChatReplyUndoable)
      |> sink("out")
    end
  end

  defmodule BenchLoopBounded do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "bench_loop_bounded"
    def loop_mode, do: :per_input
    def self_terminating?, do: false
    # A soak outlives the chat loop's 600 s.
    def max_duration_ms, do: 24 * 3600 * 1000

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.BoundedLoop.BenchTurn)
      |> named("mid")
      |> into(ElixirRunner.BoundedLoop.ChatReply)
      |> sink("out")
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
    |> sink("out")
  end

  @doc false
  def bench_steps(count), do: Enum.take(@bench_steps, count)

  defmodule BenchChain1 do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "bench_chain_1"

    def channels(c),
      do: ElixirRunner.BoundedLoop.chain(c, ElixirRunner.BoundedLoop.bench_steps(1))
  end

  defmodule BenchChain4 do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "bench_chain_4"

    def channels(c),
      do: ElixirRunner.BoundedLoop.chain(c, ElixirRunner.BoundedLoop.bench_steps(4))
  end

  defmodule BenchChain16 do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "bench_chain_16"

    def channels(c),
      do: ElixirRunner.BoundedLoop.chain(c, ElixirRunner.BoundedLoop.bench_steps(16))
  end

  defmodule BenchChainOptOut4 do
    @moduledoc false
    @behaviour Flezha.SDK.BoundedOperator
    def factory_id, do: "bench_chain_optout_4"
    def persistence_enabled, do: false

    def channels(c),
      do: ElixirRunner.BoundedLoop.chain(c, ElixirRunner.BoundedLoop.bench_steps(4))
  end

  @doc "The durable-loop and bench bounded classes this runner advertises."
  def all do
    [
      ChatLoopBounded,
      StatelessLoopBounded,
      KeyedLoopBounded,
      ChatRewindBounded,
      ReplyBlockLoopBounded,
      BenchChain1,
      BenchChain4,
      BenchChain16,
      BenchChainOptOut4,
      BenchLoopBounded
    ]
  end

  @doc false
  def crash_turn, do: @crash_turn
end
