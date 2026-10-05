# Twins of the rust_runner durable-loop and bench steps fixtures
# (test/apps/rust_runner/src/steps_loop.rs): the chat loop with suspension,
# its stateless twin, the rewindable variant, and the chains and loop
# `mix bench.steps` drives. The crash-resume family is in steps.ex,
# which also holds the release gate all of these share.

defmodule ElixirRunner.StepsLoop do
  @moduledoc false
  alias ElixirRunner.Steps.Gate

  # The turn whose payload is this blocks while the release env is unset, so a
  # SIGKILL lands with the earlier iterations durable and that one in flight.
  # Keying the block on the PAYLOAD rather than an invocation count keeps it
  # deterministic across the park/rehydrate cycles, which re-run the driver but
  # not the turns.
  @crash_turn "mid-crash"

  defmodule ChatTurn do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "chat_turn"

    alias ElixirRunner.Steps.Gate

    use ElixirRunner.Steps.Seen

    # The closing turn is not a turn of the conversation: it is not recorded,
    # and it ends the session with the number of turns the state holds.
    @impl true
    def run("bye", state, ctx) do
      emit(ctx, "bye")
      complete(ctx, Integer.to_string(length(state.items)))
      {:ok, state}
    end

    def run(input, state, ctx) do
      if input == "mid-crash", do: Gate.block_until_released()
      # Keyed by the turn, so the record count is the number of DISTINCT turns
      # that ran — a re-run of a committed iteration would show up.
      state = insert(state, ctx, input)
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule ChatReply do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "chat_reply"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, "reply:" <> input)
      {:ok, state}
    end
  end

  # The rewindable reply: the undo handle lives in Cell state, because a
  # rewind's compensation is rebuilt from the journal and is handed NO retained
  # outputs. So the hook reads the last thing it said, retracts it, and pops it.
  defmodule ChatReplyUndoable do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "chat_reply_undoable"

    @impl true
    def init_state(_), do: %{said: []}

    defchange said(state, turn) do
      %{state | said: state.said ++ [turn]}
    end

    defchange retracted(state) do
      %{state | said: Enum.drop(state.said, -1)}
    end

    @impl true
    def run(input, state, ctx) do
      state = said(state, ctx, input)
      emit(ctx, "reply:" <> input)
      {:ok, state}
    end

    @impl true
    def compensate(_outputs, state, ctx) do
      case List.last(state.said) do
        nil ->
          {:ok, state}

        turn ->
          state = retracted(state, ctx)
          emit(ctx, "retract:" <> turn)
          {:ok, state}
      end
    end
  end

  # One turn's durable state for the bench: how many turns ran and the last
  # one — bounded, so a million-turn soak snapshots the same few bytes at every
  # iteration.
  defmodule BenchTurn do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "bench_turn"

    @impl true
    def init_state(_), do: %{turns: 0, last: ""}

    defchange turn(state, turn) do
      %{state | turns: state.turns + 1, last: turn}
    end

    # With `FLEZHA_BENCH_WAKE_AFTER_MS` set each turn arms a timer that far
    # out, whose firing sends the bench's wake marker, `wake:<fired_at>`.
    @impl true
    def run(input, state, ctx) do
      state = turn(state, ctx, input)

      case System.get_env("FLEZHA_BENCH_WAKE_AFTER_MS") do
        nil -> :ok
        after_ms -> {:ok, _at} = timer_after(ctx, String.to_integer(after_ms))
      end

      emit(ctx, input)
      {:ok, state}
    end

    @impl true
    def on_timer(fired_at, _kind, state, ctx) do
      emit(ctx, "wake:#{fired_at}")
      {:ok, state}
    end
  end

  # The sixteen chain positions the bench chains are built from. Each needs its
  # own tasklet — a chain position is a node — so they are generated from the
  # same body `defstep` writes rather than written out sixteen times.
  for n <- 0..15 do
    Module.create(
      Module.concat(__MODULE__, "BenchS#{n}"),
      ElixirRunner.Steps.Step.durable_body("bench_s#{n}"),
      Macro.Env.location(__ENV__)
    )
  end

  @bench_steps for n <- 0..15, do: Module.concat(__MODULE__, "BenchS#{n}")

  # -- classes --------------------------------------------------------------

  defmodule ChatLoopSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "chat_loop_steps"
    def run, do: {:steps, each: :per_input}
    def durable, do: ElixirRunner.fixture_durable()

    # A chat session ends itself on the turn `bye`; the duration backstop keeps
    # a stray e2e instance from outliving the test run.
    def max_duration_ms, do: 600_000

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.StepsLoop.ChatTurn)
      |> named("mid")
      |> into(ElixirRunner.StepsLoop.ChatReply)
      |> sink("out")
    end
  end

  # The reply that blocks on the turn `reply-crash` while the release env is
  # unset, after the root has committed: what `reply_block_loop_steps`
  # crashes in.
  defmodule BlockingReply do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "blocking_reply"

    alias ElixirRunner.Steps.Gate

    @impl true
    def run(input, state, ctx) do
      if input == "reply-crash", do: Gate.block_until_released()
      emit(ctx, "reply:" <> input)
      {:ok, state}
    end
  end

  # The chat loop with the block moved AFTER the root's commit, so a SIGKILL
  # lands between a root's `task_committed` and the pass's `iter_committed`.
  defmodule ReplyBlockLoopSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "reply_block_loop_steps"
    def run, do: {:steps, each: :per_input}
    def durable, do: ElixirRunner.fixture_durable()
    def max_duration_ms, do: 600_000

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.StepsLoop.ChatTurn)
      |> named("mid")
      |> into(ElixirRunner.StepsLoop.BlockingReply)
      |> sink("out")
    end
  end

  # A durable per-input loop keyed on its entry edge by the text before the
  # first `:`, so one Cell counts one symbol's messages and emits
  # `<symbol>:<count>`: the shape a partition group of steps relay members takes.
  defmodule KeyedLoopCount do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "keyed_loop_count"

    @impl true
    def init_state(_), do: 0

    defchange add(count, delta) do
      count + delta
    end

    @impl true
    def run(input, count, ctx) do
      count = add(count, ctx, 1)
      emit(ctx, ElixirRunner.Key.prefix(input) <> ":" <> Integer.to_string(count))
      {:ok, count}
    end
  end

  defmodule KeyedLoopSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "keyed_loop_steps"
    def run, do: {:steps, each: :per_input}
    def durable, do: ElixirRunner.fixture_durable()
    def max_duration_ms, do: 600_000

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("trades")
      |> key_by(&ElixirRunner.Key.prefix/1)
      |> into(ElixirRunner.StepsLoop.KeyedLoopCount)
      |> sink("keyed_loop_steps_out")
    end
  end

  # The same durable per-input loop minus its stateful first tasklet, so
  # nothing writes a Cell record and the controller ships no restore on a
  # rehydrating bind: the instance's task-journal reaches the driver only if it
  # rides the bind itself.
  defmodule StatelessLoopSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "stateless_loop_steps"
    def run, do: {:steps, each: :per_input}
    def durable, do: ElixirRunner.fixture_durable()
    def max_duration_ms, do: 600_000

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.StepsLoop.ChatReply)
      |> sink("out")
    end
  end

  # `failing_turn_loop_steps`: a session whose turn can fail after `note`
  # committed. `note` records each message and its undo moves the latest one
  # to `undone`; `answer` replies `re:<message>` on the sink `answer`, and
  # fails every attempt on `boom`, on `boom-once` while the release env is
  # unset (the first runner of a crash drill), and on `boom-until-healed`
  # until a `heal` turn set its state.
  defmodule Note do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "note"

    @impl true
    def init_state(_), do: %{notes: [], undone: []}

    defchange noted(state, message) do
      %{state | notes: state.notes ++ [message]}
    end

    defchange undone(state) do
      case List.pop_at(state.notes, -1) do
        {nil, _} -> state
        {message, notes} -> %{state | notes: notes, undone: state.undone ++ [message]}
      end
    end

    @impl true
    def run(input, state, ctx) do
      state = noted(state, ctx, input)
      emit(ctx, input)
      {:ok, state}
    end

    @impl true
    def compensate(_outputs, state, ctx), do: {:ok, undone(state, ctx)}
  end

  defmodule Answer do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "answer"

    alias ElixirRunner.Steps.Gate

    @impl true
    def init_state(_), do: %{healed: false}

    defchange healed(state) do
      %{state | healed: true}
    end

    @impl true
    def run(input, state, ctx) do
      if fails?(input, state) do
        {:error, "answer refused " <> input}
      else
        state = if input == "heal", do: healed(state, ctx), else: state
        emit(ctx, "re:" <> input)
        {:ok, state}
      end
    end

    defp fails?("boom", _state), do: true
    defp fails?("boom-once", _state), do: not Gate.released?()
    defp fails?("boom-until-healed", state), do: not state.healed
    defp fails?(_input, _state), do: false
  end

  defmodule FailingTurnLoopSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "failing_turn_loop_steps"
    def run, do: {:steps, each: :per_input, on_failure: :keep_progress}
    def durable, do: ElixirRunner.fixture_durable()
    def max_duration_ms, do: 600_000
    def channels(c), do: ElixirRunner.StepsLoop.failing_turn_graph(c)
  end

  defmodule FailingTurnUndoAllSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "failing_turn_undo_all_steps"
    def run, do: {:steps, each: :per_input, on_failure: :undo_all}
    def durable, do: ElixirRunner.fixture_durable()
    def max_duration_ms, do: 600_000
    def channels(c), do: ElixirRunner.StepsLoop.failing_turn_graph(c)
  end

  defmodule ChatRewindSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "chat_rewind_steps"
    def run, do: {:steps, each: :per_input}
    def durable, do: ElixirRunner.fixture_durable()
    def max_duration_ms, do: 600_000

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.StepsLoop.ChatTurn)
      |> named("mid")
      |> into(ElixirRunner.StepsLoop.ChatReplyUndoable)
      |> sink("out")
    end
  end

  defmodule BenchLoopSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "bench_loop_steps"
    def run, do: {:steps, each: :per_input}
    def durable, do: ElixirRunner.fixture_durable()
    # A soak outlives the chat loop's 600 s.
    def max_duration_ms, do: 24 * 3600 * 1000

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(ElixirRunner.StepsLoop.BenchTurn)
      |> named("mid")
      |> into(ElixirRunner.StepsLoop.ChatReply)
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
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    def durable, do: ElixirRunner.fixture_durable()
    def factory_id, do: "bench_chain_1"

    def channels(c),
      do: ElixirRunner.StepsLoop.chain(c, ElixirRunner.StepsLoop.bench_steps(1))
  end

  defmodule BenchChain4 do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    def durable, do: ElixirRunner.fixture_durable()
    def factory_id, do: "bench_chain_4"

    def channels(c),
      do: ElixirRunner.StepsLoop.chain(c, ElixirRunner.StepsLoop.bench_steps(4))
  end

  defmodule BenchChain16 do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    def durable, do: ElixirRunner.fixture_durable()
    def factory_id, do: "bench_chain_16"

    def channels(c),
      do: ElixirRunner.StepsLoop.chain(c, ElixirRunner.StepsLoop.bench_steps(16))
  end

  defmodule BenchChainOptOut4 do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "bench_chain_optout_4"
    def run, do: :steps
    def durable, do: false

    def channels(c),
      do: ElixirRunner.StepsLoop.chain(c, ElixirRunner.StepsLoop.bench_steps(4))
  end

  # A flow class that echoes each input on `complete_flow_out` and ends its
  # instance on `bye`, the echo of `bye` included.
  defmodule CompleteFlow do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "complete_flow_tasklet"

    @impl true
    def run(input, state, ctx) do
      if input == "bye", do: complete(ctx, input)
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule CompleteFlowOp do
    @moduledoc false
    use Flezha.SDK.Relay, factory_id: "complete_flow"

    def channels(c),
      do:
        c
        |> input("input")
        |> into(ElixirRunner.StepsLoop.CompleteFlow)
        |> sink("complete_flow_out")
  end

  # A durable chained loop: each pass prints `chained_pass <n>` and hands the
  # next pass `n - 1` on its own channel; the pass at zero hands nothing, and
  # the empty batch ends the instance.
  defmodule ChainedCountdown do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "chained_countdown"

    @impl true
    def run(input, state, ctx) do
      IO.puts("chained_pass #{input}")
      n = String.to_integer(input)
      if n > 0, do: emit(ctx, Integer.to_string(n - 1))
      {:ok, state}
    end
  end

  defmodule ChainedSteps do
    @moduledoc false
    use Flezha.SDK.Relay, factory_id: "chained_steps", run: {:steps, each: :chained}

    def channels(c),
      do: c |> input("in") |> into(ElixirRunner.StepsLoop.ChainedCountdown) |> named("carry")
  end

  @doc "The durable-loop and bench steps classes this runner advertises."
  def all do
    [
      CompleteFlowOp,
      ChainedSteps,
      ChatLoopSteps,
      StatelessLoopSteps,
      KeyedLoopSteps,
      ChatRewindSteps,
      FailingTurnLoopSteps,
      FailingTurnUndoAllSteps,
      ReplyBlockLoopSteps,
      BenchChain1,
      BenchChain4,
      BenchChain16,
      BenchChainOptOut4,
      BenchLoopSteps
    ]
  end

  @doc false
  def crash_turn, do: @crash_turn

  @doc false
  def failing_turn_graph(c) do
    import Flezha.SDK.Channels

    c
    |> input("in")
    |> into(ElixirRunner.StepsLoop.Note)
    |> named("noted")
    |> into(ElixirRunner.StepsLoop.Answer)
    |> sink("answer")
  end
end
