defmodule Flezha.SDK.Workflow.DriverTest do
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Channels, Counters, Ctx, Spawn}
  alias Flezha.SDK.Operative.Inbox
  alias Flezha.SDK.Workflow.{Driver, Resume, Spec}
  alias Flezha.SDK.Workflow.Ctx, as: WorkflowCtx

  # A stand-in for the runner: it answers every barrier the way a controller
  # that journalled it would, and forwards what it was asked to ship so a test
  # can read the wire rather than the driver's own state.
  defmodule StubRunner do
    @moduledoc false
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, Map.new(opts))
    def init(opts), do: {:ok, Map.put_new(opts, :error, "")}

    def handle_cast({:barrier, driver, ref, command}, s) do
      send(s.test, {:shipped, command})
      send(driver, {:barrier_ack, ref, "", s.error})
      {:noreply, s}
    end

    def handle_cast({:notify, command}, s) do
      send(s.test, {:shipped, command})
      {:noreply, s}
    end

    # A spawn request, answered as a controller that started the instance would.
    def handle_cast({:spawn, caller, spawn, payload, if_running}, s) do
      send(s.test, {:spawn_requested, payload, if_running})
      Spawn.answer(caller, %{spawn | claimed?: true}, "spawned", "")
      {:noreply, s}
    end

    def handle_cast(_other, s), do: {:noreply, s}
  end

  # --- fixtures ---

  defmodule Tagger do
    @behaviour Flezha.SDK.Procedure
    def name, do: "tagger"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      Ctx.emit(ctx, "a:" <> input)
      {:ok, state}
    end
  end

  defmodule Left do
    @behaviour Flezha.SDK.Procedure
    def name, do: "left"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      Ctx.emit(ctx, "l:" <> input)
      {:ok, state}
    end
  end

  defmodule Right do
    @behaviour Flezha.SDK.Procedure
    def name, do: "right"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      Ctx.emit(ctx, "r:" <> input)
      {:ok, state}
    end
  end

  defmodule Join do
    @behaviour Flezha.SDK.Procedure
    def name, do: "join"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      Ctx.emit(ctx, "j:" <> input)
      {:ok, state}
    end
  end

  defmodule Boom do
    @behaviour Flezha.SDK.Procedure
    def name, do: "boom"
    def init_state(_), do: nil
    def run(_input, _state, _ctx), do: {:error, {:non_retryable, "boom"}}
  end

  defmodule Undoable do
    @behaviour Flezha.SDK.Procedure
    def name, do: "undoable"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      Ctx.emit(ctx, input)
      {:ok, state}
    end

    def compensate(_outputs, state, _ctx) do
      send(:workflow_driver_test_collector, {:undone, name()})
      {:ok, state}
    end
  end

  defmodule Undoable2 do
    @behaviour Flezha.SDK.Procedure
    def name, do: "undoable2"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      Ctx.emit(ctx, input)
      {:ok, state}
    end

    def compensate(_outputs, state, _ctx) do
      send(:workflow_driver_test_collector, {:undone, name()})
      {:ok, state}
    end
  end

  # --- classes ---

  defmodule LinearFlow do
    def factory_id, do: "linear_flow"

    def procedures(c) do
      c |> Channels.input("in") |> Channels.into(Tagger) |> Channels.into(Join)
    end
  end

  defmodule DiamondFlow do
    def factory_id, do: "diamond_flow"

    def procedures(c) do
      fan = c |> Channels.input("in") |> Channels.into(Tagger)
      fan |> Channels.into(Left) |> Channels.into(Join)
      fan |> Channels.into(Right) |> Channels.into(Join)
    end
  end

  defmodule FailingFlow do
    def factory_id, do: "failing_flow"

    def procedures(c) do
      c |> Channels.input("in") |> Channels.into(Undoable) |> Channels.into(Boom)
    end
  end

  defmodule SagaFlow do
    def factory_id, do: "saga_flow"

    def procedures(c) do
      c
      |> Channels.input("in")
      |> Channels.into(Undoable)
      |> Channels.into(Undoable2)
      |> Channels.into(Boom)
    end
  end

  defmodule OptedOutFlow do
    def factory_id, do: "opted_out_flow"
    def persistence_enabled, do: false

    def procedures(c) do
      c |> Channels.input("in") |> Channels.into(Tagger)
    end
  end

  defmodule Echo do
    def factory_id, do: "echo_flow"

    def run(_ctx, args), do: send(:workflow_driver_test_collector, {:ran, args}) && :ok
  end

  defmodule Exploding do
    def factory_id, do: "exploding_flow"
    def run(_ctx, _args), do: raise("nope")
  end

  # A run-sugar body that spawns, reads its handle and reports what it resolved to.
  defmodule SpawningSugar do
    def factory_id, do: "spawning_sugar"

    def run(ctx, args) do
      handle = WorkflowCtx.spawn_workflow(ctx, "child-" <> args, "worker", args, :deliver)
      send(:workflow_driver_test_collector, {:spawned, Spawn.await(handle, 5_000)})
      :ok
    end
  end

  # The same request from a structured procedure, released as its stage's handoff.
  defmodule SpawnStep do
    @behaviour Flezha.SDK.Procedure
    def name, do: "spawn_step"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      handle = Ctx.spawn_workflow(ctx, "child-" <> input, "worker", input, :error)
      Ctx.emit(ctx, inspect(Spawn.await(handle, 5_000)))
      {:ok, state}
    end
  end

  defmodule SpawningFlow do
    def factory_id, do: "spawning_flow"
    def procedures(c), do: c |> Channels.input("in") |> Channels.into(SpawnStep)
  end

  # --- harness ---

  # A driver of `module` on a stub runner, reporting to this process.
  defp start_driver(module, opts) do
    Process.register(self(), :workflow_driver_test_collector)
    assert {:ok, spec} = Spec.build(module)
    {error, opts} = Keyword.pop(opts, :error, "")
    {:ok, runner} = StubRunner.start_link(test: self(), error: error)
    defaults = [operative_id: "wf-1", args: "go", counters: Counters.new()]

    {:ok, driver} =
      Driver.start_link(Keyword.merge(defaults, opts) ++ [spec: spec, runner: runner])

    driver
  end

  defp drive(module, opts \\ []) do
    driver = start_driver(module, opts)

    # A run-sugar body can finish before the monitor is set, which reports
    # `:noproc` rather than `:normal`; both mean the same thing here.
    ref = Process.monitor(driver)
    assert_receive {:DOWN, ^ref, :process, ^driver, reason}, 5_000
    assert reason in [:normal, :noproc]
    drain()
  end

  # The driver's exit does not mean the stub has forwarded its terminal frame
  # yet, so collect until that frame arrives rather than until the mailbox
  # happens to be empty — under a loaded suite the two are not the same.
  defp drain(acc \\ []) do
    receive do
      {:shipped, {:workflow_completed, _}} = msg -> Enum.reverse([msg | acc])
      msg -> drain([msg | acc])
    after
      5_000 -> Enum.reverse(acc)
    end
  end

  defp commits(messages) do
    for {:shipped, {:proc_commit, commit}} <- messages, do: commit
  end

  defp completion(messages) do
    Enum.find_value(messages, fn
      {:shipped, {:workflow_completed, done}} -> done
      _ -> nil
    end)
  end

  # --- run-sugar ---

  test "a run-sugar body runs once and completes ok" do
    messages = drive(Echo, args: "hello")
    assert {:ran, "hello"} in messages
    assert %{status: "ok", error: ""} = completion(messages)
  end

  test "a raising run-sugar body completes as an error naming it" do
    messages = drive(Exploding)
    assert %{status: "error", error: error} = completion(messages)
    assert error =~ "nope"
  end

  test "a run-sugar body's spawn reaches the runner and its handle resolves" do
    messages = drive(SpawningSugar, args: "go")
    assert {:spawn_requested, "go", :deliver} in messages
    assert {:spawned, {:ok, :spawned}} in messages
    assert %{status: "ok"} = completion(messages)
  end

  test "a structured procedure's spawn reaches the runner through its stage Entity" do
    messages = drive(SpawningFlow)
    assert {:spawn_requested, "go", :error} in messages
    assert [commit] = commits(messages)

    assert commit.handoff |> hd() |> Map.get(:payload) |> Resume.decode() ==
             inspect({:ok, :spawned})
  end

  # --- the structured chain ---

  test "a linear chain commits each stage in DAG order and completes ok" do
    messages = drive(LinearFlow)
    assert [first, second] = commits(messages)
    assert first.proc_idx == 0
    assert second.proc_idx == 1
    assert first.handoff |> hd() |> Map.get(:payload) |> Resume.decode() == "a:go"
    assert second.handoff |> hd() |> Map.get(:payload) |> Resume.decode() == "j:a:go"
    assert %{status: "ok"} = completion(messages)
  end

  test "a stage commits the entity identities that ran" do
    messages = drive(LinearFlow, operative_id: "wf-ids")
    assert [first | _] = commits(messages)
    assert [identity] = first.entities_ran
    assert is_binary(identity)
  end

  # This is the KI-141 shape: one producing channel with two consumers. Both
  # arms must see what the producer released, and the join must be dispatched
  # once with the batch of both.
  test "a fan-out reaches every consumer and the join runs once over both arms" do
    messages = drive(DiamondFlow)
    payloads = for c <- commits(messages), h <- c.handoff, do: Resume.decode(h.payload)

    assert "a:go" in payloads
    assert "l:a:go" in payloads
    assert "r:a:go" in payloads
    assert %{status: "ok"} = completion(messages)

    join = Enum.find(commits(messages), &(&1.proc_idx == 3))
    assert length(join.handoff) == 2

    assert join.handoff |> Enum.map(&Resume.decode(&1.payload)) |> Enum.sort() ==
             ["j:l:a:go", "j:r:a:go"]
  end

  # The negative control for the assertion above: the same class with one arm
  # removed releases one payload at the join, not two. A two-payload assertion
  # that was never shown to fail proves the test ran, not that the fan-out works.
  defmodule OneArmedFlow do
    def factory_id, do: "one_armed_flow"

    def procedures(c) do
      fan = c |> Channels.input("in") |> Channels.into(Tagger)
      fan |> Channels.into(Left) |> Channels.into(Join)
    end
  end

  test "removing one arm of the fan-out leaves the join one payload, not two" do
    join = OneArmedFlow |> drive() |> commits() |> Enum.find(&(&1.proc_idx == 2))
    assert Enum.map(join.handoff, &Resume.decode(&1.payload)) == ["j:l:a:go"]
  end

  test "the join is committed once, not once per producer" do
    commits = DiamondFlow |> drive() |> commits()
    assert commits |> Enum.map(& &1.proc_idx) |> Enum.sort() == [0, 1, 2, 3]
  end

  # --- failure and the saga ---

  test "a failing procedure ends the run as an error naming it" do
    messages = drive(FailingFlow)
    assert %{status: "error", error: error} = completion(messages)
    assert error =~ "boom"
  end

  test "a failure runs every declared compensate hook in reverse order" do
    messages = drive(SagaFlow)
    undone = for {:undone, name} <- messages, do: name
    assert undone == ["undoable2", "undoable"]
    assert %{status: "error"} = completion(messages)
  end

  test "a class with no hook on what ran unwinds with nothing to undo" do
    messages = drive(FailingFlow)
    assert [{:undone, "undoable"}] == for({:undone, n} = m <- messages, n == "undoable", do: m)
  end

  # --- loop modes ---

  defmodule ChatLoop do
    def factory_id, do: "chat_loop"
    def loop_mode, do: :per_input
    def self_terminating?, do: false
    def max_duration_ms, do: 600_000

    def procedures(c) do
      c |> Channels.input("in") |> Channels.into(Tagger)
    end
  end

  # Reports every input it runs over, so a class that journals nothing, and so
  # commits nothing, can still be seen to have run.
  defmodule Heard do
    @behaviour Flezha.SDK.Procedure
    def name, do: "heard"
    def init_state(_), do: nil

    def run(input, state, _ctx) do
      send(:workflow_driver_test_collector, {:heard, input})
      {:ok, state}
    end
  end

  defmodule QuietChatLoop do
    def factory_id, do: "quiet_chat_loop"
    def loop_mode, do: :per_input
    def self_terminating?, do: false
    def max_duration_ms, do: 600_000
    def persistence_enabled, do: false

    def procedures(c) do
      c |> Channels.input("in") |> Channels.into(Heard)
    end
  end

  defmodule Countdown do
    @behaviour Flezha.SDK.Procedure
    def name, do: "countdown"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      # Ends the chain by releasing nothing on the last pass, which is what a
      # handoff-chained loop reads as "no more work".
      if input != "0", do: Ctx.emit(ctx, Integer.to_string(String.to_integer(input) - 1))
      {:ok, state}
    end
  end

  defmodule ChainedLoop do
    def factory_id, do: "chained_loop"
    def loop_mode, do: :chained
    def persistence_enabled, do: false

    def procedures(c) do
      c |> Channels.input("in") |> Channels.into(Countdown)
    end
  end

  test "a chained loop re-seeds each pass from the previous terminal batch" do
    messages = drive(ChainedLoop, operative_id: "wf-loop", args: "3")
    assert %{status: "ok"} = completion(messages)
  end

  # A per-input loop started with spawn args. A state read is answered only
  # after the bootstrap, so whatever the test sends next reaches a loop that has
  # already looked at its empty inbox.
  defp start_loop(module, opts) do
    driver = start_driver(module, [operative_id: "wf-chat", args: "hello"] ++ opts)
    _state = :sys.get_state(driver)
    driver
  end

  test "a per-input loop's first iteration consumes its first input, never the args, and commits its offset" do
    driver = start_loop(ChatLoop, park_idle_ms: 60_000)

    Inbox.input(driver, "in", "turn", %{"flz.inbox_offset" => "7"})
    assert_receive {:shipped, {:iter_commit, %{consumed_offset: 7}}}, 5_000
    Driver.stop_workflow(driver)

    messages = drain()
    assert for(c <- commits(messages), h <- c.handoff, do: Resume.decode(h.payload)) == ["a:turn"]
    assert %{status: "cancelled"} = completion(messages)
  end

  test "a per-input loop parks after its idle window with nothing to consume" do
    # Not `start_loop/2`: with no idle window the loop can park before a state
    # read is answered.
    driver = start_driver(ChatLoop, operative_id: "wf-chat", args: "hello", park_idle_ms: 0)
    ref = Process.monitor(driver)
    assert_receive {:DOWN, ^ref, :process, ^driver, reason}, 5_000
    assert reason in [:normal, :noproc]

    messages = collect_all()
    # A park is NOT a terminal outcome: no WorkflowCompleted is shipped.
    assert Enum.any?(messages, &match?({:shipped, {:iter_park, _}}, &1))
    refute Enum.any?(messages, &match?({:shipped, {:workflow_completed, _}}, &1))
    assert commits(messages) == []
  end

  test "the tail a rehydrating bind still owes is waited for past the idle window" do
    driver = start_loop(ChatLoop, park_idle_ms: 0, owed_inbox: 1)

    Driver.push_inbox(driver, "owed")
    assert_receive {:shipped, {:proc_commit, commit}}, 5_000
    assert Enum.map(commit.handoff, &Resume.decode(&1.payload)) == ["a:owed"]
    assert_receive {:shipped, {:iter_park, _}}, 5_000
  end

  test "a loop that journals nothing waits for its next input instead of parking, until a stop" do
    driver = start_loop(QuietChatLoop, park_idle_ms: 0)

    Driver.push_inbox(driver, "late")
    assert_receive {:heard, "late"}, 5_000
    refute_received {:heard, "hello"}
    Driver.stop_workflow(driver)

    messages = drain()
    refute Enum.any?(messages, &match?({:shipped, {:iter_park, _}}, &1))
    assert %{status: "cancelled"} = completion(messages)
  end

  defp collect_all(acc \\ []) do
    receive do
      msg -> collect_all([msg | acc])
    after
      200 -> Enum.reverse(acc)
    end
  end

  # --- the transactional outbox ---

  defmodule HybridFlow do
    def factory_id, do: "hybrid_flow"

    def procedures(c) do
      c
      |> Channels.input("in")
      |> Channels.into(Tagger)
      |> Channels.named("mid")
      |> Channels.sink_with(fn v -> "a-out:" <> v end)
      |> Channels.into(Join)
    end
  end

  test "a hybrid channel stages its plugin output into the stage's commit" do
    messages = drive(HybridFlow)
    assert [producer, consumer] = commits(messages)

    assert [item] = producer.outbox
    assert item.payload == "a-out:a:go"
    assert item.channel == "mid"
    assert item.seq_in_proc == 0

    # The consumer still receives the value itself, not the sink's bytes: one
    # release reaches both the plugin and the next procedure.
    assert consumer.handoff |> hd() |> Map.get(:payload) |> Resume.decode() == "j:a:go"
    assert consumer.outbox == []
    assert %{status: "ok"} = completion(messages)
  end

  test "nothing is staged for a class that journals nothing" do
    assert OptedOutFlow |> drive() |> commits() == []
  end

  # --- crash resume ---

  defp resume(module, journal), do: drive(module, operative_id: "wf-resume", journal: journal)

  test "a committed procedure is skipped and its child re-seeds from the journal" do
    messages =
      resume(LinearFlow, %{
        committed_proc_idxs: [0],
        iter: 0,
        proc_handoffs: %{0 => [{0, Resume.encode("a:journaled")}]},
        committed_procs: [%{proc_idx: 0, iter: 0, entities_ran: []}]
      })

    # Only the uncommitted stage commits, and it ran on what the journal held
    # rather than on anything the skipped producer would have released now.
    assert [only] = commits(messages)
    assert only.proc_idx == 1
    assert Enum.map(only.handoff, & &1.payload) == [Resume.encode("j:a:journaled")]
    assert %{status: "ok"} = completion(messages)
  end

  test "a join with one producer still uncommitted banks and waits for the other" do
    messages =
      resume(DiamondFlow, %{
        committed_proc_idxs: [0, 1],
        iter: 0,
        proc_handoffs: %{
          0 => [{0, Resume.encode("a:j")}],
          1 => [{1, Resume.encode("l:a:j")}]
        },
        committed_procs: []
      })

    committed = commits(messages)
    assert committed |> Enum.map(& &1.proc_idx) |> Enum.sort() == [2, 3]

    join = Enum.find(committed, &(&1.proc_idx == 3))
    payloads = join.handoff |> Enum.map(&decode_payload(&1.payload)) |> Enum.sort()
    assert payloads == ["j:l:a:j", "j:r:a:j"]
  end

  test "the journal is consumed once — a later iteration seeds fresh" do
    assert {:ok, spec} = Spec.build(LinearFlow)
    instance = Flezha.SDK.Workflow.Instance.new("wf", spec, journal: %{iter: 3})
    assert instance.iter == 3
    assert {%{iter: 3}, consumed} = Flezha.SDK.Workflow.Instance.take_journal(instance)
    assert consumed.journal == nil
  end

  defp decode_payload(payload), do: Resume.decode(payload)

  # --- durability posture ---

  test "an opted-out class journals nothing and still completes" do
    messages = drive(OptedOutFlow)
    assert commits(messages) == []
    assert %{status: "ok"} = completion(messages)
  end

  test "a refused barrier ends the run abnormally after its ladder" do
    messages = drive(LinearFlow, error: "disk full")
    assert %{status: "error", error: "disk full"} = completion(messages)
    # first send plus the re-sends the class's barrier_retry_attempts allows
    assert length(commits(messages)) == 3
  end
end
