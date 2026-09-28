defmodule Flezha.SDK.Bounded.DriverTest do
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Channels, Codec, Commands, Counters, Declarations, KeyGroup}
  alias Flezha.SDK.{Operator, Operators, Park, Spawn}
  alias Flezha.SDK.Operator.Inbox
  alias Flezha.SDK.Test.StubRunner
  alias Flezha.SDK.Proto.RunnerCommands
  alias Flezha.SDK.Bounded.{Driver, Instance, Resume, Spec}

  # --- fixtures ---

  defmodule Tagger do
    use Flezha.SDK.Tasklet, name: "tagger"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, "a:" <> input)
      {:ok, state}
    end
  end

  defmodule Left do
    use Flezha.SDK.Tasklet, name: "left"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, "l:" <> input)
      {:ok, state}
    end
  end

  defmodule Right do
    use Flezha.SDK.Tasklet, name: "right"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, "r:" <> input)
      {:ok, state}
    end
  end

  defmodule Join do
    use Flezha.SDK.Tasklet, name: "join"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, "j:" <> input)
      {:ok, state}
    end
  end

  defmodule Boom do
    use Flezha.SDK.Tasklet, name: "boom"
    @impl true
    def run(_input, _state, _ctx), do: {:error, {:non_retryable, "boom"}}
  end

  defmodule Undoable do
    use Flezha.SDK.Tasklet, name: "undoable"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end

    @impl true
    def compensate(_outputs, state, _ctx) do
      send(:bounded_driver_test_collector, {:undone, name()})
      {:ok, state}
    end
  end

  defmodule Undoable2 do
    use Flezha.SDK.Tasklet, name: "undoable2"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end

    @impl true
    def compensate(_outputs, state, _ctx) do
      send(:bounded_driver_test_collector, {:undone, name()})
      {:ok, state}
    end
  end

  # --- classes ---

  defmodule LinearFlow do
    def factory_id, do: "linear_flow"

    def channels(c) do
      c |> Channels.input("in") |> Channels.into(Tagger) |> Channels.into(Join)
    end
  end

  defmodule DiamondFlow do
    def factory_id, do: "diamond_flow"

    def channels(c) do
      fan = c |> Channels.input("in") |> Channels.into(Tagger)
      fan |> Channels.into(Left) |> Channels.into(Join)
      fan |> Channels.into(Right) |> Channels.into(Join)
    end
  end

  defmodule FailingFlow do
    def factory_id, do: "failing_flow"

    def channels(c) do
      c |> Channels.input("in") |> Channels.into(Undoable) |> Channels.into(Boom)
    end
  end

  defmodule SagaFlow do
    def factory_id, do: "saga_flow"

    def channels(c) do
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

    def channels(c) do
      c |> Channels.input("in") |> Channels.into(Tagger)
    end
  end

  # A spawn request from a tasklet, released as its stage's handoff.
  defmodule SpawnStep do
    use Flezha.SDK.Tasklet, name: "spawn_step"

    @impl true
    def run(input, state, ctx) do
      handle = spawn_bounded(ctx, "worker", input, id: "child-" <> input, if_running: :error)
      emit(ctx, inspect(Spawn.await(handle, 5_000)))
      {:ok, state}
    end
  end

  defmodule SpawningFlow do
    def factory_id, do: "spawning_flow"
    def channels(c), do: c |> Channels.input("in") |> Channels.into(SpawnStep)
  end

  # Fails every attempt, retryably, and reports each one.
  defmodule Flaky do
    use Flezha.SDK.Tasklet, name: "flaky"

    @impl true
    def run(_input, _state, _ctx) do
      send(:bounded_driver_test_collector, :attempt)
      {:error, "flaky"}
    end
  end

  defmodule EdgeLadderFlow do
    def factory_id, do: "edge_ladder_flow"

    def channels(c) do
      c
      |> Channels.input("in")
      |> Channels.retry_max(2)
      |> Channels.retry_backoff_ms(1)
      |> Channels.into(Flaky)
    end
  end

  # `after:<ms>` arms a timer that far out and emits `armed:<at>`;
  # `after-slow:<ms>` does the same, marked for the relay to linger on;
  # `hold:<ms>` holds the stage that long. A firing emits `fired:<at>`.
  defmodule Tick do
    use Flezha.SDK.Tasklet, name: "tick"

    @impl true
    def run("hold:" <> ms = input, state, ctx) do
      Process.sleep(String.to_integer(ms))
      emit(ctx, input)
      {:ok, state}
    end

    def run("after-slow:" <> ms, state, ctx) do
      emit(ctx, "armed-slow:#{arm(ctx, String.to_integer(ms))}")
      {:ok, state}
    end

    def run("after:" <> ms, state, ctx) do
      emit(ctx, "armed:#{arm(ctx, String.to_integer(ms))}")
      {:ok, state}
    end

    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end

    @impl true
    def on_timer(at, _kind, state, ctx) do
      emit(ctx, "fired:#{at}")
      {:ok, state}
    end

    defp arm(ctx, ms) do
      {:ok, at} = timer_after(ctx, ms)
      send(:bounded_driver_test_collector, {:armed, at})
      at
    end
  end

  defmodule Relay do
    use Flezha.SDK.Tasklet, name: "relay"

    @impl true
    def run(input, state, ctx) do
      if String.starts_with?(input, "armed-slow"), do: Process.sleep(150)
      emit(ctx, "relay:" <> input)
      {:ok, state}
    end
  end

  defmodule TimerLoop do
    def factory_id, do: "timer_loop"
    def loop_mode, do: :per_input

    def channels(c) do
      c |> Channels.input("in") |> Channels.into(Tick) |> Channels.into(Relay)
    end
  end

  defmodule Forward do
    use Flezha.SDK.Tasklet, name: "forward"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  # `tick` behind the entry channel's root.
  defmodule LateTimerLoop do
    def factory_id, do: "late_timer_loop"
    def loop_mode, do: :per_input

    def channels(c) do
      c
      |> Channels.input("in")
      |> Channels.into(Forward)
      |> Channels.into(Tick)
      |> Channels.into(Relay)
    end
  end

  # --- harness ---

  # A driver of `module` on a stub runner, reporting to this process.
  defp start_driver(module, opts) do
    Process.register(self(), :bounded_driver_test_collector)
    assert {:ok, spec} = Spec.build(module)
    {error, opts} = Keyword.pop(opts, :error, "")
    {:ok, runner} = StubRunner.start_link(test: self(), error: error)
    defaults = [operator_id: "wf-1", counters: Counters.new()]

    {:ok, driver} =
      Driver.start_link(Keyword.merge(defaults, opts) ++ [spec: spec, runner: runner])

    driver
  end

  # Start `module` and hand it its first message (`"go"` unless `:message`
  # says otherwise, `nil` for none), then drain it to its terminal frame.
  defp drive(module, opts \\ []) do
    {message, opts} = Keyword.pop(opts, :message, "go")
    driver = start_driver(module, opts)
    if message, do: Inbox.input(driver, nil, message, %{})

    # A short run can finish before the monitor is set, which reports
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
      {:shipped, {:bounded_completed, _}} = msg -> Enum.reverse([msg | acc])
      msg -> drain([msg | acc])
    after
      5_000 -> Enum.reverse(acc)
    end
  end

  defp commits(messages) do
    for {:shipped, {:task_commit, commit}} <- messages, do: commit
  end

  defp completion(messages) do
    Enum.find_value(messages, fn
      {:shipped, {:bounded_completed, done}} -> done
      _ -> nil
    end)
  end

  # --- spawn ---

  test "a structured tasklet's spawn reaches the runner through its stage Cell" do
    messages = drive(SpawningFlow)
    assert {:spawn_requested, "go", :error} in messages
    assert [commit] = commits(messages)

    assert commit.handoff |> hd() |> Map.get(:payload) |> Resume.decode() ==
             inspect({:ok, %{id: "child-go", outcome: :spawned}})
  end

  test "a spawn naming an unbounded factory resolves factory_not_bounded" do
    handle = Spawn.request(self(), false, "prices", "p", [], "")
    assert_receive {:"$gen_cast", {:spawn, _caller, spawn, "p", :deliver}}
    Spawn.answer(self(), %{spawn | claimed?: true}, {"factory_not_bounded", ""}, "")
    assert Spawn.await(handle, 1_000) == {:error, {:factory_not_bounded, "prices"}}
  end

  # --- the structured chain ---

  test "a linear chain commits each stage in DAG order and completes ok" do
    messages = drive(LinearFlow)
    assert [first, second] = commits(messages)
    assert first.task_idx == 0
    assert second.task_idx == 1
    assert first.handoff |> hd() |> Map.get(:payload) |> Resume.decode() == "a:go"
    assert second.handoff |> hd() |> Map.get(:payload) |> Resume.decode() == "j:a:go"
    assert %{status: "ok"} = completion(messages)
  end

  test "a stage commits the cell identities that ran" do
    messages = drive(LinearFlow, operator_id: "wf-ids")
    assert [first | _] = commits(messages)
    assert [%{cell_id: identity}] = first.cells_ran
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

    join = Enum.find(commits(messages), &(&1.task_idx == 3))
    assert length(join.handoff) == 2

    assert join.handoff |> Enum.map(&Resume.decode(&1.payload)) |> Enum.sort() ==
             ["j:l:a:go", "j:r:a:go"]
  end

  # The negative control for the assertion above: the same class with one arm
  # removed releases one payload at the join, not two. A two-payload assertion
  # that was never shown to fail proves the test ran, not that the fan-out works.
  defmodule OneArmedFlow do
    def factory_id, do: "one_armed_flow"

    def channels(c) do
      fan = c |> Channels.input("in") |> Channels.into(Tagger)
      fan |> Channels.into(Left) |> Channels.into(Join)
    end
  end

  test "removing one arm of the fan-out leaves the join one payload, not two" do
    join = OneArmedFlow |> drive() |> commits() |> Enum.find(&(&1.task_idx == 2))
    assert Enum.map(join.handoff, &Resume.decode(&1.payload)) == ["j:l:a:go"]
  end

  test "the join is committed once, not once per producer" do
    commits = DiamondFlow |> drive() |> commits()
    assert commits |> Enum.map(& &1.task_idx) |> Enum.sort() == [0, 1, 2, 3]
  end

  # --- failure and the saga ---

  test "a failing tasklet ends the run as an error naming it" do
    messages = drive(FailingFlow)
    assert %{status: "error", error: error} = completion(messages)
    assert error =~ "boom"
  end

  test "a stage's ladder is the edge's: retry_max(2) runs the tasklet twice, then fails it" do
    messages = drive(EdgeLadderFlow)
    assert Enum.count(messages, &(&1 == :attempt)) == 2
    assert %{status: "error", error: error} = completion(messages)
    assert error =~ "flaky"
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

    def channels(c) do
      c |> Channels.input("in") |> Channels.into(Tagger)
    end
  end

  # Reports every input it runs over, so a class that journals nothing, and so
  # commits nothing, can still be seen to have run.
  defmodule Heard do
    use Flezha.SDK.Tasklet, name: "heard"

    @impl true
    def run(input, state, _ctx) do
      send(:bounded_driver_test_collector, {:heard, input})
      {:ok, state}
    end
  end

  defmodule QuietChatLoop do
    def factory_id, do: "quiet_chat_loop"
    def loop_mode, do: :per_input
    def self_terminating?, do: false
    def max_duration_ms, do: 600_000
    def persistence_enabled, do: false

    def channels(c) do
      c |> Channels.input("in") |> Channels.into(Heard)
    end
  end

  defmodule Countdown do
    use Flezha.SDK.Tasklet, name: "countdown"

    @impl true
    def run(input, state, ctx) do
      # Ends the chain by releasing nothing on the last pass, which is what a
      # handoff-chained loop reads as "no more work".
      if input != "0", do: emit(ctx, Integer.to_string(String.to_integer(input) - 1))
      {:ok, state}
    end
  end

  defmodule ChainedLoop do
    def factory_id, do: "chained_loop"
    def loop_mode, do: :chained
    def persistence_enabled, do: false

    def channels(c) do
      c |> Channels.input("in") |> Channels.into(Countdown)
    end
  end

  test "a chained loop re-seeds each pass from the previous terminal batch" do
    messages = drive(ChainedLoop, operator_id: "wf-loop", message: "3")
    assert %{status: "ok"} = completion(messages)
  end

  defmodule JournaledChainedLoop do
    def factory_id, do: "journaled_chained_loop"
    def loop_mode, do: :chained

    def channels(c) do
      c |> Channels.input("in") |> Channels.into(Countdown)
    end
  end

  test "a chained loop commits its message's offset once, and no offset for a carry-fed pass" do
    driver = start_driver(JournaledChainedLoop, operator_id: "wf-countdown")
    ref = Process.monitor(driver)
    Inbox.input(driver, "in", "2", %{"flz.offset" => "0"})
    assert_receive {:DOWN, ^ref, :process, ^driver, _reason}, 5_000

    offsets = for {:shipped, {:iter_commit, c}} <- drain(), do: c.consumed_offset
    assert offsets == [0, nil, nil]
  end

  test "an input the controller did not journal commits no offset" do
    driver = start_loop(ChatLoop, park_idle_ms: 60_000)

    Inbox.input(driver, "in", "turn", %{})
    assert_receive {:shipped, {:iter_commit, %{consumed_offset: nil}}}, 5_000
    Driver.stop_bounded(driver)
    assert %{status: "cancelled"} = completion(drain())
  end

  test "a message the instance already took starts no pass" do
    driver = start_loop(ChatLoop, park_idle_ms: 60_000)

    Inbox.input(driver, "in", "turn", %{"flz.offset" => "3"})
    assert_receive {:shipped, {:iter_commit, %{consumed_offset: 3}}}, 5_000
    Inbox.input(driver, "in", "turn", %{"flz.offset" => "3"})
    Inbox.input(driver, "in", "turn", %{"flz.offset" => "4"})
    assert_receive {:shipped, {:iter_commit, %{consumed_offset: 4}}}, 5_000
    refute_received {:shipped, {:iter_commit, %{consumed_offset: 3}}}
    Driver.stop_bounded(driver)
  end

  test "the journal's released offset is what the instance already took" do
    assert {:ok, spec} = Spec.build(LinearFlow)
    instance = Instance.new("wf", spec, journal: %Instance.Resume{released_offset: 5})
    at = &%{"flz.offset" => Integer.to_string(&1)}

    assert {:repeat, instance} = Instance.take(instance, at.(5))
    assert {:ok, instance} = Instance.take(instance, at.(6))
    assert {:repeat, instance} = Instance.take(instance, at.(6))
    assert {:ok, _instance} = Instance.take(instance, %{})
  end

  # --- every message enters through the stream's entrance ---

  defmodule TwoChains do
    def factory_id, do: "two_chains"
    def loop_mode, do: :per_input
    def self_terminating?, do: false
    def max_duration_ms, do: 600_000

    def channels(c) do
      a = c |> Channels.input("a") |> Channels.into(Left)
      b = c |> Channels.input("b") |> Channels.into(Right)
      Channels.into(a, Join)
      Channels.into(b, Join)
    end
  end

  test "a message runs only the chain of the channel it names, and the join waits for that chain alone" do
    driver = start_loop(TwoChains, park_idle_ms: 60_000)

    Inbox.input(driver, "b", "one", %{"flz.offset" => "0"})
    assert_receive {:shipped, {:iter_commit, %{consumed_offset: 0, entry_channel: "b"}}}, 5_000
    Inbox.input(driver, "a", "two", %{"flz.offset" => "1"})
    assert_receive {:shipped, {:iter_commit, %{consumed_offset: 1, entry_channel: "a"}}}, 5_000
    Driver.stop_bounded(driver)

    messages = drain()
    ran = for c <- commits(messages), h <- c.handoff, do: Resume.decode(h.payload)
    assert ran == ["r:one", "j:r:one", "l:two", "j:l:two"]
  end

  test "a message naming a channel the class does not declare parks and starts no pass" do
    driver = start_loop(TwoChains, park_idle_ms: 60_000)

    Inbox.input(driver, "c", "lost", %{})
    assert_receive {:shipped, {:park_payload, park}}, 5_000
    assert park.channel == "c"
    assert park.error =~ "no entry channel `c`"
    Driver.stop_bounded(driver)

    messages = drain()
    assert commits(messages) == []
    assert %{status: "cancelled"} = completion(messages)
  end

  defmodule Strict do
    def factory_id, do: "strict"

    def channels(c) do
      c
      |> Channels.input_with("in", fn
        "bad" -> raise ArgumentError, "not readable"
        bytes -> {:ok, bytes}
      end)
      |> Channels.into(Tagger)
    end
  end

  test "an unreadable message parks under its channel, and the once pass runs on the next one" do
    driver = start_driver(Strict, operator_id: "wf-strict", park_idle_ms: 60_000)

    Inbox.input(driver, "in", "bad", %{})
    assert_receive {:shipped, {:park_payload, park}}, 5_000
    assert park.channel == "in"
    assert park.error =~ "not readable"
    refute_received {:shipped, {:bounded_completed, _}}

    Inbox.input(driver, "in", "fine", %{})
    messages = drain()
    assert [commit] = commits(messages)
    assert Enum.map(commit.handoff, &Resume.decode(&1.payload)) == ["a:fine"]
    assert %{status: "ok"} = completion(messages)
  end

  defmodule EdgeStrict do
    def factory_id, do: "edge_strict"

    def channels(c) do
      c
      |> Channels.input("in")
      |> Channels.map(fn
        "bad" -> raise ArgumentError, "not mappable"
        value -> value
      end)
      |> Channels.into(Tagger)
    end
  end

  test "a message its entry edge rejects parks under the channel, and the park releases it" do
    driver =
      start_driver(EdgeStrict, operator_id: "wf-edge-strict", park_idle_ms: 0, owed_inbox: 1)

    Inbox.input(driver, "in", "bad", %{"flz.offset" => "5"})
    assert_receive {:shipped, {:park_payload, park}}, 5_000
    assert park.channel == "in"
    assert park.error =~ "entry edge failed: not mappable"
    assert_receive {:shipped, {:iter_park, %{iter: 0, consumed_offset: 5}}}, 5_000
  end

  test "an unreadable message a rebound run owed settles the tail, and the park releases it" do
    driver =
      start_driver(Strict, operator_id: "wf-strict-owed", park_idle_ms: 0, owed_inbox: 1)

    Inbox.input(driver, "in", "bad", %{"flz.offset" => "4"})
    assert_receive {:shipped, {:park_payload, _park}}, 5_000
    assert_receive {:shipped, {:iter_park, %{iter: 0, consumed_offset: 4}}}, 5_000
  end

  test "a once class started without a message parks after the idle window" do
    driver = start_driver(LinearFlow, operator_id: "wf-once-parks", park_idle_ms: 0)
    ref = Process.monitor(driver)
    assert_receive {:DOWN, ^ref, :process, ^driver, reason}, 5_000
    assert reason in [:normal, :noproc]

    messages = collect_all()

    assert [{:shipped, {:iter_park, %{iter: 0}}}] =
             Enum.filter(messages, &match?({:shipped, {:iter_park, _}}, &1))

    refute Enum.any?(messages, &match?({:shipped, {:bounded_completed, _}}, &1))
  end

  test "a once class that journals nothing waits for its message instead of parking" do
    driver = start_driver(OptedOutFlow, operator_id: "wf-once-waits", park_idle_ms: 0)
    _state = :sys.get_state(driver)
    refute_receive {:shipped, _}, 300

    Inbox.input(driver, "in", "late", %{})
    messages = drain()
    assert %{status: "ok"} = completion(messages)
    refute Enum.any?(messages, &match?({:shipped, {:iter_park, _}}, &1))
  end

  # A per-input loop started without a message. A state read is answered only
  # after the bootstrap, so whatever the test sends next reaches a loop that has
  # already looked at its empty inbox.
  defp start_loop(module, opts) do
    driver = start_driver(module, Keyword.merge([operator_id: "wf-chat"], opts))
    _state = :sys.get_state(driver)
    driver
  end

  test "a per-input loop's first iteration consumes its first input and commits its offset" do
    driver = start_loop(ChatLoop, park_idle_ms: 60_000)

    Inbox.input(driver, "in", "turn", %{"flz.offset" => "7"})
    assert_receive {:shipped, {:iter_commit, %{consumed_offset: 7}}}, 5_000
    Driver.stop_bounded(driver)

    messages = drain()
    assert for(c <- commits(messages), h <- c.handoff, do: Resume.decode(h.payload)) == ["a:turn"]
    assert %{status: "cancelled"} = completion(messages)
  end

  test "a per-input loop parks after its idle window with nothing to consume" do
    # Not `start_loop/2`: with no idle window the loop can park before a state
    # read is answered.
    driver = start_driver(ChatLoop, operator_id: "wf-chat", park_idle_ms: 0)
    ref = Process.monitor(driver)
    assert_receive {:DOWN, ^ref, :process, ^driver, reason}, 5_000
    assert reason in [:normal, :noproc]

    messages = collect_all()
    # A park is NOT a terminal outcome: no BoundedCompleted is shipped.
    assert Enum.any?(messages, &match?({:shipped, {:iter_park, _}}, &1))
    refute Enum.any?(messages, &match?({:shipped, {:bounded_completed, _}}, &1))
    assert commits(messages) == []
  end

  test "the tail a rehydrating bind still owes is waited for past the idle window" do
    driver = start_loop(ChatLoop, park_idle_ms: 0, owed_inbox: 1)

    Driver.push_inbox(driver, "owed")
    assert_receive {:shipped, {:task_commit, commit}}, 5_000
    assert Enum.map(commit.handoff, &Resume.decode(&1.payload)) == ["a:owed"]
    assert_receive {:shipped, {:iter_park, _}}, 5_000
  end

  test "a loop that journals nothing waits for its next input instead of parking, until a stop" do
    driver = start_loop(QuietChatLoop, park_idle_ms: 0)

    Driver.push_inbox(driver, "late")
    assert_receive {:heard, "late"}, 5_000
    refute_received {:heard, "hello"}
    Driver.stop_bounded(driver)

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

  # --- timers ---

  defp handoff_of(commit), do: Enum.map(commit.handoff, &Resume.decode(&1.payload))

  defp await_commit(task_idx) do
    assert_receive {:shipped, {:task_commit, %{task_idx: ^task_idx} = commit}}, 5_000
    commit
  end

  test "A timer inside a bounded tasklet fires: a firing names the chain it starts" do
    driver = start_loop(TimerLoop, park_idle_ms: 60_000)

    Inbox.input(driver, "in", "after:30", %{})
    assert_receive {:armed, at}, 5_000
    assert %{trigger: nil} = await_commit(0)
    assert handoff_of(await_commit(1)) == ["relay:armed:#{at}"]
    assert_receive {:shipped, {:iter_commit, %{iter: 0}}}, 5_000

    # With nothing sent, the firing opens a pass at `tick` and its commit names it.
    root = await_commit(0)
    assert %RunnerCommands.TimerTrigger{scheduled_at: ^at, event_time: false} = root.trigger
    assert handoff_of(root) == ["fired:#{at}"]
    assert %{trigger: nil} = relay = await_commit(1)
    assert handoff_of(relay) == ["relay:fired:#{at}"]
    assert_receive {:shipped, {:iter_commit, %{iter: 1}}}, 5_000
    Driver.stop_bounded(driver)
  end

  test "A firing opens its pass at the tasklet that set it, past the entry channel's root" do
    driver = start_loop(LateTimerLoop, park_idle_ms: 60_000)

    Inbox.input(driver, "in", "after:30", %{})
    assert_receive {:armed, at}, 5_000
    for idx <- 0..2, do: await_commit(idx)
    assert_receive {:shipped, {:iter_commit, %{iter: 0}}}, 5_000

    root = await_commit(1)
    assert %RunnerCommands.TimerTrigger{scheduled_at: ^at} = root.trigger
    assert handoff_of(await_commit(2)) == ["relay:fired:#{at}"]
    assert_receive {:shipped, {:iter_commit, %{iter: 1}}}, 5_000
    refute_received {:shipped, {:task_commit, %{task_idx: 0, iter: 1}}}
    Driver.stop_bounded(driver)
  end

  test "A firing during a pass waits for the boundary, ahead of a message that arrived later" do
    driver = start_loop(TimerLoop, park_idle_ms: 60_000)

    Inbox.input(driver, "in", "after-slow:30", %{})
    assert_receive {:armed, at}, 5_000
    # Due while `relay` lingers; the message comes after it is due.
    Process.sleep(80)
    Inbox.input(driver, "in", "later", %{})

    ticks = for _ <- 1..3, do: handoff_of(await_commit(0))
    assert ticks == [["armed-slow:#{at}"], ["fired:#{at}"], ["later"]]
    Driver.stop_bounded(driver)
  end

  test "A firing joins its own stage while that stage runs" do
    driver = start_loop(TimerLoop, park_idle_ms: 60_000)

    # The timer comes due while `tick` holds the next message's pass.
    Inbox.input(driver, "in", "after:60", %{})
    Inbox.input(driver, "in", "hold:250", %{})
    assert_receive {:armed, at}, 5_000
    assert handoff_of(await_commit(0)) == ["armed:#{at}"]
    held = await_commit(0)
    assert held.trigger == nil
    assert handoff_of(held) == ["hold:250", "fired:#{at}"]
    assert_receive {:shipped, {:iter_commit, %{iter: 1}}}, 5_000
    refute_receive {:shipped, {:task_commit, %{task_idx: 0}}}, 300
    Driver.stop_bounded(driver)
  end

  test "The park names the earliest timer" do
    driver = start_loop(TimerLoop, park_idle_ms: 300)

    Inbox.input(driver, "in", "after:90000", %{})
    assert_receive {:armed, later}, 5_000
    Inbox.input(driver, "in", "after:60000", %{})
    assert_receive {:armed, earlier}, 5_000
    assert earlier < later
    assert_receive {:shipped, {:iter_park, park}}, 5_000
    assert park.wake_deadline_ms == earlier
  end

  test "a resume after a timer-fed pass's root committed runs the timer's chain and does not fire again" do
    trigger = %{cell_key: nil, kind: :processing_time, at: 123}

    journal = [
      iter: 1,
      entry_channel: "in",
      committed_task_idxs: [0],
      task_handoffs: %{0 => [{0, Resume.encode("fired:123")}]},
      committed_tasks: [%{task_idx: 0, iter: 1, cells_ran: [], completion: nil, trigger: trigger}]
    ]

    driver = start_loop(TimerLoop, park_idle_ms: 60_000, journal: journal)
    assert handoff_of(await_commit(1)) == ["relay:fired:123"]
    assert_receive {:shipped, {:iter_commit, %{iter: 1}}}, 5_000
    refute_received {:shipped, {:task_commit, %{task_idx: 0}}}
    Driver.stop_bounded(driver)
  end

  # --- a stage's plugin output ---

  defmodule HybridFlow do
    def factory_id, do: "hybrid_flow"

    def channels(c) do
      c
      |> Channels.input("in")
      |> Channels.into(Tagger)
      |> Channels.named("mid")
      |> Channels.sink_with(fn v -> "a-out:" <> v end)
      |> Channels.into(Join)
    end
  end

  test "a hybrid channel's plugin output rides its Cell's record, which the commit names" do
    messages = drive(HybridFlow)
    assert [producer, consumer] = commits(messages)

    assert [%{channels: ["mid"], first_seq: seq, seq: seq}] = producer.cells_ran
    assert is_integer(seq)

    # The consumer still receives the value itself, not the sink's bytes: one
    # release reaches both the plugin and the next tasklet.
    assert consumer.handoff |> hd() |> Map.get(:payload) |> Resume.decode() == "j:a:go"
    assert [%{channels: []}] = consumer.cells_ran
    assert %{status: "ok"} = completion(messages)
  end

  test "a stateless stage Cell of a journaling class restores from its records" do
    driver = start_driver(HybridFlow, operator_id: "wf-stateless-restore")
    assert {:ok, cell} = Operator.lookup(driver, "tagger", "")
    assert {:ok, _terminal, false} = Flezha.SDK.Cell.restore(cell, <<>>, <<>>, <<>>, 0)
    Driver.stop_bounded(driver)
  end

  test "nothing is staged for a class that journals nothing" do
    assert OptedOutFlow |> drive() |> commits() == []
  end

  # --- crash resume ---

  defp resume(module, journal), do: drive(module, operator_id: "wf-resume", journal: journal)

  test "a committed tasklet is skipped and its child re-seeds from the journal" do
    messages =
      resume(LinearFlow, %{
        committed_task_idxs: [0],
        iter: 0,
        task_handoffs: %{0 => [{0, Resume.encode("a:journaled")}]},
        committed_tasks: [%{task_idx: 0, iter: 0, cells_ran: []}]
      })

    # Only the uncommitted stage commits, and it ran on what the journal held
    # rather than on anything the skipped producer would have released now.
    assert [only] = commits(messages)
    assert only.task_idx == 1
    assert Enum.map(only.handoff, & &1.payload) == [Resume.encode("j:a:journaled")]
    assert %{status: "ok"} = completion(messages)
  end

  test "a join with one producer still uncommitted banks and waits for the other" do
    messages =
      resume(DiamondFlow, %{
        committed_task_idxs: [0, 1],
        iter: 0,
        task_handoffs: %{
          0 => [{0, Resume.encode("a:j")}],
          1 => [{1, Resume.encode("l:a:j")}]
        },
        committed_tasks: []
      })

    committed = commits(messages)
    assert committed |> Enum.map(& &1.task_idx) |> Enum.sort() == [2, 3]

    join = Enum.find(committed, &(&1.task_idx == 3))
    payloads = join.handoff |> Enum.map(&decode_payload(&1.payload)) |> Enum.sort()
    assert payloads == ["j:l:a:j", "j:r:a:j"]
  end

  test "the journal is consumed once — a later iteration seeds fresh" do
    assert {:ok, spec} = Spec.build(LinearFlow)
    instance = Flezha.SDK.Bounded.Instance.new("wf", spec, journal: %{iter: 3})
    assert instance.iter == 3
    assert {%{iter: 3}, consumed} = Flezha.SDK.Bounded.Instance.take_journal(instance)
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
    # every send the runner's fixed re-send count allows, the first included
    assert length(commits(messages)) == Flezha.SDK.Knob.barrier_resend_attempts()
  end

  # --- a tasklet ending its instance ---

  # Ends the instance on `bye` with how many turns it counted before, and
  # passes every input on, so the stage after it still has work to do.
  defmodule Closer do
    use Flezha.SDK.Tasklet, name: "closer"
    @impl true
    def init_state(_), do: 0

    @impl true
    def run("bye", turns, ctx) do
      complete(ctx, "turns:#{turns}")
      emit(ctx, "bye")
      {:ok, turns}
    end

    def run(input, turns, ctx) do
      emit(ctx, input)
      {:ok, turns + 1}
    end
  end

  defmodule ClosingLoop do
    def factory_id, do: "closing_loop"
    def loop_mode, do: :per_input
    def max_duration_ms, do: 600_000

    def channels(c) do
      c |> Channels.input("in") |> Channels.into(Closer) |> Channels.into(Tagger)
    end
  end

  defmodule ClosingPass do
    def factory_id, do: "closing_pass"

    def channels(c) do
      c |> Channels.input("in") |> Channels.into(Closer) |> Channels.into(Tagger)
    end
  end

  defmodule ClosingThenFailing do
    def factory_id, do: "closing_then_failing"

    def channels(c) do
      c |> Channels.input("in") |> Channels.into(Closer) |> Channels.into(Boom)
    end
  end

  # Every Cell of the stage asks to complete, each with its own key. The keys
  # are declared out of order, so the one that sorts last is neither the first
  # dispatched nor the last.
  defmodule KeyedCloser do
    use Flezha.SDK.Tasklet, name: "keyed_closer", cell_keys: ["b", "c", "a"]

    @impl true
    def run(input, state, ctx) do
      complete(ctx, "key:" <> cell_key(ctx))
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule LateCloser do
    use Flezha.SDK.Tasklet, name: "late_closer"

    @impl true
    def run(input, state, ctx) do
      complete(ctx, "late")
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule KeyedClosing do
    def factory_id, do: "keyed_closing"
    def channels(c), do: c |> Channels.input("in") |> Channels.into(KeyedCloser)
  end

  # Two tasklets of one frontier both complete: the later-declared one wins
  # whichever answers first.
  defmodule FannedClosing do
    def factory_id, do: "fanned_closing"

    def channels(c) do
      fan = c |> Channels.input("in") |> Channels.into(Tagger)
      Channels.into(fan, KeyedCloser)
      Channels.into(fan, LateCloser)
    end
  end

  defp iter_commits(messages), do: for({:shipped, {:iter_commit, c}} <- messages, do: c)

  test "a per-input loop ends on a tasklet's completion once its iteration commits, and does not park" do
    # No idle window once the owed tail has arrived: a loop that went on to wait
    # after the last turn would park at once.
    driver = start_loop(ClosingLoop, park_idle_ms: 0, owed_inbox: 2)
    Driver.push_inbox(driver, "hi")
    Driver.push_inbox(driver, "bye")

    messages = drain()
    assert [first, last] = iter_commits(messages)
    assert %{completes: false, result: ""} = first
    assert %{completes: true, result: "turns:1"} = last

    # The stage after the completing tasklet still ran, inside that iteration,
    # and the completing tasklet's own commit carries the completion too.
    assert for(
             c <- commits(messages),
             c.iter == last.iter,
             do: {c.task_idx, c.completes, c.result}
           ) ==
             [{0, true, "turns:1"}, {1, false, ""}]

    refute Enum.any?(messages, &match?({:shipped, {:iter_park, _}}, &1))
    assert %{status: "ok", error: "", result: "turns:1"} = completion(messages)
  end

  test "a once pass ends with the result a tasklet completed with, after every stage ran" do
    messages = drive(ClosingPass, message: "bye")
    assert commits(messages) |> Enum.map(& &1.task_idx) == [0, 1]
    assert iter_commits(messages) == []
    assert %{status: "ok", result: "turns:0"} = completion(messages)
  end

  test "a once pass no tasklet completed ends with an empty result" do
    assert %{status: "ok", result: ""} = ClosingPass |> drive(message: "hi") |> completion()
  end

  test "of several completions in one tasklet, the Cell whose key sorts last decides" do
    assert %{status: "ok", result: "key:c"} = KeyedClosing |> drive() |> completion()
  end

  test "of completions in several tasklets, the one latest in the chain decides" do
    assert %{status: "ok", result: "late"} = FannedClosing |> drive() |> completion()
  end

  test "an iteration that fails after a tasklet completed discards the completion" do
    messages = drive(ClosingThenFailing, message: "bye")
    assert %{status: "error", error: error, result: ""} = completion(messages)
    assert error =~ "boom"
  end

  test "a resume whose last committed iteration completed ends at once with its result" do
    journal =
      Codec.journal_facts(%RunnerCommands.TaskJournalState{
        iter: 3,
        completed: true,
        result: "turns:2"
      })

    messages = drive(ClosingLoop, operator_id: "wf-resume", journal: journal, park_idle_ms: 0)
    assert commits(messages) == []
    refute Enum.any?(messages, &match?({:shipped, {:iter_park, _}}, &1))
    assert %{status: "ok", result: "turns:2"} = completion(messages)
  end

  test "a resume after the completing tasklet committed, before its iteration did, completes" do
    journal =
      Codec.journal_facts(%RunnerCommands.TaskJournalState{
        iter: 1,
        committed_task_idxs: [0],
        task_handoffs: [
          %RunnerCommands.TaskEdgeHandoff{
            task_idx: 0,
            handoff: [
              %RunnerCommands.TaskHandoffItem{channel_id: 1, payload: Resume.encode("bye")}
            ]
          }
        ],
        committed_tasks: [
          %RunnerCommands.TaskCommittedTask{task_idx: 0, iter: 0},
          %RunnerCommands.TaskCommittedTask{
            task_idx: 0,
            iter: 1,
            completes: true,
            result: "turns:1"
          }
        ]
      })

    # The controller re-forwards the interrupted pass's message, and the bind
    # says so: without the owed count the loop could park before it lands.
    messages =
      drive(ClosingLoop,
        operator_id: "wf-resume",
        journal: journal,
        park_idle_ms: 0,
        owed_inbox: 1
      )

    assert for(c <- commits(messages), do: c.task_idx) == [1]
    assert [%{iter: 1, completes: true, result: "turns:1"}] = iter_commits(messages)
    refute Enum.any?(messages, &match?({:shipped, {:iter_park, _}}, &1))
    assert %{status: "ok", result: "turns:1"} = completion(messages)
  end

  test "a resume whose last committed iteration did not complete waits for input" do
    journal = Codec.journal_facts(%RunnerCommands.TaskJournalState{iter: 3})

    driver =
      start_driver(ClosingLoop, operator_id: "wf-resume", journal: journal, park_idle_ms: 0)

    ref = Process.monitor(driver)
    assert_receive {:DOWN, ^ref, :process, ^driver, reason}, 5_000
    assert reason in [:normal, :noproc]

    messages = collect_all()
    assert Enum.any?(messages, &match?({:shipped, {:iter_park, _}}, &1))
    refute Enum.any?(messages, &match?({:shipped, {:bounded_completed, _}}, &1))
  end

  # --- keyed tasklets ---

  defmodule Splitter do
    use Flezha.SDK.Tasklet, name: "splitter"

    @impl true
    def run(input, state, ctx) do
      input |> String.split(",") |> Enum.each(&emit(ctx, &1))
      {:ok, state}
    end
  end

  # Keyed by the edge into it: emits `<key>:<item>`, reports its undo.
  defmodule KeyedUndo do
    use Flezha.SDK.Tasklet, name: "keyed_undo", idle_ttl_seconds: 1

    @impl true
    def run(input, state, ctx) do
      emit(ctx, cell_key(ctx) <> ":" <> input)
      {:ok, state}
    end

    @impl true
    def compensate(_outputs, state, ctx) do
      send(:bounded_driver_test_collector, {:undone_key, name(), cell_key(ctx)})
      {:ok, state}
    end
  end

  defmodule KeyedPlain do
    use Flezha.SDK.Tasklet, name: "keyed_plain", idle_ttl_seconds: 1

    @impl true
    def run(input, state, _ctx) do
      send(:bounded_driver_test_collector, {:plain_ran, input})
      {:ok, state}
    end
  end

  # Holds its pass until the test releases it.
  defmodule Blocker do
    use Flezha.SDK.Tasklet, name: "blocker"

    @impl true
    def run(_input, state, _ctx) do
      send(:bounded_driver_test_collector, {:blocking, self()})

      receive do
        :release -> {:ok, state}
      end
    end
  end

  # Two static Cells of which `cells_for` names one, and it emits nothing.
  defmodule Silent do
    use Flezha.SDK.Tasklet, name: "silent", cell_keys: ["a", "b"]
    @impl true
    def cells_for(_value), do: ["a"]
    @impl true
    def run(_input, state, _ctx), do: {:ok, state}

    @impl true
    def compensate(_outputs, state, ctx) do
      send(:bounded_driver_test_collector, {:undone_key, name(), cell_key(ctx)})
      {:ok, state}
    end
  end

  defp keyed(c), do: c |> Channels.input("in") |> Channels.into(Splitter) |> Channels.key_by(& &1)

  defmodule KeyedFlow do
    def factory_id, do: "keyed_flow"

    def channels(c),
      do: c |> Flezha.SDK.Bounded.DriverTest.keyed_entry() |> Channels.into(KeyedUndo)
  end

  defmodule KeyedSagaFlow do
    def factory_id, do: "keyed_saga_flow"

    def channels(c) do
      c
      |> Flezha.SDK.Bounded.DriverTest.keyed_entry()
      |> Channels.into(KeyedUndo)
      |> Channels.into(Boom)
    end
  end

  defmodule KeyedBlockFlow do
    def factory_id, do: "keyed_block_flow"
    def persistence_enabled, do: false

    def channels(c) do
      c
      |> Flezha.SDK.Bounded.DriverTest.keyed_entry()
      |> Channels.into(KeyedUndo)
      |> Channels.into(Blocker)
    end
  end

  defmodule SilentFlow do
    def factory_id, do: "silent_flow"

    def channels(c) do
      entry = Channels.input(c, "in")
      Channels.into(entry, Silent)
      entry |> Channels.into(Tagger) |> Channels.into(Boom)
    end
  end

  # A per-message loop that journals nothing, so it waits for its next
  # message instead of parking: a keyed compensable stage, then a keyed
  # hookless one.
  defp keyed_loop(c) do
    c
    |> keyed_entry()
    |> Channels.into(KeyedUndo)
    |> Channels.key_by(& &1)
    |> Channels.into(KeyedPlain)
  end

  def keyed_entry(c), do: keyed(c)
  def keyed_loop_channels(c), do: keyed_loop(c)

  defmodule KeyedLoopAllOrNothing do
    def factory_id, do: "keyed_loop_aon"
    def loop_mode, do: :per_input
    def self_terminating?, do: false
    def max_duration_ms, do: 600_000
    def persistence_enabled, do: false
    def channels(c), do: Flezha.SDK.Bounded.DriverTest.keyed_loop_channels(c)
  end

  defmodule KeyedLoopKeepProgress do
    def factory_id, do: "keyed_loop_kp"
    def loop_mode, do: :per_input
    def rollback_mode, do: :keep_progress
    def self_terminating?, do: false
    def max_duration_ms, do: 600_000
    def persistence_enabled, do: false
    def channels(c), do: Flezha.SDK.Bounded.DriverTest.keyed_loop_channels(c)
  end

  defp identity(operator_id, tasklet, key), do: Park.identity(operator_id, tasklet, key)

  test "a keyed stage mints a Cell per key on dispatch, and its commit names each once" do
    messages = drive(KeyedFlow, operator_id: "wf-keyed", message: "a,b,a")
    assert %{status: "ok"} = completion(messages)
    assert [_split, keyed] = commits(messages)

    assert keyed.cells_ran |> Enum.map(& &1.cell_id) |> Enum.sort() ==
             Enum.map(~w(a b), &identity("wf-keyed", "keyed_undo", &1))

    assert keyed.handoff |> Enum.map(&Resume.decode(&1.payload)) |> Enum.sort() ==
             ["a:a", "a:a", "b:b"],
           "the second value keyed `a` reached the Cell the first one minted"
  end

  test "a keyed stage is undone once per key" do
    messages = drive(KeyedSagaFlow, operator_id: "wf-keyed-saga", message: "a,b,c")
    assert %{status: "error"} = completion(messages)
    assert Enum.sort(for {:undone_key, "keyed_undo", key} <- messages, do: key) == ~w(a b c)
  end

  test "a Cell that emitted nothing is recorded and undone; one that received nothing is neither" do
    messages = drive(SilentFlow, operator_id: "wf-silent")
    assert %{status: "error"} = completion(messages)
    ran = for c <- commits(messages), not c.compensating, %{cell_id: id} <- c.cells_ran, do: id
    assert identity("wf-silent", "silent", "a") in ran
    refute identity("wf-silent", "silent", "b") in ran
    assert for({:undone_key, "silent", key} <- messages, do: key) == ["a"]
  end

  test "a keyed Cell is swept only between passes, and all-or-nothing keeps what it could undo" do
    for {class, id, kept} <- [
          {KeyedLoopAllOrNothing, "wf-sweep-aon", true},
          {KeyedLoopKeepProgress, "wf-sweep-kp", false}
        ] do
      handler = attach_dynamic(id)
      driver = start_loop(class, operator_id: id)
      Inbox.input(driver, "in", "a", %{})
      assert_receive {:plain_ran, "a:a"}, 5_000
      # The one-second TTL passes with the loop waiting for its next message;
      # the compensable Cell, idle longer, is judged on the same sweep or earlier.
      for _evicted <- if(kept, do: [:plain], else: [:plain, :undo]),
          do: assert_receive({:dynamic, :evicted}, 5_000)

      :telemetry.detach(handler)

      assert match?({:ok, _}, Operator.lookup(driver, "keyed_undo", "a")) == kept,
             "#{id}: the compensable keyed Cell"

      assert Operator.lookup(driver, "keyed_plain", "a:a") == {:error, :cell_evicted},
             "#{id}: the hookless keyed Cell"

      Driver.stop_bounded(driver)
      assert %{status: "cancelled"} = completion(drain())
      Process.unregister(:bounded_driver_test_collector)
    end
  end

  test "no keyed Cell is swept while a pass runs" do
    handler = attach_dynamic("wf-sweep-block")
    driver = start_driver(KeyedBlockFlow, operator_id: "wf-sweep-block")
    Inbox.input(driver, nil, "a", %{})
    assert_receive {:blocking, blocker}, 5_000
    # The keyed Cell sits idle past its one-second TTL, and sweep ticks queue
    # up, while the last stage runs.
    Process.sleep(1_500)
    send(blocker, :release)
    messages = drain()
    :telemetry.detach(handler)
    assert %{status: "ok"} = completion(messages)
    assert {:dynamic, :created} in messages
    refute {:dynamic, :evicted} in messages
  end

  # Every `[:flezha, :sdk, :dynamic]` event of one operator, sent to the test.
  defp attach_dynamic(operator_id) do
    test = self()
    handler = "#{operator_id}-#{inspect(test)}"

    :telemetry.attach(
      handler,
      [:flezha, :sdk, :dynamic],
      fn _event, _measure, %{operator_id: oid, event: event}, _ ->
        if oid == operator_id, do: send(test, {:dynamic, event})
      end,
      nil
    )

    handler
  end

  test "every restore frame the bind owed opens the gate, whether or not a Cell took it" do
    driver = start_driver(KeyedFlow, operator_id: "wf-gate", resume_cell_count: 2)
    s = %{operators: %{"wf-gate" => {driver, "keyed_flow"}}}
    test = self()

    for {tasklet, key} <- [{"keyed_undo", "k"}, {"a_tasklet_the_class_dropped", ""}] do
      cmd = %{
        cell_id: identity("wf-gate", tasklet, key),
        snapshot_header: <<>>,
        snapshot_bytes: <<>>,
        change_log_bytes: <<>>,
        terminal_seq: 3,
        has_compensation_intent: false,
        has_compensation_done: false
      }

      Commands.dispatch(
        :restore_init_state,
        cmd,
        fn status, _, _, _ -> send(test, {:restore, status}) end,
        s
      )

      assert_receive {:restore, _status}, 2_000
    end

    assert {:ok, _} = Operator.lookup(driver, "keyed_undo", "k"),
           "the frame minted its keyed Cell"

    Inbox.input(driver, nil, "a", %{})
    assert %{status: "ok"} = completion(drain())
  end

  # --- partitions ---

  defp entry_keyed(c),
    do: c |> Channels.input("in") |> Channels.key_by(& &1) |> Channels.into(KeyedPlain)

  defmodule EntryKeyedLoop do
    def factory_id, do: "entry_keyed_loop"
    def loop_mode, do: :per_input
    def self_terminating?, do: false
    def max_duration_ms, do: 600_000
    def channels(c), do: Flezha.SDK.Bounded.DriverTest.entry_keyed_channels(c)
  end

  defmodule EntryKeyedOnce do
    def factory_id, do: "entry_keyed_once"
    def channels(c), do: Flezha.SDK.Bounded.DriverTest.entry_keyed_channels(c)
  end

  defmodule EntryKeyedChained do
    def factory_id, do: "entry_keyed_chained"
    def loop_mode, do: :chained
    def persistence_enabled, do: false
    def channels(c), do: Flezha.SDK.Bounded.DriverTest.entry_keyed_channels(c)
  end

  def entry_keyed_channels(c), do: entry_keyed(c)

  defp partitioned_add(factory_id) do
    %RunnerCommands.AddOperator{
      operator_id: "p#p1",
      factory_id: factory_id,
      kind: "bounded",
      partition_index: 1,
      partition_count: 2,
      key_group_from: 128,
      key_group_to: 255
    }
  end

  test "a partitioned bind refuses a one-message loop before a keyless one, and names the loop mode" do
    topologies =
      Map.new([EntryKeyedOnce, EntryKeyedChained, ChatLoop], fn module ->
        assert {:ok, spec} = Spec.build(module)
        {module.factory_id(), spec}
      end)

    s = %{topologies: topologies}

    for {factory, mode} <- [{"entry_keyed_once", "once"}, {"entry_keyed_chained", "chained"}] do
      assert {:error, "PartitionsWithoutPerInputLoop: class " <> _ = reason, ^s} =
               Operators.add(s, partitioned_add(factory))

      assert reason =~ factory
      assert reason =~ "(loop_mode #{mode})"
      assert reason =~ "declare loop_mode per_input"
      assert Declarations.for(topologies[factory])["loop_mode"] == mode
    end

    assert {:error, "PartitionsWithoutKeyedTasklet" <> _, ^s} =
             Operators.add(s, partitioned_add("chat_loop"))

    assert Declarations.for(topologies["chat_loop"])["loop_mode"] == "per_input"
  end

  # KB-451: a committed tasklet's stage record carries the cells it ran in
  # the resumed iteration, not in the first iteration that ran it.
  test "a resumed iteration's committed tasklet reports the cells of that iteration" do
    {:ok, spec} = Spec.build(TimerLoop)

    journal = %Instance.Resume{
      committed_task_idxs: [0],
      iter: 1,
      committed_tasks: [
        %Instance.Committed{task_idx: 0, iter: 0, cells_ran: ["from-iter-0"]},
        %Instance.Committed{task_idx: 0, iter: 1, cells_ran: ["from-iter-1"]}
      ]
    }

    instance = Instance.new("resume_cells", spec, journal: journal)

    assert [%{task_idx: 0, cells: ["from-iter-1"]}] =
             Resume.completed_stages(%{instance: instance, spec: spec})
  end

  # "a" hashes to key group 100 and "BTC-USD" to 129, so partition 1 of 2
  # owns the second and not the first.
  test "a partition member parks a key it does not own and opens no pass" do
    driver =
      start_loop(EntryKeyedLoop, range: KeyGroup.decode(1, 2, 128, 255), park_idle_ms: 60_000)

    Inbox.input(driver, "in", "a", %{"flz.offset" => "3"})
    assert_receive {:shipped, {:park_payload, %{error: error, channel: "in"}}}, 5_000
    assert error =~ "key 'a' hashes to key group 100"
    refute_received {:plain_ran, _}
    refute_received {:shipped, {:task_commit, _}}

    Inbox.input(driver, "in", "BTC-USD", %{"flz.offset" => "4"})
    assert_receive {:plain_ran, "BTC-USD"}, 5_000
    assert_receive {:shipped, {:iter_commit, %{iter: 0, consumed_offset: 4}}}, 5_000

    minted =
      for {_node, keyed} <- :sys.get_state(driver).cells.cells, key <- Map.keys(keyed), do: key

    assert minted == ["BTC-USD"]

    Driver.stop_bounded(driver)
    assert %{status: "cancelled"} = completion(drain())
  end
end
