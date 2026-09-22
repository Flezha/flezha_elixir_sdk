defmodule Flezha.SDK.LocalTest do
  @moduledoc """
  The Elixir SDK's local mode: a user's own operative module, its declared
  two-tasklet graph, and no controller anywhere. `Normalise`, `Tally` and
  `WordCount` below are the worked example in `docs/quickstart.md`, copied
  verbatim so the documented graph is the one that runs here.
  """
  # async: false: the process-wide notice and unhandled-sink counters.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO, only: [with_io: 2]
  import ExUnit.CaptureLog, only: [capture_log: 1]

  alias Flezha.SDK.{Channels, Ctx, Local, Operative, Spawn}

  # --- a changeful tasklet, and one that asks for a spawn ---

  defmodule CounterTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "counter"
    def init_state(_), do: %{value: 0}
    def apply_change(s, {:delta, n}), do: %{s | value: s.value + n}

    def run(input, s, ctx) do
      s = Ctx.apply(ctx, s, {:delta, input})
      Ctx.emit(ctx, Integer.to_string(s.value))
      {:ok, s}
    end
  end

  defmodule CounterOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "counter_op"

    def channels(c),
      do:
        c
        |> Channels.input_with("in", &{:ok, String.to_integer(&1)})
        |> Channels.into(CounterTask)
        |> Channels.sink("c")
  end

  defmodule Spawner do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "spawner"
    def init_state(_), do: nil

    def run(instance_id, state, ctx) do
      handle = Ctx.spawn_workflow(ctx, "worker", "payload", id: instance_id)
      Ctx.emit(ctx, inspect(Spawn.await(handle, 5_000)))
      {:ok, state}
    end
  end

  defmodule SpawnerOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "spawner"

    def channels(c),
      do: c |> Channels.input("in") |> Channels.into(Spawner) |> Channels.sink("out")
  end

  # --- the moduledoc's worked example ---

  defmodule Normalise do
    @behaviour Flezha.SDK.Tasklet
    def name, do: "normalise"
    def init_state(_key), do: nil

    def run(word, state, ctx) do
      Flezha.SDK.Ctx.emit(ctx, String.downcase(word))
      {:ok, state}
    end
  end

  defmodule Tally do
    @behaviour Flezha.SDK.Tasklet
    def name, do: "tally"
    def init_state(_key), do: 0

    def run(word, count, ctx) do
      Flezha.SDK.Ctx.emit(ctx, "#{word} #{count + 1}")
      {:ok, count + 1}
    end
  end

  defmodule WordCount do
    @behaviour Flezha.SDK.StreamOperative
    alias Flezha.SDK.Channels

    def factory_id, do: "word_count"

    def channels(c) do
      c
      |> Channels.input("lines")
      |> Channels.filter(&(not String.starts_with?(&1, "#")))
      |> Channels.flat_map(&String.split/1)
      |> Channels.into(Normalise)
      |> Channels.key_by(& &1)
      |> Channels.into(Tally)
      |> Channels.named("counts")
      |> Channels.keyed()
      |> Channels.sink()
    end
  end

  # --- classes and channels local mode has something to say about ---

  defmodule TieredOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "local_tiered"

    def channels(c) do
      Channels.effectively_once(c)
      |> Channels.input("in")
      |> Channels.into(Normalise)
      |> Channels.named("out")
      |> Channels.keyed()
      |> Channels.sink()
    end
  end

  defmodule Job do
    @behaviour Flezha.SDK.WorkflowOperative
    def factory_id, do: "local_job"
    def channels(c), do: c |> Channels.input("in") |> Channels.into(Normalise)
  end

  # Asks to end its operative on `end`, which a stream never does. Reports every
  # invocation, so a retry would show.
  @closer_probe :local_mode_closer

  defmodule Closer do
    @behaviour Flezha.SDK.Tasklet
    def name, do: "closer"
    def init_state(_key), do: nil

    def run(input, state, ctx) do
      send(:local_mode_closer, {:ran, input})
      if input == "end", do: Ctx.complete(ctx, "done")
      Ctx.emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule CloserOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "local_closer"

    def channels(c),
      do: c |> Channels.input("in") |> Channels.into(Closer) |> Channels.sink("out")
  end

  defmodule StrictOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "local_strict"

    def channels(c) do
      c
      |> Channels.input_with("lines", fn _bytes -> {:error, "unreadable"} end)
      |> Channels.into(Normalise)
      |> Channels.named("out")
      |> Channels.sink()
    end
  end

  # A tasklet that does not return until the test releases it, so what makes
  # `run_until_idle` reach its budget is the test's own word rather than a
  # sleep long enough to hope for.
  @held_probe :local_mode_held_tasklet

  defmodule Held do
    @behaviour Flezha.SDK.Tasklet
    def name, do: "held"
    def init_state(_key), do: nil

    def run(_input, state, _ctx) do
      send(Process.whereis(:local_mode_held_tasklet), {:held, self()})

      receive do
        :release -> {:ok, state}
      end
    end
  end

  defmodule HeldOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "local_held"

    def channels(c) do
      c
      |> Channels.input("in")
      |> Channels.into(Held)
      |> Channels.named("out")
      |> Channels.sink()
    end
  end

  # --- helpers ---

  defp start_run(module) do
    # Every start writes the non-durability notice to stderr, by design and
    # unconditionally: an operator running without a controller has to be told,
    # whatever the log level. It is captured here rather than silenced, so the
    # write still happens; the case that asserts it reads `Local.notice/0` and
    # the emit counter, neither of which reads this device.
    {{:ok, run}, _notice} = with_io(:stderr, fn -> Local.start(module) end)
    on_exit(fn -> Local.stop(run) end)
    run
  end

  defp collect(run, channel) do
    reader = self()
    :ok = Local.on_sink(run, channel, fn output -> send(reader, {:sink, output}) end)
  end

  defp drained(acc \\ []) do
    receive do
      {:sink, output} -> drained([output | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp payloads, do: Enum.map(drained(), & &1.payload)

  # --- the graph runs ---

  # The lines to feed and the output to release, read from the file the Rust and
  # Java local-mode suites read: this SDK's own core and `flz_core` are compared
  # against ONE recorded expectation rather than by inspection.
  @scenario "test/fixtures/local_mode_word_count.txt"

  defp scenario_rows(prefix) do
    @scenario
    |> Flezha.SDK.Contract.path!()
    |> File.read!()
    |> String.split("\n", trim: true)
    |> Enum.filter(&String.starts_with?(&1, prefix))
    |> Enum.map(&String.replace_prefix(&1, prefix, ""))
  end

  test "the worked example releases the output every core's local mode releases" do
    run = start_run(WordCount)
    collect(run, "counts")

    Enum.each(scenario_rows("lines: "), &(:ok = Local.feed(run, "lines", &1)))
    assert :ok = Local.run_until_idle(run, 5_000)

    released = payloads()
    assert Enum.sort(released) == Enum.sort(scenario_rows("totals: "))

    # One Cell's releases keep their order whatever the other keys do.
    assert Enum.find_index(released, &(&1 == "the 1")) <
             Enum.find_index(released, &(&1 == "the 2"))
  end

  test "one Cell exists per resolved key" do
    run = start_run(WordCount)
    collect(run, "counts")

    :ok = Local.feed(run, "lines", "a b a c")
    assert :ok = Local.run_until_idle(run, 5_000)

    {"tally", :dynamic, keyed} =
      List.keyfind(Operative.cells(Local.operative(run)), "tally", 0)

    assert keyed |> Enum.map(&elem(&1, 0)) |> Enum.sort() == ~w(a b c)
  end

  test "a filtered value never reaches the consumer and flat_map fans out" do
    run = start_run(WordCount)
    collect(run, "counts")

    :ok = Local.feed(run, "lines", "# a comment line")
    :ok = Local.feed(run, "lines", "one two")
    assert :ok = Local.run_until_idle(run, 5_000)

    assert Enum.sort(payloads()) == ["one 1", "two 1"]
  end

  test "a keyed sink carries the identity a deployed run would stamp" do
    run = start_run(WordCount)
    collect(run, "counts")

    :ok = Local.feed(run, "lines", "fox")
    assert :ok = Local.run_until_idle(run, 5_000)

    assert [%Local.Output{channel: "counts", payload: "fox 1", metadata: metadata}] = drained()
    assert metadata["flz.cell"] == "word_count-local/tally/fox"
    assert metadata["flz.key"] == "fox"
    assert metadata["flz.seq"] =~ ~r/^\d+\.\d+$/
  end

  test "a record frame is discarded at the outbound edge, never taken for sink output" do
    run = start_run(CounterOp)
    collect(run, "c")
    unhandled = Local.unhandled_sink_outputs()

    :ok = Local.feed(run, "in", "2")
    :ok = Local.feed(run, "in", "3")
    assert :ok = Local.run_until_idle(run, 5_000)

    # A changeful tasklet: two dispatches wrote two change records onto the
    # same stream the sink rides. Only the two sink values reach the callback.
    assert payloads() == ~w(2 5)
    assert Local.unhandled_sink_outputs() == unhandled
  end

  # --- what local mode refuses ---

  test "an unknown channel is refused, naming the channel and the operative" do
    run = start_run(WordCount)

    assert {:error, entry} = Local.feed(run, "lynes", "x")
    assert entry =~ "lynes" and entry =~ "word_count-local" and entry =~ "lines"

    assert {:error, sink} = Local.on_sink(run, "totals", fn _output -> :ok end)
    assert sink =~ "totals" and sink =~ "word_count-local" and sink =~ "counts"
  end

  test "a controller-only declaration is refused at startup, naming the capability" do
    assert {:error, tier} = Local.start(TieredOp)
    assert tier =~ "effectively_once" and tier =~ "requires a controller"

    assert {:error, workflow} = Local.start(Job)
    assert workflow =~ "a workflow" and workflow =~ "requires a controller"
  end

  test "a value released on a sink with no callback is reported by name and counted" do
    run = start_run(WordCount)
    before = Local.unhandled_sink_outputs()

    log =
      capture_log(fn ->
        :ok = Local.feed(run, "lines", "fox")
        assert :ok = Local.run_until_idle(run, 5_000)
      end)

    assert log =~ "released on sink `counts`"
    assert Local.unhandled_sink_outputs() == before + 1
  end

  test "an entry the channel's parser refuses is parked and named" do
    run = start_run(StrictOp)

    log =
      capture_log(fn ->
        :ok = Local.feed(run, "lines", "anything")
        assert :ok = Local.run_until_idle(run, 5_000)
      end)

    assert log =~ "parked an input on channel `lines`"
    assert log =~ "unreadable"
  end

  test "a stream tasklet asking to complete fails that input once, by name, and the stream runs on" do
    Process.register(self(), @closer_probe)
    run = start_run(CloserOp)
    collect(run, "out")

    log =
      capture_log(fn ->
        :ok = Local.feed(run, "in", "hi")
        :ok = Local.feed(run, "in", "end")
        assert :ok = Local.run_until_idle(run, 5_000)
      end)

    assert log =~ "parked an input on channel `in`: CompleteOutsideWorkflow"
    # Not retried: the ladder would have run it again.
    assert [{:ran, "hi"}, {:ran, "end"}] == for({:ran, _} = m <- collect_ran(), do: m)
    # The failed input's emit is discarded with it; the one before it shipped.
    assert payloads() == ["hi"]
    assert Process.alive?(Local.operative(run))
  end

  defp collect_ran(acc \\ []) do
    receive do
      {:ran, _} = m -> collect_ran([m | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  test "a run still working when the budget elapses is reported, not passed off as done" do
    Process.register(self(), @held_probe)
    run = start_run(HeldOp)

    :ok = Local.feed(run, "in", "x")
    assert_receive {:held, cell}
    assert {:error, message} = Local.run_until_idle(run, 30)
    assert message =~ "local_held-local" and message =~ "still working"

    send(cell, :release)
  end

  # --- the notice, and the class both surfaces accept ---

  test "the non-durability notice names what needs a controller and is emitted once per run" do
    notice = Local.notice()
    assert notice =~ "state is held in memory and NOTHING is durable"

    for named <- ~w(Durability plugins placement migration dead-letter observability authz),
        do: assert(notice =~ named)

    assert notice =~ "multi-runner scale"

    before = Local.notices_emitted()
    start_run(WordCount)
    start_run(WordCount)
    assert Local.notices_emitted() == before + 2
  end

  test "the same module is accepted by local mode and by register_factory/2" do
    assert %Flezha.SDK{classes: [{WordCount, _topology}]} =
             "local-mode" |> Flezha.SDK.create_runner() |> Flezha.SDK.register_factory(WordCount)

    assert is_pid(start_run(WordCount))
  end

  test "a tasklet's spawn request is answered unavailable at once: there is no control plane" do
    run = start_run(SpawnerOp)
    collect(run, "out")

    :ok = Local.feed(run, "in", "w-local")
    assert :ok = Local.run_until_idle(run, 5_000)
    assert payloads() == [inspect({:error, :unavailable})]
  end
end
