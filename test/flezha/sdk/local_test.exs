defmodule Flezha.SDK.LocalTest do
  @moduledoc """
  The Elixir SDK's local mode: a user's own relay module, its declared
  two-tasklet graph, and no controller anywhere. `Normalise`, `Tally` and
  `WordCount` below are the word count of the "Local mode" section of
  https://github.com/flezha/flezha/blob/main/docs/public/topics/core/running_and_testing.md#local-mode.
  """
  # async: false: a local run's tasklets reach the test through registered names.
  use ExUnit.Case, async: false

  # Every run logs the core's local-mode banner, asserted where it starts.
  @moduletag :capture_log

  import ExUnit.CaptureLog, only: [capture_log: 1]
  import Flezha.SDK.Test.Notices, only: [start_local: 1]

  alias Flezha.SDK.{ChannelBuilder, Local, Spawn}
  alias Flezha.SDK.Test.Notices

  # --- a changeful tasklet, and one that asks for a spawn ---

  defmodule CounterTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "counter"
    @impl true
    def init_state(_), do: %{value: 0}

    defchange delta(s, n) do
      %{s | value: s.value + n}
    end

    @impl true
    def run(input, s, ctx) do
      s = delta(s, ctx, input)
      emit(ctx, Integer.to_string(s.value))
      {:ok, s}
    end
  end

  defmodule CounterOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "counter_op"

    def channels(c),
      do:
        c
        |> ChannelBuilder.input_with("in", &{:ok, String.to_integer(&1)})
        |> ChannelBuilder.into(CounterTask)
        |> ChannelBuilder.output("c")
  end

  defmodule Spawner do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "spawner"

    @impl true
    def run(instance_id, state, ctx) do
      opts = if instance_id == "no-id", do: [if_running: :error], else: [id: instance_id]
      handle = spawn_steps(ctx, "worker", "payload", opts)
      {:error, refusal} = Spawn.await(handle, 5_000)
      emit(ctx, refusal.code)
      {:ok, state}
    end
  end

  defmodule Effector do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "effector"

    @impl true
    def run(_input, state, ctx) do
      thrown =
        try do
          effect(ctx, "thrown", fn -> throw(:boom) end)
        catch
          :throw, :boom -> :caught
        end

      {:ok, after_it} = effect(ctx, "after", fn -> "ran" end)
      emit(ctx, "#{thrown} #{after_it}")
      {:ok, state}
    end
  end

  defmodule EffectorOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "local_effector"

    def channels(c),
      do:
        c
        |> ChannelBuilder.input("in", codec: :text)
        |> ChannelBuilder.into(Effector)
        |> ChannelBuilder.output("out")
  end

  defmodule SpawnerOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "spawner"

    def channels(c),
      do:
        c
        |> ChannelBuilder.input("in", codec: :text)
        |> ChannelBuilder.into(Spawner)
        |> ChannelBuilder.output("out")
  end

  # --- the moduledoc's worked example ---

  defmodule Normalise do
    use Flezha.SDK.Tasklet, name: "normalise"

    @impl true
    def run(word, state, ctx) do
      emit(ctx, String.downcase(word))
      {:ok, state}
    end
  end

  defmodule Tally do
    use Flezha.SDK.Tasklet, name: "tally"

    state do
      field(:count, :integer, default: 0)
    end

    defchange counted(state) do
      %{state | count: state.count + 1}
    end

    @impl true
    def run(word, state, ctx) do
      state = counted(state, ctx)
      emit(ctx, "#{word} #{state.count}")
      {:ok, state}
    end
  end

  defmodule WordCount do
    @behaviour Flezha.SDK.Relay
    alias Flezha.SDK.ChannelBuilder

    def factory_id, do: "word_count"

    def channels(c) do
      c
      |> ChannelBuilder.input("lines", codec: :text)
      |> ChannelBuilder.filter(&(not String.starts_with?(&1, "#")))
      |> ChannelBuilder.flat_map(&String.split/1)
      |> ChannelBuilder.into(Normalise)
      |> ChannelBuilder.key_by(& &1)
      |> ChannelBuilder.into(Tally)
      |> ChannelBuilder.named("counts")
      |> ChannelBuilder.keyed()
      |> ChannelBuilder.output()
    end
  end

  # --- classes and channels local mode has something to say about ---

  defmodule TieredOp do
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "local_tiered"
    def run, do: {:flow, exactly: :effectively_once}

    def channels(c) do
      c
      |> ChannelBuilder.input("in", codec: :text)
      |> ChannelBuilder.into(Normalise)
      |> ChannelBuilder.named("out")
      |> ChannelBuilder.keyed()
      |> ChannelBuilder.output()
    end
  end

  defmodule Job do
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    def factory_id, do: "local_job"

    def channels(c),
      do: c |> ChannelBuilder.input("in", codec: :text) |> ChannelBuilder.into(Normalise)
  end

  # Asks to end its relay on `end`. Reports every invocation, so a retry
  # would show.
  @closer_probe :local_mode_closer

  defmodule Closer do
    use Flezha.SDK.Tasklet, name: "closer"

    @impl true
    def run(input, state, ctx) do
      send(:local_mode_closer, {:ran, input})
      if input == "end", do: complete(ctx, "done")
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule CloserOp do
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "local_closer"

    def channels(c),
      do:
        c
        |> ChannelBuilder.input("in", codec: :text)
        |> ChannelBuilder.into(Closer)
        |> ChannelBuilder.output("out")
  end

  # A tasklet that does not return until the test releases it, so what makes
  # `run_until_idle` reach its budget is the test's own word rather than a
  # sleep long enough to hope for.
  @held_probe :local_mode_held_tasklet

  defmodule Held do
    use Flezha.SDK.Tasklet, name: "held"

    @impl true
    def run(_input, state, _ctx) do
      send(Process.whereis(:local_mode_held_tasklet), {:held, self()})

      receive do
        :release -> {:ok, state}
      end
    end
  end

  defmodule HeldOp do
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "local_held"

    def channels(c) do
      c
      |> ChannelBuilder.input("in", codec: :text)
      |> ChannelBuilder.into(Held)
      |> ChannelBuilder.named("out")
      |> ChannelBuilder.output()
    end
  end

  # Reports the stable id of every attempt. `flaky` fails its first attempt;
  # `die` ends the worker running it untrappably the first time it runs, before
  # its change is recorded, so the input runs again on another worker.
  @stamps_probe :local_mode_stamps

  defmodule Stamper do
    use Flezha.SDK.Tasklet, name: "stamper"

    @impl true
    def init_state(_key), do: 0

    defchange add(count, n) do
      count + n
    end

    @impl true
    def run(input, count, ctx) do
      send(:local_mode_stamps, {:stamp, input, stable_id(ctx)})

      cond do
        input == "flaky" and :persistent_term.get({__MODULE__, :tried}, false) == false ->
          :persistent_term.put({__MODULE__, :tried}, true)
          {:error, "once"}

        input == "die" and :persistent_term.get({__MODULE__, :died}, false) == false ->
          die()

        true ->
          {:ok, add(count, ctx, 1)}
      end
    end

    defp die do
      :persistent_term.put({__MODULE__, :died}, true)
      Process.exit(self(), :kill)
    end
  end

  defmodule StamperOp do
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "local_stamper"

    def channels(c),
      do: c |> ChannelBuilder.input("in", codec: :text) |> ChannelBuilder.into(Stamper)
  end

  # --- helpers ---

  defp collect(run, channel) do
    reader = self()
    :ok = Local.on_output(run, channel, fn output -> send(reader, {:sink, output}) end)
  end

  defp drained(acc \\ []) do
    receive do
      {:sink, output} -> drained([output | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp payloads, do: Enum.map(drained(), & &1.payload)

  # --- the core's notices ---

  test "core notices reach Logger under [:flezha] at FLEZHA_LOG_LEVEL, the banner at any level" do
    on_exit(fn -> System.delete_env("FLEZHA_LOG_LEVEL") end)

    # At `error` the first run's unclaimed output is not logged: the second
    # run's banner, written after it, is the next notice.
    System.put_env("FLEZHA_LOG_LEVEL", "error")
    first = start_local(WordCount)
    :ok = Local.feed(first, "lines", "a")
    :ok = Local.run_until_idle(first, 5_000)
    Local.close(first)
    second = start_local(WordCount)

    System.delete_env("FLEZHA_LOG_LEVEL")
    :ok = Local.feed(second, "lines", "a")
    :ok = Local.run_until_idle(second, 5_000)

    assert {:warning, "local mode: 3 bytes were released on output `counts`" <> _} =
             Notices.next()
  end

  # --- the graph runs ---

  test "a retried attempt answers the stable id the first did, and a dead worker's input runs again" do
    Process.register(self(), @stamps_probe)

    on_exit(fn ->
      :persistent_term.erase({Stamper, :died})
      :persistent_term.erase({Stamper, :tried})
    end)

    run = start_local(StamperOp)

    :ok = Local.feed(run, "in", "flaky")
    assert_receive {:stamp, "flaky", first}, 5_000
    assert_receive {:stamp, "flaky", ^first}, 5_000

    :ok = Local.feed(run, "in", "die")
    assert_receive {:stamp, "die", _killed}, 5_000
    assert_receive {:stamp, "die", _again}, 5_000
    assert :ok = Local.run_until_idle(run, 5_000)
  end

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
    run = start_local(WordCount)
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
    run = start_local(WordCount)
    collect(run, "counts")

    :ok = Local.feed(run, "lines", "a b a c")
    assert :ok = Local.run_until_idle(run, 5_000)

    assert Enum.sort(payloads()) == ["a 1", "a 2", "b 1", "c 1"]
  end

  test "a filtered value never reaches the consumer and flat_map fans out" do
    run = start_local(WordCount)
    collect(run, "counts")

    :ok = Local.feed(run, "lines", "# a comment line")
    :ok = Local.feed(run, "lines", "one two")
    assert :ok = Local.run_until_idle(run, 5_000)

    assert Enum.sort(payloads()) == ["one 1", "two 1"]
  end

  test "a keyed output carries the identity a deployed run would stamp" do
    run = start_local(WordCount)
    collect(run, "counts")

    :ok = Local.feed(run, "lines", "fox")
    assert :ok = Local.run_until_idle(run, 5_000)

    assert [%Local.Output{channel: "counts", payload: "fox 1", metadata: metadata}] = drained()
    assert metadata["flz.cell"] == "word_count-local/tally/fox"
    assert metadata["flz.key"] == "fox"
    assert metadata["flz.seq"] =~ ~r/^\d+\.\d+$/
  end

  test "a changeful tasklet releases its outputs and nothing of its records" do
    run = start_local(CounterOp)
    collect(run, "c")

    :ok = Local.feed(run, "in", "2")
    :ok = Local.feed(run, "in", "3")
    assert :ok = Local.run_until_idle(run, 5_000)

    assert payloads() == ~w(2 5)
  end

  # --- what local mode refuses ---

  test "an unknown channel is refused, naming the channel and the relay" do
    run = start_local(WordCount)

    assert {:error, %{code: "runtime.channel_undeclared", message: entry}} =
             Local.feed(run, "lynes", "x")

    assert entry =~ "lynes" and entry =~ "word_count-local" and entry =~ "lines"

    assert {:error, %{code: "runtime.channel_undeclared", message: sink}} =
             Local.on_output(run, "totals", fn _output -> :ok end)

    assert sink =~ "totals" and sink =~ "word_count-local" and sink =~ "counts"
  end

  test "a controller-only declaration is refused at startup, naming the capability" do
    assert {:error, %{code: "runtime.local_needs_controller", message: tier}} =
             Local.start(TieredOp)

    assert tier =~ "effectively_once" and tier =~ "requires a controller"

    assert {:error, %{code: "runtime.local_needs_controller", message: steps}} =
             Local.start(Job)

    assert steps =~ "steps" and steps =~ "requires a controller"
  end

  test "a flow tasklet's completion is the controller's to act on: a local run runs on" do
    Process.register(self(), @closer_probe)
    run = start_local(CloserOp)
    collect(run, "out")

    log =
      capture_log(fn ->
        :ok = Local.feed(run, "in", "hi")
        :ok = Local.feed(run, "in", "end")
        assert :ok = Local.run_until_idle(run, 5_000)
      end)

    refute log =~ "parked an input"
    assert [{:ran, "hi"}, {:ran, "end"}] == for({:ran, _} = m <- collect_ran(), do: m)
    assert payloads() == ["hi", "end"]
    assert Process.alive?(run)
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
    run = start_local(HeldOp)

    :ok = Local.feed(run, "in", "x")
    assert_receive {:held, cell}, 5_000

    assert {:error, %{code: "runtime.local_run_busy", message: message}} =
             Local.run_until_idle(run, 30)

    assert message =~ "local_held-local" and message =~ "still working"

    send(cell, :release)
  end

  # --- the class both surfaces accept ---

  test "the same module is accepted by local mode and by register_factory/2" do
    assert %Flezha.SDK{modules: [WordCount]} =
             "local-mode" |> Flezha.SDK.create_runner() |> Flezha.SDK.register_factory(WordCount)

    assert is_pid(start_local(WordCount))
  end

  test "an effect that throws is released, so the next one in the invocation runs" do
    run = start_local(EffectorOp)
    collect(run, "out")

    :ok = Local.feed(run, "in", "x")
    assert :ok = Local.run_until_idle(run, 5_000)
    assert payloads() == ["caught ran"]
  end

  test "a run whose pool died still frees the core for the next run" do
    Process.flag(:trap_exit, true)
    Notices.forward()
    {:ok, run} = Local.start(WordCount)
    Notices.banner()
    ref = Process.monitor(run)

    log =
      capture_log(fn ->
        Process.exit(:sys.get_state(run).pool, :kill)
        assert_receive {:DOWN, ^ref, :process, ^run, _reason}, 10_000
      end)

    assert log =~ "terminating" and log =~ "** (stop) killed"

    assert is_pid(start_local(CounterOp))
  end

  test "one local run is live in this VM at a time" do
    first = start_local(WordCount)
    assert {:error, %Flezha.SDK.Error{code: "runtime.core_in_use"}} = Local.start(CounterOp)

    :ok = Local.close(first)
    assert is_pid(start_local(CounterOp))
  end

  test "a tasklet's spawn request is answered unavailable at once: there is no control plane" do
    run = start_local(SpawnerOp)
    collect(run, "out")

    :ok = Local.feed(run, "in", "w-local")
    assert :ok = Local.run_until_idle(run, 5_000)
    assert payloads() == ["spawn.unavailable"]
  end

  test "if_running without an id resolves the core's refusal at the call" do
    run = start_local(SpawnerOp)
    collect(run, "out")

    :ok = Local.feed(run, "in", "no-id")
    assert :ok = Local.run_until_idle(run, 5_000)
    assert payloads() == ["spawn.if_running_without_id"]
  end
end
