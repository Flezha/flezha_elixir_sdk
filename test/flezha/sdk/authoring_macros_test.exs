defmodule Flezha.SDK.AuthoringMacrosTest do
  @moduledoc """
  The `use` macros, the `state` block, `defchange` and the §5 short form.

  What is proved here is the surface: which callbacks a declaration defines and
  which it deliberately leaves undefined, what a recorder journals, and what
  each malformed declaration refuses. The topology those declarations build is
  measured against `test/fixtures/reference_topologies.txt` by the conformance
  run, as every other SDK's is.
  """
  use ExUnit.Case, async: true

  # A local run logs the core's local-mode banner, asserted where it starts.
  @moduletag :capture_log

  import ExUnit.CaptureIO, only: [with_io: 2]
  import Flezha.SDK.Test.Notices, only: [start_local: 1]
  import Flezha.SDK.Test.Refused

  alias Flezha.SDK.{Channels, Ctx, Declaration, Local, Tasklet}

  # --- what `use` defines, and what it does not ---------------------------

  defmodule Written do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "position", max_cells: 50_000

    def run(input, state, _ctx), do: {:ok, Map.put(state || %{}, :last, input)}
  end

  defmodule Stateless do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "echo"

    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  test "a written option is a callback, an unwritten one is not" do
    assert Written.name() == "position"
    assert Written.max_cells() == 50_000

    refute function_exported?(Written, :mailbox_capacity, 0),
           "an option the module never wrote must leave the env knob deciding"

    for option <- [
          :cell_keys,
          :idle_ttl_seconds,
          :compact_after_bytes,
          :schema_version,
          :min_readable_schema_version,
          :logical_clock,
          :max_timers_per_cell
        ] do
      refute function_exported?(Written, option, 0), "`#{option}` was never written"
    end
  end

  test "a stateless tasklet needs no init_state" do
    assert Stateless.init_state(nil) == nil
    assert function_exported?(Stateless, :run, 3)
  end

  test "a misspelt option fails to compile, naming it" do
    error =
      refused("""
      use Flezha.SDK.Tasklet, name: "p", max_entites: 10
      def run(_i, s, _c), do: {:ok, s}
      """)

    assert error.description =~ "max_entites"
  end

  # A nested tasklet inherits the relay module's imports, so `use` has to
  # take `Channels.input/2` back before the tasklet's own `input/0` is read.
  defmodule NestingRelay do
    @moduledoc false
    use Flezha.SDK.Relay, factory_id: "nesting_relay"

    defmodule Nested do
      @moduledoc false
      use Flezha.SDK.Tasklet, name: "nested"

      def input, do: "the nested tasklet's own"
      def run(_input, state, _ctx), do: {:ok, state}
    end

    @impl true
    def channels(c) do
      c |> input("in") |> into(Nested) |> output("out")
    end
  end

  test "a nested tasklet's own input/0 does not clash with the builder's" do
    assert NestingRelay.Nested.input() == "the nested tasklet's own"
    assert {:ok, derived} = Declaration.check(NestingRelay)
    assert entry_names(NestingRelay) == ["in"]
    assert sink_names(derived) == ["out"]
  end

  # --- the behaviour form is unchanged -------------------------------------

  defmodule HandWritten do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet

    @impl true
    def name, do: "hand_written"
    @impl true
    def init_state(_key), do: 0
    @impl true
    def idle_ttl_seconds, do: 1
    @impl true
    def event_time(input), do: String.to_integer(input)
    @impl true
    def apply_change(count, {:add, n}), do: count + n

    @impl true
    def run(input, count, ctx) do
      Ctx.emit(ctx, Integer.to_string(Ctx.now(ctx)))
      {:ok, Ctx.apply(ctx, count, {:add, String.to_integer(input)})}
    end
  end

  defmodule HandWrittenRelay do
    @moduledoc false
    @behaviour Flezha.SDK.Relay

    @impl true
    def factory_id, do: "hand_written_relay"

    @impl true
    def channels(c) do
      c
      |> Channels.input("in", codec: :text)
      |> Channels.into(HandWritten)
      |> Channels.output("out")
    end
  end

  test "the behaviour form registers and runs, with its clock and idle TTL read from its callbacks" do
    assert {:ok, _derived} = Declaration.check(HandWrittenRelay)
    refute function_exported?(HandWritten, :mailbox_capacity, 0)

    assert Tasklet.idle_ttl_ms(HandWritten) == 1_000
    assert Tasklet.clock(HandWritten) == :event_time
    assert {[{:add, 7}], 7} = journal(HandWritten, "7")
    assert HandWritten.apply_change(HandWritten.init_state(nil), {:add, 7}) == 7

    run = start_local(HandWrittenRelay)
    collect(run, "out")
    :ok = Local.feed(run, "in", "1234")
    :ok = Local.run_until_idle(run, 5_000)

    assert ["1234"] = Enum.map(drained(), & &1.payload),
           "`now/1` serves the input's own event time"
  end

  # --- the relay macros -------------------------------------------------

  defmodule TunedSteps do
    @moduledoc false
    use Flezha.SDK.Relay,
      factory_id: "tuned_steps",
      run: {:steps, each: :chained, on_failure: :keep_progress}

    @impl true
    def channels(c) do
      c |> input("in") |> into(Stateless) |> named("out")
    end
  end

  test "a relay macro defines only the settings written" do
    assert TunedSteps.factory_id() == "tuned_steps"
    assert TunedSteps.run() == {:steps, each: :chained, on_failure: :keep_progress}
    refute function_exported?(TunedSteps, :durable, 0)
    refute function_exported?(TunedSteps, :max_duration_ms, 0)
  end

  test "a retired steps option fails to compile, as any unknown option does" do
    for option <- [
          "retry_attempts: 5",
          "retry_backoff_ms: 20",
          "barrier_retry_attempts: 3",
          "wake_after_ms: 5000"
        ] do
      error =
        refused("""
        use Flezha.SDK.Relay, factory_id: "w", #{option}, run: :steps
        def channels(c), do: c
        """)

      [key | _] = String.split(option, ":")
      assert error.description =~ "has no option :#{key}"
    end
  end

  test "a setting outside its choices is refused at registration, naming the setting and the value" do
    for {option, setting, value} <- [
          {"run: {:steps, each: :forever}", "each", "forever"},
          {"run: {:steps, on_failure: :undo_everything}", "on_failure", "undo_everything"},
          {"run: :bounded", "run", "bounded"},
          {"durable: {:local, sleep_after: 0}", "sleep_after", "0"}
        ] do
      assert %{code: "topology.invalid_setting", details: details} = registered(option)
      assert {details["setting"], details["value"]} == {setting, value}, option
    end
  end

  test "a setting under a parent that does not carry it is refused at registration, naming both" do
    for {option, parent, setting} <- [
          {"run: {:flow, each: :per_input}", "flow", "each"},
          {"run: {:steps, exactly: :effectively_once}", "steps", "exactly"},
          {"run: {:steps, correction_ms: 5_000}", "steps", "correction_ms"},
          {"run: {:flow, exactly: {:at_least_once, ack: :checkpoint}}", "at_least_once", "ack"},
          {"durable: {false, sleep_after: 1_000}", "false", "sleep_after"}
        ] do
      assert %{code: "topology.unknown_field", details: details} = registered(option)
      assert details == %{"declaration" => "'#{parent}' of relay 'w'", "fields" => setting}
    end

    # A member of the tree written beside it is no option of the macro.
    error = refused(~s|use Flezha.SDK.Relay, factory_id: "w", each: :chained|)
    assert error.description =~ "has no option :each"
  end

  # The core's verdict on a class whose `use` writes `option`: the tree
  # compiles as written and is judged when the class is registered.
  defp registered(option) do
    [{module, _binary}] =
      Code.compile_string("""
      defmodule Flezha.SDK.Tree#{System.unique_integer([:positive])} do
        use Flezha.SDK.Relay, factory_id: "w", #{option}
        def channels(c), do: c |> input("in") |> into(#{inspect(Stateless)})
      end
      """)

    assert {:error, error} =
             Flezha.SDK.register_factory(Flezha.SDK.create_runner("tree_ns"), module)

    error
  end

  test "a relay macro refuses an unknown option and a missing factory_id" do
    unknown =
      refused("""
      use Flezha.SDK.Relay, factory_id: "x", persistance_enabled: false
      def channels(_c), do: :ok
      """)

    assert unknown.description =~ "persistance_enabled"

    missing =
      refused("""
      use Flezha.SDK.Relay, description: "no id"
      def channels(_c), do: :ok
      """)

    assert missing.description =~ "factory_id"
  end

  # --- a merge pipe needs one import ----------------------------------------

  defmodule MergedByImport do
    @moduledoc false
    use Flezha.SDK.Relay, factory_id: "merged_by_import"

    @impl true
    def channels(c) do
      ticks = input(c, "ticks")
      quotes = input(c, "quotes")

      c
      |> merge()
      |> from(ticks, &{:tick, &1})
      |> from(quotes, &{:quote, &1})
      |> key_by(fn _ -> "k" end)
      |> into(Stateless)
      |> output("merged")
    end
  end

  defmodule MergedByPrefix do
    @moduledoc false
    @behaviour Flezha.SDK.Relay

    @impl true
    def factory_id, do: "merged_by_prefix"

    @impl true
    def channels(c) do
      ticks = Channels.input(c, "ticks")
      quotes = Channels.input(c, "quotes")

      c
      |> Channels.merge()
      |> Channels.from(ticks, &{:tick, &1})
      |> Channels.from(quotes, &{:quote, &1})
      |> Channels.key_by(fn _ -> "k" end)
      |> Channels.into(Stateless)
      |> Channels.output("merged")
    end
  end

  test "a merge pipe written with one import registers the topology the prefixed form did" do
    assert {:ok, imported} = Declaration.check(MergedByImport)
    assert {:ok, prefixed} = Declaration.check(MergedByPrefix)

    assert entry_names(MergedByImport) == entry_names(MergedByPrefix)
    assert route_shape(imported) == route_shape(prefixed)
  end

  # --- reading a built topology --------------------------------------------
  #
  # The compiled topology keeps its entries in a map, so the builder's own
  # record is what states their order — the same read `conformance.ex` makes.

  defp entry_names(module) do
    c = Channels.new()
    _ = module.channels(c)
    c |> Channels.take() |> Map.fetch!(:entry_order) |> Enum.uniq()
  end

  defp sink_names(derived), do: for(%{"sink" => sink} <- derived["channels"], sink, do: sink)

  # Each channel by what it is and what it reaches, as the core derived it.
  defp route_shape(derived) do
    for channel <- derived["channels"],
        do: {channel["name"], channel["kind"], channel["consumers"], channel["sink"]}
  end

  # --- the `state` block and `defchange` -----------------------------------

  defmodule Position do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "position_state"

    state do
      field(:qty, :integer, default: 0)
      field(:fill_ids, :list, default: [])
    end

    defchange record_fill(state, qty, id) do
      %{state | qty: state.qty + qty, fill_ids: state.fill_ids ++ [id]}
    end

    @impl true
    def run({:fill, qty, id}, state, ctx), do: {:ok, record_fill(state, ctx, qty, id)}
  end

  defmodule Defaults do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "defaults"

    state do
      field(:qty, :integer, default: 0)
      field(:seen, :list, default: [])
    end

    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule Tally do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "counter"

    @impl true
    def init_state(_key), do: 0

    defchange add(count, n) do
      count + n
    end

    @impl true
    def run(_input, count, ctx), do: {:ok, add(count, ctx, 1)}
  end

  defmodule Noted do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "noted"

    @impl true
    def init_state(_key), do: 0

    defchange add(count, n, _note) do
      count + n
    end

    @impl true
    def run(_input, count, ctx), do: {:ok, add(count, ctx, 1, "live")}
  end

  test "a change argument the fold ignores is recorded, and written with an underscore" do
    assert {[{:add, 1, "live"}], 1} = journal(Noted, "x")

    {_module, warnings} =
      with_io(:stderr, fn ->
        Code.compile_string("""
        defmodule Flezha.SDK.AuthoringMacrosTest.NotedTwin do
          use Flezha.SDK.Tasklet, name: "noted_twin"
          @impl true
          def init_state(_key), do: 0
          defchange add(count, n, _note) do
            count + n
          end
          @impl true
          def run(_input, count, ctx), do: {:ok, add(count, ctx, 1, "live")}
        end
        """)
      end)

    assert warnings == ""
  end

  test "the block generates the defaults and the JSON read" do
    assert Position.init_state(nil) == %{qty: 0, fill_ids: []}
    assert Position.state_from_json(%{"qty" => 7}) == %{qty: 7, fill_ids: []}
    assert Position.state_from_json(%{}) == %{qty: 0, fill_ids: []}
  end

  test "a state block generates no recorder and no change" do
    assert Defaults.init_state(nil) == %{qty: 0, seen: []}
    assert function_exported?(Defaults, :state_from_json, 1)

    for {fun, arity} <- [apply_change: 2, set_qty: 3, push_seen: 3, set_seen: 3],
        do: refute(function_exported?(Defaults, fun, arity), "#{fun}/#{arity}")

    refute Tasklet.changeful?(Defaults)
  end

  test "defchange works without a state block" do
    assert {[{:add, 1}], 1} = journal(Tally, "tick")

    assert Tally.apply_change(Tally.apply_change(Tally.init_state(nil), {:add, 1}), {:add, 1}) ==
             2

    assert Tasklet.changeful?(Tally)
  end

  test "a named change is one record for several fields" do
    assert {[{:record_fill, 3, "f1"}], live} = journal(Position, {:fill, 3, "f1"})
    assert live == %{qty: 3, fill_ids: ["f1"]}

    # The restore folds the recorded change against a fresh state and reaches
    # the same one, which is the whole contract a journal carries.
    assert Position.apply_change(Position.init_state(nil), {:record_fill, 3, "f1"}) == live
  end

  test "the block owns state_from_json" do
    error =
      refused("""
      use Flezha.SDK.Tasklet, name: "conflicting"
      state do
        field(:qty, :integer, default: 0)
      end
      def state_from_json(j), do: j
      def run(_i, s, _c), do: {:ok, s}
      """)

    assert error.description =~ "state_from_json/1" and error.description =~ "`state` block"
  end

  test "a hand-written apply_change does not compile in the use form" do
    block = "state do\n  field(:qty, :integer, default: 0)\nend"
    change = "defchange add(s, n) do\n  s + n\nend"

    by_hand = "def apply_change(s, _c), do: s"

    for {macro, extra, definition} <- [
          {~s|Tasklet, name: "by_hand"|, "", by_hand},
          {~s|Tasklet, name: "by_hand"|, block, by_hand},
          {~s|Tasklet, name: "by_hand"|, change, by_hand},
          {~s|Tasklet, name: "by_hand"|, "", "defp apply_change(s, _c), do: s"},
          {~s|Relay, factory_id: "by_hand"|, "", by_hand}
        ] do
      error =
        refused("""
        use Flezha.SDK.#{macro}
        #{extra}
        #{definition}
        def run(_i, s, _c), do: {:ok, s}
        """)

      assert error.description =~ "apply_change/2" and error.description =~ "defchange",
             "read: #{error.description}"
    end
  end

  # --- the §5 short form ----------------------------------------------------

  defmodule Upcase do
    @moduledoc false
    use Flezha.SDK.Relay, factory_id: "upcase", output: "out"

    def run(input, state, ctx) do
      emit(ctx, String.upcase(input))
      {:ok, state}
    end
  end

  defmodule UpcaseTwin do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "upcase_twin"

    def run(input, state, ctx) do
      emit(ctx, String.upcase(input))
      {:ok, state}
    end
  end

  defmodule UpcaseTwinRelay do
    @moduledoc false
    use Flezha.SDK.Relay, factory_id: "upcase_twin"

    @impl true
    def channels(c), do: c |> input("input") |> into(UpcaseTwin) |> output("out")
  end

  defmodule Place do
    @moduledoc false
    use Flezha.SDK.Relay,
      factory_id: "place",
      output: "placed",
      input: "order",
      codec: :text,
      run: :steps

    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule Chat do
    @moduledoc false
    use Flezha.SDK.Relay, factory_id: "chat", output: "replies", run: {:steps, each: :per_input}

    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule CappedShort do
    @moduledoc false
    use Flezha.SDK.Relay,
      factory_id: "capped_short",
      run: {:steps, each: :chained},
      durable: false,
      max_duration_ms: 5_000

    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule Counter do
    @moduledoc false
    use Flezha.SDK.Relay, factory_id: "counter", input: "ticks", max_cells: 10

    state do
      field(:seen, :list, default: [])
    end

    defchange seen(state, item) do
      %{state | seen: state.seen ++ [item]}
    end

    def run(input, state, ctx), do: {:ok, seen(state, ctx, input)}
  end

  test "a relay module with a run body is a whole flow relay" do
    assert Upcase.factory_id() == "upcase"
    assert function_exported?(Upcase, :channels, 1)

    assert {:ok, short} = Declaration.check(Upcase)
    assert {:ok, hand} = Declaration.check(UpcaseTwinRelay)
    assert entry_names(Upcase) == entry_names(UpcaseTwinRelay)
    assert codec_of(Upcase, "input") == "json"
    assert sink_names(short) == sink_names(hand)
  end

  test "a relay module with a run body is a whole steps relay" do
    assert Place.factory_id() == "place"
    assert function_exported?(Place, :channels, 1)

    assert {:ok, _derived} = Declaration.check(Place)
    assert Place.run() == :steps
    assert entry_names(Place) == ["order"]
    assert codec_of(Place, "order") == "text"
  end

  test "a short form that runs in steps declares its loop shape" do
    assert Chat.run() == {:steps, each: :per_input}
    assert {:ok, _derived} = Declaration.check(Chat)
    assert entry_names(Chat) == ["input"]
  end

  test "the generated tasklet is a module of its own, named after the factory id" do
    assert Upcase.Tasklet.name() == "upcase"
    assert Flezha.SDK.Tasklet in behaviours(Upcase.Tasklet)
    refute Flezha.SDK.Tasklet in behaviours(Upcase)
    refute function_exported?(Upcase, :name, 0)

    # `run/3` is the author's, reached through the tasklet module.
    assert {[], nil} = journal(Upcase.Tasklet, "hi")
  end

  test "a short form with no output wires its entry channel into the tasklet alone" do
    assert {:ok, derived} = Declaration.check(Counter)
    assert entry_names(Counter) == ["ticks"]
    assert sink_names(derived) == []

    # A tasklet option, and the `defchange` recorder, reach the generated tasklet.
    assert Counter.Tasklet.max_cells() == 10
    assert {[{:seen, "a"}], %{seen: ["a"]}} = journal(Counter.Tasklet, "a")
    assert Counter.Tasklet.apply_change(%{seen: []}, {:seen, "a"}) == %{seen: ["a"]}
  end

  test "register_factory reads how a short form runs" do
    runner = Flezha.SDK.create_runner("short_form_ns")

    assert %Flezha.SDK{modules: [Upcase]} = Flezha.SDK.register_factory(runner, Upcase)
    assert %Flezha.SDK{modules: [Place]} = Flezha.SDK.register_factory(runner, Place)
    assert Jason.decode!(class_json(Place))["run"] == "steps"
    refute Map.has_key?(Jason.decode!(class_json(Upcase)), "run")
  end

  test "register_factory and unregister_factory take a hand-written module of either run" do
    runner = Flezha.SDK.create_runner("hand_written_ns")

    declared =
      runner
      |> Flezha.SDK.register_factory(UpcaseTwinRelay)
      |> Flezha.SDK.register_factory(TunedSteps)

    assert Enum.map(declared.modules, & &1.factory_id()) == ["upcase_twin", "tuned_steps"]

    left = Flezha.SDK.unregister_factory(declared, TunedSteps)
    assert Enum.map(left.modules, & &1.factory_id()) == ["upcase_twin"]

    assert {:error, %Flezha.SDK.Error{code: "runtime.class_not_registered", details: details}} =
             Flezha.SDK.unregister_factory(left, TunedSteps)

    assert details == %{"factory" => "tuned_steps"}
  end

  # How it runs is the settings', not the body's: both runs write `channels/1`.
  defmodule HandWrittenSteps do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def run, do: :steps
    def factory_id, do: "hand_written_steps"
    def channels(c), do: c |> Channels.input("in") |> Channels.into(HandWritten)
  end

  # A body and no behaviour: nothing says it is a relay.
  defmodule NotARelay do
    @moduledoc false
    def factory_id, do: "not_a_relay"
    def channels(c), do: c |> Channels.input("in") |> Channels.into(HandWritten)
  end

  test "how a module runs is read from its settings" do
    runner = Flezha.SDK.create_runner("run_ns")

    assert %Flezha.SDK{modules: [HandWrittenSteps]} =
             Flezha.SDK.register_factory(runner, HandWrittenSteps)

    assert Jason.decode!(class_json(HandWrittenSteps))["run"] == "steps"
    refute Map.has_key?(Jason.decode!(class_json(HandWrittenRelay)), "run")
  end

  test "a module that is not a relay is refused at registration" do
    runner = Flezha.SDK.create_runner("not_a_relay_ns")

    assert {:error, %Flezha.SDK.Error{code: "topology.no_relay_exported", details: details}} =
             Flezha.SDK.register_factory(runner, NotARelay)

    assert details == %{"module" => inspect(NotARelay)}
  end

  test "a duplicate factory id is refused, however the class runs" do
    runner = Flezha.SDK.create_runner("duplicate_ns")

    assert {:error,
            %Flezha.SDK.Error{
              code: "topology.duplicate_factory_id",
              details: %{"factory_id" => "upcase"}
            }} =
             runner
             |> Flezha.SDK.register_factory(Upcase)
             |> Flezha.SDK.register_factory(Upcase)
  end

  test "a run body beside a topology body does not compile" do
    error =
      refused("""
      use Flezha.SDK.Relay, factory_id: "x", output: "out"
      def run(_i, s, _c), do: {:ok, s}
      def channels(c), do: c
      """)

    assert String.starts_with?(error.description, "topology.short_form_with_topology"),
           "expected the message to begin with the code, read: #{error.description}"
  end

  test "a relay with neither body does not compile, naming both" do
    error = refused(~s|use Flezha.SDK.Relay, factory_id: "x"|)
    assert error.description =~ "run/3" and error.description =~ "channels/1"
  end

  test "a relay option on a tasklet does not compile, naming the relay macro" do
    for option <- [~s|output: "out"|, ~s|input: "ticks"|] do
      error =
        refused("""
        use Flezha.SDK.Tasklet, name: "p", #{option}
        def run(_i, s, _c), do: {:ok, s}
        """)

      [key | _] = String.split(option, ":")

      assert error.description =~ "`#{key}:`" and
               error.description =~ "Flezha.SDK.Relay"
    end
  end

  test "a short form takes the class settings, and a full form no short-form option" do
    assert {:ok, _derived} = Declaration.check(CappedShort)

    assert %{
             "run" => %{"steps" => %{"each" => "chained"}},
             "max_duration_ms" => 5_000,
             "durable" => false
           } =
             Jason.decode!(class_json(CappedShort))

    for option <- [~s|output: "out"|, "max_cells: 10"] do
      stray =
        refused("""
        use Flezha.SDK.Relay, factory_id: "x", #{option}
        def channels(c), do: c
        """)

      [key | _] = String.split(option, ":")
      assert stray.description =~ "`#{key}:`"
    end
  end

  test "a short form runs" do
    run = start_local(Upcase)
    collect(run, "out")
    :ok = Local.feed(run, "input", ~s("hello"))
    :ok = Local.run_until_idle(run, 5_000)

    assert ["HELLO"] = Enum.map(drained(), & &1.payload)
  end

  # --- driving a tasklet through the ctx -----------------------------------

  # What one input recorded, and the state it left. `Ctx.take_changes/1` is the
  # runtime's own read of the journal, so the test reads it the way a commit
  # does rather than reaching into the process dictionary itself.
  defp journal(module, input) do
    ctx = %Ctx{module: module, ref: make_ref(), now_ms: 0, cell_key: nil}
    {:ok, state} = module.run(input, module.init_state(nil), ctx)
    {Ctx.take_changes(ctx), state}
  end

  defp class_json(module) do
    {:ok, class} = Declaration.build(module)
    class.json
  end

  defp codec_of(module, channel),
    do:
      Enum.find(Jason.decode!(class_json(module))["channels"], &(&1["name"] == channel))["codec"]

  defp behaviours(module),
    do: module.module_info(:attributes) |> Keyword.get_values(:behaviour) |> List.flatten()

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
end
