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

  import ExUnit.CaptureIO, only: [with_io: 2]

  alias Flezha.SDK.{Channels, Ctx, Local, Topology}

  # --- what `use` defines, and what it does not ---------------------------

  defmodule Written do
    @moduledoc false
    use Flezha.SDK.Procedure, name: "position", max_entities: 50_000

    def run(input, state, _ctx), do: {:ok, Map.put(state || %{}, :last, input)}
  end

  defmodule Stateless do
    @moduledoc false
    use Flezha.SDK.Procedure, name: "echo"

    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  test "a written option is a callback, an unwritten one is not" do
    assert Written.name() == "position"
    assert Written.max_entities() == 50_000

    refute function_exported?(Written, :mailbox_capacity, 0),
           "an option the module never wrote must leave the env knob deciding"

    for option <- [
          :entity_keys,
          :entity_idle_ttl_ms,
          :compact_after_bytes,
          :schema_version,
          :min_readable_schema_version,
          :clock,
          :max_timers_per_entity
        ] do
      refute function_exported?(Written, option, 0), "`#{option}` was never written"
    end
  end

  test "a stateless procedure needs no init_state" do
    assert Stateless.init_state(nil) == nil
    assert function_exported?(Stateless, :run, 3)
  end

  test "a misspelt option fails to compile, naming it" do
    error =
      refused("""
      use Flezha.SDK.Procedure, name: "p", max_entites: 10
      def run(_i, s, _c), do: {:ok, s}
      """)

    assert error.description =~ "max_entites"
  end

  # A nested procedure inherits the operative module's imports, so `use` has to
  # take `Channels.input/2` back before the procedure's own `input/0` is read.
  defmodule NestingOperative do
    @moduledoc false
    use Flezha.SDK.StreamOperative, factory_id: "nesting_operative"

    defmodule Nested do
      @moduledoc false
      use Flezha.SDK.Procedure, name: "nested"

      def input, do: "the nested procedure's own"
      def run(_input, state, _ctx), do: {:ok, state}
    end

    @impl true
    def channels(c) do
      c |> input("in") |> into(Nested) |> sink("out")
    end
  end

  test "a nested procedure's own input/0 does not clash with the builder's" do
    assert NestingOperative.Nested.input() == "the nested procedure's own"
    assert {:ok, topo} = Topology.build(NestingOperative)
    assert entry_names(NestingOperative) == ["in"]
    assert sink_names(topo) == ["out"]
  end

  # --- the behaviour form is unchanged -------------------------------------

  defmodule HandWritten do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure

    @impl true
    def name, do: "hand_written"
    @impl true
    def init_state(_key), do: nil
    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule HandWrittenOperative do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative

    @impl true
    def factory_id, do: "hand_written_operative"

    @impl true
    def channels(c) do
      c |> Channels.input("in") |> Channels.into(HandWritten) |> Channels.sink("out")
    end
  end

  test "the behaviour form registers exactly as before" do
    assert {:ok, topo} = Topology.build(HandWrittenOperative)
    assert topo.factory_id == "hand_written_operative"
    refute function_exported?(HandWritten, :mailbox_capacity, 0)
  end

  # --- the operative macros -------------------------------------------------

  defmodule TunedWorkflow do
    @moduledoc false
    use Flezha.SDK.WorkflowOperative,
      factory_id: "tuned_workflow",
      loop_mode: :chained,
      retry_attempts: 7

    @impl true
    def procedures(c) do
      c |> input("in") |> into(Stateless) |> sink("out")
    end
  end

  test "an operative macro defines only the knobs written" do
    assert TunedWorkflow.factory_id() == "tuned_workflow"
    assert TunedWorkflow.loop_mode() == :chained
    assert TunedWorkflow.retry_attempts() == 7
    refute function_exported?(TunedWorkflow, :wake_after_ms, 0)
    refute function_exported?(TunedWorkflow, :rollback_mode, 0)
  end

  test "an operative macro refuses an unknown option and a missing factory_id" do
    unknown =
      refused("""
      use Flezha.SDK.StreamOperative, factory_id: "x", persistance_enabled: false
      def channels(_c), do: :ok
      """)

    assert unknown.description =~ "persistance_enabled"

    missing =
      refused("""
      use Flezha.SDK.StreamOperative, description: "no id"
      def channels(_c), do: :ok
      """)

    assert missing.description =~ "factory_id"
  end

  # --- a merge pipe needs one import ----------------------------------------

  defmodule MergedByImport do
    @moduledoc false
    use Flezha.SDK.StreamOperative, factory_id: "merged_by_import"

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
      |> sink("merged")
    end
  end

  defmodule MergedByPrefix do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative

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
      |> Channels.sink("merged")
    end
  end

  test "a merge pipe written with one import registers the topology the prefixed form did" do
    assert {:ok, imported} = Topology.build(MergedByImport)
    assert {:ok, prefixed} = Topology.build(MergedByPrefix)

    assert entry_names(MergedByImport) == entry_names(MergedByPrefix)
    assert sink_names(imported) == sink_names(prefixed)
    assert map_size(imported.node_seq) == map_size(prefixed.node_seq)
    assert route_shape(imported) == route_shape(prefixed)
  end

  # --- reading a built topology --------------------------------------------
  #
  # The compiled topology keeps its entries in a map, so the builder's own
  # record is what states their order — the same read `conformance.ex` makes.

  defp entry_names(module, declare \\ :channels) do
    c = Channels.new()
    _ = apply(module, declare, [c])
    c |> Channels.take() |> Map.fetch!(:entry_order) |> Enum.uniq()
  end

  defp sink_names(topology) do
    for {_source, routes} <- topology.routes,
        %{to: {:sink, plugin}} <- routes,
        do: plugin
  end

  # Each source's links by what they reach, with the closures dropped: two
  # declarations of one graph carry different closure identities and the same
  # shape.
  defp route_shape(topology) do
    topology.routes
    |> Enum.map(fn {_source, routes} ->
      Enum.map(routes, fn route ->
        {elem(route.to, 0), Map.get(route, :channel), is_function(route.key_by, 1)}
      end)
    end)
    |> Enum.sort()
  end

  # --- the `state` block and `defchange` -----------------------------------

  defmodule Position do
    @moduledoc false
    use Flezha.SDK.Procedure, name: "position_state"

    state do
      field(:qty, :integer, default: 0)
      field(:fill_ids, :list, default: [])
    end

    defchange record_fill(state, qty, id) do
      %{state | qty: state.qty + qty, fill_ids: state.fill_ids ++ [id]}
    end

    def run({:set, qty}, state, ctx), do: {:ok, set_qty(state, ctx, qty)}
    def run({:push, id}, state, ctx), do: {:ok, push_fill_ids(state, ctx, id)}
    def run({:fill, qty, id}, state, ctx), do: {:ok, record_fill(state, ctx, qty, id)}
  end

  test "the block generates the defaults, the JSON read and the fold" do
    assert Position.init_state(nil) == %{qty: 0, fill_ids: []}
    assert Position.state_from_json(%{"qty" => 7}) == %{qty: 7, fill_ids: []}
    assert Position.state_from_json(%{}) == %{qty: 0, fill_ids: []}

    assert Position.apply_change(%{qty: 0, fill_ids: []}, {:set, :qty, 5}) ==
             %{qty: 5, fill_ids: []}

    assert Position.apply_change(%{qty: 0, fill_ids: ["a"]}, {:push, :fill_ids, "b"}) ==
             %{qty: 0, fill_ids: ["a", "b"]}
  end

  test "a field recorder journals one change, and a restore folds it back" do
    assert {[{:set, :qty, 5}], %{qty: 5, fill_ids: []}} = journal(Position, {:set, 5})
    assert {[{:push, :fill_ids, "f1"}], %{fill_ids: ["f1"]}} = journal(Position, {:push, "f1"})
  end

  test "a named change is one record for several fields" do
    assert {[{:record_fill, 3, "f1"}], live} = journal(Position, {:fill, 3, "f1"})
    assert live == %{qty: 3, fill_ids: ["f1"]}

    # The restore folds the recorded change against a fresh state and reaches
    # the same one, which is the whole contract a journal carries.
    assert Position.apply_change(Position.init_state(nil), {:record_fill, 3, "f1"}) == live
  end

  test "the block owns apply_change and state_from_json" do
    for callback <- ["def apply_change(state, _c), do: state", "def state_from_json(j), do: j"] do
      error =
        refused("""
        use Flezha.SDK.Procedure, name: "conflicting"
        state do
          field(:qty, :integer, default: 0)
        end
        #{callback}
        def run(_i, s, _c), do: {:ok, s}
        """)

      assert error.description =~ "state" and error.description =~ "block"
    end
  end

  # --- the §5 short form ----------------------------------------------------

  defmodule Upcase do
    @moduledoc false
    use Flezha.SDK.Procedure, name: "upcase", kind: :stream, sink: "out"

    def run(input, state, ctx) do
      emit(ctx, String.upcase(input))
      {:ok, state}
    end
  end

  defmodule UpcaseTwin do
    @moduledoc false
    use Flezha.SDK.Procedure, name: "upcase_twin"

    def run(input, state, ctx) do
      emit(ctx, String.upcase(input))
      {:ok, state}
    end
  end

  defmodule UpcaseTwinOperative do
    @moduledoc false
    use Flezha.SDK.StreamOperative, factory_id: "upcase_twin"

    @impl true
    def channels(c), do: c |> input("input") |> into(UpcaseTwin) |> sink("out")
  end

  defmodule Place do
    @moduledoc false
    use Flezha.SDK.Procedure, name: "place", kind: :workflow, sink: "placed", input: "order"

    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  test "a procedure module is a whole stream operative" do
    assert Upcase.factory_id() == "upcase"
    assert function_exported?(Upcase, :channels, 1)
    refute function_exported?(Upcase, :procedures, 1)

    assert {:ok, short} = Topology.build(Upcase)
    assert {:ok, hand} = Topology.build(UpcaseTwinOperative)
    assert entry_names(Upcase) == entry_names(UpcaseTwinOperative)
    assert sink_names(short) == sink_names(hand)
    assert route_shape(short) == route_shape(hand)
  end

  test "a procedure module is a whole workflow operative" do
    assert Place.factory_id() == "place"
    assert function_exported?(Place, :procedures, 1)
    refute function_exported?(Place, :channels, 1)

    assert {:ok, spec} = Flezha.SDK.Workflow.Spec.build(Place)
    assert spec.knobs.loop_mode == :once
    assert entry_names(Place, :procedures) == ["order"]
  end

  test "register_factory reads a short form's kind" do
    runner = Flezha.SDK.create_runner("short_form_ns")

    assert %Flezha.SDK{classes: [{Upcase, stream}]} = Flezha.SDK.register_factory(runner, Upcase)
    assert stream.factory_id == "upcase"

    assert %Flezha.SDK{classes: [{Place, workflow}]} = Flezha.SDK.register_factory(runner, Place)
    assert workflow.factory_id == "place"
  end

  test "register_factory and unregister_factory take a hand-written module of either kind" do
    runner = Flezha.SDK.create_runner("hand_written_ns")

    declared =
      runner
      |> Flezha.SDK.register_factory(UpcaseTwinOperative)
      |> Flezha.SDK.register_factory(TunedWorkflow)

    assert Enum.map(declared.classes, fn {_m, c} -> c.factory_id end) ==
             ["upcase_twin", "tuned_workflow"]

    left = Flezha.SDK.unregister_factory(declared, TunedWorkflow)
    assert Enum.map(left.classes, fn {_m, c} -> c.factory_id end) == ["upcase_twin"]

    assert {:error, {:not_defined, "tuned_workflow"}} =
             Flezha.SDK.unregister_factory(left, TunedWorkflow)
  end

  # Declared without `@behaviour`, because the kind is read from the callbacks a
  # module exports, not from what it says it implements — and naming both
  # behaviours here would only add the compiler's own conflict warnings to a
  # shape this test is about refusing.
  defmodule BothKinds do
    @moduledoc false
    def factory_id, do: "both_kinds"
    def channels(_c), do: :ok
    def procedures(_c), do: :ok
  end

  defmodule NeitherKind do
    @moduledoc false
    def factory_id, do: "neither_kind"
  end

  test "an operative of both kinds, or of neither, is refused at registration" do
    runner = Flezha.SDK.create_runner("kind_refusals_ns")

    assert {:error, {:AmbiguousOperativeKind, BothKinds}} =
             Flezha.SDK.register_factory(runner, BothKinds)

    assert {:error, {:UnknownOperativeKind, NeitherKind}} =
             Flezha.SDK.register_factory(runner, NeitherKind)
  end

  test "a duplicate factory id is refused, whichever kind holds it" do
    runner = Flezha.SDK.create_runner("duplicate_ns")

    assert {:error, {:duplicate_factory_id, "upcase"}} =
             runner
             |> Flezha.SDK.register_factory(Upcase)
             |> Flezha.SDK.register_factory(Upcase)
  end

  test "the short form's three refusals are compile errors naming themselves" do
    cases = [
      {"SinkWithoutId", ~s|name: "", kind: :stream, sink: "out"|},
      {"SinkWithoutKind", ~s|name: "upcase", sink: "out"|},
      {"ShorthandKeyWithoutSink", ~s|name: "upcase", kind: :stream, input: "prices"|}
    ]

    for {refusal, options} <- cases do
      error =
        refused("""
        use Flezha.SDK.Procedure, #{options}
        def run(_i, s, _c), do: {:ok, s}
        """)

      assert String.starts_with?(error.description, refusal),
             "expected the message to begin with #{refusal}, read: #{error.description}"
    end
  end

  test "both short-form kinds run" do
    run = start_run(Upcase)
    collect(run, "out")
    :ok = Local.feed(run, "input", "hello")
    :ok = Local.run_until_idle(run, 5_000)

    assert ["HELLO"] = Enum.map(drained(), & &1.payload)
  end

  # --- driving a procedure through the ctx -----------------------------------

  # The `CompileError` a module body earns, compiled under a name of its own so
  # one test can refuse several bodies.
  defp refused(body) do
    assert_raise CompileError, fn ->
      Code.compile_string("""
      defmodule Flezha.SDK.AuthoringMacrosTest.Refused#{System.unique_integer([:positive])} do
      #{body}
      end
      """)
    end
  end

  # What one input recorded, and the state it left. `Ctx.take_changes/1` is the
  # runtime's own read of the journal, so the test reads it the way a commit
  # does rather than reaching into the process dictionary itself.
  defp journal(module, input) do
    ctx = %Ctx{module: module, ref: make_ref(), now_ms: 0, entity_key: nil}
    {:ok, state} = module.run(input, module.init_state(nil), ctx)
    {Ctx.take_changes(ctx), state}
  end

  defp start_run(module) do
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
end
