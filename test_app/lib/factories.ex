# Twins of the rust_runner e2e fixtures (test/apps/rust_runner/src/main.rs)
# under the same factory ids, tasklet names, channel names and output bytes;
# the persisted-state family is in factories_state.ex. Elixir SDK deviations,
# each noted on its fixture: no serializer-panic isolation.

defmodule ElixirRunner.Key do
  @moduledoc false

  @doc "The text before the first `:`, the key every keyed string fixture routes on."
  def prefix(value), do: value |> to_string() |> String.split(":") |> hd()
end

# -- compare_filter ---------------------------------------------------------

defmodule ElixirRunner.CompareFilterTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "compare_filter_tasklet"

  @impl true
  def init_state(_), do: %{comparable: "default", inverted: false}

  @impl true
  def run(input, %{comparable: comparable} = s, ctx) do
    if input == comparable, do: emit(ctx, input)
    {:ok, s}
  end
end

defmodule ElixirRunner.CompareFilterOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "compare_filter"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.CompareFilterTask)
    |> output("sink")
  end
end

# -- static_cell / sugar_static_cell / mixed_relay ------------------

defmodule ElixirRunner.StaticEntryTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "static_entry_tasklet", cell_keys: ["a", "b", "c", "d"]

  @impl true
  def init_state(_), do: %{seen: []}
  @impl true
  def cells_for("a" <> _), do: ["a"]
  def cells_for("b" <> _), do: ["b"]
  def cells_for("c" <> _), do: ["c"]
  def cells_for(_), do: ["d"]

  @impl true
  def run(input, s, ctx) do
    emit(ctx, input)
    {:ok, %{s | seen: [input | s.seen]}}
  end
end

defmodule ElixirRunner.StaticDownstreamTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "static_downstream_tasklet"

  @impl true
  def run(input, s, ctx),
    do:
      (
        emit(ctx, input)
        {:ok, s}
      )
end

defmodule ElixirRunner.StaticCellOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "static_cell"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.StaticEntryTask)
    |> named("hop")
    |> into(ElixirRunner.StaticDownstreamTask)
    |> output("kafka")
  end
end

defmodule ElixirRunner.MacroStaticEntryTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "macro_static_entry_tasklet"
  @impl true
  defdelegate init_state(key), to: ElixirRunner.StaticEntryTask
  @impl true
  defdelegate cell_keys, to: ElixirRunner.StaticEntryTask
  @impl true
  defdelegate cells_for(input), to: ElixirRunner.StaticEntryTask
  @impl true
  defdelegate run(input, state, ctx), to: ElixirRunner.StaticEntryTask
end

defmodule ElixirRunner.MacroStaticDownstreamTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "macro_static_downstream_tasklet"
  @impl true
  defdelegate init_state(key), to: ElixirRunner.StaticDownstreamTask
  @impl true
  defdelegate run(input, state, ctx), to: ElixirRunner.StaticDownstreamTask
end

defmodule ElixirRunner.SugarStaticCellOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "sugar_static_cell"

  # The Rust `relay!` pipeline leaves the hop unnamed and lets the output
  # name the producing channel.
  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.MacroStaticEntryTask)
    |> into(ElixirRunner.MacroStaticDownstreamTask)
    |> output("test_kafka")
  end
end

defmodule ElixirRunner.MixedMacroEntryTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "mixed_macro_entry_tasklet"

  @impl true
  def run(input, s, ctx),
    do:
      (
        emit(ctx, input)
        {:ok, s}
      )
end

defmodule ElixirRunner.MixedHandDownstreamTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "mixed_hand_downstream"

  @impl true
  def run(input, s, ctx),
    do:
      (
        emit(ctx, input)
        {:ok, s}
      )
end

defmodule ElixirRunner.MixedOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "mixed_relay"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.MixedMacroEntryTask)
    |> named("hop")
    |> into(ElixirRunner.MixedHandDownstreamTask)
    |> output("kafka")
  end
end

# -- the §5 short forms -----------------------------------------------------
#
# One `use` each: a relay module whose `run/3` is its one tasklet's
# body, listed under `relays:` in `application.ex`. No hand-written
# relay stands beside them.

defmodule ElixirRunner.SugarDoNothing do
  @moduledoc false
  use Flezha.SDK.Relay,
    factory_id: "sugar_do_nothing",
    output: "sugar_do_nothing_echo"

  def run(input, state, ctx) do
    emit(ctx, input)
    {:ok, state}
  end
end

defmodule ElixirRunner.SugarStoreInput do
  @moduledoc false
  use Flezha.SDK.Relay,
    factory_id: "sugar_store_input",
    output: "sugar_store_sink"

  state do
    field(:text_inputs, :list, default: [])
  end

  defchange text_input(state, text) do
    %{state | text_inputs: state.text_inputs ++ [text]}
  end

  def run(input, state, ctx) do
    emit(ctx, input)
    {:ok, text_input(state, ctx, input)}
  end
end

defmodule ElixirRunner.SugarNamedInput do
  @moduledoc false
  use Flezha.SDK.Relay,
    factory_id: "sugar_named_input",
    output: "sugar_named_input_sink",
    input: "prices"

  def run(input, state, ctx) do
    emit(ctx, input)
    {:ok, state}
  end
end

defmodule ElixirRunner.SugarSteps do
  @moduledoc false
  use Flezha.SDK.Relay,
    factory_id: "sugar_steps",
    output: "sugar_steps_placed",
    input: "order",
    run: :steps

  @impl true
  def durable, do: ElixirRunner.fixture_durable()

  def run(input, state, ctx) do
    emit(ctx, input)
    {:ok, state}
  end
end

# A steps short form that loops: one pass per message on `input`, and the
# instance stays after each.
defmodule ElixirRunner.SugarStepsPerInput do
  @moduledoc false
  use Flezha.SDK.Relay,
    factory_id: "sugar_steps_per_input",
    output: "sugar_steps_per_input_out",
    run: {:steps, each: :per_input}

  @impl true
  def durable, do: ElixirRunner.fixture_durable()

  def run(input, state, ctx) do
    emit(ctx, input)
    {:ok, state}
  end
end

# The short form with no output: `ticks` feeds a tasklet that only keeps state.
defmodule ElixirRunner.SugarStateOnly do
  @moduledoc false
  use Flezha.SDK.Relay, factory_id: "sugar_state_only", input: "ticks"

  state do
    field(:seen, :list, default: [])
  end

  defchange seen(state, item) do
    %{state | seen: state.seen ++ [item]}
  end

  def run(input, state, ctx), do: {:ok, seen(state, ctx, input)}
end

# -- hybrid_channel ---------------------------------------------------------

defmodule ElixirRunner.HybridProducerTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "hybrid_producer_tasklet"

  @impl true
  def run(input, s, ctx) do
    n =
      case Integer.parse(input) do
        {n, ""} when n >= 0 and n <= 0xFFFFFFFF -> n
        _ -> 0
      end

    emit(ctx, n)
    {:ok, s}
  end
end

defmodule ElixirRunner.HybridConsumerTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "hybrid_consumer_tasklet"

  @impl true
  def run(input, s, _ctx) do
    IO.puts("hybrid_consumer_tasklet: received #{input}")
    {:ok, s}
  end
end

defmodule ElixirRunner.HybridChannelOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "hybrid_channel"

  # The sunk handle fans out to the consumer as well. Unlike Rust, the SDK does
  # not isolate a serializer raise: the sentinel takes the Relay down
  # instead of being logged by the producer.
  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.HybridProducerTask)
    |> output_with("hybrid_indicator", &__MODULE__.serialize/1)
    |> into(ElixirRunner.HybridConsumerTask)
  end

  def serialize(0xDEADBEEF),
    do: raise("hybrid_indicators: serializer rejected sentinel 0xDEADBEEF")

  def serialize(v), do: <<v::unsigned-big-32>>
end

# -- merged_input / transform_chain -----------------------------------------

defmodule ElixirRunner.MergeLabelTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "merge_label_tasklet"

  @impl true
  def run(%{source: source, payload: payload}, s, ctx) do
    emit(ctx, "#{source}:#{payload}")
    {:ok, s}
  end
end

defmodule ElixirRunner.MergedInputOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "merged_input"

  # Each source's mapping tags the value with the channel it came from, and the
  # merge keys on the symbol the mapping read.
  def channels(c) do
    import Flezha.SDK.Channels

    ticks = input(c, "ticks")
    quotes = input(c, "quotes")

    c
    |> merge()
    |> from(ticks, &tagged("ticks", &1))
    |> from(quotes, &tagged("quotes", &1))
    |> key_by(& &1.symbol)
    |> into(ElixirRunner.MergeLabelTask)
    |> keyed()
    |> output("merged")
  end

  defp tagged(source, payload), do: %{source: source, symbol: prefix(payload), payload: payload}

  @doc false
  # The text before the first `:`, which both stream-graph fixtures key on.
  def prefix(value), do: value |> String.split(":", parts: 2) |> hd()
end

defmodule ElixirRunner.TransformLabelTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "transform_label_tasklet"
  @impl true
  defdelegate init_state(key), to: ElixirRunner.StaticDownstreamTask
  @impl true
  defdelegate run(input, state, ctx), to: ElixirRunner.StaticDownstreamTask
end

defmodule ElixirRunner.TransformChainOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "transform_chain"

  # Every value transform on one edge: `drop` is filtered out, a comma list fans
  # out into one value per element, each is upper-cased and keyed on its prefix.
  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("trades")
    |> filter(&(&1 != "drop"))
    |> flat_map(&String.split(&1, ","))
    |> map(&String.upcase/1)
    |> key_by(&ElixirRunner.MergedInputOp.prefix/1)
    |> into(ElixirRunner.TransformLabelTask)
    |> keyed()
    |> output("transformed")
  end
end

# -- poison_probe / wedge_probe ---------------------------------------------

defmodule ElixirRunner.PoisonProbeTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "poison_probe_tasklet"

  @antidote {__MODULE__, :antidote}

  @impl true
  def run(input, s, ctx) do
    if input == "antidote", do: :persistent_term.put(@antidote, true)

    if input == "poison" and not :persistent_term.get(@antidote, false) do
      {:error, "poison payload"}
    else
      emit(ctx, "ok:" <> input)
      {:ok, s}
    end
  end
end

defmodule ElixirRunner.PoisonProbeOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "poison_probe"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> retry_backoff_ms(25)
    |> into(ElixirRunner.PoisonProbeTask)
    |> output("out")
  end
end

defmodule ElixirRunner.WedgeProbeTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "wedge_probe_tasklet"

  @aborted {__MODULE__, :aborted}

  # Blocks the dispatch forever: the runner stays connected and heartbeating
  # while its dispatch counter freezes — the `runner_pool_wedged` signature.
  @impl true
  def run("wedge", _s, _ctx), do: Process.sleep(:infinity)

  # Ends the activation as violently as this host allows, WITHOUT emitting,
  # once per runner. On the BEAM that is a real worker death — the one host
  # where user code can take its Cell process down untrappably.
  def run("abort", s, ctx) do
    if :persistent_term.get(@aborted, false) do
      emit(ctx, "ok:abort")
      {:ok, s}
    else
      :persistent_term.put(@aborted, true)
      Process.exit(self(), :kill)
    end
  end

  def run(input, s, ctx),
    do:
      (
        emit(ctx, "ok:" <> input)
        {:ok, s}
      )
end

defmodule ElixirRunner.WedgeProbeOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "wedge_probe"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.WedgeProbeTask)
    |> output("out")
  end
end

# -- bounded_mailbox_{drop_nothing,drop_newest,drop_oldest} ------------------------

defmodule ElixirRunner.DropNothingTestTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "drop_nothing_test_tasklet", mailbox_capacity: 4

  @impl true
  def init_state(_), do: %{processed: 0}

  @impl true
  def run(input, s, ctx) do
    emit(ctx, input)
    {:ok, %{s | processed: s.processed + 1}}
  end
end

defmodule ElixirRunner.DropNewestTestTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "drop_newest_test_tasklet"
  @impl true
  defdelegate init_state(key), to: ElixirRunner.DropNothingTestTask
  @impl true
  defdelegate mailbox_capacity, to: ElixirRunner.DropNothingTestTask
  @impl true
  defdelegate run(input, state, ctx), to: ElixirRunner.DropNothingTestTask
end

defmodule ElixirRunner.DropOldestTestTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "drop_oldest_test_tasklet"
  @impl true
  defdelegate init_state(key), to: ElixirRunner.DropNothingTestTask
  @impl true
  defdelegate mailbox_capacity, to: ElixirRunner.DropNothingTestTask
  @impl true
  defdelegate run(input, state, ctx), to: ElixirRunner.DropNothingTestTask
end

defmodule ElixirRunner.BoundedMailboxDropNothingOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "bounded_mailbox_drop_nothing"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("drop_nothing_input")
    |> on_full(:drop_nothing)
    |> into(ElixirRunner.DropNothingTestTask)
    |> output("drop_nothing_echo")
  end
end

defmodule ElixirRunner.BoundedMailboxDropNewestOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "bounded_mailbox_drop_newest"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("drop_newest_input")
    |> on_full(:drop_newest)
    |> into(ElixirRunner.DropNewestTestTask)
    |> output("drop_newest_echo")
  end
end

defmodule ElixirRunner.BoundedMailboxDropOldestOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "bounded_mailbox_drop_oldest"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("drop_oldest_input")
    |> on_full(:drop_oldest)
    |> into(ElixirRunner.DropOldestTestTask)
    |> output("drop_oldest_echo")
  end
end

# -- partition_pass --------------------------------------------------------

defmodule ElixirRunner.PartitionPassTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "partition_pass_tasklet"

  @impl true
  def init_state(_), do: %{seen: []}

  defchange push(s, input) do
    %{s | seen: s.seen ++ [input]}
  end

  @impl true
  def state_from_json(%{"seen" => seen}), do: %{seen: seen}

  # The Cell is dynamic, keyed by the prefix, so a plain emit carries that
  # key — the reference's `emit_keyed(prefix, input)` with the same key by
  # construction.
  @impl true
  def run(input, s, ctx) do
    s = push(s, ctx, input)
    emit(ctx, input)
    {:ok, s}
  end
end

defmodule ElixirRunner.PartitionPassOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "partition_pass"

  # The plugin channel is named `input` because a forwarded frame keeps its
  # producing channel's name in metadata, and that is what selects the
  # receiving relay's entry channel — the downstream fixture's `input`.
  # The entry here is `in` so the two names cannot collide inside this class.
  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("in")
    |> key_by(&ElixirRunner.Key.prefix/1)
    |> into(ElixirRunner.PartitionPassTask)
    |> keyed()
    |> output("input")
  end
end

# -- spawn_requester --------------------------------------------------------

defmodule ElixirRunner.SpawnRequesterTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "spawn_requester_tasklet"

  alias Flezha.SDK.Spawn

  @steps "stateless_loop_steps"

  # The results are printed because the runner's stdout is the only place the
  # scenario can read them.
  @impl true
  def run(input, s, ctx) do
    unnamed = spawn_steps(ctx, @steps, input)
    named = spawn_steps(ctx, @steps, input, id: input)
    report("unnamed", Spawn.await(unnamed))
    report("named", Spawn.await(named))
    {:ok, s}
  end

  @doc false
  def report(label, {:ok, %{id: id, outcome: outcome}}),
    do: IO.puts("spawn_result #{label} #{id} #{outcome}")

  def report(label, {:error, reason}),
    do: IO.puts("spawn_result #{label} error #{inspect(reason)}")
end

defmodule ElixirRunner.SpawnRequesterOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "spawn_requester"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("in")
    |> into(ElixirRunner.SpawnRequesterTask)
  end
end

# -- spawn_hold_requester ---------------------------------------------------

# An effectively-once stream whose tasklet spawns `stateless_loop_steps`
# with no id, prints the answer as `spawn_result hold <id> <outcome>`, then
# blocks until the release env is set, so a runner killed there dies with the
# spawn answered and its input not durably consumed.
defmodule ElixirRunner.SpawnHoldRequesterTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "spawn_hold_requester"

  alias Flezha.SDK.Spawn

  @impl true
  def run(input, s, ctx) do
    handle = spawn_steps(ctx, "stateless_loop_steps", input)
    ElixirRunner.SpawnRequesterTask.report("hold", Spawn.await(handle))
    ElixirRunner.Steps.Gate.block_until_released()
    {:ok, s}
  end
end

defmodule ElixirRunner.SpawnHoldRequesterOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "spawn_hold_requester"
  def run, do: {:flow, exactly: :effectively_once}

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("in")
    |> on_full(:drop_nothing)
    |> into(ElixirRunner.SpawnHoldRequesterTask)
    |> keyed()
    |> output("out")
  end
end

# -- spawn_once_requester ---------------------------------------------------

defmodule ElixirRunner.SpawnOnceRequesterTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "spawn_once_requester_tasklet"

  alias Flezha.SDK.Spawn

  # Spawns the `once` steps relay with the input as its JSON ticket and prints the
  # handle's answer; the spawned pass proves itself on stdout too.
  @impl true
  def run(input, s, ctx) do
    handle = spawn_steps(ctx, "typed_once_steps", %{"text" => input})
    ElixirRunner.SpawnRequesterTask.report("once", Spawn.await(handle))
    {:ok, s}
  end
end

defmodule ElixirRunner.SpawnOnceRequesterOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "spawn_once_requester"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("in")
    |> into(ElixirRunner.SpawnOnceRequesterTask)
  end
end

defmodule ElixirRunner.Factories do
  @moduledoc "The e2e factory relays this standalone runner advertises."

  def all do
    [
      ElixirRunner.CompareFilterOp,
      ElixirRunner.StaticCellOp,
      ElixirRunner.SugarStaticCellOp,
      ElixirRunner.MixedOp,
      ElixirRunner.HybridChannelOp,
      ElixirRunner.MergedInputOp,
      ElixirRunner.TransformChainOp,
      ElixirRunner.PoisonProbeOp,
      ElixirRunner.WedgeProbeOp,
      ElixirRunner.BoundedMailboxDropNothingOp,
      ElixirRunner.BoundedMailboxDropNewestOp,
      ElixirRunner.BoundedMailboxDropOldestOp,
      ElixirRunner.PartitionPassOp,
      ElixirRunner.SpawnRequesterOp,
      ElixirRunner.SpawnHoldRequesterOp,
      ElixirRunner.SpawnOnceRequesterOp
    ] ++ ElixirRunner.FactoriesState.all()
  end

  @doc """
  The §5 short forms this app advertises: each is a relay module whose
  `run/3` is its one tasklet's body.
  """
  def shorthand do
    [
      ElixirRunner.SugarDoNothing,
      ElixirRunner.SugarStoreInput,
      ElixirRunner.SugarNamedInput,
      ElixirRunner.SugarSteps,
      ElixirRunner.SugarStepsPerInput,
      ElixirRunner.SugarStateOnly
    ]
  end
end
