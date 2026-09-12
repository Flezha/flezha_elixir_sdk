# Twins of the rust_runner e2e fixtures (test/apps/rust_runner/src/main.rs)
# under the same factory ids, procedure names, channel names and output bytes;
# the persisted-state family is in factories_state.ex. Elixir SDK deviations,
# each noted on its fixture: no serializer-panic isolation.

# -- compare_filter ---------------------------------------------------------

defmodule ElixirRunner.CompareFilterProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "compare_filter_procedure"
  def init_state(_), do: %{comparable: "default", inverted: false}

  def run(input, %{comparable: comparable} = s, ctx) do
    if input == comparable, do: Ctx.emit(ctx, input)
    {:ok, s}
  end
end

defmodule ElixirRunner.CompareFilterOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "compare_filter"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.CompareFilterProc)
    |> named("sink")
    |> sink("compare_filter_sink")
  end
end

# -- static_entity / sugar_static_entity / mixed_operative ------------------

defmodule ElixirRunner.StaticEntryProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "static_entry_procedure"
  def init_state(_), do: %{seen: []}
  def entity_keys, do: ["a", "b", "c", "d"]
  def entities_for("a" <> _), do: ["a"]
  def entities_for("b" <> _), do: ["b"]
  def entities_for("c" <> _), do: ["c"]
  def entities_for(_), do: ["d"]

  def run(input, s, ctx) do
    Ctx.emit(ctx, input)
    {:ok, %{s | seen: [input | s.seen]}}
  end
end

defmodule ElixirRunner.StaticDownstreamProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "static_downstream_procedure"
  def init_state(_), do: nil

  def run(input, s, ctx),
    do:
      (
        Ctx.emit(ctx, input)
        {:ok, s}
      )
end

defmodule ElixirRunner.StaticEntityOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "static_entity"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.StaticEntryProc)
    |> named("hop")
    |> into(ElixirRunner.StaticDownstreamProc)
    |> named("kafka")
    |> sink("test_kafka")
  end
end

defmodule ElixirRunner.MacroStaticEntryProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "macro_static_entry_procedure"
  defdelegate init_state(key), to: ElixirRunner.StaticEntryProc
  defdelegate entity_keys, to: ElixirRunner.StaticEntryProc
  defdelegate entities_for(input), to: ElixirRunner.StaticEntryProc
  defdelegate run(input, state, ctx), to: ElixirRunner.StaticEntryProc
end

defmodule ElixirRunner.MacroStaticDownstreamProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "macro_static_downstream_procedure"
  defdelegate init_state(key), to: ElixirRunner.StaticDownstreamProc
  defdelegate run(input, state, ctx), to: ElixirRunner.StaticDownstreamProc
end

defmodule ElixirRunner.SugarStaticEntityOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "sugar_static_entity"

  # The Rust `operative!` pipeline leaves the hop unnamed and names the plugin
  # channel after the plugin.
  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.MacroStaticEntryProc)
    |> into(ElixirRunner.MacroStaticDownstreamProc)
    |> named("test_kafka")
    |> sink("test_kafka")
  end
end

defmodule ElixirRunner.MixedMacroEntryProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "mixed_macro_entry_procedure"
  def init_state(_), do: nil

  def run(input, s, ctx),
    do:
      (
        Ctx.emit(ctx, input)
        {:ok, s}
      )
end

defmodule ElixirRunner.MixedHandDownstreamProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "mixed_hand_downstream"
  def init_state(_), do: nil

  def run(input, s, ctx),
    do:
      (
        Ctx.emit(ctx, input)
        {:ok, s}
      )
end

defmodule ElixirRunner.MixedOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "mixed_operative"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.MixedMacroEntryProc)
    |> named("hop")
    |> into(ElixirRunner.MixedHandDownstreamProc)
    |> named("kafka")
    |> sink("test_kafka")
  end
end

# -- sugar_do_nothing / sugar_store_input -----------------------------------

defmodule ElixirRunner.SugarDoNothingProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "sugar_do_nothing"
  def init_state(_), do: nil

  def run(input, s, ctx),
    do:
      (
        Ctx.emit(ctx, input)
        {:ok, s}
      )
end

defmodule ElixirRunner.SugarDoNothingOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "sugar_do_nothing"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.SugarDoNothingProc)
    |> named("sugar_do_nothing_echo")
    |> sink("sugar_do_nothing_echo")
  end
end

defmodule ElixirRunner.SugarStoreInputProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "sugar_store_input"
  def init_state(_), do: %{text_inputs: []}

  def run(input, s, ctx) do
    Ctx.emit(ctx, input)
    {:ok, %{s | text_inputs: [input | s.text_inputs]}}
  end
end

defmodule ElixirRunner.SugarStoreInputOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "sugar_store_input"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.SugarStoreInputProc)
    |> named("sugar_store_sink")
    |> sink("sugar_store_sink")
  end
end

# -- hybrid_channel ---------------------------------------------------------

defmodule ElixirRunner.HybridProducerProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "hybrid_producer_procedure"
  def init_state(_), do: nil

  def run(input, s, ctx) do
    n =
      case Integer.parse(input) do
        {n, ""} when n >= 0 and n <= 0xFFFFFFFF -> n
        _ -> 0
      end

    Ctx.emit(ctx, n)
    {:ok, s}
  end
end

defmodule ElixirRunner.HybridConsumerProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "hybrid_consumer_procedure"
  def init_state(_), do: nil

  def run(input, s, _ctx) do
    IO.puts("hybrid_consumer_procedure: received #{input}")
    {:ok, s}
  end
end

defmodule ElixirRunner.HybridChannelOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "hybrid_channel"

  # The sunk handle fans out to the consumer as well. Unlike Rust, the SDK does
  # not isolate a serializer raise: the sentinel takes the Operative down
  # instead of being logged by the producer.
  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.HybridProducerProc)
    |> named("hybrid_indicator")
    |> sink_with("hybrid_indicators", &__MODULE__.serialize/1)
    |> into(ElixirRunner.HybridConsumerProc)
  end

  def serialize(0xDEADBEEF),
    do: raise("hybrid_indicators: serializer rejected sentinel 0xDEADBEEF")

  def serialize(v), do: <<v::unsigned-big-32>>
end

# -- poison_probe / wedge_probe ---------------------------------------------

defmodule ElixirRunner.PoisonProbeProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  @antidote {__MODULE__, :antidote}
  def name, do: "poison_probe_procedure"
  def init_state(_), do: nil

  def run(input, s, ctx) do
    if input == "antidote", do: :persistent_term.put(@antidote, true)

    if input == "poison" and not :persistent_term.get(@antidote, false) do
      {:error, "poison payload"}
    else
      Ctx.emit(ctx, "ok:" <> input)
      {:ok, s}
    end
  end
end

defmodule ElixirRunner.PoisonProbeOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "poison_probe"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> retry_backoff_ms(25)
    |> into(ElixirRunner.PoisonProbeProc)
    |> named("out")
    |> sink("probe_sink")
  end
end

defmodule ElixirRunner.WedgeProbeProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  @aborted {__MODULE__, :aborted}

  def name, do: "wedge_probe_procedure"
  def init_state(_), do: nil

  # Blocks the dispatch forever: the runner stays connected and heartbeating
  # while its dispatch counter freezes — the `runner_pool_wedged` signature.
  def run("wedge", _s, _ctx), do: Process.sleep(:infinity)

  # Ends the activation as violently as this host allows, WITHOUT emitting,
  # once per runner. On the BEAM that is a real worker death — the one host
  # where user code can take its Entity process down untrappably.
  def run("abort", s, ctx) do
    if :persistent_term.get(@aborted, false) do
      Ctx.emit(ctx, "ok:abort")
      {:ok, s}
    else
      :persistent_term.put(@aborted, true)
      Process.exit(self(), :kill)
    end
  end

  def run(input, s, ctx),
    do:
      (
        Ctx.emit(ctx, "ok:" <> input)
        {:ok, s}
      )
end

defmodule ElixirRunner.WedgeProbeOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "wedge_probe"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.WedgeProbeProc)
    |> named("out")
    |> sink("wedge_sink")
  end
end

# -- bounded_mailbox_{drop_nothing,drop_newest,drop_oldest} ------------------------

defmodule ElixirRunner.DropNothingTestProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "drop_nothing_test_procedure"
  def init_state(_), do: %{processed: 0}
  def mailbox_capacity, do: 4

  def run(input, s, ctx) do
    Ctx.emit(ctx, input)
    {:ok, %{s | processed: s.processed + 1}}
  end
end

defmodule ElixirRunner.DropNewestTestProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "drop_newest_test_procedure"
  defdelegate init_state(key), to: ElixirRunner.DropNothingTestProc
  defdelegate mailbox_capacity, to: ElixirRunner.DropNothingTestProc
  defdelegate run(input, state, ctx), to: ElixirRunner.DropNothingTestProc
end

defmodule ElixirRunner.DropOldestTestProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "drop_oldest_test_procedure"
  defdelegate init_state(key), to: ElixirRunner.DropNothingTestProc
  defdelegate mailbox_capacity, to: ElixirRunner.DropNothingTestProc
  defdelegate run(input, state, ctx), to: ElixirRunner.DropNothingTestProc
end

defmodule ElixirRunner.BoundedMailboxDropNothingOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "bounded_mailbox_drop_nothing"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("drop_nothing_input")
    |> on_full(:drop_nothing)
    |> into(ElixirRunner.DropNothingTestProc)
    |> named("drop_nothing_echo")
    |> sink("bounded_mailbox_echo")
  end
end

defmodule ElixirRunner.BoundedMailboxDropNewestOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "bounded_mailbox_drop_newest"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("drop_newest_input")
    |> on_full(:drop_newest)
    |> into(ElixirRunner.DropNewestTestProc)
    |> named("drop_newest_echo")
    |> sink("bounded_mailbox_echo")
  end
end

defmodule ElixirRunner.BoundedMailboxDropOldestOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "bounded_mailbox_drop_oldest"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("drop_oldest_input")
    |> on_full(:drop_oldest)
    |> into(ElixirRunner.DropOldestTestProc)
    |> named("drop_oldest_echo")
    |> sink("bounded_mailbox_echo")
  end
end

# -- partition_relay --------------------------------------------------------

defmodule ElixirRunner.PartitionRelayProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "partition_relay_procedure"
  def init_state(_), do: %{seen: []}
  def apply_change(s, {:push, input}), do: %{s | seen: s.seen ++ [input]}
  def state_from_json(%{"seen" => seen}), do: %{seen: seen}

  # The Entity is dynamic, keyed by the prefix, so a plain emit carries that
  # key — the reference's `emit_keyed(prefix, input)` with the same key by
  # construction.
  def run(input, s, ctx) do
    s = Ctx.apply(ctx, s, {:push, input})
    Ctx.emit(ctx, input)
    {:ok, s}
  end
end

defmodule ElixirRunner.PartitionRelayOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "partition_relay"

  # The plugin channel is named `input` because a forwarded frame keeps its
  # producing channel's name in metadata, and that is what selects the
  # receiving operative's entry channel — the downstream fixture's `input`.
  # The entry here is `in` so the two names cannot collide inside this class.
  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("in")
    |> key_by(fn v -> v |> to_string() |> String.split(":") |> hd() end)
    |> into(ElixirRunner.PartitionRelayProc)
    |> named("input")
    |> keyed()
    |> sink("relay_out")
  end
end

defmodule ElixirRunner.Factories do
  @moduledoc "The e2e factory operatives this standalone runner advertises."

  def all do
    [
      ElixirRunner.CompareFilterOp,
      ElixirRunner.StaticEntityOp,
      ElixirRunner.SugarStaticEntityOp,
      ElixirRunner.MixedOp,
      ElixirRunner.SugarDoNothingOp,
      ElixirRunner.SugarStoreInputOp,
      ElixirRunner.HybridChannelOp,
      ElixirRunner.PoisonProbeOp,
      ElixirRunner.WedgeProbeOp,
      ElixirRunner.BoundedMailboxDropNothingOp,
      ElixirRunner.BoundedMailboxDropNewestOp,
      ElixirRunner.BoundedMailboxDropOldestOp,
      ElixirRunner.PartitionRelayOp
    ] ++ ElixirRunner.FactoriesState.all()
  end
end
