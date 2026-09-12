# Fixtures whose subject is persisted Entity state — twins of
# test/apps/rust_runner/src/state.rs and entity.rs under the same factory ids,
# procedure names, channel names and output bytes: the durable-state reference,
# the counter and the coalescing counter, the persistence-off class under the
# reference's own procedure names, and the resume probe. `bench_keyed`, the
# round-trip bench's durable twin, lives beside `bench_keyed_nopersist` in
# lib/mix/tasks/bench/elixir_factories.ex. `coalescing_counter` ships
# uncoalesced, as every veneer does: the fold is idempotent per `(interval, ts)`.
# `schema_evolution_v1` / `_v2` are the same class either side of a deploy that
# edits its own state type: both declare the procedure `evolving_cart`, so an
# operative started under v1 and restarted under v2 restores from the very WAL
# v1 wrote. `session_timeout` is the worked timer shape: durable, so the e2e's
# re-seat half folds its pending timer back out of the change log.
# `checkpoint_store` / `checkpoint_batched_store` are the tier's two
# checkpoint postures over one procedure; `tier_store` and `bench_checkpoint`
# come from the bench factories the application registers beside these.

# -- store_input ------------------------------------------------------------

defmodule ElixirRunner.StoreInputProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "store_input_procedure"
  def init_state(_), do: %{binary_inputs: [], text_inputs: []}
  def apply_change(s, {:push_text, text}), do: %{s | text_inputs: s.text_inputs ++ [text]}

  def state_from_json(%{"binary_inputs" => binaries, "text_inputs" => texts}),
    do: %{binary_inputs: binaries, text_inputs: texts}

  def run(input, s, ctx) do
    s = Ctx.apply(ctx, s, {:push_text, input})
    Ctx.emit(ctx, input)
    {:ok, s}
  end
end

defmodule ElixirRunner.StoreInputOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "store_input"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.StoreInputProc)
    |> sink("sink")
  end
end

# -- stateful_counter / persistence_off_counter -----------------------------

defmodule ElixirRunner.CounterEntryProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "counter_entry_procedure"
  def init_state(_), do: nil

  def run(input, s, ctx) do
    Ctx.emit(ctx, ElixirRunner.Numbers.int(input))
    {:ok, s}
  end
end

defmodule ElixirRunner.CounterProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "counter_procedure"
  def init_state(_), do: %{value: 0}
  def apply_change(s, {:delta, delta}), do: %{s | value: s.value + delta}
  def state_from_json(%{"value" => value}), do: %{value: value}
  def run(delta, s, ctx), do: {:ok, Ctx.apply(ctx, s, {:delta, delta})}
end

defmodule ElixirRunner.StatefulCounterOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "stateful_counter"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.CounterEntryProc)
    |> named("ticks")
    |> into(ElixirRunner.CounterProc)
  end
end

# The reference names the persistence-off class's procedures after the class;
# the code is the counter's.
defmodule ElixirRunner.PersistenceOffEntryProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "persistence_off_entry_procedure"
  defdelegate init_state(key), to: ElixirRunner.CounterEntryProc
  defdelegate run(input, state, ctx), to: ElixirRunner.CounterEntryProc
end

defmodule ElixirRunner.PersistenceOffCounterProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "persistence_off_counter_procedure"
  defdelegate init_state(key), to: ElixirRunner.CounterProc
  defdelegate apply_change(state, change), to: ElixirRunner.CounterProc
  defdelegate state_from_json(json), to: ElixirRunner.CounterProc
  defdelegate run(input, state, ctx), to: ElixirRunner.CounterProc
end

defmodule ElixirRunner.PersistenceOffCounterOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "persistence_off_counter"
  def persistence_enabled, do: false

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.PersistenceOffEntryProc)
    |> named("ticks")
    |> into(ElixirRunner.PersistenceOffCounterProc)
  end
end

# -- coalescing_counter -----------------------------------------------------

defmodule ElixirRunner.TickEntryProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "tick_entry_procedure"
  def init_state(_), do: nil

  # Wire format: `<interval>:<ts>:<delta>`.
  def run(input, s, ctx) do
    [interval, ts, delta] =
      input |> String.split(":", parts: 3) |> Enum.concat(["0", "0", "0"]) |> Enum.take(3)

    Ctx.emit(ctx, %{
      interval: ElixirRunner.Numbers.int(interval),
      ts: ElixirRunner.Numbers.int(ts),
      delta: ElixirRunner.Numbers.int(delta)
    })

    {:ok, s}
  end
end

defmodule ElixirRunner.TickProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "tick_procedure"
  def init_state(_), do: %{last: 0}
  def apply_change(s, %{delta: delta}), do: %{s | last: delta}
  def state_from_json(%{"last" => last}), do: %{last: last}
  def run(tick, s, ctx), do: {:ok, Ctx.apply(ctx, s, tick)}
end

defmodule ElixirRunner.CoalescingCounterOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "coalescing_counter"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.TickEntryProc)
    |> named("ticks")
    |> into(ElixirRunner.TickProc)
  end
end

# -- resume_probe -----------------------------------------------------------

defmodule ElixirRunner.ResumeProbeProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "resume_probe_procedure"
  def init_state(_), do: %{total: 0}
  def apply_change(s, {:add, delta}), do: %{s | total: s.total + delta}
  def state_from_json(%{"total" => total}), do: %{total: total}

  # Emits the running total, so a resume is observable on the wire.
  def run(input, s, ctx) do
    s = Ctx.apply(ctx, s, {:add, ElixirRunner.Numbers.int(input)})
    Ctx.emit(ctx, Integer.to_string(s.total))
    {:ok, s}
  end
end

defmodule ElixirRunner.ResumeProbeOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "resume_probe"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.ResumeProbeProc)
    |> sink("total")
  end
end

# -- schema_evolution_v1 / schema_evolution_v2 -------------------------------

defmodule ElixirRunner.EvolvingCartV1Proc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "evolving_cart"
  def init_state(_), do: %{total: 0}
  def apply_change(s, {:add, delta}), do: %{s | total: s.total + delta}
  def state_from_json(%{"total" => total}), do: %{total: total}

  def run(input, s, ctx) do
    s = Ctx.apply(ctx, s, {:add, ElixirRunner.Numbers.int(input)})
    Ctx.emit(ctx, "#{s.total}:")
    {:ok, s}
  end
end

# v2: `currency` joined the state and a note joined the change, at schema
# version 2 reading down to 1 through the migrations; the output carries the
# migrated field so the e2e reads it off the wire.
defmodule ElixirRunner.EvolvingCartV2Proc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "evolving_cart"
  def schema_version, do: 2
  def min_readable_schema_version, do: 1
  def init_state(_), do: %{total: 0, currency: ""}
  def apply_change(s, {:add, delta, _note}), do: %{s | total: s.total + delta}

  def state_from_json(%{"total" => total, "currency" => currency}),
    do: %{total: total, currency: currency}

  def migrate_state(1, %{total: total}), do: {:ok, %{total: total, currency: "EUR"}}
  def migrate_state(from, _old), do: {:error, "no migration from #{from}"}
  def migrate_change(1, {:add, delta}), do: {:ok, {:add, delta, "migrated"}}
  def migrate_change(from, _old), do: {:error, "no migration from #{from}"}

  def run(input, s, ctx) do
    s = Ctx.apply(ctx, s, {:add, ElixirRunner.Numbers.int(input), "live"})
    currency = if s.currency == "", do: "EUR", else: s.currency
    Ctx.emit(ctx, "#{s.total}:#{currency}")
    {:ok, s}
  end
end

defmodule ElixirRunner.SchemaEvolutionV1Op do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "schema_evolution_v1"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.EvolvingCartV1Proc)
    |> sink("total")
  end
end

defmodule ElixirRunner.SchemaEvolutionV2Op do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "schema_evolution_v2"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.EvolvingCartV2Proc)
    |> sink("total")
  end
end

# -- session_timeout ----------------------------------------------------------

# The worked session-timeout shape (`entity_timers`): each input pushes the
# idle deadline out, and the firing closes the session. Single-entity so the
# e2e's crash-resume half exercises a durable Entity. The timeout is read from
# `TIMER_TEST_TIMEOUT_MS` (the e2e sets it short).
defmodule ElixirRunner.SessionTimeoutProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "session_timeout_procedure"
  def init_state(_), do: %{touched: [], closed_at: []}
  def apply_change(s, {:touched, input}), do: %{s | touched: s.touched ++ [input]}
  def apply_change(s, {:closed, at}), do: %{s | closed_at: s.closed_at ++ [at]}

  def state_from_json(%{"touched" => touched, "closed_at" => closed_at}),
    do: %{touched: touched, closed_at: closed_at}

  def run(input, s, ctx) do
    s = Ctx.apply(ctx, s, {:touched, input})
    # A processing-time timer, because the answer to "nothing has arrived"
    # cannot be an event-time one: nothing would advance the clock.
    {:ok, _at} = Ctx.timer_after(ctx, session_timeout_ms())
    {:ok, s}
  end

  def on_timer(fired_at, _kind, s, ctx) do
    s = Ctx.apply(ctx, s, {:closed, fired_at})
    Ctx.emit(ctx, "closed@#{fired_at}")
    {:ok, s}
  end

  defp session_timeout_ms do
    case Integer.parse(System.get_env("TIMER_TEST_TIMEOUT_MS", "")) do
      {ms, ""} -> ms
      _ -> 1_000
    end
  end
end

defmodule ElixirRunner.SessionTimeoutOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "session_timeout"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(ElixirRunner.SessionTimeoutProc)
    |> sink("kafka")
  end
end

# -- checkpoint_store / checkpoint_batched_store ------------------------------

# The same keyed upsert as `tier_store`, acknowledging only at an explicit
# checkpoint: a `commit:` input closes the interval and records nothing, every
# other input folds into it. `checkpoint_batched_store` adds
# `outputs_at_checkpoint`, so the interval's outputs are held with its changes
# and ship inside its closing record; `checkpoint_every` is deliberately absent
# from both, so the e2e drives every close itself.
defmodule ElixirRunner.CheckpointStoreProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.Bench.TierStoreProc
  alias Flezha.SDK.Ctx

  def name, do: "checkpoint_store_procedure"
  defdelegate init_state(key), to: TierStoreProc
  defdelegate apply_change(state, change), to: TierStoreProc
  def state_from_json(json), do: json

  def run(input, s, ctx) do
    case TierStoreProc.split(input) do
      {"commit", _} ->
        Ctx.checkpoint(ctx)
        {:ok, s}

      {key, value} ->
        TierStoreProc.upsert(key, value, s, ctx)
    end
  end
end

defmodule ElixirRunner.CheckpointStoreOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "checkpoint_store"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> effectively_once()
    |> checkpoint_ack()
    |> input("input")
    |> into(ElixirRunner.CheckpointStoreProc)
    |> keyed()
    |> sink("sink")
  end
end

defmodule ElixirRunner.CheckpointBatchedOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "checkpoint_batched_store"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> effectively_once()
    |> checkpoint_ack()
    |> outputs_at_checkpoint(1_500)
    |> input("input")
    |> into(ElixirRunner.CheckpointStoreProc)
    |> keyed()
    |> sink("sink")
  end
end

defmodule ElixirRunner.Numbers do
  @moduledoc false

  # The reference's lenient parse: anything that is not an integer is 0.
  def int(text) do
    case Integer.parse(String.trim(text)) do
      {n, ""} -> n
      _ -> 0
    end
  end
end

# -- candle (stream-late-input-correction) ------------------------------------
#
# A trade stream keyed by an EVENT time carried in the input itself
# ("<event_time>:<price>"), with a declared correction window. State records the
# event times in the order they were folded, which is the whole point: a late
# trade must end up folded in event-time order, not arrival order, and the only
# way to prove that is to look at the order.
#
# Send 100, 300, then the late 200. Without correction the fold order is
# [100, 300, 200] and the candle is permanently wrong. With it, the engine
# rewinds behind 200, replays the retained input log sorted by event time, and
# the fold order becomes [100, 200, 300].
defmodule ElixirRunner.CandleProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "candle_procedure"
  def clock, do: :event_time

  # : Event times in fold order. The assertion surface.
  def init_state(_), do: %{folded: []}
  def apply_change(s, {:fold, at}), do: %{s | folded: s.folded ++ [at]}
  def state_from_json(%{"folded" => folded}), do: %{folded: folded}

  def event_time_of(input),
    do: input |> String.split(":", parts: 2) |> hd() |> ElixirRunner.Numbers.int()

  def run(input, s, ctx) do
    # `Ctx.now/1` IS the extracted event time under an event-time clock.
    at = Ctx.now(ctx)
    s = Ctx.apply(ctx, s, {:fold, at})
    # One row per candle bucket, so a corrected replay overwrites by key rather
    # than appending a second row.
    bucket = div(at, 1_000)
    Ctx.emit_keyed(ctx, "candle/#{bucket}", "#{bucket}=#{input}")
    {:ok, s}
  end
end

defmodule ElixirRunner.CandleOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "candle"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> effectively_once()
    |> correction_window(600_000)
    |> input("input")
    |> into(ElixirRunner.CandleProc)
    |> keyed()
    |> sink("sink")
  end
end

defmodule ElixirRunner.FactoriesState do
  @moduledoc "The persisted-state factory operatives this standalone runner advertises."

  def all do
    [
      ElixirRunner.StoreInputOp,
      ElixirRunner.StatefulCounterOp,
      ElixirRunner.PersistenceOffCounterOp,
      ElixirRunner.CoalescingCounterOp,
      ElixirRunner.ResumeProbeOp,
      ElixirRunner.SchemaEvolutionV1Op,
      ElixirRunner.SchemaEvolutionV2Op,
      ElixirRunner.SessionTimeoutOp,
      ElixirRunner.CheckpointStoreOp,
      ElixirRunner.CheckpointBatchedOp,
      ElixirRunner.CandleOp
    ]
  end
end
