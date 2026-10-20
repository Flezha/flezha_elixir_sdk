# Fixtures whose subject is persisted Cell state — twins of
# test/apps/rust_runner/src/state.rs and cell.rs under the same factory ids,
# tasklet names, channel names and output bytes: the durable-state reference,
# the counter and the coalescing counter, the persistence-off class under the
# reference's own tasklet names, and the resume probe. `bench_keyed`, the
# round-trip bench's durable twin, lives beside `bench_keyed_nopersist` in
# lib/mix/tasks/bench/elixir_factories.ex. `coalescing_counter` ships
# uncoalesced, as every veneer does: the fold is idempotent per `(interval, ts)`.
# `schema_evolution_v1` / `_v2` are the same class either side of a deploy that
# edits its own state type: both declare the tasklet `evolving_cart`, so an
# relay started under v1 and restarted under v2 restores from the very WAL
# v1 wrote. `session_timeout` is the worked timer shape: durable, so the e2e's
# re-seat half folds its pending timer back out of the change log, and
# `timer_cap` the core's settle-time refusal of a Cell's timers.
# `checkpoint_store` / `checkpoint_batched_store` are the tier's two
# checkpoint postures over one tasklet; `tier_store` and `bench_checkpoint`
# come from the bench factories the application registers beside these.

# -- store_input ------------------------------------------------------------

defmodule ElixirRunner.StoreInputTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "store_input_tasklet"

  @impl true
  def init_state(_), do: %{binary_inputs: [], text_inputs: []}

  defchange push_text(s, text) do
    %{s | text_inputs: s.text_inputs ++ [text]}
  end

  @impl true
  def state_from_json(%{"binary_inputs" => binaries, "text_inputs" => texts}),
    do: %{binary_inputs: binaries, text_inputs: texts}

  @impl true
  def run(input, s, ctx) do
    s = push_text(s, ctx, input)
    emit(ctx, input)
    {:ok, s}
  end
end

defmodule ElixirRunner.StoreInputOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "store_input"

  def channels(c) do
    import Flezha.SDK.ChannelBuilder

    c
    |> input("input", codec: :text)
    |> into(ElixirRunner.StoreInputTask)
    |> output("sink")
  end
end

# -- stateful_counter / persistence_off_counter -----------------------------

defmodule ElixirRunner.CounterEntryTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "counter_entry_tasklet"

  @impl true
  def run(input, s, ctx) do
    emit(ctx, ElixirRunner.Numbers.int(input))
    {:ok, s}
  end
end

defmodule ElixirRunner.CounterTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "counter_tasklet"

  @impl true
  def init_state(_), do: %{value: 0}

  defchange delta(s, delta) do
    %{s | value: s.value + delta}
  end

  @impl true
  def state_from_json(%{"value" => value}), do: %{value: value}
  @impl true
  def run(delta, s, ctx), do: {:ok, delta(s, ctx, delta)}
end

defmodule ElixirRunner.StatefulCounterOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "stateful_counter"

  def channels(c) do
    import Flezha.SDK.ChannelBuilder

    c
    |> input("input", codec: :text)
    |> into(ElixirRunner.CounterEntryTask)
    |> named("ticks")
    |> into(ElixirRunner.CounterTask)
  end
end

# The reference names the persistence-off class's tasklets after the class;
# the code is the counter's.
defmodule ElixirRunner.PersistenceOffEntryTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "persistence_off_entry_tasklet"
  @impl true
  defdelegate init_state(key), to: ElixirRunner.CounterEntryTask
  @impl true
  defdelegate run(input, state, ctx), to: ElixirRunner.CounterEntryTask
end

defmodule ElixirRunner.PersistenceOffCounterTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "persistence_off_counter_tasklet"
  @impl true
  defdelegate init_state(key), to: ElixirRunner.CounterTask
  @impl true
  defdelegate state_from_json(json), to: ElixirRunner.CounterTask

  defchange delta(s, delta) do
    %{s | value: s.value + delta}
  end

  @impl true
  def run(delta, s, ctx), do: {:ok, delta(s, ctx, delta)}
end

defmodule ElixirRunner.PersistenceOffCounterOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "persistence_off_counter"
  def durable, do: false

  def channels(c) do
    import Flezha.SDK.ChannelBuilder

    c
    |> input("input", codec: :text)
    |> into(ElixirRunner.PersistenceOffEntryTask)
    |> named("ticks")
    |> into(ElixirRunner.PersistenceOffCounterTask)
  end
end

# -- sleeping_counter -------------------------------------------------------

# A flow relay that sleeps when idle: `key:delta` inputs fold into one total per
# key, and every input emits `key=total` on the `totals` output.
defmodule ElixirRunner.SleepingCounterTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "sleeping_counter_tasklet"

  @impl true
  defdelegate init_state(key), to: ElixirRunner.CounterTask
  @impl true
  defdelegate state_from_json(json), to: ElixirRunner.CounterTask

  defchange delta(s, delta) do
    %{s | value: s.value + delta}
  end

  @impl true
  def run(input, s, ctx) do
    [key | rest] = input |> to_string() |> String.split(":", parts: 2)
    s = delta(s, ctx, ElixirRunner.Numbers.int(Enum.join(rest)))
    emit(ctx, "#{key}=#{s.value}")
    {:ok, s}
  end
end

defmodule ElixirRunner.SleepingCounterOp do
  @moduledoc false
  use Flezha.SDK.Relay, factory_id: "sleeping_counter"

  @impl true
  def durable, do: ElixirRunner.fixture_durable()

  @impl true
  def channels(c) do
    c
    |> input("input", codec: :text)
    |> key_by(&ElixirRunner.Key.prefix/1)
    |> into(ElixirRunner.SleepingCounterTask)
    |> output("totals")
  end
end

# -- coalescing_counter -----------------------------------------------------

defmodule ElixirRunner.TickEntryTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "tick_entry_tasklet"

  # Wire format: `<interval>:<ts>:<delta>`.
  @impl true
  def run(input, s, ctx) do
    [interval, ts, delta] =
      input |> String.split(":", parts: 3) |> Enum.concat(["0", "0", "0"]) |> Enum.take(3)

    emit(ctx, %{
      interval: ElixirRunner.Numbers.int(interval),
      ts: ElixirRunner.Numbers.int(ts),
      delta: ElixirRunner.Numbers.int(delta)
    })

    {:ok, s}
  end
end

defmodule ElixirRunner.TickTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "tick_tasklet"

  @impl true
  def init_state(_), do: %{last: 0}

  defchange tick(s, tick) do
    %{s | last: tick.delta}
  end

  @impl true
  def state_from_json(%{"last" => last}), do: %{last: last}
  @impl true
  def run(tick, s, ctx), do: {:ok, tick(s, ctx, tick)}
end

defmodule ElixirRunner.CoalescingCounterOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "coalescing_counter"

  def channels(c) do
    import Flezha.SDK.ChannelBuilder

    c
    |> input("input", codec: :text)
    |> into(ElixirRunner.TickEntryTask)
    |> named("ticks")
    |> into(ElixirRunner.TickTask)
  end
end

# -- resume_probe -----------------------------------------------------------

defmodule ElixirRunner.ResumeProbeTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "resume_probe_tasklet"

  @impl true
  def init_state(_), do: %{total: 0}

  defchange add(s, delta) do
    %{s | total: s.total + delta}
  end

  @impl true
  def state_from_json(%{"total" => total}), do: %{total: total}

  # Emits the running total, so a resume is observable on the wire.
  @impl true
  def run(input, s, ctx) do
    s = add(s, ctx, ElixirRunner.Numbers.int(input))
    emit(ctx, Integer.to_string(s.total))
    {:ok, s}
  end
end

defmodule ElixirRunner.ResumeProbeOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "resume_probe"

  def channels(c) do
    import Flezha.SDK.ChannelBuilder

    c
    |> input("input", codec: :text)
    |> into(ElixirRunner.ResumeProbeTask)
    |> output("total")
  end
end

# -- schema_evolution_v1 / schema_evolution_v2 -------------------------------

defmodule ElixirRunner.EvolvingCartV1Task do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "evolving_cart"

  @impl true
  def init_state(_), do: %{total: 0}

  defchange add(s, delta) do
    %{s | total: s.total + delta}
  end

  @impl true
  def state_from_json(%{"total" => total}), do: %{total: total}

  @impl true
  def run(input, s, ctx) do
    s = add(s, ctx, ElixirRunner.Numbers.int(input))
    emit(ctx, "#{s.total}:")
    {:ok, s}
  end
end

# v2: `currency` joined the state and a note joined the change, at schema
# version 2 reading down to 1 through the migrations; the output carries the
# migrated field so the e2e reads it off the wire.
defmodule ElixirRunner.EvolvingCartV2Task do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "evolving_cart", schema_version: 2, min_readable_schema_version: 1

  @impl true
  def init_state(_), do: %{total: 0, currency: ""}

  defchange add(s, delta, _note) do
    %{s | total: s.total + delta}
  end

  @impl true
  def state_from_json(%{"total" => total, "currency" => currency}),
    do: %{total: total, currency: currency}

  @impl true
  def migrate_state(1, %{total: total}), do: {:ok, %{total: total, currency: "EUR"}}
  def migrate_state(from, _old), do: {:error, "no migration from #{from}"}
  @impl true
  def migrate_change(1, {:add, delta}), do: {:ok, {:add, delta, "migrated"}}
  def migrate_change(from, _old), do: {:error, "no migration from #{from}"}

  @impl true
  def run(input, s, ctx) do
    s = add(s, ctx, ElixirRunner.Numbers.int(input), "live")
    currency = if s.currency == "", do: "EUR", else: s.currency
    emit(ctx, "#{s.total}:#{currency}")
    {:ok, s}
  end
end

defmodule ElixirRunner.SchemaEvolutionV1Op do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "schema_evolution_v1"

  def channels(c) do
    import Flezha.SDK.ChannelBuilder

    c
    |> input("input", codec: :text)
    |> into(ElixirRunner.EvolvingCartV1Task)
    |> output("total")
  end
end

defmodule ElixirRunner.SchemaEvolutionV2Op do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "schema_evolution_v2"

  def channels(c) do
    import Flezha.SDK.ChannelBuilder

    c
    |> input("input", codec: :text)
    |> into(ElixirRunner.EvolvingCartV2Task)
    |> output("total")
  end
end

# -- session_timeout ----------------------------------------------------------

# The worked session-timeout shape (`cell_timers`): each input pushes the
# idle deadline out, and the firing closes the session. Single-cell so the
# e2e's crash-resume half exercises a durable Cell. The timeout is read from
# `TIMER_TEST_TIMEOUT_MS` (the e2e sets it short).
defmodule ElixirRunner.SessionTimeoutTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "session_timeout_tasklet"

  @impl true
  def init_state(_), do: %{touched: [], closed_at: []}

  defchange touched(s, input) do
    %{s | touched: s.touched ++ [input]}
  end

  defchange closed(s, at) do
    %{s | closed_at: s.closed_at ++ [at]}
  end

  @impl true
  def state_from_json(%{"touched" => touched, "closed_at" => closed_at}),
    do: %{touched: touched, closed_at: closed_at}

  @impl true
  def run(input, s, ctx) do
    s = touched(s, ctx, input)
    # A processing-time timer, because the answer to "nothing has arrived"
    # cannot be an event-time one: nothing would advance the clock.
    {:ok, _at} = timer_after(ctx, session_timeout_ms())
    {:ok, s}
  end

  @impl true
  def on_timer(%{scheduled_at: fired_at}, s, ctx) do
    s = closed(s, ctx, fired_at)
    emit(ctx, "closed@#{fired_at}")
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
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "session_timeout"

  def channels(c) do
    import Flezha.SDK.ChannelBuilder

    c
    |> input("input", codec: :text)
    |> into(ElixirRunner.SessionTimeoutTask)
    |> output("kafka")
  end
end

# -- timer_cap ----------------------------------------------------------------

# The core's refusal of a Cell's timers when the item settles (`cell_timers`):
# past the cap of two pending timers, or an event-time timer under a tasklet
# whose clock is not event time. `event` asks for the event-time timer, a
# number `n` for a processing-time one `n` hours ahead; the change follows.
defmodule ElixirRunner.TimerCapTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "timer_cap_tasklet", max_timers_per_cell: 2

  @impl true
  def init_state(_), do: %{touched: []}

  defchange touched(s, input) do
    %{s | touched: s.touched ++ [input]}
  end

  @impl true
  def state_from_json(%{"touched" => touched}), do: %{touched: touched}

  @impl true
  def run("event", s, ctx) do
    {:ok, _at} = timer_at_event_time(ctx, 1)
    {:ok, touched(s, ctx, "event")}
  end

  def run(input, s, ctx) do
    {:ok, _at} = timer_after(ctx, String.to_integer(input) * 3_600_000)
    {:ok, touched(s, ctx, input)}
  end
end

defmodule ElixirRunner.TimerCapOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "timer_cap"

  def channels(c) do
    import Flezha.SDK.ChannelBuilder

    c
    |> input("input", codec: :text)
    |> into(ElixirRunner.TimerCapTask)
    |> output("kafka")
  end
end

# -- checkpoint_store / checkpoint_batched_store ------------------------------

# The same keyed upsert as `tier_store`, acknowledging only at an explicit
# checkpoint: a `commit:` input closes the interval and records nothing, every
# other input folds into it. `checkpoint_batched_store` adds
# `outputs: :at_checkpoint`, so the interval's outputs are held with its changes
# and ship inside its closing record; `every` is deliberately absent
# from both, so the e2e drives every close itself.
defmodule ElixirRunner.CheckpointStoreTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "checkpoint_store_tasklet"

  alias Flezha.Bench.TierStoreTask
  @impl true
  defdelegate init_state(key), to: TierStoreTask
  @impl true
  def state_from_json(json), do: json

  defchange set(values, key, value) do
    Map.put(values, key, value)
  end

  @impl true
  def run(input, s, ctx) do
    case TierStoreTask.split(input) do
      {"commit", _} ->
        checkpoint(ctx)
        {:ok, s}

      {key, value} ->
        TierStoreTask.upsert(key, value, s, ctx)
    end
  end
end

# -- tier_chain ---------------------------------------------------------------

# An effectively-once stream of two tasklets: `tier_pass` counts what it
# forwards and passes each `key:value` on; `tier_tally` counts each key's inputs
# and emits `key=count`. Until the release env is set the tally fails the
# value `hold` and keeps retrying it, so a runner killed there has made the
# pass's record durable and not the tally's.
defmodule ElixirRunner.TierPassTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "tier_pass"

  @impl true
  def init_state(_), do: 0

  defchange forwarded(count) do
    count + 1
  end

  @impl true
  def run(input, count, ctx) do
    count = forwarded(count, ctx)
    emit(ctx, input)
    {:ok, count}
  end
end

defmodule ElixirRunner.TierTallyTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "tier_tally"

  alias Flezha.Bench.TierStoreTask

  @impl true
  def init_state(_), do: 0

  defchange tallied(count) do
    count + 1
  end

  @impl true
  def run(input, count, ctx) do
    case TierStoreTask.split(input) do
      {key, "hold"} ->
        if System.get_env("FLEZHA_STEPS_E2E_RELEASE"),
          do: tally(key, count, ctx),
          else: {:error, "held until released"}

      {key, _value} ->
        tally(key, count, ctx)
    end
  end

  defp tally(key, count, ctx) do
    count = tallied(count, ctx)
    emit(ctx, "#{key}=#{count}", key: key)
    {:ok, count}
  end
end

defmodule ElixirRunner.TierChainOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "tier_chain"
  def run, do: {:flow, exactly: :effectively_once}

  def channels(c) do
    import Flezha.SDK.ChannelBuilder

    c
    |> input("input", codec: :text)
    |> on_full(:drop_nothing)
    |> into(ElixirRunner.TierPassTask)
    |> key_by(&(&1 |> Flezha.Bench.TierStoreTask.split() |> elem(0)))
    |> retry_max(1_000)
    |> into(ElixirRunner.TierTallyTask)
    |> keyed()
    |> output("sink")
  end
end

defmodule ElixirRunner.CheckpointStoreOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "checkpoint_store"
  def run, do: {:flow, exactly: {:effectively_once, ack: :checkpoint}}

  def channels(c) do
    import Flezha.SDK.ChannelBuilder

    c
    |> input("input", codec: :text)
    |> into(ElixirRunner.CheckpointStoreTask)
    |> keyed()
    |> output("sink")
  end
end

defmodule ElixirRunner.CheckpointBatchedOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "checkpoint_batched_store"

  def run,
    do:
      {:flow,
       exactly:
         {:effectively_once, ack: {:checkpoint, outputs: :at_checkpoint, max_delay_ms: 1_500}}}

  def channels(c) do
    import Flezha.SDK.ChannelBuilder

    c
    |> input("input", codec: :text)
    |> into(ElixirRunner.CheckpointStoreTask)
    |> keyed()
    |> output("sink")
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
defmodule ElixirRunner.CandleTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "candle_tasklet"

  # : Event times in fold order. The assertion surface.
  @impl true
  def init_state(_), do: %{folded: []}

  defchange fold(s, at) do
    %{s | folded: s.folded ++ [at]}
  end

  @impl true
  def state_from_json(%{"folded" => folded}), do: %{folded: folded}

  @impl true
  def event_time(input),
    do: input |> String.split(":", parts: 2) |> hd() |> ElixirRunner.Numbers.int()

  @impl true
  def run(input, s, ctx) do
    # `Ctx.now/1` IS the extracted event time under an event-time clock.
    at = now(ctx)
    s = fold(s, ctx, at)
    # One row per candle bucket, so a corrected replay overwrites by key rather
    # than appending a second row.
    bucket = div(at, 1_000)
    emit(ctx, "#{bucket}=#{input}", key: "candle/#{bucket}")
    {:ok, s}
  end
end

defmodule ElixirRunner.CandleOp do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "candle"
  def run, do: {:flow, exactly: {:effectively_once, correction_ms: 600_000}}

  def channels(c) do
    import Flezha.SDK.ChannelBuilder

    c
    |> input("input", codec: :text)
    |> into(ElixirRunner.CandleTask)
    |> keyed()
    |> output("sink")
  end
end

defmodule ElixirRunner.FactoriesState do
  @moduledoc "The persisted-state factory relays this standalone runner advertises."

  def all do
    [
      ElixirRunner.StoreInputOp,
      ElixirRunner.StatefulCounterOp,
      ElixirRunner.PersistenceOffCounterOp,
      ElixirRunner.SleepingCounterOp,
      ElixirRunner.CoalescingCounterOp,
      ElixirRunner.ResumeProbeOp,
      ElixirRunner.SchemaEvolutionV1Op,
      ElixirRunner.SchemaEvolutionV2Op,
      ElixirRunner.SessionTimeoutOp,
      ElixirRunner.TimerCapOp,
      ElixirRunner.TierChainOp,
      ElixirRunner.CheckpointStoreOp,
      ElixirRunner.CheckpointBatchedOp,
      ElixirRunner.CandleOp
    ]
  end
end
