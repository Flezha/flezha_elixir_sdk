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
    |> named("sink")
    |> sink("store_input_sink")
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
    |> named("total")
    |> sink("resume_probe_out")
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
    |> named("total")
    |> sink("schema_evolution_out")
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
    |> named("total")
    |> sink("schema_evolution_out")
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
    |> named("kafka")
    |> sink("test_kafka")
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
      ElixirRunner.SessionTimeoutOp
    ]
  end
end
