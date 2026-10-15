# The Elixir bench factories, mirroring test/apps/rust_runner/src/main.rs
# under the same factory ids and topologies so `mix bench --runtime elixir`
# measures the same shapes:
#   do_nothing     — regression: input → echo → output
#   cell_stress  — cell:     A(8 keys) → B → C → D → output (fan-out then fan-in)
#   dynamic_cell — dynamic:    filter + key_by(prefix) → keyed Cell → output
# plus the `mix bench.roundtrip` twins:
#   bench_echo            — do_nothing with a declared Block on the entry mailbox
#   bench_keyed           — "key:value" upsert, emits "key=value" (Single Cell), persisted
#   bench_keyed_nopersist — the same upsert with the class's persistence off
#   tier_store            — the same upsert under the effectively-once tier
#   bench_checkpoint      — the tier acknowledging on a declared input cadence
# The controller's in-process `--transport native` bench and its in-node tests
# reach the same modules through a path dependency on this app.

defmodule Flezha.Bench.EchoTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "do_nothing_tasklet"

  @impl true
  def run(input, s, ctx),
    do:
      (
        emit(ctx, input)
        {:ok, s}
      )
end

defmodule Flezha.Bench.DoNothing do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "do_nothing"

  def channels(c) do
    import Flezha.SDK.Channels
    c |> input("input") |> into(Flezha.Bench.EchoTask) |> output("echo")
  end
end

defmodule Flezha.Bench.StressA do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "stress_a"

  @impl true
  def run(input, s, ctx),
    do:
      (
        emit(ctx, input)
        {:ok, s}
      )

  # The shared 8-key static set (Rust's `STRESS_KEYS`), every key routed; B/C/D
  # declare the same keyset and narrow the routing to `p0`.
  @impl true
  def cell_keys, do: ~w(p0 p1 p2 p3 p4 p5 p6 p7)
  @impl true
  def cells_for(_), do: cell_keys()
end

defmodule Flezha.Bench.StressB do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "stress_b"

  @impl true
  def run(input, s, ctx),
    do:
      (
        emit(ctx, input)
        {:ok, s}
      )

  @impl true
  defdelegate cell_keys, to: Flezha.Bench.StressA
  @impl true
  def cells_for(_), do: ["p0"]
end

defmodule Flezha.Bench.StressC do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "stress_c"

  @impl true
  def run(input, s, ctx),
    do:
      (
        emit(ctx, input)
        {:ok, s}
      )

  @impl true
  defdelegate cell_keys, to: Flezha.Bench.StressA
  @impl true
  def cells_for(_), do: ["p0"]
end

defmodule Flezha.Bench.StressD do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "stress_d"

  @impl true
  def run(input, s, ctx),
    do:
      (
        emit(ctx, input)
        {:ok, s}
      )

  @impl true
  defdelegate cell_keys, to: Flezha.Bench.StressA
  @impl true
  def cells_for(_), do: ["p0"]
end

defmodule Flezha.Bench.CellStress do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "cell_stress"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(Flezha.Bench.StressA)
    |> named("a_to_b")
    |> into(Flezha.Bench.StressB)
    |> named("b_to_c")
    |> into(Flezha.Bench.StressC)
    |> named("c_to_d")
    |> into(Flezha.Bench.StressD)
    |> output("echo")
  end
end

defmodule Flezha.Bench.DynamicTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "dynamic_cell_tasklet"

  @impl true
  def init_state(_), do: %{seen: []}

  defchange push(s, input) do
    %{s | seen: s.seen ++ [input]}
  end

  @impl true
  def state_from_json(%{"seen" => seen}), do: %{seen: seen}

  # The e2e sets the idle TTL short, in milliseconds, which round up to whole
  # seconds; unset, the Cells live for the relay's lifetime, as the
  # reference's do.
  @impl true
  def idle_ttl_seconds do
    case System.get_env("DYN_TEST_TTL_MS") do
      nil -> nil
      ms -> ms |> String.to_integer() |> Kernel.+(999) |> div(1_000)
    end
  end

  @impl true
  def run(input, s, ctx) do
    s = push(s, ctx, input)
    emit(ctx, input)
    {:ok, s}
  end
end

defmodule Flezha.Bench.DynamicCell do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "dynamic_cell"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> on_full(:drop_nothing)
    |> filter(fn s -> s != "drop" end)
    |> key_by(&ElixirRunner.Key.prefix/1)
    |> into(Flezha.Bench.DynamicTask)
    |> output("kafka")
  end
end

defmodule Flezha.Bench.BenchEcho do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "bench_echo"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> on_full(:drop_nothing)
    |> into(Flezha.Bench.EchoTask)
    |> output("echo")
  end
end

defmodule Flezha.Bench.TierStoreTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "tier_store_tasklet"

  @impl true
  def init_state(_), do: %{}

  defchange set(values, key, value) do
    Map.put(values, key, value)
  end

  # Emits `key=value` under `key`, as the reference does: the bench outputs are
  # keyed, so the durable path pays for its stamps here as it does there.
  @impl true
  def run(input, values, ctx) do
    case split(input) do
      {_key, "poison"} -> {:error, "poison value"}
      {key, value} -> upsert(key, value, values, ctx)
    end
  end

  # The lenient `key:value` split and the upsert, shared with the checkpoint
  # postures over this same store.
  def split(input) do
    case String.split(input, ":", parts: 2) do
      [key, value] -> {key, value}
      [key] -> {key, ""}
    end
  end

  def upsert(key, value, values, ctx) do
    values = set(values, ctx, key, value)
    emit_keyed(ctx, key, "#{key}=#{value}")
    {:ok, values}
  end
end

defmodule Flezha.Bench.BenchKeyed do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "bench_keyed"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> on_full(:drop_nothing)
    |> into(Flezha.Bench.TierStoreTask)
    |> keyed()
    |> output("sink")
  end
end

defmodule Flezha.Bench.KeyedNoPersist do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "bench_keyed_nopersist"

  # The same upsert with the class's persistence off — the heap-only posture
  # the bench isolates against the durable one.
  def durable, do: false
  defdelegate channels(c), to: Flezha.Bench.BenchKeyed
end

defmodule Flezha.Bench.TierStore do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "tier_store"
  def run, do: {:flow, exactly: :effectively_once}

  # `bench_keyed` under the tier: the durable posture the round-trip bench
  # measures with `--durability on`, and the conformance fixture the tier's
  # crash and dead-letter scenarios drive. The 300 ms backoff base keeps the
  # min-watermark freeze observable while the ladder runs; DropNothing because
  # a durable-input-log relay that shed at its mailbox would hold the
  # min-watermark on the shed offset for ever.
  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> on_full(:drop_nothing)
    |> retry_backoff_ms(300)
    |> into(Flezha.Bench.TierStoreTask)
    |> keyed()
    |> output("sink")
  end
end

defmodule Flezha.Bench.RemoteTierStore do
  @moduledoc false
  @behaviour Flezha.SDK.Relay
  def factory_id, do: "remote_tier_store"
  def run, do: {:flow, exactly: :effectively_once}

  # `tier_store` kept `durable: remote`: an input is acknowledged only once the
  # durability backend holds the records it produced.
  def durable, do: :remote
  defdelegate channels(c), to: Flezha.Bench.TierStore
end

defmodule Flezha.Bench.BenchCheckpointTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "bench_checkpoint_tasklet"

  # The same upsert under its own tasklet name, so the declaration a
  # checkpoint class registers is distinguishable from `tier_store`'s.
  @impl true
  defdelegate init_state(key), to: Flezha.Bench.TierStoreTask

  defchange set(values, key, value) do
    Map.put(values, key, value)
  end

  @impl true
  defdelegate run(input, state, ctx), to: Flezha.Bench.TierStoreTask
end

defmodule Flezha.Bench.BenchCheckpoint do
  @moduledoc false

  alias Flezha.SDK.Knob

  @behaviour Flezha.SDK.Relay
  def factory_id, do: "bench_checkpoint"

  # `tier_store` driven by a declared cadence instead of a `commit:` input:
  # the amortised posture, acknowledging every FLEZHA_BENCH_CHECKPOINT_EVERY
  # inputs per Cell. FLEZHA_BENCH_CHECKPOINT_OUTPUTS=at_checkpoint
  # additionally holds the interval's outputs until it closes.
  def run do
    every = [every: env("FLEZHA_BENCH_CHECKPOINT_EVERY", :positive_int, 500)]

    held =
      if System.get_env("FLEZHA_BENCH_CHECKPOINT_OUTPUTS") == "at_checkpoint",
        do: [
          outputs: :at_checkpoint,
          max_delay_ms: env("FLEZHA_BENCH_CHECKPOINT_DELAY_MS", :ms, 50)
        ],
        else: []

    {:flow, exactly: {:effectively_once, ack: {:checkpoint, every ++ held}}}
  end

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> on_full(:drop_nothing)
    |> into(Flezha.Bench.BenchCheckpointTask)
    |> keyed()
    |> output("sink")
  end

  # The bench harness hands a spawned runner these variables; the defaults are
  # the harness's own, the ones `Flezha.Config.Keys` declares.
  defp env(var, type, default) do
    with raw when is_binary(raw) <- System.get_env(var),
         {:ok, value} <- Knob.parse(type, raw) do
      value
    else
      _ -> default
    end
  end
end

defmodule ElixirRunner.BenchFactories do
  @moduledoc "Registry of the Elixir bench factory relays (see this file's header)."

  @doc "Every bench factory relay module."
  def all do
    [
      Flezha.Bench.DoNothing,
      Flezha.Bench.CellStress,
      Flezha.Bench.DynamicCell,
      Flezha.Bench.BenchEcho,
      Flezha.Bench.BenchKeyed,
      Flezha.Bench.KeyedNoPersist,
      Flezha.Bench.TierStore,
      Flezha.Bench.RemoteTierStore,
      Flezha.Bench.BenchCheckpoint
    ]
  end
end
