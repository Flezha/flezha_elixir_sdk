# The Elixir bench factories, mirroring test/apps/rust_runner/src/main.rs
# under the same factory ids and topologies so `mix bench --runtime elixir`
# measures the same shapes:
#   do_nothing     — regression: input → echo → sink
#   entity_stress  — entity:     A(8 keys) → B → C → D → sink (fan-out then fan-in)
#   dynamic_entity — dynamic:    filter + key_by(prefix) → keyed Entity → sink
# plus the `mix bench.roundtrip` twins:
#   bench_echo            — do_nothing with a declared Block on the entry mailbox
#   bench_keyed           — "key:value" upsert, emits "key=value" (Single Entity), persisted
#   bench_keyed_nopersist — the same upsert with the class's persistence off
#   tier_store            — the same upsert under the effectively-once tier
#   bench_checkpoint      — the tier acknowledging on a declared input cadence
# The controller's in-process `--transport native` bench and its in-node tests
# reach the same modules through a path dependency on this app.

defmodule Flezha.Bench.EchoProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "do_nothing_procedure"
  def init_state(_), do: nil

  def run(input, s, ctx),
    do:
      (
        Ctx.emit(ctx, input)
        {:ok, s}
      )
end

defmodule Flezha.Bench.DoNothing do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "do_nothing"

  def channels(c) do
    import Flezha.SDK.Channels
    c |> input("input") |> into(Flezha.Bench.EchoProc) |> sink("echo")
  end
end

defmodule Flezha.Bench.StressA do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "stress_a"
  def init_state(_), do: nil

  def run(input, s, ctx),
    do:
      (
        Ctx.emit(ctx, input)
        {:ok, s}
      )

  # The shared 8-key static set (Rust's `STRESS_KEYS`); B/C/D declare the same
  # keyset and narrow the routing to `p0` through `entities_for/1`.
  def entity_keys, do: ~w(p0 p1 p2 p3 p4 p5 p6 p7)
end

defmodule Flezha.Bench.StressB do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "stress_b"
  def init_state(_), do: nil

  def run(input, s, ctx),
    do:
      (
        Ctx.emit(ctx, input)
        {:ok, s}
      )

  defdelegate entity_keys, to: Flezha.Bench.StressA
  def entities_for(_), do: ["p0"]
end

defmodule Flezha.Bench.StressC do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "stress_c"
  def init_state(_), do: nil

  def run(input, s, ctx),
    do:
      (
        Ctx.emit(ctx, input)
        {:ok, s}
      )

  defdelegate entity_keys, to: Flezha.Bench.StressA
  def entities_for(_), do: ["p0"]
end

defmodule Flezha.Bench.StressD do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "stress_d"
  def init_state(_), do: nil

  def run(input, s, ctx),
    do:
      (
        Ctx.emit(ctx, input)
        {:ok, s}
      )

  defdelegate entity_keys, to: Flezha.Bench.StressA
  def entities_for(_), do: ["p0"]
end

defmodule Flezha.Bench.EntityStress do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "entity_stress"

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
    |> sink("echo")
  end
end

defmodule Flezha.Bench.DynamicProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "dynamic_entity_procedure"
  def init_state(_), do: %{seen: []}
  def apply_change(s, {:push, input}), do: %{s | seen: s.seen ++ [input]}
  def state_from_json(%{"seen" => seen}), do: %{seen: seen}

  # The e2e sets the idle TTL short; unset, the Entities live for the
  # operative's lifetime, as the reference's do.
  def entity_idle_ttl_ms do
    case System.get_env("DYN_TEST_TTL_MS") do
      nil -> nil
      ms -> String.to_integer(ms)
    end
  end

  def run(input, s, ctx) do
    s = Ctx.apply(ctx, s, {:push, input})
    Ctx.emit(ctx, input)
    {:ok, s}
  end
end

defmodule Flezha.Bench.DynamicEntity do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "dynamic_entity"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> on_full(:drop_nothing)
    |> filter(fn s -> s != "drop" end)
    |> key_by(fn s -> s |> to_string() |> String.split(":") |> hd() end)
    |> into(Flezha.Bench.DynamicProc)
    |> sink("kafka")
  end
end

defmodule Flezha.Bench.BenchEcho do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "bench_echo"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> on_full(:drop_nothing)
    |> into(Flezha.Bench.EchoProc)
    |> sink("echo")
  end
end

defmodule Flezha.Bench.TierStoreProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  def name, do: "tier_store_procedure"
  def init_state(_), do: %{}
  def apply_change(values, {:set, key, value}), do: Map.put(values, key, value)

  # Emits `key=value` under `key`, as the reference does: the bench sinks are
  # keyed, so the durable path pays for its stamps here as it does there.
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
    values = Ctx.apply(ctx, values, {:set, key, value})
    Ctx.emit_keyed(ctx, key, "#{key}=#{value}")
    {:ok, values}
  end
end

defmodule Flezha.Bench.BenchKeyed do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "bench_keyed"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> on_full(:drop_nothing)
    |> into(Flezha.Bench.TierStoreProc)
    |> keyed()
    |> sink("sink")
  end
end

defmodule Flezha.Bench.KeyedNoPersist do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "bench_keyed_nopersist"

  # The same upsert with the class's persistence off — the heap-only posture
  # the bench isolates against the durable one.
  def persistence_enabled, do: false
  defdelegate channels(c), to: Flezha.Bench.BenchKeyed
end

defmodule Flezha.Bench.TierStore do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "tier_store"

  # `bench_keyed` under the tier: the durable posture the round-trip bench
  # measures with `--durability on`, and the conformance fixture the tier's
  # crash and dead-letter scenarios drive. The 300 ms backoff base keeps the
  # min-watermark freeze observable while the ladder runs; DropNothing because
  # a durable-input-log operative that shed at its mailbox would hold the
  # min-watermark on the shed offset for ever.
  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> effectively_once()
    |> input("input")
    |> on_full(:drop_nothing)
    |> retry_backoff_ms(300)
    |> into(Flezha.Bench.TierStoreProc)
    |> keyed()
    |> sink("sink")
  end
end

defmodule Flezha.Bench.BenchCheckpointProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  # The same upsert under its own procedure name, so the declaration a
  # checkpoint class registers is distinguishable from `tier_store`'s.
  def name, do: "bench_checkpoint_procedure"
  defdelegate init_state(key), to: Flezha.Bench.TierStoreProc
  defdelegate apply_change(state, change), to: Flezha.Bench.TierStoreProc
  defdelegate run(input, state, ctx), to: Flezha.Bench.TierStoreProc
end

defmodule Flezha.Bench.BenchCheckpoint do
  @moduledoc false

  alias Flezha.SDK.Knob

  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "bench_checkpoint"

  # `tier_store` driven by a declared cadence instead of a `commit:` input:
  # the amortised posture, acknowledging every FLEZHA_BENCH_CHECKPOINT_EVERY
  # inputs per Entity. FLEZHA_BENCH_CHECKPOINT_OUTPUTS=at_checkpoint
  # additionally holds the interval's outputs until it closes.
  def channels(c) do
    import Flezha.SDK.Channels

    c =
      c
      |> effectively_once()
      |> checkpoint_every(env("FLEZHA_BENCH_CHECKPOINT_EVERY", :positive_int, 500))

    c =
      if System.get_env("FLEZHA_BENCH_CHECKPOINT_OUTPUTS") == "at_checkpoint",
        do: outputs_at_checkpoint(c, env("FLEZHA_BENCH_CHECKPOINT_DELAY_MS", :ms, 50)),
        else: c

    c
    |> input("input")
    |> on_full(:drop_nothing)
    |> into(Flezha.Bench.BenchCheckpointProc)
    |> keyed()
    |> sink("sink")
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
  @moduledoc "Registry of the Elixir bench factory operatives (see this file's header)."

  @doc "Every bench factory operative module."
  def all do
    [
      Flezha.Bench.DoNothing,
      Flezha.Bench.EntityStress,
      Flezha.Bench.DynamicEntity,
      Flezha.Bench.BenchEcho,
      Flezha.Bench.BenchKeyed,
      Flezha.Bench.KeyedNoPersist,
      Flezha.Bench.TierStore,
      Flezha.Bench.BenchCheckpoint
    ]
  end
end
