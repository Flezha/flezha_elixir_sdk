defmodule Flezha.SDK.DispatchTest do
  @moduledoc """
  Every activation kind `flz_core` hands a host, answered by the dispatcher
  for a class declared to a core context of the test's own: the activation as
  the native library maps it, the result as the core reads it back.
  """
  use ExUnit.Case, async: false

  alias Flezha.SDK.{Channels, Declaration, Dispatch, Native}

  defmodule Counter do
    use Flezha.SDK.Tasklet, name: "counter", schema_version: 2, min_readable_schema_version: 1

    defchange add(state, n) do
      %{state | value: state.value + n}
    end

    @impl true
    def init_state(_key), do: %{value: 0}

    @impl true
    def run(input, state, ctx) do
      [key, n] = String.split(input, ":")
      state = add(state, ctx, String.to_integer(n))
      emit(ctx, "#{key}=#{state.value}")
      emit_with_meta(ctx, "again", %{"note" => "second"})
      {:ok, state}
    end

    @impl true
    def state_from_json(%{"value" => value}), do: %{value: value}

    @impl true
    def migrate_state(1, %{v: v}), do: {:ok, %{value: v}}

    @impl true
    def compensate(outputs, state, ctx) do
      emit(ctx, "undo #{Enum.join(outputs, ",")}")
      {:ok, state}
    end
  end

  defmodule Probe do
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "dispatch_probe"

    def channels(c) do
      c
      |> Channels.input_with("in", fn
        "bad" -> {:error, :unreadable_by_design}
        bytes -> {:ok, bytes}
      end)
      |> Channels.key_by(&(&1 |> String.split(":") |> hd()))
      |> Channels.into(Counter)
      |> Channels.keyed()
      |> Channels.output("out")
    end
  end

  setup_all do
    core = Native.context_new(Native.schema_version())
    {:ok, class} = Declaration.declare(core, Probe)
    :ok = Dispatch.register(class)
    on_exit(fn -> Native.context_free(core) end)
    %{core: core}
  end

  setup %{core: core}, do: %{env: %{core: core, cells: Dispatch.cells()}}

  defp activation(kind, fields) do
    Map.merge(
      %{
        token: System.unique_integer([:positive]),
        relay: "r-1",
        tasklet: 0,
        cell_key: "a",
        change_seq: 5,
        factory: "dispatch_probe",
        kind: kind
      },
      fields
    )
  end

  defp input(payload, extra \\ %{}),
    do:
      Map.merge(
        %{kind: :input, channel: 0, payload: payload, metadata: [], replay: false, now: 1_000},
        extra
      )

  defp live(env) do
    %{outcome: :state_json, json: json} = Dispatch.answer(activation(:live_state_read, %{}), env)
    Jason.decode!(json)
  end

  test "a work item runs the tasklet and answers its effect record", %{env: env} do
    act = activation(:work, %{items: [input("a:3", %{consumed_offset: 7})]})

    assert %{outcome: :work, effects: [effect]} = Dispatch.answer(act, env)
    assert %{verdict: :ok, consumed_offset: 7, event_time: 1_000, complete: nil} = effect
    assert Enum.map(effect.changes, &:erlang.binary_to_term/1) == [{:add, 3}]
    assert live(env) == %{"value" => 3}

    # Keyed: every plugin output carries the identity, its ordinal counted per item.
    [first, second] = effect.outputs
    assert %{target: :plugin, plugin_channel: "out", key: "a", payload: "a=3"} = first

    assert {"flz.seq", "5.0"} in first.metadata and
             {"flz.cell", "r-1/counter/a"} in first.metadata

    assert {"note", "second"} in second.metadata and {"flz.seq", "5.1"} in second.metadata
  end

  test "an entry its parser refuses is unreadable, and a raise is retryable", %{env: env} do
    act = activation(:work, %{items: [input("bad"), input("a:1")]})

    assert %{effects: [%{verdict: :unreadable, message: message}]} = Dispatch.answer(act, env)
    assert message =~ "unreadable_by_design"

    act = activation(:work, %{items: [input("a:not_a_number")]})
    assert %{effects: [%{verdict: :retryable}]} = Dispatch.answer(act, env)
    assert live(env) == %{"value" => 0}, "a failed item records nothing"
  end

  test "a restore folds the snapshot and the changes, migrating what asks for it", %{env: env} do
    snapshot = %{bytes: :erlang.term_to_binary(%{v: 10}), from_version: 1}
    changes = [%{bytes: :erlang.term_to_binary({:add, 2})}]

    assert %{outcome: :restored} =
             Dispatch.answer(
               activation(:restore_fold, %{snapshot: snapshot, changes: changes}),
               env
             )

    assert live(env) == %{"value" => 12}

    unmigratable = %{bytes: :erlang.term_to_binary({:add, 1}), from_version: 1}

    assert %{outcome: :failed, non_retryable: true, error: error} =
             Dispatch.answer(activation(:restore_fold, %{changes: [unmigratable]}), env)

    assert error =~ "no migration from schema_version 1"
  end

  test "a snapshot, a state applied and a migration read answer the Cell", %{env: env} do
    assert %{outcome: :state_applied} =
             Dispatch.answer(activation(:apply_state, %{state_json: ~s({"value": 4})}), env)

    assert %{outcome: :snapshot, state: state} =
             Dispatch.answer(activation(:snapshot_request, %{}), env)

    assert :erlang.binary_to_term(state) == %{value: 4}

    assert %{outcome: :migration_state, json: json} =
             Dispatch.answer(activation(:migration_serialize, %{}), env)

    assert Jason.decode!(json) == %{"value" => 4}
  end

  test "a key request answers each payload's keys", %{env: env} do
    request = %{channel: 0, payloads: ["b:1", "bad"], want_keys: true, want_event_time: false}

    assert %{outcome: :prepared, prepared: [%{keys: ["b"]}, %{non_retryable: true}]} =
             Dispatch.answer(activation(:key_request, %{key_request: request}), env)
  end

  test "a compensation runs the hook over the outputs it undoes", %{env: env} do
    outputs = Enum.map(["x", "y"], &:erlang.term_to_binary/1)

    assert %{outcome: :work, effects: [%{verdict: :ok, outputs: [undo]}]} =
             Dispatch.answer(activation(:compensate_run, %{outputs: outputs}), env)

    assert undo.payload == "undo x,y"
  end
end
