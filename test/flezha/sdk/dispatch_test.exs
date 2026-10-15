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

  defmodule Stamp do
    use Flezha.SDK.Tasklet, name: "stamp"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule Timed do
    use Flezha.SDK.Tasklet, name: "timed"

    @impl true
    def event_time(input), do: String.to_integer(input)

    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule Staged do
    use Flezha.SDK.Relay, factory_id: "dispatch_staged", run: {:steps, each: :per_input}

    @impl true
    def channels(c),
      do: c |> input("in", codec: :text) |> into(Stamp) |> map(&String.trim/1) |> into(Timed)
  end

  defmodule Shown do
    use Flezha.SDK.Tasklet, name: "shown"

    @impl true
    def run(input, state, ctx) do
      emit(ctx, inspect(input))
      {:ok, state}
    end
  end

  # One entry channel per codec into one tasklet, each keyed on its decoded value.
  defmodule Coded do
    use Flezha.SDK.Relay, factory_id: "dispatch_coded"

    @impl true
    def channels(c) do
      c |> input("json") |> key_by(& &1["page"]) |> into(Shown)
      c |> input("text", codec: :text) |> key_by(& &1) |> into(Shown)

      c
      |> input("bytes", codec: :bytes)
      |> key_by(&Base.encode16/1)
      |> into(Shown)
      |> output("out")
    end
  end

  setup_all do
    core = Native.context_new(Native.schema_version())
    {:ok, class} = Declaration.declare(core, Probe)
    :ok = Dispatch.register(class)
    :ok = Dispatch.declare(core, Staged)
    :ok = Dispatch.declare(core, Coded)
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

  defp live(env, key \\ "a") do
    %{outcome: :state_json, json: json} =
      Dispatch.answer(activation(:live_state_read, %{cell_key: key}), env)

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

  # A Cell of its own: a restored Cell's next work reads its timers from a live core.
  test "a restore folds the snapshot and the changes, migrating what asks for it", %{env: env} do
    snapshot = %{bytes: :erlang.term_to_binary(%{v: 10}), from_version: 1}
    changes = [%{bytes: :erlang.term_to_binary({:add, 2})}]
    fold = %{snapshot: snapshot, changes: changes, cell_key: "restored"}

    assert %{outcome: :restored} = Dispatch.answer(activation(:restore_fold, fold), env)
    assert live(env, "restored") == %{"value" => 12}

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
    request = %{
      channel: coded("json"),
      payloads: ["b:1", "bad"],
      want_keys: true,
      want_event_time: false
    }

    assert %{outcome: :prepared, prepared: [%{keys: ["b"]}, %{non_retryable: true}]} =
             Dispatch.answer(activation(:key_request, %{key_request: request}), env)
  end

  test "a steps edge carries the event time its consumer reads off the value", %{env: env} do
    act = activation(:work, %{factory: "dispatch_staged", items: [input(" 42 ")]})

    assert %{effects: [%{verdict: :ok, outputs: [edge]}]} = Dispatch.answer(act, env)
    assert %{target: :edge, consumer: 1, event_time: 42} = edge

    # A value the consumer's clock cannot read fails where it is read, as in flow.
    act = activation(:work, %{factory: "dispatch_staged", items: [input("not a number")]})
    assert %{effects: [%{verdict: :retryable}]} = Dispatch.answer(act, env)
  end

  defp coded(name) do
    class = :persistent_term.get({Dispatch, "dispatch_coded"})
    Enum.find(Tuple.to_list(class.channels), &(&1.name == name)).index
  end

  # What the tasklet shows for `payload`, run on the Cell its decoded value keys to.
  defp shown(env, channel, payload, key) do
    act =
      activation(:work, %{
        factory: "dispatch_coded",
        cell_key: key,
        items: [input(payload, %{channel: coded(channel)})]
      })

    case Dispatch.answer(act, env) do
      %{effects: [%{verdict: :ok, outputs: [%{payload: shown}]}]} -> shown
      %{effects: [%{verdict: verdict}]} -> verdict
    end
  end

  test "a JSON entry hands its key and its tasklet the decoded value", %{env: env} do
    request = %{
      channel: 0,
      payloads: [~s({"page":"home"})],
      want_keys: true,
      want_event_time: false
    }

    assert %{prepared: [%{keys: ["home"]}]} =
             Dispatch.answer(
               activation(:key_request, %{factory: "dispatch_coded", key_request: request}),
               env
             )

    assert shown(env, "json", ~s({"page":"home"}), "home") == ~s(%{"page" => "home"})
    assert shown(env, "json", "{not json", "home") == :unreadable
  end

  test "a text entry is its UTF-8 string and a bytes entry its payload as it came", %{env: env} do
    assert shown(env, "text", "héllo", "héllo") == ~s("héllo")
    assert shown(env, "text", <<0xFF>>, "x") == :unreadable
    assert shown(env, "bytes", <<0xFF>>, "FF") == "<<255>>"
  end

  test "a firing on a tasklet without on_timer/4 parks by name, not retried", %{env: env} do
    timer = %{kind: :timer, timer_kind: :processing_time, scheduled_at: 2_000, replay: false}

    assert %{effects: [%{verdict: :non_retryable, message: message}]} =
             Dispatch.answer(activation(:work, %{items: [timer]}), env)

    assert message =~ "counter" and message =~ "on_timer/4"
  end

  test "a compensation runs the hook over the outputs it undoes", %{env: env} do
    outputs = Enum.map(["x", "y"], &:erlang.term_to_binary/1)

    assert %{outcome: :work, effects: [%{verdict: :ok, outputs: [undo]}]} =
             Dispatch.answer(activation(:compensate_run, %{outputs: outputs}), env)

    assert undo.payload == "undo x,y"
  end
end
