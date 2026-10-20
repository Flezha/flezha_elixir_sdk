defmodule Flezha.SDK.Dispatch do
  @moduledoc """
  One activation in, one result out, and every failure a result. `flz_core`
  hands the activation; this module runs the tasklet code it names and answers
  what the core folds: an effect record per work item, a restored or migrated
  state, a snapshot, a JSON view of a Cell, the keys and event time of a
  payload, a compensation.

  A Cell's live state is the host's, held in the runner's Cell table keyed by
  relay, tasklet and Cell key; the core keeps its log and the Cell's timers.
  State snapshots, change bodies and payloads between two tasklets are Erlang
  terms (format id `7`), read back with `:safe`.
  """

  alias Flezha.SDK.{Ctx, Declaration, Error, Native, Tasklet}

  @typedoc "What a worker answers activations with: the core and the Cell table."
  @type env :: %{core: reference(), cells: :ets.tid()}

  @doc "A Cell table, owned by the calling process."
  @spec cells() :: :ets.tid()
  def cells, do: :ets.new(__MODULE__, [:public, :set, read_concurrency: true])

  @doc "Declare `module` to `core` and keep the class its activations name."
  @spec declare(Native.core(), module()) :: :ok | {:error, Error.t()}
  def declare(core, module) do
    with {:ok, class} <- Declaration.declare(core, module), do: register(class)
  end

  @doc "Keep the declared class the core's activations name by factory id."
  @spec register(Declaration.t()) :: :ok
  def register(class), do: :persistent_term.put({__MODULE__, class.factory_id}, class)

  @doc "The answer to `act`."
  @spec answer(map(), env()) :: map()
  def answer(%{token: token} = act, env) do
    class = :persistent_term.get({__MODULE__, act.factory})
    module = elem(class.nodes, act.tasklet)
    cell = {act.relay, act.tasklet, Map.get(act, :cell_key)}
    answer_kind(act.kind, act, %{env: env, class: class, module: module, cell: cell})
  rescue
    e -> failed(token, Exception.message(e), false)
  catch
    kind, reason -> failed(token, Exception.format_banner(kind, reason), false)
  end

  defp answer_kind(:work, act, at) do
    {effects, state} =
      act.items
      |> Enum.with_index()
      |> Enum.reduce_while({[], load(at)}, fn {item, index}, {effects, state} ->
        case run_item(item, index, act, at, state) do
          {:ok, effect, state} -> {:cont, {[effect | effects], state}}
          {:error, effect} -> {:halt, {[effect | effects], state}}
        end
      end)

    store(at, state)
    %{token: act.token, outcome: :work, effects: Enum.reverse(effects)}
  end

  defp answer_kind(:restore_fold, act, %{module: module, cell: {_, _, key}} = at) do
    with {:ok, state} <- restored_snapshot(Map.get(act, :snapshot), module, key),
         {:ok, state} <- fold_changes(act.changes, module, state) do
      store(at, state)
      %{token: act.token, outcome: :restored}
    else
      {:error, reason} -> failed(act.token, reason, true)
    end
  end

  defp answer_kind(:snapshot_request, act, at),
    do: %{token: act.token, outcome: :snapshot, state: encode_term(load(at))}

  defp answer_kind(:live_state_read, act, at),
    do: %{token: act.token, outcome: :state_json, json: state_json(load(at))}

  defp answer_kind(:apply_state, act, %{module: module} = at) do
    store(at, Tasklet.state_from_json(module, Jason.decode!(act.state_json)))
    %{token: act.token, outcome: :state_applied}
  end

  defp answer_kind(:key_request, act, at),
    do: %{token: act.token, outcome: :prepared, prepared: prepare(act.key_request, at.class)}

  defp answer_kind(:compensate_run, act, %{module: module} = at) do
    if Tasklet.compensates?(module) do
      state = load(at)
      outputs = Enum.map(act.outputs, &decode_term/1)
      ctx = new_ctx(act, at, 0, %{now: System.os_time(:millisecond)}, timers?: false)

      case settle(invoke(fn -> module.compensate(outputs, state, ctx) end), ctx, %{}, act, at) do
        {:ok, effect, state} ->
          store(at, state)
          %{token: act.token, outcome: :work, effects: [effect]}

        {:error, effect} ->
          %{token: act.token, outcome: :work, effects: [effect]}
      end
    else
      failed(act.token, "#{module.name()} declares no compensate hook", true)
    end
  end

  defp answer_kind(:leave, act, %{env: env, cell: {relay, _, _} = cell}) do
    if act.whole_relay,
      do: :ets.match_delete(env.cells, {{relay, :_, :_}, :_}),
      else: :ets.delete(env.cells, cell)

    %{token: act.token, outcome: :state_applied}
  end

  defp answer_kind(kind, act, _at), do: failed(act.token, "unknown activation kind #{kind}", true)

  @doc "A whole activation answered as failed."
  @spec failed(non_neg_integer(), String.t(), boolean()) :: map()
  def failed(token, message, non_retryable?),
    do: %{token: token, outcome: :failed, error: message, non_retryable: non_retryable?}

  defp load(%{env: env, cell: {_, _, key} = cell, module: module}) do
    case :ets.lookup(env.cells, cell) do
      [{_, state}] -> state
      [] -> module.init_state(key)
    end
  end

  defp store(%{env: env, cell: cell}, state), do: :ets.insert(env.cells, {cell, state})

  defp state_json(state) do
    case Jason.encode(state) do
      {:ok, json} -> json
      {:error, _} -> Jason.encode!(inspect(state))
    end
  end

  # --- restore ------------------------------------------------------------------

  defp restored_snapshot(nil, module, key), do: {:ok, module.init_state(key)}
  defp restored_snapshot(body, module, _key), do: decode_body(body, module, :snapshot)

  defp fold_changes(bodies, module, state) do
    Enum.reduce_while(bodies, {:ok, state}, fn body, {:ok, state} ->
      case decode_body(body, module, :change) do
        {:ok, change} -> {:cont, {:ok, module.apply_change(state, change)}}
        {:error, _} = refused -> {:halt, refused}
      end
    end)
  end

  defp decode_body(%{bytes: bytes} = body, module, what) do
    case Map.get(body, :from_version) do
      nil -> {:ok, decode_term(bytes)}
      from -> migrate(module, what, from, decode_term(bytes))
    end
  end

  # The decoded term goes to the migration: the body codec is the language's
  # own, so there is no second codec for an author to choose.
  defp migrate(module, what, from, old) do
    migrated =
      try do
        if what == :snapshot,
          do: Tasklet.migrate_state(module, from, old),
          else: Tasklet.migrate_change(module, from, old)
      rescue
        e -> {:error, Exception.message(e)}
      end

    case migrated do
      {:ok, term} ->
        {:ok, term}

      :undeclared ->
        {:error, "no migration from schema_version #{from}: the tasklet declares none (#{what})"}

      {:error, reason} ->
        {:error, "migration from schema_version #{from} failed (#{what}): #{describe(reason)}"}
    end
  end

  defp describe(reason) when is_binary(reason), do: reason
  defp describe(reason), do: inspect(reason)

  defp decode_term(bytes), do: :erlang.binary_to_term(bytes, [:safe])

  # --- one work item --------------------------------------------------------------

  defp run_item(%{kind: :input} = item, index, act, %{module: module} = at, state) do
    channel = elem(at.class.channels, item.channel)
    ctx = new_ctx(act, at, index, item, replay: item.replay)

    result =
      invoke(fn ->
        item.payload
        |> values_in(channel, act.tasklet, at.class.steps?)
        |> Enum.filter(&routes_to?(&1, channel, act.tasklet, module, at.cell))
        |> Enum.reduce_while({:ok, state}, fn value, {:ok, state} ->
          case module.run(value, state, ctx) do
            {:ok, state} -> {:cont, {:ok, state}}
            other -> {:halt, other}
          end
        end)
      end)

    settle(result, ctx, item, act, at)
  end

  defp run_item(%{kind: :timer} = item, index, act, %{module: module} = at, state) do
    item = Map.put(item, :now, item.scheduled_at)
    ctx = new_ctx(act, at, index, item, [])

    # A tasklet with no handler parks the firing by name, without the retries
    # a raise would climb.
    result =
      if Tasklet.on_timer?(module),
        do: invoke(fn -> module.on_timer(item.scheduled_at, item.timer_kind, state, ctx) end),
        else:
          {:error,
           {:non_retryable,
            "tasklet `#{module.name()}` registered a timer but does not export on_timer/4"}}

    settle(result, ctx, item, act, at)
  end

  defp new_ctx(
         act,
         %{class: class, module: module, cell: {relay, node, key}} = at,
         index,
         item,
         opts
       ) do
    {channel, keyed?, part_encode} =
      case out_channel(class, node) do
        nil -> {nil, false, nil}
        ch -> {ch.name, ch.keyed?, ch.sink}
      end

    Ctx.new(
      module,
      key,
      item.now,
      [
        channel: channel,
        keyed_sink?: keyed?,
        part_encode: part_encode,
        core: at.env.core,
        token: act.token,
        item: index,
        offset: Map.get(item, :consumed_offset),
        seq: act.change_seq,
        identity: Ctx.cell_identity(relay, elem(class.names, node), key)
      ] ++ opts
    )
  end

  defp invoke(fun) do
    case fun.() do
      {:ok, _state} = ok -> ok
      {:error, _reason} = error -> error
      other -> {:error, {:non_retryable, "run answered #{inspect(other)}, not {:ok, state}"}}
    end
  rescue
    e -> {:error, Exception.message(e)}
  catch
    {:unreadable, reason} -> {:error, {:unreadable, reason}}
  end

  defp settle({:ok, state}, ctx, item, act, at) do
    changes = ctx |> Ctx.take_changes() |> Enum.map(&encode_term/1)
    emits = Ctx.take_emits(ctx)

    outputs = Enum.flat_map(emits, &outputs(&1, ctx, act, at))

    effect = %{
      verdict: :ok,
      changes: changes,
      outputs: outputs,
      timers: Ctx.take_timers(ctx),
      consumed_offset: Map.get(item, :consumed_offset),
      event_time: Map.get(item, :now),
      checkpoint: Ctx.checkpointed?(ctx),
      complete: Ctx.completion(ctx)
    }

    Ctx.discard(ctx)
    {:ok, effect, state}
  rescue
    e ->
      Ctx.discard(ctx)
      settle({:error, Exception.message(e)}, ctx, item, act, at)
  end

  defp settle({:error, reason}, ctx, item, _act, _at) do
    Ctx.discard(ctx)

    {verdict, reason} =
      case reason do
        {:non_retryable, r} -> {:non_retryable, r}
        {:unreadable, r} -> {:unreadable, r}
        r -> {:retryable, r}
      end

    {:error,
     %{
       verdict: verdict,
       message: message(reason),
       consumed_offset: Map.get(item, :consumed_offset),
       event_time: Map.get(item, :now)
     }}
  end

  defp message(reason) when is_binary(reason), do: reason
  defp message(%Error{} = error), do: "#{error.message} (#{error.code})"
  defp message(reason), do: inspect(reason)

  # --- payloads --------------------------------------------------------------------

  # An entry payload is parsed and runs its edge's transforms here; so does
  # every payload in steps, where the producer handed it on untransformed. In
  # flow an inter-tasklet payload arrives transformed by its producer.
  defp values_in(payload, %{kind: "entry"} = channel, node, _steps?) do
    case channel.parser.(payload) do
      {:ok, value} -> transform(value, edge_into(channel, node))
      {:error, reason} -> throw({:unreadable, inspect(reason)})
    end
  end

  defp values_in(payload, channel, node, true),
    do: transform(decode_term(payload), edge_into(channel, node))

  defp values_in(payload, _channel, _node, false), do: [decode_term(payload)]

  defp edge_into(channel, node) do
    case Enum.find(channel.consumers, &(&1.tasklet == node)) do
      nil -> nil
      edge -> edge.transforms
    end
  end

  # The core hands an untransformed entry payload to every Cell its key
  # request named, so a fan across keys keeps only the values keyed to this one.
  defp routes_to?(_value, _channel, _node, _module, {_, _, nil}), do: true

  defp routes_to?(value, channel, node, module, {_, _, key}) do
    edge = Enum.find(channel.consumers, &(&1.tasklet == node))

    cond do
      edge && edge.key -> to_string(edge.key.(value)) == key
      Tasklet.cell_keys(module) != [] -> key in module.cells_for(value)
      true -> true
    end
  end

  defp transform(value, nil), do: [value]

  defp transform(value, transforms) do
    Enum.reduce(transforms, [value], fn
      {:map, f}, vs -> Enum.map(vs, f)
      {:filter, f}, vs -> Enum.filter(vs, f)
      {:flat_map, f}, vs -> Enum.flat_map(vs, &Enum.to_list(f.(&1)))
    end)
  end

  # One emit's outputs.
  defp outputs({value, meta, key}, ctx, act, %{class: class}) do
    case out_channel(class, act.tasklet) do
      nil ->
        []

      channel ->
        meta = Enum.map(meta, fn {k, v} -> {to_string(k), to_string(v)} end)
        edges = edges(class.steps?, channel, act.tasklet, value, key, ctx.cell_key, meta, class)
        edges ++ calls(channel, value, meta) ++ plugin(channel, value, key || ctx.cell_key, meta)
    end
  end

  # The channel the tasklet at `node` produces on, or nil.
  defp out_channel(class, node) do
    if idx = class.out_channel[node], do: elem(class.channels, idx)
  end

  # In steps a value rides to its first consumer untransformed, and an
  # inter-tasklet terminal's value goes to the core as the pass's handoff.
  defp edges(true, channel, node, value, _key, _cell_key, _meta, class) do
    {consumer, transforms} =
      cond do
        channel.consumers != [] ->
          {hd(channel.consumers).tasklet, hd(channel.consumers).transforms}

        channel.sink == nil and channel.call == nil ->
          {node, nil}

        true ->
          {nil, nil}
      end

    if consumer,
      do: [
        %{
          target: :edge,
          channel: channel.index,
          consumer: consumer,
          event_time: steps_event_time(elem(class.nodes, consumer), value, transforms),
          metadata: [],
          payload: encode_term(value)
        }
      ],
      else: []
  end

  defp edges(false, channel, _node, value, key, cell_key, meta, class) do
    for edge <- channel.consumers, v <- transform(value, edge.transforms) do
      consumer = elem(class.nodes, edge.tasklet)

      %{
        target: :edge,
        channel: channel.index,
        consumer: edge.tasklet,
        event_time: event_time_of(consumer, [v]),
        key: key || (edge.key && to_string(edge.key.(v))) || cell_key,
        metadata: meta,
        payload: encode_term(v)
      }
    end
  end

  defp encode_term(value), do: :erlang.term_to_binary(value)

  # The event time a consumer reads off the first value it will run, when it
  # declares a clock that reads one.
  defp event_time_of(module, [value | _]),
    do: if(Tasklet.exports?(module, :event_time, 1), do: module.event_time(value))

  defp event_time_of(_module, []), do: nil

  # In steps the consumer runs its edge's transforms on its own side, so a
  # transform failing here is left for it to report rather than failing the
  # producer; a failing `event_time/1` is the producer's to report, as in flow.
  defp steps_event_time(module, value, transforms),
    do: event_time_of(module, transformed(value, transforms))

  defp transformed(value, transforms) do
    transform(value, transforms)
  rescue
    _ -> []
  end

  defp calls(%{call: nil}, _value, _meta), do: []

  defp calls(%{call: call} = channel, value, meta) do
    for request <- transform(value, call.transforms) do
      %{
        target: :plugin,
        plugin_channel: channel.name,
        key: call.key && to_string(call.key.(request)),
        metadata: meta,
        payload: call.call.(request)
      }
    end
  end

  defp plugin(%{sink: nil}, _value, _key, _meta), do: []

  defp plugin(channel, value, key, meta) do
    [
      %{
        target: :plugin,
        plugin_channel: channel.name,
        key: key,
        metadata: meta,
        payload: channel.sink.(value)
      }
    ]
  end

  # --- the key request -------------------------------------------------------------

  # One key set for the whole payload, which the core hands every keyed
  # consumer, so the first keyed consumer's edge is read: an unkeyed one's
  # filter must not drop the key.
  defp prepare(%{channel: idx, payloads: payloads} = request, class) do
    channel = elem(class.channels, idx)

    edge =
      Enum.find(channel.consumers, fn e ->
        e.key || Tasklet.cell_keys(elem(class.nodes, e.tasklet)) != []
      end) || List.first(channel.consumers)

    Enum.map(payloads, &prepare_one(&1, request, channel, edge, class))
  end

  defp prepare_one(payload, request, channel, edge, class) do
    module = edge && elem(class.nodes, edge.tasklet)
    values = values_in(payload, channel, edge && edge.tasklet, class.steps?)

    keys =
      if request.want_keys and edge do
        values
        |> Enum.flat_map(fn v ->
          if edge.key, do: [to_string(edge.key.(v))], else: module.cells_for(v)
        end)
        |> Enum.uniq()
      else
        []
      end

    event_time =
      if request.want_event_time and module != nil and values != [] and
           Tasklet.exports?(module, :event_time, 1),
         do: module.event_time(hd(values))

    %{keys: keys, event_time: event_time}
  rescue
    e -> %{error: Exception.message(e), non_retryable: false}
  catch
    {:unreadable, reason} -> %{error: reason, non_retryable: true}
  end
end
