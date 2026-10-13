defmodule Flezha.SDK.Codec do
  @moduledoc """
  The single protobuf seam for the Elixir SDK's control channel. Encodes and
  decodes `RunnerCommands.CommandRequest`/`CommandResponse` through the modules
  `Flezha.SDK.Proto` generates.
  """

  alias Flezha.SDK.Error
  alias Flezha.SDK.Proto.RunnerCommands
  alias Flezha.SDK.Topology
  alias Flezha.SDK.Steps.Spec
  alias Flezha.SDK.Tasklet

  # The Elixir SDK's allocated language byte (`flz_core::header`'s table of
  # record: 1 Rust, 2 Java, 3 Elixir). Spelled ONCE: it rides both halves of
  # the factory-advertisement seam, and a factory whose two halves disagreed
  # would be placeable on one path and not the other; `Flezha.SDK.Record`
  # stamps the same byte on every durable record. The SDK advertises factory
  # ids the Rust and Java test apps also advertise — the controller's
  # placement guard is what keeps those ids from crossing runtimes.
  @sdk_lang 3

  @doc "The SDK's allocated language byte, carried on every factory it advertises."
  @spec sdk_lang() :: pos_integer()
  def sdk_lang, do: @sdk_lang

  @doc "Decode an inbound control message into a `CommandRequest`."
  @spec decode_command(binary()) :: {:ok, struct()} | {:error, term()}
  def decode_command(bin) when is_binary(bin) do
    RunnerCommands.CommandRequest.decode(bin)
  end

  @doc "Encode a `CommandResponse` reply (protobuf bytes for the control frame)."
  @spec encode_response(String.t(), String.t(), [Error.t()], binary()) :: binary()
  def encode_response(reference_id, status, errors \\ [], raw_data \\ <<>>) do
    response = %RunnerCommands.CommandResponse{
      reference_id: reference_id,
      status: status,
      errors: Enum.map(errors, &wire_error/1),
      raw_data: raw_data
    }

    {:ok, iodata, _size} = Protox.encode(response)
    IO.iodata_to_binary(iodata)
  end

  @doc "Encode an SDK-initiated `CommandRequest` (e.g. `UpdateFactories`)."
  @spec encode_request(String.t(), {atom(), struct()}) :: binary()
  def encode_request(reference_id, command) do
    request = %RunnerCommands.CommandRequest{reference_id: reference_id, command: command}
    {:ok, iodata, _size} = Protox.encode(request)
    IO.iodata_to_binary(iodata)
  end

  @doc "Encode an `AddRelayResponse` metadata map into its raw_data bytes."
  @spec encode_add_relay_response(map()) :: binary()
  def encode_add_relay_response(metadata) when is_map(metadata) do
    {:ok, iodata, _size} = Protox.encode(%RunnerCommands.AddRelayResponse{metadata: metadata})
    IO.iodata_to_binary(iodata)
  end

  @doc "Encode a `PingResponse` carrying the nonce."
  @spec encode_ping_response(String.t()) :: binary()
  def encode_ping_response(nonce) do
    {:ok, iodata, _size} = Protox.encode(%RunnerCommands.PingResponse{nonce: nonce})
    IO.iodata_to_binary(iodata)
  end

  @doc "Encode a `RestoreInitStateResponse` — the raw_data of a `RestoreInitState` reply."
  @spec encode_restore_response(binary(), non_neg_integer(), boolean()) :: binary()
  def encode_restore_response(cell_id, terminal_seq, migrated) do
    encode(%RunnerCommands.RestoreInitStateResponse{
      cell_id: cell_id,
      terminal_seq: terminal_seq,
      migrated: migrated
    })
  end

  @doc "Encode a `CompactCellResponse` — the raw_data of a `CompactCell` reply."
  @spec encode_compact_response(binary(), non_neg_integer(), non_neg_integer()) :: binary()
  def encode_compact_response(cell_id, snapshot_seq, snapshot_bytes_size) do
    encode(%RunnerCommands.CompactCellResponse{
      cell_id: cell_id,
      snapshot_seq: snapshot_seq,
      snapshot_bytes_size: snapshot_bytes_size
    })
  end

  @doc "Encode a `CreditWindowAck` — the raw_data that marks the session credit-speaking."
  @spec encode_credit_ack(String.t()) :: binary()
  def encode_credit_ack(stream_id),
    do: encode(%RunnerCommands.CreditWindowAck{stream_id: stream_id})

  @doc "The `RunnerShutdownNotification` command tuple for `encode_request/2`."
  @spec shutdown_notification_command(String.t(), binary()) :: {atom(), struct()}
  def shutdown_notification_command(relay_id, serialized_state) do
    {:runner_shutdown_notification,
     %RunnerCommands.RunnerShutdownNotification{
       relay_id: relay_id,
       serialized_state: serialized_state
     }}
  end

  @doc """
  Build the `UpdateFactories` command tuple from a list of
  `{id, description, run}`, `run` as `run/1` answers it with `:api` as
  `api/1` answers it and `:graph` as `graph/1` does.
  """
  @spec update_factories_command([{String.t(), String.t() | nil, map()}]) ::
          {atom(), struct()}
  def update_factories_command(factories) do
    decls =
      Enum.map(factories, fn {id, desc, run} ->
        %RunnerCommands.FactoryDecl{
          id: id,
          description: desc || "",
          sdk_lang: @sdk_lang,
          run: run.run,
          each: run.each,
          on_failure: run.on_failure,
          api: run |> Map.get(:api, []) |> Enum.map(&struct(RunnerCommands.ApiEntry, &1)),
          graph: graph_decl(run[:graph])
        }
      end)

    {:update_factories, %RunnerCommands.UpdateFactories{factories: decls}}
  end

  defp graph_decl(nil), do: nil

  defp graph_decl(graph),
    do: %RunnerCommands.RelayGraph{
      channels: Enum.map(graph.channels, &struct(RunnerCommands.GraphChannel, &1)),
      tasklets: Enum.map(graph.tasklets, &struct(RunnerCommands.GraphTasklet, &1))
    }

  @doc """
  How a registered class runs, as its announcement names it: `"flow"`, or
  `"steps"` with its loop shape and failure mode (empty for flow).
  """
  @spec run(Spec.t() | Topology.t()) :: %{
          run: String.t(),
          each: String.t(),
          on_failure: String.t()
        }
  def run(%Spec{settings: settings}),
    do: %{
      run: "steps",
      each: Atom.to_string(settings.each),
      on_failure: Atom.to_string(settings.on_failure)
    }

  def run(%Topology{}), do: %{run: "flow", each: "", on_failure: ""}

  @doc """
  A registered class's API, as its announcement carries it: one entry per
  entry channel that is not a call's Response channel, with the format, schema
  and description it declares and the format of the outputs reachable from it
  (`""` when none is, `"bytes"` when they disagree), the rule
  `flz_core::relay::registry` builds a host's entries by.
  """
  @spec api(Spec.t() | Topology.t()) :: [map()]
  def api(%Spec{topology: topology}), do: api(topology)

  def api(%Topology{} = t) do
    responses = for {_source, routes} <- t.routes, %{call: %{response: name}} <- routes, do: name

    for name <- t.entry_order, name not in responses do
      entry = Map.fetch!(t.entries, name)

      %{
        channel: name,
        codec: entry.codec,
        schema_json: if(entry.schema, do: Jason.encode!(entry.schema), else: ""),
        description: entry.description,
        answer_codec: answer_codec(t.routes, [{:entry, name}], MapSet.new(), MapSet.new()),
        answer_schema_json: ""
      }
    end
  end

  # The outputs reachable from the pending sources along their consumer
  # edges. An output names no schema in this SDK, so its format decides.
  defp answer_codec(_routes, [], _seen, codecs) do
    case MapSet.to_list(codecs) do
      [] -> ""
      [codec] -> codec
      _disagree -> "bytes"
    end
  end

  defp answer_codec(routes, [source | pending], seen, codecs) do
    if source in seen do
      answer_codec(routes, pending, seen, codecs)
    else
      here = Map.get(routes, source, [])

      outputs =
        for %{to: {:sink, _}, call: nil} = out <- here, into: codecs, do: output_codec(out)

      next = for %{to: {:node, _} = node} <- here, do: node
      answer_codec(routes, next ++ pending, MapSet.put(seen, source), outputs)
    end
  end

  @doc """
  A registered class's graph, as its announcement carries it: every channel
  with its kind, producer, consumers, plugin end, call target and overflow,
  and every tasklet in declaration order with its cell mode, cell keys and
  mailbox capacity, the shape `flz_core::relay::registry` announces a host's
  class in. A producer's output nothing closes on is no channel.
  """
  @spec graph(Spec.t() | Topology.t()) :: %{channels: [map()], tasklets: [map()]}
  def graph(%Spec{topology: topology}), do: graph(topology)

  def graph(%Topology{} = t) do
    nodes = Enum.sort_by(t.node_seq, &elem(&1, 1))
    entries = for name <- t.entry_order, do: graph_channel(t, {:entry, name}, name, "")

    outputs =
      for {ref, _seq} <- nodes,
          channel = graph_channel(t, {:node, ref}, t.node_channel[ref], tasklet_name(t, ref)),
          channel.kind != nil,
          do: channel

    tasklets =
      for {ref, _seq} <- nodes do
        module = t.node_modules[ref]

        %{
          name: tasklet_name(t, ref),
          mode: Atom.to_string(t.node_mode[ref]),
          cell_keys: Enum.map(Tasklet.cell_keys(module), &to_string/1),
          mailbox_capacity: Tasklet.mailbox_capacity(module)
        }
      end

    %{channels: entries ++ outputs, tasklets: tasklets}
  end

  defp graph_channel(t, source, name, producer) do
    routes = Map.get(t.routes, source, [])
    consumers = for %{to: {:node, ref}} <- routes, do: ref
    ends = for %{to: {:sink, _}} = route <- routes, do: route
    call = Enum.find_value(ends, & &1.call)

    %{
      name: name,
      kind: graph_kind(source, consumers, ends),
      producer: producer,
      consumers: Enum.map(consumers, &tasklet_name(t, &1)),
      plugin_end:
        cond do
          call -> "call"
          ends != [] -> "output"
          true -> "none"
        end,
      call_target: if(call, do: call.target, else: ""),
      # This SDK applies a policy to the consuming tasklet's mailbox, so a
      # channel announces the first one its consumers declare.
      overflow:
        consumers
        |> Enum.find_value(&get_in(t.node_knobs, [&1, :on_full]))
        |> Kernel.||(:drop_nothing)
        |> Atom.to_string()
    }
  end

  defp graph_kind({:entry, _name}, _consumers, _ends), do: "entry"
  defp graph_kind(_node, [], []), do: nil
  defp graph_kind(_node, [], _ends), do: "plugin_bound"
  defp graph_kind(_node, _consumers, _ends), do: "inter_tasklet"

  defp tasklet_name(t, ref), do: t.node_name[ref] || t.node_modules[ref].name()

  defp output_codec(%{serializer: serializer}),
    do: if(serializer == (&Flezha.SDK.Channels.default_serialize/1), do: "json", else: "custom")

  @doc """
  The `AdminRequest` command tuple: the verb, and its arguments as a JSON
  array. Neither a namespace nor a principal rides it; the controller fills
  both from the connection.
  """
  @spec admin_request_command(String.t(), String.t()) :: {atom(), struct()}
  def admin_request_command(verb, args) do
    {:admin_request, %RunnerCommands.AdminRequest{verb: verb, args: args}}
  end

  @doc """
  The `SpawnStepsRequest` command tuple. No namespace rides it: the
  controller resolves the request in the namespace of the connection it
  arrived on.
  """
  @spec spawn_steps_request_command(
          String.t(),
          String.t(),
          binary(),
          :deliver | :error,
          String.t()
        ) :: {atom(), struct()}
  def spawn_steps_request_command(instance_id, factory_id, payload, if_running, identity) do
    {:spawn_steps_request,
     %RunnerCommands.SpawnStepsRequest{
       instance_id: instance_id,
       factory_id: factory_id,
       payload: payload,
       if_running: Atom.to_string(if_running),
       identity: identity
     }}
  end

  @doc """
  The `EffectResult` command tuple: a `ctx.effect` result for the controller
  to keep under `identity`, for the input at `offset` the Cell `cell` took.
  """
  @spec effect_result_command(
          String.t(),
          String.t(),
          non_neg_integer(),
          String.t(),
          binary(),
          binary()
        ) :: {atom(), struct()}
  def effect_result_command(relay_id, identity, offset, name, payload, cell) do
    {:effect_result,
     %RunnerCommands.EffectResult{
       relay_id: relay_id,
       identity: identity,
       offset: offset,
       name: name,
       payload: payload,
       cell_identity: cell
     }}
  end

  @doc """
  The `RelayExited` command tuple for an instance whose process exited
  abnormally; the reason is rendered for a log line, bounded.
  """
  @spec relay_exited_command(String.t(), term()) :: {atom(), struct()}
  def relay_exited_command(relay_id, reason) do
    {:relay_exited,
     %RunnerCommands.RelayExited{
       relay_id: relay_id,
       reason: inspect(reason, limit: 20, printable_limit: 256)
     }}
  end

  @doc "The `TaskCommit` command tuple for one completed stage."
  @spec task_commit_command(
          String.t(),
          non_neg_integer(),
          non_neg_integer(),
          [map()],
          [
            {non_neg_integer(), binary()}
          ],
          boolean(),
          boolean(),
          binary(),
          map() | nil
        ) :: {atom(), struct()}
  def task_commit_command(
        relay_id,
        task_idx,
        iter,
        cells_ran,
        handoff,
        compensating,
        completes,
        result,
        trigger \\ nil
      ) do
    {:task_commit,
     %RunnerCommands.TaskCommit{
       relay_id: relay_id,
       task_idx: task_idx,
       iter: iter,
       cells_ran: Enum.map(cells_ran, &committed_cell/1),
       handoff: handoff_items(handoff),
       compensating: compensating,
       completes: completes,
       result: result,
       trigger: timer_trigger(trigger)
     }}
  end

  # One Cell a stage ran, with the records it wrote there.
  defp committed_cell(%{cell_id: id, first_seq: first, seq: last, channels: channels}),
    do: %RunnerCommands.CommittedCell{
      cell_id: id,
      first_seq: first,
      seq: last,
      channels: channels
    }

  # The firing a timer-fed pass's root stage names: its Cell's key, kind and
  # scheduled timestamp.
  defp timer_trigger(nil), do: nil

  defp timer_trigger(%{cell_key: key, kind: kind, at: at}),
    do: %RunnerCommands.TimerTrigger{
      cell_key: key || "",
      event_time: kind == :event_time,
      scheduled_at: at
    }

  @doc "The `IterCommit` command tuple for one finished iteration."
  @spec iter_commit_command(
          String.t(),
          non_neg_integer(),
          non_neg_integer() | nil,
          [{non_neg_integer(), binary()}],
          boolean(),
          binary(),
          String.t()
        ) :: {atom(), struct()}
  def iter_commit_command(
        relay_id,
        iter,
        consumed_offset,
        handoff,
        completes,
        result,
        entry_channel,
        dead_letter \\ nil
      ) do
    {:iter_commit,
     %RunnerCommands.IterCommit{
       relay_id: relay_id,
       iter: iter,
       consumed_offset: consumed_offset,
       handoff: handoff_items(handoff),
       completes: completes,
       result: result,
       entry_channel: entry_channel,
       dead_letter: dead_letter && struct(RunnerCommands.DeadLetter, dead_letter)
     }}
  end

  @doc """
  The `IterPark` command tuple for a loop that found its inbox empty, carrying
  the highest input-log offset the run parked to the dead-letter queue unread
  (`nil` when none).
  """
  @spec iter_park_command(
          String.t(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer() | nil
        ) :: {atom(), struct()}
  def iter_park_command(relay_id, iter, wake_deadline_ms, consumed_offset) do
    {:iter_park,
     %RunnerCommands.IterPark{
       relay_id: relay_id,
       iter: iter,
       wake_deadline_ms: wake_deadline_ms,
       consumed_offset: consumed_offset
     }}
  end

  @doc "The `RelayCompleted` command tuple for a terminated instance."
  @spec relay_completed_command(String.t(), String.t(), String.t(), binary()) ::
          {atom(), struct()}
  def relay_completed_command(relay_id, status, error, result) do
    {:relay_completed,
     %RunnerCommands.RelayCompleted{
       relay_id: relay_id,
       status: status,
       error: error,
       result: result
     }}
  end

  @doc "The committed task-journal a bind carried, as plain Elixir terms."
  @spec journal_facts(struct() | nil) :: map() | nil
  def journal_facts(nil), do: nil

  def journal_facts(%RunnerCommands.TaskJournalState{} = j) do
    %{
      committed_task_idxs: j.committed_task_idxs,
      iter: j.iter,
      entry_channel: j.entry_channel,
      handoff: Enum.map(j.handoff, &{&1.channel_id, &1.payload}),
      task_handoffs:
        Map.new(j.task_handoffs, fn e ->
          {e.task_idx, Enum.map(e.handoff, &{&1.channel_id, &1.payload})}
        end),
      committed_tasks: Enum.map(j.committed_tasks, &committed_task/1),
      consumed_offset: j.consumed_offset,
      released_offset: j.released_offset,
      rewind_undo: Enum.map(j.rewind_undo, &committed_task/1),
      completion: completion(j.completed, j.result)
    }
  end

  defp committed_task(%RunnerCommands.TaskCommittedTask{} = p),
    do: %{
      task_idx: p.task_idx,
      iter: p.iter,
      cells_ran: p.cells_ran,
      completion: completion(p.completes, p.result),
      trigger: journaled_trigger(p.trigger)
    }

  defp journaled_trigger(nil), do: nil

  defp journaled_trigger(%RunnerCommands.TimerTrigger{} = t),
    do: %{
      cell_key: if(t.cell_key == "", do: nil, else: t.cell_key),
      kind: if(t.event_time, do: :event_time, else: :processing_time),
      at: t.scheduled_at
    }

  defp completion(true, result), do: result
  defp completion(false, _result), do: nil

  defp handoff_items(handoff) do
    Enum.map(handoff, fn {channel_id, payload} ->
      %RunnerCommands.TaskHandoffItem{channel_id: channel_id, payload: payload}
    end)
  end

  defp encode(message) do
    {:ok, iodata, _size} = Protox.encode(message)
    IO.iodata_to_binary(iodata)
  end

  # An error as the wire's `Error` message.
  defp wire_error(%Error{} = error) do
    %RunnerCommands.Error{
      code: error.code,
      class: Atom.to_string(error.class),
      message: error.message,
      details_json: Error.details_json(error),
      hint: error.hint || ""
    }
  end
end
