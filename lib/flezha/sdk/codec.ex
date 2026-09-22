defmodule Flezha.SDK.Codec do
  @moduledoc """
  The single protobuf seam for the Elixir SDK's control channel. Encodes and
  decodes `RunnerCommands.CommandRequest`/`CommandResponse` through the modules
  `Flezha.SDK.Proto` generates.
  """

  alias Flezha.SDK.Proto.RunnerCommands
  alias Flezha.SDK.Topology
  alias Flezha.SDK.Workflow.Spec

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
  @spec encode_response(String.t(), String.t(), [String.t()], binary()) :: binary()
  def encode_response(reference_id, status, errors \\ [], raw_data \\ <<>>) do
    response = %RunnerCommands.CommandResponse{
      reference_id: reference_id,
      status: status,
      errors: errors,
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

  @doc "Encode an `AddOperativeResponse` metadata map into its raw_data bytes."
  @spec encode_add_operative_response(map()) :: binary()
  def encode_add_operative_response(metadata) when is_map(metadata) do
    {:ok, iodata, _size} = Protox.encode(%RunnerCommands.AddOperativeResponse{metadata: metadata})
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
  def shutdown_notification_command(operative_id, serialized_state) do
    {:runner_shutdown_notification,
     %RunnerCommands.RunnerShutdownNotification{
       operative_id: operative_id,
       serialized_state: serialized_state
     }}
  end

  @doc "Build the `UpdateFactories` command tuple from a list of `{id, description, kind}`."
  @spec update_factories_command([{String.t(), String.t() | nil, String.t()}]) ::
          {atom(), struct()}
  def update_factories_command(factories) do
    decls =
      Enum.map(factories, fn {id, desc, kind} ->
        %RunnerCommands.FactoryDecl{
          id: id,
          description: desc || "",
          sdk_lang: @sdk_lang,
          kind: kind
        }
      end)

    {:update_factories, %RunnerCommands.UpdateFactories{factories: decls}}
  end

  @doc """
  The kind a registered class is announced as. `Flezha.SDK.register_factory/2`
  builds a workflow spec for a module of the workflow behaviour and a topology
  for a stream, so the class it registered already states the kind.
  """
  @spec kind(Spec.t() | Topology.t()) :: String.t()
  def kind(%Spec{}), do: "workflow"
  def kind(%Topology{}), do: "stream"

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
  The `SpawnWorkflowRequest` command tuple. No namespace rides it: the
  controller resolves the request in the namespace of the connection it
  arrived on.
  """
  @spec spawn_workflow_request_command(String.t(), String.t(), binary(), :deliver | :error) ::
          {atom(), struct()}
  def spawn_workflow_request_command(instance_id, factory_id, payload, if_running) do
    {:spawn_workflow_request,
     %RunnerCommands.SpawnWorkflowRequest{
       instance_id: instance_id,
       factory_id: factory_id,
       payload: payload,
       if_running: Atom.to_string(if_running)
     }}
  end

  @doc "The `TaskCommit` command tuple for one completed stage."
  @spec task_commit_command(
          String.t(),
          non_neg_integer(),
          non_neg_integer(),
          [binary()],
          [
            {non_neg_integer(), binary()}
          ],
          [map()],
          boolean(),
          boolean(),
          binary()
        ) :: {atom(), struct()}
  def task_commit_command(
        operative_id,
        task_idx,
        iter,
        cells_ran,
        handoff,
        outbox,
        compensating,
        completes,
        result
      ) do
    {:task_commit,
     %RunnerCommands.TaskCommit{
       operative_id: operative_id,
       task_idx: task_idx,
       iter: iter,
       cells_ran: cells_ran,
       handoff: handoff_items(handoff),
       outbox: Enum.map(outbox, &outbox_item/1),
       compensating: compensating,
       completes: completes,
       result: result
     }}
  end

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
        operative_id,
        iter,
        consumed_offset,
        handoff,
        completes,
        result,
        entry_channel
      ) do
    {:iter_commit,
     %RunnerCommands.IterCommit{
       operative_id: operative_id,
       iter: iter,
       consumed_offset: consumed_offset,
       handoff: handoff_items(handoff),
       completes: completes,
       result: result,
       entry_channel: entry_channel
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
  def iter_park_command(operative_id, iter, wake_deadline_ms, consumed_offset) do
    {:iter_park,
     %RunnerCommands.IterPark{
       operative_id: operative_id,
       iter: iter,
       wake_deadline_ms: wake_deadline_ms,
       consumed_offset: consumed_offset
     }}
  end

  @doc "The `WorkflowCompleted` command tuple for a terminated instance."
  @spec workflow_completed_command(String.t(), String.t(), String.t(), binary()) ::
          {atom(), struct()}
  def workflow_completed_command(operative_id, status, error, result) do
    {:workflow_completed,
     %RunnerCommands.WorkflowCompleted{
       operative_id: operative_id,
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
      rewind_undo: Enum.map(j.rewind_undo, &committed_task/1),
      completion: completion(j.completed, j.result)
    }
  end

  defp committed_task(%RunnerCommands.TaskCommittedTask{} = p),
    do: %{
      task_idx: p.task_idx,
      iter: p.iter,
      cells_ran: p.cells_ran,
      completion: completion(p.completes, p.result)
    }

  defp completion(true, result), do: result
  defp completion(false, _result), do: nil

  defp handoff_items(handoff) do
    Enum.map(handoff, fn {channel_id, payload} ->
      %RunnerCommands.TaskHandoffItem{channel_id: channel_id, payload: payload}
    end)
  end

  defp outbox_item(%{cell_id: id, seq_in_task: seq, channel: channel, payload: payload}) do
    %RunnerCommands.TaskOutboxItem{
      cell_id: id,
      seq_in_task: seq,
      channel: channel,
      payload: payload
    }
  end

  defp encode(message) do
    {:ok, iodata, _size} = Protox.encode(message)
    IO.iodata_to_binary(iodata)
  end
end
