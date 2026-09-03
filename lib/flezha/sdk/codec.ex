defmodule Flezha.SDK.Codec do
  @moduledoc """
  The single protobuf seam for the Elixir SDK's control channel. Encodes and
  decodes `RunnerCommands.CommandRequest`/`CommandResponse` via the already
  compiled `Flezha.Runner.CommandsProto` Protox modules. No other SDK or
  native-transport module may reference Protox or `RunnerCommands` — the
  packaging-boundary gate (`scripts/check-sdk-boundary.sh`) enforces it, so the
  later hex extraction swaps only this module's internals.
  """

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
  def encode_restore_response(entity_id, terminal_seq, migrated) do
    encode(%RunnerCommands.RestoreInitStateResponse{
      entity_id: entity_id,
      terminal_seq: terminal_seq,
      migrated: migrated
    })
  end

  @doc "Encode a `CompactEntityResponse` — the raw_data of a `CompactEntity` reply."
  @spec encode_compact_response(binary(), non_neg_integer(), non_neg_integer()) :: binary()
  def encode_compact_response(entity_id, snapshot_seq, snapshot_bytes_size) do
    encode(%RunnerCommands.CompactEntityResponse{
      entity_id: entity_id,
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

  @doc "Build the `UpdateFactories` command tuple from a list of `{id, description}`."
  @spec update_factories_command([{String.t(), String.t() | nil}]) :: {atom(), struct()}
  def update_factories_command(factories) do
    decls =
      Enum.map(factories, fn {id, desc} ->
        %RunnerCommands.FactoryDecl{id: id, description: desc || "", sdk_lang: @sdk_lang}
      end)

    {:update_factories, %RunnerCommands.UpdateFactories{factories: decls}}
  end

  @doc "The `ProcCommit` command tuple for one completed stage."
  @spec proc_commit_command(
          String.t(),
          non_neg_integer(),
          non_neg_integer(),
          [binary()],
          [
            {non_neg_integer(), binary()}
          ],
          [map()],
          boolean()
        ) :: {atom(), struct()}
  def proc_commit_command(
        operative_id,
        proc_idx,
        iter,
        entities_ran,
        handoff,
        outbox,
        compensating
      ) do
    {:proc_commit,
     %RunnerCommands.ProcCommit{
       operative_id: operative_id,
       proc_idx: proc_idx,
       iter: iter,
       entities_ran: entities_ran,
       handoff: handoff_items(handoff),
       outbox: Enum.map(outbox, &outbox_item/1),
       compensating: compensating
     }}
  end

  @doc "The `IterCommit` command tuple for one finished iteration."
  @spec iter_commit_command(String.t(), non_neg_integer(), non_neg_integer(), [
          {non_neg_integer(), binary()}
        ]) :: {atom(), struct()}
  def iter_commit_command(operative_id, iter, consumed_offset, handoff) do
    {:iter_commit,
     %RunnerCommands.IterCommit{
       operative_id: operative_id,
       iter: iter,
       consumed_offset: consumed_offset,
       handoff: handoff_items(handoff)
     }}
  end

  @doc "The `IterPark` command tuple for a loop that found its inbox empty."
  @spec iter_park_command(String.t(), non_neg_integer(), non_neg_integer()) :: {atom(), struct()}
  def iter_park_command(operative_id, iter, wake_deadline_ms) do
    {:iter_park,
     %RunnerCommands.IterPark{
       operative_id: operative_id,
       iter: iter,
       wake_deadline_ms: wake_deadline_ms
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

  @doc "The committed proc-journal a bind carried, as plain Elixir terms."
  @spec journal_facts(struct() | nil) :: map() | nil
  def journal_facts(nil), do: nil

  def journal_facts(%RunnerCommands.ProcJournalState{} = j) do
    %{
      committed_proc_idxs: j.committed_proc_idxs,
      iter: j.iter,
      handoff: Enum.map(j.handoff, &{&1.channel_id, &1.payload}),
      proc_handoffs:
        Map.new(j.proc_handoffs, fn e ->
          {e.proc_idx, Enum.map(e.handoff, &{&1.channel_id, &1.payload})}
        end),
      committed_procs: Enum.map(j.committed_procs, &committed_proc/1),
      consumed_offset: j.consumed_offset,
      parked: j.parked,
      wake_deadline_ms: j.wake_deadline_ms,
      rewind_undo: Enum.map(j.rewind_undo, &committed_proc/1)
    }
  end

  defp committed_proc(%RunnerCommands.ProcCommittedProc{} = p),
    do: %{proc_idx: p.proc_idx, iter: p.iter, entities_ran: p.entities_ran}

  defp handoff_items(handoff) do
    Enum.map(handoff, fn {channel_id, payload} ->
      %RunnerCommands.ProcHandoffItem{channel_id: channel_id, payload: payload}
    end)
  end

  defp outbox_item(%{entity_id: id, seq_in_proc: seq, channel: channel, payload: payload}) do
    %RunnerCommands.ProcOutboxItem{
      entity_id: id,
      seq_in_proc: seq,
      channel: channel,
      payload: payload
    }
  end

  defp encode(message) do
    {:ok, iodata, _size} = Protox.encode(message)
    IO.iodata_to_binary(iodata)
  end
end
