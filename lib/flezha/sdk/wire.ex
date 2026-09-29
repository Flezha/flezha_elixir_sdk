defmodule Flezha.SDK.Wire do
  @moduledoc """
  Wire-protocol codec for the per-Cell persistence path.

  Two payload-id strings are reserved on the data stream, both SDK ->
  controller: `"cell_state_change"`, carrying one Change record destined for
  the per-Cell WAL, and `"cell_state_snapshot"`, carrying a serialized
  `Self::State` produced by a controller-initiated `CompactCell`. Both share
  one frame structure — the difference is only what the body means — and that
  structure is the protocol's.

  `cell_id_bytes` is structured rather than opaque so the controller can route
  a frame to the right per-Cell `WalWriter` without a side table. The
  controller's `runner_id` is implicit: the controller knows which runner sent
  the frame from the connection it arrived on.
  """

  @payload_id_cell_state_change "cell_state_change"
  @payload_id_cell_state_snapshot "cell_state_snapshot"
  @payload_id_cell_effect_record "cell_effect_record"
  @payload_id_delete_cell_state "delete_cell_state"
  @payload_id_compensation_intent "compensation_intent"
  @payload_id_compensation_done "compensation_done"
  @payload_id_park_payload "park_payload"

  alias Flezha.SDK.RecordFrame

  @header_size RecordFrame.header_size()
  @format_id_effect_record RecordFrame.format_id(:effect_record)

  @spec payload_id_cell_state_change() :: binary()
  def payload_id_cell_state_change(), do: @payload_id_cell_state_change

  @doc """
  Payload-id for an atomic effect record (effectively-once tier,
  SDK → controller): one invocation's state change(s), plugin outputs, and
  consumed input-log offset, persisted as ONE WAL record under ONE fsync.
  Same frame layout as `cell_state_change` with a `format_id = 2` header;
  the body is the postcard `EffectRecordBody` decoded by
  `decode_effect_body/1`.
  """
  @spec payload_id_cell_effect_record() :: binary()
  def payload_id_cell_effect_record(), do: @payload_id_cell_effect_record

  @spec payload_id_cell_state_snapshot() :: binary()
  def payload_id_cell_state_snapshot(), do: @payload_id_cell_state_snapshot

  @doc """
  Payload-id for the dynamic-mode hard-delete RPC (SDK → controller). The
  frame body is the bare structured `cell_id` (`decode_cell_id/1`);
  there is no seq / header / crc. The SDK ships it under the durable
  guarantee class when it evicts a persistence-enabled dynamic Cell, so a
  stream that is down holds the frame for the rebind rather than losing it,
  and the controller drops the WAL/snapshot/outbox files so a later recreate
  cold-starts fresh.

  The response (`{cell_id, status}`) is returned synchronously to the
  CLI delete path (`flz state delete`); the reaper path does not read a
  reply (fire-and-forget), so no response frame is written back on the
  data stream.
  """
  @spec payload_id_delete_cell_state() :: binary()
  def payload_id_delete_cell_state(), do: @payload_id_delete_cell_state

  @doc """
  Payload-ids reserved on the data stream for the bounded-operator saga
  change-log (SDK → controller), same frame layout as `cell_state_change`:
  `compensation_intent` records a tasklet's completion (so a crash
  mid-compensation knows what to undo) and `compensation_done` records a
  compensation hook's completion (so a respawn skips already-undone steps).
  """
  @spec payload_id_compensation_intent() :: binary()
  def payload_id_compensation_intent(), do: @payload_id_compensation_intent

  @spec payload_id_compensation_done() :: binary()
  def payload_id_compensation_done(), do: @payload_id_compensation_done

  @doc """
  Payload-id for a dead-letter park (SDK → controller, tier-off sessions):
  a poison payload whose retry budget is exhausted, shipped with its error
  context for the durable DLQ store. Same frame layout as
  `cell_state_change` (zero header); the body is the postcard `ParkBody`
  decoded by `decode_park_body/1`. The controller answers with a
  `ParkPayloadAck` control command once the entry is fsynced.
  """
  @spec payload_id_park_payload() :: binary()
  def payload_id_park_payload(), do: @payload_id_park_payload

  @doc """
  Encode a `cell_id` triple into its on-wire byte form.
  """
  @spec encode_cell_id(binary(), binary(), binary()) :: binary()
  def encode_cell_id(operator_class, tasklet_id, cell_key)
      when is_binary(operator_class) and is_binary(tasklet_id) and is_binary(cell_key) do
    rc_len = byte_size(operator_class)
    sh_len = byte_size(tasklet_id)
    pk_len = byte_size(cell_key)

    if rc_len > 255 or sh_len > 255 do
      raise ArgumentError, "operator_class / tasklet_id must be ≤ 255 bytes"
    end

    if pk_len > 0xFFFF do
      raise ArgumentError, "cell_key must be ≤ 65535 bytes"
    end

    <<rc_len::8, operator_class::binary, sh_len::8, tasklet_id::binary, pk_len::16-big,
      cell_key::binary>>
  end

  @doc """
  Decode the on-wire `cell_id_bytes` into `{operator_class, tasklet_id, cell_key}`.
  """
  @spec decode_cell_id(binary()) :: {:ok, {binary(), binary(), binary()}} | {:error, term()}
  def decode_cell_id(
        <<rc_len::8, operator_class::binary-size(rc_len), sh_len::8,
          tasklet_id::binary-size(sh_len), pk_len::16-big, cell_key::binary-size(pk_len)>>
      ) do
    {:ok, {operator_class, tasklet_id, cell_key}}
  end

  def decode_cell_id(_), do: {:error, :bad_cell_id}

  @doc """
  The reply-frame metadata key carrying the `change_seq` a single selected
  Cell was serialized at.

  One Elixir owner beside `flz_core`'s `CHANGE_SEQ_META_KEY`.
  """
  @spec change_seq_key() :: binary()
  def change_seq_key, do: "flz.change_seq"

  @doc """
  Assemble a durable frame: the Cell identity, its seq, the 16-byte record
  header and the body under its CRC. The inverse of `decode_frame/1` and the
  byte-for-byte twin of the runner core's `build_cell_frame`, so one
  controller handler serves every SDK.
  """
  @spec encode_frame(binary(), non_neg_integer(), binary(), binary()) :: binary()
  def encode_frame(cell_id, seq, header, body)
      when byte_size(header) == @header_size do
    <<byte_size(cell_id)::16-big, cell_id::binary, seq::64-big, header::binary,
      byte_size(body)::32-big, body::binary, :erlang.crc32(body)::32-big>>
  end

  @doc """
  Decode an inbound `cell_state_change` / `cell_state_snapshot`
  frame.

  Returns `{:ok, %{cell_id: {operator_class, tasklet_id, cell_key},
                   seq: u64, header: <<_::128>>, body: bytes}}` or an error.
  Verifies the trailing crc32 over the body.
  """
  @spec decode_frame(binary()) ::
          {:ok,
           %{
             cell_id: {binary(), binary(), binary()},
             seq: non_neg_integer(),
             header: <<_::128>>,
             body: binary()
           }}
          | {:error, term()}
  def decode_frame(<<cell_id_len::16-big, rest::binary>>) do
    case rest do
      <<cell_id_bytes::binary-size(^cell_id_len), seq::64-big, header::binary-size(@header_size),
        body_len::32-big, body::binary-size(body_len), crc::32-big>> ->
        if :erlang.crc32(body) == crc do
          case decode_cell_id(cell_id_bytes) do
            {:ok, identity} ->
              {:ok, %{cell_id: identity, seq: seq, header: header, body: body}}

            err ->
              err
          end
        else
          {:error, :crc_mismatch}
        end

      _ ->
        {:error, :short_read}
    end
  end

  def decode_frame(_), do: {:error, :short_read}

  @doc """
  Whether a WAL record header marks an effect record (`format_id = 2` at
  header byte 9) — the durable marker that keeps mixed WALs readable.
  """
  @spec effect_record_header?(binary()) :: boolean()
  def effect_record_header?(<<_::binary-size(9), format_id::8, _::binary>>),
    do: format_id == @format_id_effect_record

  def effect_record_header?(_), do: false

  @doc """
  Decode a postcard `EffectRecordBody` (the body of a
  `cell_effect_record` frame / format-2 WAL record):

  ```
  changes:         Vec<Vec<u8>>            — postcard S::Change bytes each
  outputs:         Vec<OutputRecord>       — {channel, key, metadata, payload}
  consumed_offset: Option<u64>
  dlq:             Option<DlqEntryBody>    — {channel, error, attempts, payload}
  event_time:      u64                     — the invocation's declared clock value
  timers:          Vec<TimerOp>            — {kind, scheduled_at, register}
  ```

  `event_time` is the extracted event time under an `EventTimeFrom` clock —
  the column a late-input correction resolves its baseline seq against.

  `timers` carries the SDK's mutations of the Cell's pending-timer set. The
  controller stores them and hands them back on restore without interpreting
  them — the same property that makes `changes` SDK-owned bytes — but decoding
  the field is what keeps the body's trailing-byte check honest.

  Postcard encodes `Vec`/`String` as a LEB128 varint length + contents,
  tuples/structs as concatenated fields, and `Option` as `0x00 | 0x01 ++ T`.
  """
  @spec decode_effect_body(binary()) ::
          {:ok,
           %{
             changes: [binary()],
             outputs: [
               %{
                 channel: binary(),
                 key: binary(),
                 metadata: [{binary(), binary()}],
                 payload: binary()
               }
             ],
             consumed_offset: non_neg_integer() | nil,
             dlq: map() | nil,
             event_time: non_neg_integer(),
             timers: [
               %{
                 kind: :event_time | :processing_time,
                 scheduled_at: non_neg_integer(),
                 register: boolean()
               }
             ],
             edges: [edge()]
           }}
          | {:error, term()}
  def decode_effect_body(body) when is_binary(body) do
    with {:ok, changes, rest} <- decode_vec(body, &decode_bytes/1),
         {:ok, outputs, rest} <- decode_vec(rest, &decode_output/1),
         {:ok, consumed_offset, rest} <- decode_option(rest, &decode_varint/1),
         {:ok, dlq, rest} <- decode_option(rest, &decode_dlq_entry/1),
         {:ok, event_time, rest} <- decode_varint(rest),
         {:ok, timers, rest} <- decode_vec(rest, &decode_timer_op/1),
         {:ok, edges, <<>>} <- decode_vec(rest, &decode_edge/1) do
      {:ok,
       %{
         changes: changes,
         outputs: outputs,
         consumed_offset: consumed_offset,
         dlq: dlq,
         event_time: event_time,
         timers: timers,
         edges: edges
       }}
    else
      {:ok, _, _trailing} -> {:error, :trailing_bytes}
      {:error, _} = err -> err
    end
  end

  @doc """
  Encode an effect body back to postcard bytes (byte-exact inverse of
  `decode_effect_body/1`). Used by tests and the WAL crash-tear fixtures.
  """
  @spec encode_effect_body(%{
          required(:changes) => [binary()],
          required(:outputs) => [
            %{
              channel: binary(),
              key: binary(),
              metadata: [{binary(), binary()}],
              payload: binary()
            }
          ],
          required(:consumed_offset) => non_neg_integer() | nil,
          optional(:dlq) => map() | nil,
          optional(:event_time) => non_neg_integer(),
          optional(:timers) => [map()],
          optional(:edges) => [edge()]
        }) :: binary()
  def encode_effect_body(%{changes: changes, outputs: outputs, consumed_offset: offset} = body) do
    IO.iodata_to_binary([
      encode_vec(changes, &encode_bytes/1),
      encode_vec(outputs, &encode_output/1),
      encode_option(offset, &encode_varint/1),
      encode_option(Map.get(body, :dlq), &encode_dlq_entry/1),
      encode_varint(Map.get(body, :event_time, 0)),
      encode_vec(Map.get(body, :timers, []), &encode_timer_op/1),
      encode_vec(Map.get(body, :edges, []), &encode_edge/1)
    ])
  end

  @typedoc """
  One inter-tasklet value an effect record keeps: the channel it was emitted
  on and the consuming tasklet, the input-log offset it derives from, and the
  value through the channel's codec.
  """
  @type edge :: %{
          channel: non_neg_integer(),
          consumer: non_neg_integer(),
          origin: non_neg_integer() | nil,
          payload: binary()
        }

  defp decode_edge(bin) do
    with {:ok, channel, rest} <- decode_varint(bin),
         {:ok, consumer, rest} <- decode_varint(rest),
         {:ok, origin, rest} <- decode_option(rest, &decode_varint/1),
         {:ok, payload, rest} <- decode_bytes(rest) do
      {:ok, %{channel: channel, consumer: consumer, origin: origin, payload: payload}, rest}
    end
  end

  defp encode_edge(%{channel: channel, consumer: consumer, origin: origin, payload: payload}) do
    [
      encode_varint(channel),
      encode_varint(consumer),
      encode_option(origin, &encode_varint/1),
      encode_bytes(payload)
    ]
  end

  @typedoc "One mutation of a Cell's pending-timer set, as the postcard `TimerOp` carries it."
  @type timer_op :: %{
          kind: :event_time | :processing_time,
          scheduled_at: non_neg_integer(),
          register: boolean()
        }

  @doc """
  The body of a `format_id = 3` timer-ops record — a postcard `Vec<TimerOp>`,
  the shape every SDK writes for its pending-timer mutations and the fold of
  any of them reads back; `decode_timer_ops/1` is its exact inverse.
  """
  @spec encode_timer_ops([timer_op()]) :: binary()
  def encode_timer_ops(ops), do: IO.iodata_to_binary(encode_vec(ops, &encode_timer_op/1))

  @spec decode_timer_ops(binary()) :: {:ok, [timer_op()]} | {:error, term()}
  def decode_timer_ops(body) when is_binary(body) do
    case decode_vec(body, &decode_timer_op/1) do
      {:ok, ops, <<>>} -> {:ok, ops}
      {:ok, _ops, _trailing} -> {:error, :trailing_bytes}
      {:error, _} = err -> err
    end
  end

  @doc """
  Decode a postcard `ParkBody` (the body of a `park_payload` frame,
  tier-off dead-letter park):

  ```
  park_id:       String              — SDK-minted ack-correlation id
  origin_offset: Option<u64>
  entry:         DlqEntryBody        — {channel, error, attempts, payload},
                                       the same record a tier-on park embeds
                                       in its effect record
  reply_to:      Option<String>      — the token of the outside call waiting
                                       on the parked input
  ```

  Returned flattened (`park_id`, `origin_offset` and `reply_to` merged into
  the entry map) — the store consumes one shape for both park paths.
  """
  @spec decode_park_body(binary()) ::
          {:ok,
           %{
             park_id: binary(),
             channel: binary(),
             origin_offset: non_neg_integer() | nil,
             error: binary(),
             attempts: non_neg_integer(),
             payload: binary() | nil,
             reply_to: binary() | nil
           }}
          | {:error, term()}
  def decode_park_body(body) when is_binary(body) do
    with {:ok, park_id, rest} <- decode_bytes(body),
         {:ok, origin_offset, rest} <- decode_option(rest, &decode_varint/1),
         {:ok, entry, rest} <- decode_dlq_entry(rest),
         {:ok, reply_to, <<>>} <- decode_option(rest, &decode_bytes/1) do
      {:ok,
       Map.merge(entry, %{park_id: park_id, origin_offset: origin_offset, reply_to: reply_to})}
    else
      {:ok, _, _trailing} -> {:error, :trailing_bytes}
      {:error, _} = err -> err
    end
  end

  @doc """
  Encode a `ParkBody` to postcard bytes — the exact inverse of
  `decode_park_body/1`, byte-compatible with the cross-language fixture
  (`flz_core/src/park.rs`, `test/flezha/state/wire_test.exs`). The map carries
  the flattened DLQ entry: `park_id`, `origin_offset`, `channel`, `error`,
  `attempts`, `payload`, `reply_to`. Sharing the module's postcard helpers is what keeps the
  SDK's park frame from drifting from the decoder.
  """
  @spec encode_park_body(%{
          required(:park_id) => binary(),
          optional(:origin_offset) => non_neg_integer() | nil,
          required(:channel) => binary(),
          required(:error) => binary(),
          required(:attempts) => non_neg_integer(),
          optional(:payload) => binary() | nil,
          optional(:reply_to) => binary() | nil
        }) :: binary()
  def encode_park_body(
        %{park_id: park_id, channel: channel, error: error, attempts: attempts} = body
      ) do
    entry = %{
      channel: channel,
      error: error,
      attempts: attempts,
      payload: Map.get(body, :payload)
    }

    IO.iodata_to_binary([
      encode_bytes(park_id),
      encode_option(Map.get(body, :origin_offset), &encode_varint/1),
      encode_dlq_entry(entry),
      encode_option(Map.get(body, :reply_to), &encode_bytes/1)
    ])
  end

  @doc """
  Decode a WAL record as an effect record, or `nil` when it is not one (a plain
  change record, or a body this build cannot read).

  The header check and the body decode always travel together — every reader
  that wants an effect record's fields wants exactly this — so they live here,
  beside the codec they belong to, rather than being re-paired at each call site.
  """
  @spec decode_effect_record(binary(), binary()) :: map() | nil
  def decode_effect_record(header, body) do
    with true <- effect_record_header?(header),
         {:ok, decoded} <- decode_effect_body(body) do
      decoded
    else
      _ -> nil
    end
  end

  @doc """
  Encode a postcard `CorrectionRequestBody`: the mirror of
  `decode_correction_request/1`, one varint and nothing else.
  """
  @spec encode_correction_request(non_neg_integer()) :: binary()
  def encode_correction_request(target_event_time)
      when is_integer(target_event_time) and target_event_time >= 0,
      do: encode_varint(target_event_time)

  @doc """
  Decode a postcard `CorrectionRequestBody` (the body of a `correction_request`
  frame): a single `target_event_time: u64`. The Cell identity rides the
  frame envelope, so the body carries nothing else — the controller re-reads the
  input-log range itself, because the late input is already in it.
  """
  @spec decode_correction_request(binary()) :: {:ok, non_neg_integer()} | {:error, term()}
  def decode_correction_request(body) when is_binary(body) do
    case decode_varint(body) do
      {:ok, target, <<>>} -> {:ok, target}
      {:ok, _, _trailing} -> {:error, :trailing_bytes}
      {:error, _} = err -> err
    end
  end

  # ---------- postcard primitives ----------

  defp decode_vec(bin, item_fun) do
    case decode_varint(bin) do
      {:ok, count, rest} -> decode_items(rest, count, item_fun, [])
      err -> err
    end
  end

  defp decode_items(rest, 0, _fun, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_items(rest, n, fun, acc) do
    case fun.(rest) do
      {:ok, item, rest} -> decode_items(rest, n - 1, fun, [item | acc])
      err -> err
    end
  end

  defp decode_bytes(bin) do
    with {:ok, len, rest} <- decode_varint(bin),
         <<bytes::binary-size(^len), rest::binary>> <- rest do
      {:ok, bytes, rest}
    else
      {:error, _} = err -> err
      _ -> {:error, :short_read}
    end
  end

  defp decode_output(bin) do
    with {:ok, channel, rest} <- decode_bytes(bin),
         {:ok, key, rest} <- decode_bytes(rest),
         {:ok, metadata, rest} <- decode_vec(rest, &decode_pair/1),
         {:ok, payload, rest} <- decode_bytes(rest) do
      {:ok, %{channel: channel, key: key, metadata: metadata, payload: payload}, rest}
    end
  end

  defp decode_pair(bin) do
    with {:ok, k, rest} <- decode_bytes(bin),
         {:ok, v, rest} <- decode_bytes(rest) do
      {:ok, {k, v}, rest}
    end
  end

  defp decode_dlq_entry(bin) do
    with {:ok, channel, rest} <- decode_bytes(bin),
         {:ok, error, rest} <- decode_bytes(rest),
         {:ok, attempts, rest} <- decode_varint(rest),
         {:ok, payload, rest} <- decode_option(rest, &decode_bytes/1) do
      {:ok, %{channel: channel, error: error, attempts: attempts, payload: payload}, rest}
    end
  end

  defp encode_dlq_entry(%{channel: channel, error: error, attempts: attempts, payload: payload}) do
    [
      encode_bytes(channel),
      encode_bytes(error),
      encode_varint(attempts),
      encode_option(payload, &encode_bytes/1)
    ]
  end

  # One pending-timer mutation: `[kind varint][scheduled_at varint][register u8]`.
  # The kind's ordinal order is the SDKs' `TimerKind`: 0 event time, 1 processing
  # time.
  defp decode_timer_op(bin) do
    with {:ok, kind, rest} <- decode_varint(bin),
         {:ok, scheduled_at, rest} <- decode_varint(rest),
         <<register::8, rest::binary>> <- rest,
         true <- register <= 1 do
      {:ok, %{kind: timer_kind(kind), scheduled_at: scheduled_at, register: register == 1}, rest}
    else
      {:error, _} = err -> err
      _ -> {:error, :bad_timer_op}
    end
  end

  defp timer_kind(0), do: :event_time
  defp timer_kind(1), do: :processing_time
  defp timer_kind(other), do: {:unknown, other}

  defp encode_timer_op(%{kind: kind, scheduled_at: scheduled_at, register: register}) do
    [
      encode_varint(timer_kind_ordinal(kind)),
      encode_varint(scheduled_at),
      <<if(register, do: 1, else: 0)>>
    ]
  end

  defp timer_kind_ordinal(:event_time), do: 0
  defp timer_kind_ordinal(:processing_time), do: 1

  defp decode_option(<<0, rest::binary>>, _item_fun), do: {:ok, nil, rest}
  defp decode_option(<<1, rest::binary>>, item_fun), do: item_fun.(rest)
  defp decode_option(_, _), do: {:error, :bad_option}

  defp encode_option(nil, _item_fun), do: <<0>>
  defp encode_option(value, item_fun), do: [<<1>>, item_fun.(value)]

  defp decode_varint(bin, shift \\ 0, acc \\ 0)

  defp decode_varint(<<byte::8, rest::binary>>, shift, acc) when byte < 128,
    do: {:ok, acc + Bitwise.bsl(byte, shift), rest}

  defp decode_varint(<<byte::8, rest::binary>>, shift, acc) when shift < 64,
    do: decode_varint(rest, shift + 7, acc + Bitwise.bsl(byte - 128, shift))

  defp decode_varint(_, _, _), do: {:error, :bad_varint}

  defp encode_vec(items, item_fun),
    do: [encode_varint(length(items)) | Enum.map(items, item_fun)]

  defp encode_bytes(bytes), do: [encode_varint(byte_size(bytes)), bytes]

  defp encode_output(%{channel: channel, key: key, metadata: metadata, payload: payload}) do
    [
      encode_bytes(channel),
      encode_bytes(key),
      encode_vec(metadata, fn {k, v} -> [encode_bytes(k), encode_bytes(v)] end),
      encode_bytes(payload)
    ]
  end

  defp encode_varint(n) when n < 128, do: <<n>>

  defp encode_varint(n),
    do: <<Bitwise.bor(Bitwise.band(n, 127), 128), encode_varint(Bitwise.bsr(n, 7))::binary>>
end
