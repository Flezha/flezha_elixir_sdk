defmodule Flezha.SDK.RecordFrame do
  @moduledoc """
  The durable record frame every SDK writes and the controller stores: the
  16-byte header, the framed record around it, and the one decoder and byte walk
  over it.

  A record is `[seq: u64 BE][len: u32 BE][header: 16 B][body][crc32: u32 BE]`,
  self-delimiting and CRC-framed. There is no length at the tail, so a reader
  walks forward from the start and stops at the first record that does not
  decode. What it does then is decided by one rule, `repairable?/3`: a record
  log is truncated only where no complete record can begin after the failure
  point. A torn tail, the append protocol's own crash, satisfies that proof; a
  crc failure with complete records behind it does not, and is refused by name.

  Everything here is pure: it takes bytes and answers terms. Walking a file on a
  device is the controller's.
  """

  require Logger

  @header_size 16

  # The Elixir mirror of `flz_core`'s header allocation table: the writing SDK at
  # byte 0 and the body codec at byte 9. One row per allocated value, held to the
  # kernel's own by the contract set's `durable_frame.txt`.
  @sdk_langs %{
    0 => "unspecified",
    1 => "rust",
    2 => "java",
    3 => "elixir",
    4 => "node",
    5 => "ruby",
    6 => "php",
    7 => "python"
  }

  @format_ids %{
    postcard: 1,
    effect_record: 2,
    timer_ops: 3,
    kryo: 4,
    msgpack: 5,
    json: 6,
    etf: 7
  }

  @typedoc """
  A refused record: the decoder's own reason tag, the last seq that verified
  before it, and the byte offset the failure was read at.
  """
  @type refusal :: {reason :: atom(), last_seq :: integer(), offset :: non_neg_integer()}

  @typedoc """
  Where a fold's bytes came from: a record log at a path, which an append may
  have torn, or a change log shipped whole under a name.
  """
  @type source :: {:log, Path.t()} | {:payload, binary()}

  @typedoc "The 16 header bytes, decomposed."
  @type header :: %{
          sdk_lang: byte(),
          sdk_version: non_neg_integer(),
          schema_version: non_neg_integer(),
          format_id: byte(),
          crc32: non_neg_integer()
        }

  @doc "Bytes of the record header — the one definition."
  @spec header_size() :: pos_integer()
  def header_size(), do: @header_size

  @doc "The allocated name of an `sdk_lang` byte; `\"unallocated\"` for a value no SDK holds."
  @spec sdk_lang_name(byte()) :: binary()
  def sdk_lang_name(lang), do: Map.get(@sdk_langs, lang, "unallocated")

  @doc "The `format_id` byte allocated to a body codec."
  @spec format_id(atom()) :: byte()
  def format_id(codec), do: Map.fetch!(@format_ids, codec)

  @doc """
  Stamp the 16-byte header for `body`: language, SDK version, schema version,
  format, two reserved zero bytes, and the crc32 over the body.

  The write side of `decode_header/1`; the byte layout exists here and nowhere
  else on this side of the wire.
  """
  @spec encode_header(byte(), non_neg_integer(), non_neg_integer(), byte(), binary()) ::
          <<_::128>>
  def encode_header(sdk_lang, sdk_version, schema_version, format_id, body)
      when is_binary(body) do
    <<sdk_lang, sdk_version::32-big, schema_version::32-big, format_id, 0, 0,
      :erlang.crc32(body)::32-big>>
  end

  @doc """
  Decompose the 16 header bytes. `:error` when there are not 16 of them —
  every caller answers a malformed header its own way, and none of them
  re-reads the layout to do it.
  """
  @spec decode_header(binary()) :: {:ok, header()} | :error
  def decode_header(<<lang, version::32-big, schema::32-big, format, _reserved::16, crc::32-big>>) do
    {:ok,
     %{
       sdk_lang: lang,
       sdk_version: version,
       schema_version: schema,
       format_id: format,
       crc32: crc
     }}
  end

  def decode_header(_bytes), do: :error

  @doc """
  Frame a record for on-disk storage.
  """
  @spec encode_record(non_neg_integer(), binary(), binary()) :: binary()
  def encode_record(seq, header, change_bytes)
      when is_integer(seq) and seq >= 0 and is_binary(header) and is_binary(change_bytes) do
    if byte_size(header) != @header_size do
      raise ArgumentError, "header must be #{@header_size} bytes, got #{byte_size(header)}"
    end

    len = @header_size + byte_size(change_bytes)

    <<seq::64-big, len::32-big, header::binary, change_bytes::binary,
      crc32(header, change_bytes)::32-big>>
  end

  @doc """
  The crc32 over `header <> body` — the one expression in the tree.

  Public because the snapshot file, whose own format is not this frame, covers
  exactly the same two ranges with exactly the same crc; a second spelling of
  one value is a second thing to keep in step.
  """
  @spec crc32(binary(), binary()) :: non_neg_integer()
  def crc32(header, body), do: :erlang.crc32(<<header::binary, body::binary>>)

  @doc """
  Decode one record from the head of `bytes`. Returns the decoded
  `{seq, header, change_bytes}` triple, the record's own bytes verbatim and the
  byte length consumed, or the reason it did not decode and its framed extent
  where that is known.

  The one decoder: `decode_record/2` reads a device into it, and the two
  byte-oriented walks fold through it.
  """
  @spec decode_bytes(binary()) ::
          {:ok, non_neg_integer(), binary(), binary(), binary(), non_neg_integer()}
          | {:error, :crc_mismatch | :short_read | :bad_len, non_neg_integer() | nil}
  def decode_bytes(<<_seq::64-big, len::32-big, _tail::binary>>) when len < @header_size,
    do: {:error, :bad_len, nil}

  def decode_bytes(<<seq::64-big, len::32-big, tail::binary>> = bytes) do
    change_len = len - @header_size
    consumed = 12 + len + 4

    case tail do
      <<header::binary-size(@header_size), change_bytes::binary-size(^change_len), stored::32-big,
        _rest::binary>> ->
        if crc32(header, change_bytes) == stored do
          <<raw::binary-size(^consumed), _rest::binary>> = bytes
          {:ok, seq, header, change_bytes, raw, consumed}
        else
          # The extent is exact even though the content is not: the record's
          # `len` framed it and the bytes are all here. Carrying it is what
          # lets `repairable?/3` decide without a second read.
          {:error, :crc_mismatch, consumed}
        end

      _short ->
        {:error, :short_read, nil}
    end
  end

  def decode_bytes(_short), do: {:error, :short_read, nil}

  @doc """
  Whether a walk stopped by `reason`, with `remaining` bytes left from the
  failure point to the end of the file, may be repaired by truncating there.

  The one proof in the subsystem, stated once and applied at every entry point:
  a record log is truncated only where **no complete record can begin after the
  failure point**.

    * a read that ran off the end of the file has nothing behind it by
      construction;
    * a crc failure has an exact framed extent, so it is a torn tail exactly
      when that extent is the rest of the file — anything more and the records
      behind it are complete records the controller acknowledged;
    * a length field below the header size leaves the next record unlocatable,
      so it can only be judged by what remains: less than one whole record, and
      nothing can follow it.
  """
  @spec repairable?(atom(), non_neg_integer() | nil, non_neg_integer()) :: boolean()
  def repairable?(:short_read, _extent, _remaining), do: true
  def repairable?(:crc_mismatch, extent, remaining), do: extent == remaining
  def repairable?(:bad_len, _extent, remaining), do: remaining < 12 + @header_size + 4
  def repairable?(_reason, _extent, _remaining), do: false

  @doc """
  Report a refused record and shape the refusal every reader of this framing
  answers with.

  The one place a refusal's telemetry event and its error line are written, so a further reader of the framing reports by calling this rather than
  by writing a second log line of its own.
  """
  @spec refuse(Path.t(), atom(), integer(), non_neg_integer()) :: {:error, refusal()}
  def refuse(path, reason, last_seq, offset) do
    refusal = {reason, last_seq, offset}

    :telemetry.execute(
      [:flezha, :state, :record_frame, :refused],
      %{count: 1},
      %{path: path, reason: reason, last_seq: last_seq, offset: offset}
    )

    Logger.error(refusal_message(path, refusal))
    {:error, refusal}
  end

  @doc """
  The one sentence a refusal is logged with, and reported to callers as.

  `source` is the log the records came from: a path for a file, the payload's
  own name for a change log read from bytes.
  """
  @spec refusal_message(Path.t(), refusal()) :: binary()
  def refusal_message(source, {reason, last_seq, offset}) do
    "[State] durable record refused (#{reason}) in #{source} at byte offset #{offset}, " <>
      "after seq #{last_seq}: the failure is corruption and not a torn tail, so the " <>
      "log is left as it stands and refuses to serve"
  end

  @doc """
  Reduce over records already in memory, oldest first, for a reader holding a
  whole log or a change log shipped over the wire.

  `fun.({seq, header, body, raw}, acc)` returns `{:cont, acc}` or `{:halt, acc}`;
  `raw` is the record's own bytes verbatim, so a caller that ships them on does
  not re-frame what it just decoded.

  `source` says both where the bytes came from and what they are — the one
  thing a byte walk cannot read off them — and names them in a refusal:

    * `{:log, path}` — a record log an append may have torn, so `repairable?/3`
      decides;
    * `{:payload, label}` — a change log shipped whole, where nothing was
      mid-append: an incomplete trailing fragment still ends the walk, but a
      record whose crc fails is corruption and is refused however few bytes
      follow it.
  """
  @spec fold_bytes(
          binary(),
          source(),
          acc,
          ({non_neg_integer(), binary(), binary(), binary()}, acc ->
             {:cont, acc} | {:halt, acc})
        ) ::
          {:ok, acc} | {:error, refusal()}
        when acc: term()
  def fold_bytes(bytes, {kind, _name} = source, acc, fun)
      when is_binary(bytes) and kind in [:log, :payload],
      do: walk_bytes(bytes, source, 0, -1, acc, fun)

  defp walk_bytes(<<>>, _source, _position, _last_seq, acc, _fun), do: {:ok, acc}

  defp walk_bytes(bytes, source, position, last_seq, acc, fun) do
    case decode_bytes(bytes) do
      {:ok, seq, header, body, raw, consumed} ->
        case fun.({seq, header, body, raw}, acc) do
          {:cont, acc} ->
            <<_consumed::binary-size(^consumed), rest::binary>> = bytes
            walk_bytes(rest, source, position + consumed, seq, acc, fun)

          {:halt, acc} ->
            {:ok, acc}
        end

      {:error, reason, extent} ->
        case stop_or_refuse(
               source,
               position,
               last_seq,
               reason,
               extent,
               position + byte_size(bytes)
             ) do
          {:stop, _size} -> {:ok, acc}
          refused -> refused
        end
    end
  end

  @doc """
  What every walk does with a record that did not decode at `position`:
  `{:stop, size}` when the proof holds and the walk may end, or truncate, where
  it stands, the refusal otherwise. One expression, so every walker over this
  framing, in memory or on a device, answers alike.

  The one exception a source carries: a payload was shipped whole, so it has no
  torn tail to repair and the crc branch of `repairable?/3` does not apply to it.
  """
  @spec stop_or_refuse(
          source(),
          non_neg_integer(),
          integer(),
          atom(),
          non_neg_integer() | nil,
          non_neg_integer()
        ) :: {:stop, non_neg_integer()} | {:error, refusal()}
  def stop_or_refuse({:payload, name}, position, last_seq, :crc_mismatch, _extent, _size),
    do: refuse(name, :crc_mismatch, last_seq, position)

  def stop_or_refuse({_kind, name}, position, last_seq, reason, extent, size) do
    if repairable?(reason, extent, size - position),
      do: {:stop, size},
      else: refuse(name, reason, last_seq, position)
  end
end
