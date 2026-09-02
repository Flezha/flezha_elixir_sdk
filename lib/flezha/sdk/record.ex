defmodule Flezha.SDK.Record do
  @moduledoc """
  The durable record this SDK writes and reads back, re-expressed in its own
  core: the 16-byte header the shared contract fixes (`sdk_lang` 3, the SDK's
  version, the Procedure's schema version, the Erlang External Term Format id,
  a CRC over the body), the frame the controller's
  `Flezha.State.Wire.decode_frame/1` reads for changes and snapshots alike, and
  the fold that rebuilds an Entity from a `RestoreInitState` — the snapshot
  first, then the change log in the controller's on-disk framing, each record
  checked and its header resolved language first, then format, then schema,
  with the refusal wordings the shared core uses so a controller reads one
  vocabulary whichever SDK refused. A record between the Procedure's readable
  floor and its version is decoded and handed to its migration.
  """

  alias Flezha.SDK.{Codec, Procedure}
  alias Flezha.State.Wire

  # Allocated in `flz_core::header`, the table of record.
  @format_id_etf 7
  @header_size 16
  @lang_names %{
    0 => "unspecified",
    1 => "rust",
    2 => "java",
    3 => "elixir",
    4 => "node",
    5 => "ruby",
    6 => "php",
    7 => "python"
  }

  @typedoc "The folded state, the highest seq folded (never below the hint), whether a record migrated."
  @type fold :: {:ok, term(), non_neg_integer(), boolean()} | {:error, String.t()}

  @spec format_id() :: pos_integer()
  def format_id, do: @format_id_etf

  @spec header_size() :: pos_integer()
  def header_size, do: @header_size

  @doc "The header for `body`, stamped at `schema_version`: language, version, schema, format, zeros, crc."
  @spec header(binary(), pos_integer()) :: <<_::128>>
  def header(body, schema_version) when is_binary(body) do
    <<Codec.sdk_lang(), sdk_version()::32-big, schema_version::32-big, @format_id_etf, 0, 0,
      :erlang.crc32(body)::32-big>>
  end

  @doc "`(major <<< 16) ||| (minor <<< 8) ||| patch`, each clamped at 255 — the core's own encoding."
  @spec sdk_version() :: non_neg_integer()
  def sdk_version do
    [major, minor, patch] =
      (Application.spec(:flezha, :vsn) || ~c"0.0.0")
      |> to_string()
      |> String.split(["-", "+"], parts: 2)
      |> hd()
      |> String.split(".")
      |> Enum.map(&component/1)
      |> Enum.concat([0, 0, 0])
      |> Enum.take(3)

    Bitwise.bor(Bitwise.bsl(major, 16), Bitwise.bor(Bitwise.bsl(minor, 8), patch))
  end

  @doc "A change or snapshot body: External Term Format, deterministic so a replay's bytes match the run's."
  @spec encode_body(term()) :: binary()
  def encode_body(term), do: :erlang.term_to_binary(term, [:deterministic])

  # Not `:safe`: the bytes are the controller's copy of what this SDK wrote, and
  # `:safe` refuses a state whose atoms no loaded module happens to mention.
  @spec decode_body(binary()) :: {:ok, term()} | {:error, String.t()}
  def decode_body(bin) when is_binary(bin) do
    {:ok, :erlang.binary_to_term(bin)}
  rescue
    ArgumentError -> {:error, "body decode failed: not a term"}
  end

  @doc "The wire frame for `body` at `seq` — one shape for a change record and a snapshot."
  @spec frame(binary(), non_neg_integer(), binary(), pos_integer()) :: binary()
  def frame(identity, seq, body, schema_version),
    do: Wire.encode_frame(identity, seq, header(body, schema_version), body)

  @doc """
  Rebuild an Entity of `module` from a `RestoreInitState`: `init_state/1`,
  the snapshot over it when there is one, then every whole record of the
  change log folded in seq order. The terminal is the highest seq folded, never
  below `terminal_hint` (a snapshot-only restore has no record to derive it
  from). A refusal names its reason and folds nothing further.
  """
  @spec fold(module(), String.t() | nil, binary(), binary(), binary(), non_neg_integer()) ::
          fold()
  def fold(module, entity_key, snapshot_header, snapshot_bytes, change_log_bytes, terminal_hint) do
    versions = {Procedure.schema_version(module), Procedure.min_readable_schema_version(module)}

    with {:ok, state, migrated} <-
           restore_snapshot(module, entity_key, snapshot_header, snapshot_bytes, versions),
         {:ok, state, terminal, migrated} <-
           walk(change_log_bytes, module, state, 0, migrated, versions) do
      {:ok, state, max(terminal, terminal_hint), migrated}
    end
  rescue
    e -> {:error, "restore fold failed: #{Exception.message(e)}"}
  end

  @doc """
  Resolve a record header against the running procedure's `{schema, floor}`:
  the language before anything else, then the format, then the schema —
  `:decode` at the running version, `{:migrate, from}` between the floor and
  it, a refusal above it or below the floor.
  """
  @spec resolve(binary(), {pos_integer(), pos_integer()}) ::
          {:ok, :decode | {:migrate, pos_integer()}} | {:error, String.t()}
  def resolve(<<lang, _version::32, schema::32-big, format, _::16, _crc::32>>, {running, floor}) do
    # A floor above the running version reads the running version only.
    floor = min(floor, running)

    cond do
      lang != Codec.sdk_lang() ->
        {:error,
         "sdk_lang mismatch: this SDK is #{lang_name(Codec.sdk_lang())} (#{Codec.sdk_lang()}), " <>
           "the record was written by #{lang_name(lang)} (#{lang}); durable bodies are " <>
           "per-SDK and are never decoded across languages"}

      format != @format_id_etf ->
        {:error,
         "format_id #{format} is not one this SDK writes (#{@format_id_etf}): the body cannot be decoded"}

      schema == running ->
        {:ok, :decode}

      schema > running ->
        {:error,
         "record stamped schema_version #{schema} is above the running procedure's #{running}: " <>
           "older code cannot read newer state"}

      schema < floor ->
        {:error,
         "record stamped schema_version #{schema} is below the readable floor #{floor} " <>
           "the running procedure declares"}

      true ->
        {:ok, {:migrate, schema}}
    end
  end

  def resolve(_short, _versions),
    do: {:error, "malformed restoration header (truncated or invalid)"}

  # --- the fold ---

  defp restore_snapshot(module, entity_key, _header, <<>>, _versions),
    do: {:ok, module.init_state(entity_key), false}

  defp restore_snapshot(module, _entity_key, header, bytes, versions) do
    with {:ok, decision} <- resolve(header, versions),
         {:ok, state} <- decode(bytes, decision, :snapshot, module) do
      {:ok, state, decision != :decode}
    end
  end

  # One on-disk record: `[seq:64][len:32][header:16][body:len-16][crc32 over header <> body]`.
  # A trailing fragment that is not a whole record ends the walk, as the
  # controller's own reader ends it.
  defp walk(
         <<seq::64-big, len::32-big, rest::binary>>,
         module,
         state,
         terminal,
         migrated,
         versions
       )
       when len >= @header_size and byte_size(rest) >= len + 4 do
    body_len = len - @header_size

    <<header::binary-size(@header_size), body::binary-size(^body_len), crc::32-big, tail::binary>> =
      rest

    with :ok <- check_crc(header, body, crc, seq),
         {:ok, decision} <- resolve(header, versions),
         {:ok, change} <- decode(body, decision, :change, module) do
      walk(
        tail,
        module,
        module.apply_change(state, change),
        max(terminal, seq),
        migrated or decision != :decode,
        versions
      )
    end
  end

  defp walk(_torn_or_empty, _module, state, terminal, migrated, _versions),
    do: {:ok, state, terminal, migrated}

  defp check_crc(header, body, crc, seq) do
    if :erlang.crc32(:erlang.crc32(header), body) == crc,
      do: :ok,
      else: {:error, "crc32 mismatch on change record (seq=#{seq})"}
  end

  defp decode(bytes, :decode, _what, _module), do: decode_body(bytes)

  # The decoded term goes to the migration: the body codec is the language's
  # own, so there is no second codec for an author to choose.
  defp decode(bytes, {:migrate, from}, what, module) do
    with {:ok, old} <- decode_body(bytes) do
      case migrate(module, what, from, old) do
        {:ok, term} ->
          {:ok, term}

        :undeclared ->
          {:error,
           "no migration from schema_version #{from}: the procedure declares none (#{what})"}

        {:error, reason} ->
          {:error, "migration from schema_version #{from} failed (#{what}): #{describe(reason)}"}
      end
    end
  end

  defp migrate(module, :snapshot, from, old), do: Procedure.migrate_state(module, from, old)
  defp migrate(module, :change, from, old), do: Procedure.migrate_change(module, from, old)

  defp describe(reason) when is_binary(reason), do: reason
  defp describe(reason), do: inspect(reason)

  defp lang_name(lang), do: Map.get(@lang_names, lang, "unallocated")

  defp component(str) do
    case Integer.parse(str) do
      {n, _} -> min(max(n, 0), 255)
      :error -> 0
    end
  end
end
