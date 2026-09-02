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
  floor and its version is decoded and handed to its migration. A timer-ops
  record — `format_id` 3 over the postcard body every SDK writes — folds into
  the Entity's pending-timer set, never into its state. An effect record —
  `format_id` 2, the effectively-once tier's one record per invocation — folds
  its enclosed changes into the state, its timer ops into the set, and its
  consumed offset and event time into the cursors a crash-replay resumes from.
  """

  alias Flezha.SDK.{Codec, Procedure, Timers}
  alias Flezha.State.Wire

  # Allocated in `flz_core::header`, the table of record.
  @format_id_etf 7
  @format_id_timer_ops 3
  @format_id_effect_record 2
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

  @typedoc """
  What a restore rebuilt: the state, the pending timers, the highest input-log
  offset the effect records durably consumed (−1 when none did) and the
  event-time high-water they carry (nil when none did).
  """
  @type restored :: %{
          state: term(),
          timers: Timers.t(),
          floor: integer(),
          high_water: non_neg_integer() | nil
        }

  @typedoc "The restored Entity, the highest seq folded (never below the hint), whether a record migrated."
  @type fold :: {:ok, restored(), non_neg_integer(), boolean()} | {:error, String.t()}
  @typep decision :: :decode | {:migrate, pos_integer()}

  @spec format_id() :: pos_integer()
  def format_id, do: @format_id_etf

  @spec timer_ops_format_id() :: pos_integer()
  def timer_ops_format_id, do: @format_id_timer_ops

  @spec effect_record_format_id() :: pos_integer()
  def effect_record_format_id, do: @format_id_effect_record

  @spec header_size() :: pos_integer()
  def header_size, do: @header_size

  @doc "The header for `body`, stamped at `schema_version`: language, version, schema, format, zeros, crc."
  @spec header(binary(), pos_integer(), pos_integer()) :: <<_::128>>
  def header(body, schema_version, format \\ @format_id_etf) when is_binary(body) do
    <<Codec.sdk_lang(), sdk_version()::32-big, schema_version::32-big, format, 0, 0,
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
  The wire frame of an effect record at `seq`: one invocation's changes,
  outputs, consumed offset, park, event time and timer ops as the shared
  postcard body under `format_id` 2 — the one record the effectively-once tier
  writes in place of the change records and the trailing timer-ops record.
  """
  @spec effect_frame(binary(), non_neg_integer(), map(), pos_integer()) :: binary()
  def effect_frame(identity, seq, body_map, schema_version) do
    body = Wire.encode_effect_body(body_map)
    Wire.encode_frame(identity, seq, header(body, schema_version, @format_id_effect_record), body)
  end

  @doc "The wire frame of a timer-ops record at `seq`: the shared postcard body under `format_id` 3."
  @spec timer_ops_frame(binary(), non_neg_integer(), [Timers.op()], pos_integer()) :: binary()
  def timer_ops_frame(identity, seq, ops, schema_version) do
    body = Wire.encode_timer_ops(ops)
    Wire.encode_frame(identity, seq, header(body, schema_version, @format_id_timer_ops), body)
  end

  @doc """
  Rebuild an Entity of `module` from a `RestoreInitState`: `init_state/1`,
  the snapshot over it when there is one, then every whole record of the
  change log folded in seq order — a change into the state, a timer-ops record
  into the pending set, an effect record into all four. The terminal is the
  highest seq folded, never below
  `terminal_hint` (a snapshot-only restore has no record to derive it from). A
  refusal names its reason and folds nothing further.
  """
  @spec fold(module(), String.t() | nil, binary(), binary(), binary(), non_neg_integer()) ::
          fold()
  def fold(module, entity_key, snapshot_header, snapshot_bytes, change_log_bytes, terminal_hint) do
    versions = {Procedure.schema_version(module), Procedure.min_readable_schema_version(module)}

    with {:ok, state, migrated} <-
           restore_snapshot(module, entity_key, snapshot_header, snapshot_bytes, versions),
         acc = %{state: state, timers: Timers.new(), floor: -1, high_water: nil},
         {:ok, restored, terminal, migrated} <-
           walk(change_log_bytes, module, acc, 0, migrated, versions) do
      {:ok, restored, max(terminal, terminal_hint), migrated}
    end
  rescue
    e -> {:error, "restore fold failed: #{Exception.message(e)}"}
  end

  @doc """
  Resolve a record header against the running procedure's `{schema, floor}`:
  the language before anything else, then the format, then the schema —
  `:decode` at the running version, `{:migrate, from}` between the floor and
  it, a refusal above it or below the floor; a timer-ops header answers
  `{:timer_ops, decision}` under the same checks, its body never migrated.
  """
  @spec resolve(binary(), {pos_integer(), pos_integer()}) ::
          {:ok, decision() | {:timer_ops, decision()} | {:effect_record, decision()}}
          | {:error, String.t()}
  def resolve(<<lang, _version::32, schema::32-big, format, _::16, _crc::32>>, {running, floor}) do
    # A floor above the running version reads the running version only.
    floor = min(floor, running)

    cond do
      lang != Codec.sdk_lang() ->
        {:error,
         "sdk_lang mismatch: this SDK is #{lang_name(Codec.sdk_lang())} (#{Codec.sdk_lang()}), " <>
           "the record was written by #{lang_name(lang)} (#{lang}); durable bodies are " <>
           "per-SDK and are never decoded across languages"}

      format not in [@format_id_etf, @format_id_timer_ops, @format_id_effect_record] ->
        {:error,
         "format_id #{format} is not one this SDK writes " <>
           "(#{@format_id_effect_record}, #{@format_id_timer_ops}, #{@format_id_etf}): " <>
           "the body cannot be decoded"}

      format == @format_id_timer_ops ->
        with {:ok, decision} <- decide(schema, running, floor), do: {:ok, {:timer_ops, decision}}

      format == @format_id_effect_record ->
        with {:ok, decision} <- decide(schema, running, floor),
             do: {:ok, {:effect_record, decision}}

      true ->
        decide(schema, running, floor)
    end
  end

  def resolve(_short, _versions),
    do: {:error, "malformed restoration header (truncated or invalid)"}

  defp decide(schema, running, floor) do
    cond do
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

  # --- the fold ---

  defp restore_snapshot(module, entity_key, _header, <<>>, _versions),
    do: {:ok, module.init_state(entity_key), false}

  # A snapshot in a format other than the state's is malformed, as the core's
  # `resolve_state` says.
  defp restore_snapshot(module, _entity_key, header, bytes, versions) do
    case resolve(header, versions) do
      {:ok, {kind, _decision}} when kind in [:timer_ops, :effect_record] ->
        {:error, "malformed restoration header (truncated or invalid)"}

      {:ok, decision} ->
        with {:ok, state} <- decode(bytes, decision, :snapshot, module),
             do: {:ok, state, decision != :decode}

      {:error, _} = error ->
        error
    end
  end

  # One on-disk record: `[seq:64][len:32][header:16][body:len-16][crc32 over header <> body]`.
  # A trailing fragment that is not a whole record ends the walk, as the
  # controller's own reader ends it.
  defp walk(<<seq::64-big, len::32-big, rest::binary>>, module, acc, terminal, migrated, versions)
       when len >= @header_size and byte_size(rest) >= len + 4 do
    body_len = len - @header_size

    <<header::binary-size(@header_size), body::binary-size(^body_len), crc::32-big, tail::binary>> =
      rest

    with :ok <- check_crc(header, body, crc, seq),
         {:ok, resolved} <- resolve(header, versions),
         {:ok, folded, decision} <- fold_record(body, resolved, module, acc) do
      walk(tail, module, folded, max(terminal, seq), migrated or decision != :decode, versions)
    end
  end

  defp walk(_torn_or_empty, _module, folded, terminal, migrated, _versions),
    do: {:ok, folded, terminal, migrated}

  # A change into the state; timer ops into the pending set, never migrated.
  defp fold_record(body, {:timer_ops, decision}, _module, acc) do
    case Wire.decode_timer_ops(body) do
      {:ok, ops} -> {:ok, %{acc | timers: Timers.apply_all(acc.timers, ops)}, decision}
      {:error, reason} -> {:error, "timer-ops decode failed: #{inspect(reason)}"}
    end
  end

  # An effect record carries a whole invocation: its changes fold one by one
  # under the enclosing record's version decision, its timer ops into the
  # pending set, and its offset and event time rebuild the cursors the
  # crash-replay resumes from.
  defp fold_record(body, {:effect_record, decision}, module, acc) do
    with {:ok, rec} <- decode_effect(body),
         {:ok, state} <- fold_changes(rec.changes, decision, module, acc.state) do
      {:ok,
       %{
         acc
         | state: state,
           timers: Timers.apply_all(acc.timers, rec.timers),
           floor: max(acc.floor, rec.consumed_offset || acc.floor),
           high_water: max(acc.high_water || rec.event_time, rec.event_time)
       }, decision}
    end
  end

  defp fold_record(body, decision, module, acc) do
    with {:ok, change} <- decode(body, decision, :change, module),
         do: {:ok, %{acc | state: module.apply_change(acc.state, change)}, decision}
  end

  defp decode_effect(body) do
    case Wire.decode_effect_body(body) do
      {:ok, rec} -> {:ok, rec}
      {:error, reason} -> {:error, "effect-record decode failed: #{inspect(reason)}"}
    end
  end

  defp fold_changes(bodies, decision, module, state) do
    Enum.reduce_while(bodies, {:ok, state}, fn body, {:ok, state} ->
      case decode(body, decision, :change, module) do
        {:ok, change} -> {:cont, {:ok, module.apply_change(state, change)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

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
