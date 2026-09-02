defmodule Flezha.SDK.Procedure do
  @moduledoc """
  Behaviour an Elixir procedure implements. Mandatory: `name/0`, `init_state/1`
  (the entity key or nil), `run/3` (input, state, ctx) returning
  `{:ok, new_state} | {:error, reason} | {:error, {:non_retryable, reason}}`.
  Optional, with defaults: `entity_keys/0` (static keyset, `[]`), `entities_for/1`,
  `mailbox_capacity/0` (1024, env `FLEZHA_MAILBOX_CAPACITY`, range 1..=1_048_576),
  `max_entities/0` (100_000). Dynamic typing collapses the Rust type tokens —
  Input/Output/State are plain terms.

  Persistence: a procedure that exports `apply_change/2` is changeful — its
  state is rebuilt by folding recorded changes, and `Flezha.SDK.Ctx.apply/3` is
  the one path that records one (the twin of the Rust `StateHandle::apply`).
  Without it the procedure is the reference's `NoChange`: its state is a plain
  term nothing persists. `compact_after_bytes/0` (8 MiB) is the per-Entity WAL
  size at which the controller solicits a snapshot; `state_from_json/1` turns
  a JSON object read back from the controller (a migration, `init_values`)
  into this procedure's state term, the object itself by default.
  """

  alias Flezha.SDK.Ctx

  @type state :: term()
  @type run_result ::
          {:ok, state()} | {:error, term()} | {:error, {:non_retryable, term()}}

  @callback name() :: String.t()
  @callback init_state(entity_key :: String.t() | nil) :: state()
  @callback run(input :: term(), state(), Ctx.t()) :: run_result()
  @callback entity_keys() :: [String.t()]
  @callback entities_for(input :: term()) :: [String.t()]
  @callback mailbox_capacity() :: pos_integer()
  @callback max_entities() :: pos_integer()
  @callback apply_change(state(), change :: term()) :: state()
  @callback compact_after_bytes() :: pos_integer()
  @callback state_from_json(map()) :: state()

  @optional_callbacks entity_keys: 0,
                      entities_for: 1,
                      mailbox_capacity: 0,
                      max_entities: 0,
                      apply_change: 2,
                      compact_after_bytes: 0,
                      state_from_json: 1

  @default_mailbox_capacity 1024
  @max_mailbox_capacity 1_048_576
  @default_max_entities 100_000
  @env_mailbox_capacity "FLEZHA_MAILBOX_CAPACITY"
  # In lockstep with `flz_core::persistence::DEFAULT_COMPACT_AFTER_BYTES` and
  # the controller's `:default_compact_after_bytes`.
  @default_compact_after_bytes 8 * 1024 * 1024
  @default_schema_version 1

  @doc "Static entity keyset (`[]` unless the procedure overrides `entity_keys/0`)."
  def entity_keys(module) do
    if exports?(module, :entity_keys, 0), do: module.entity_keys(), else: []
  end

  @doc "Which static entities an input targets (defaults to the full keyset)."
  def entities_for(module, input) do
    if exports?(module, :entities_for, 1),
      do: module.entities_for(input),
      else: entity_keys(module)
  end

  @doc "Per-Entity mailbox capacity. Overridden callback wins; else the env; else 1024."
  def mailbox_capacity(module) do
    if exports?(module, :mailbox_capacity, 0) do
      validate_capacity!(module.mailbox_capacity(), module)
    else
      case System.get_env(@env_mailbox_capacity) do
        nil -> @default_mailbox_capacity
        val -> validate_capacity!(parse_capacity(val), module)
      end
    end
  end

  @doc "Cap on live dynamic entities (default 100_000)."
  def max_entities(module) do
    if exports?(module, :max_entities, 0),
      do: module.max_entities(),
      else: @default_max_entities
  end

  @doc "Whether the procedure journals — it exports `apply_change/2`."
  @spec changeful?(module()) :: boolean()
  def changeful?(module), do: exports?(module, :apply_change, 2)

  @doc "The schema version stamped on the procedure's records."
  @spec schema_version(module()) :: pos_integer()
  def schema_version(_module), do: @default_schema_version

  @doc "The oldest schema version the procedure reads (its own, until it declares one)."
  @spec min_readable_schema_version(module()) :: pos_integer()
  def min_readable_schema_version(module), do: schema_version(module)

  @doc "Per-Entity WAL size at which the controller solicits a snapshot (8 MiB unless overridden)."
  @spec compact_after_bytes(module()) :: pos_integer()
  def compact_after_bytes(module) do
    if exports?(module, :compact_after_bytes, 0),
      do: module.compact_after_bytes(),
      else: @default_compact_after_bytes
  end

  @doc "The compaction threshold a class need not declare."
  @spec default_compact_after_bytes() :: pos_integer()
  def default_compact_after_bytes, do: @default_compact_after_bytes

  @doc "A JSON object read back from the controller as this procedure's state term."
  @spec state_from_json(module(), map()) :: state()
  def state_from_json(module, json) do
    if exports?(module, :state_from_json, 1), do: module.state_from_json(json), else: json
  end

  @doc false
  def exports?(module, fun, arity),
    do: Code.ensure_loaded?(module) and function_exported?(module, fun, arity)

  @doc "Range-check a mailbox capacity — returns `{issue, procedure}` on violation."
  @spec validate_capacity(pos_integer(), module()) :: :ok | {atom(), module()}
  def validate_capacity(cap, module) when is_integer(cap) do
    cond do
      cap <= 0 -> {:MailboxCapacityZero, module}
      cap > @max_mailbox_capacity -> {:MailboxCapacityTooLarge, module}
      true -> :ok
    end
  end

  def validate_capacity(_cap, module), do: {:MailboxCapacityZero, module}

  defp validate_capacity!(cap, module) do
    case validate_capacity(cap, module) do
      :ok -> cap
      {:MailboxCapacityZero, _} -> @default_mailbox_capacity
      {:MailboxCapacityTooLarge, _} -> @max_mailbox_capacity
    end
  end

  defp parse_capacity(val) do
    case Integer.parse(val) do
      {n, ""} -> n
      _ -> @default_mailbox_capacity
    end
  end
end
