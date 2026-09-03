defmodule Flezha.SDK.Procedure do
  @moduledoc """
  Behaviour an Elixir procedure implements. Mandatory: `name/0`, `init_state/1`
  (the entity key or nil), `run/3` (input, state, ctx) returning
  `{:ok, new_state} | {:error, reason} | {:error, {:non_retryable, reason}}`.
  Optional, with defaults: `entity_keys/0` (static keyset, `[]`), `entities_for/1`,
  `mailbox_capacity/0` (1024, env `FLEZHA_MAILBOX_CAPACITY`, range 1..=1_048_576),
  `max_entities/0` (100_000 — the cap on a dynamic node's live Entities; at it
  a new key's input is dropped and counted), `entity_idle_ttl_ms/0` (nil — a
  dynamic Entity idle past it is evicted destructively by its Operative's
  sweep; nil never evicts). Dynamic typing collapses the Rust type tokens —
  Input/Output/State are plain terms.

  Persistence: a procedure that exports `apply_change/2` is changeful — its
  state is rebuilt by folding recorded changes, and `Flezha.SDK.Ctx.apply/3` is
  the one path that records one (the twin of the Rust `StateHandle::apply`).
  Without it the procedure is the reference's `NoChange`: its state is a plain
  term nothing persists. `compact_after_bytes/0` (8 MiB) is the per-Entity WAL
  size at which the controller solicits a snapshot; `state_from_json/1` turns
  a JSON object read back from the controller (a migration, `init_values`)
  into this procedure's state term, the object itself by default.

  Schema: `schema_version/0` (1) is stamped on every record and resolved per
  record on restore against `min_readable_schema_version/0` (the version
  itself); a record between the floor and the version is handed to
  `migrate_state/2` (a snapshot) or `migrate_change/2` (a change) with the
  stamped version and the DECODED term — this SDK's body codec is the
  language's own — answering `{:ok, term}` at the running version or
  `{:error, reason}`. Both default to refusing, so a floor declared without
  its migration fails at the first old record.

  Time: `clock/0` (`:wall`) names what `Flezha.SDK.Ctx.now/1` serves — the
  epoch millisecond at dispatch entry, `:logical` (the Entity's durable change
  seq) or `:event_time` (what `event_time_of/1` extracts from the input; a
  `:logical` clock on a procedure without `apply_change/2` and an `:event_time`
  clock without its extractor are refused at registration). Timers:
  `on_timer/4` (the scheduled timestamp, the kind, the state, a ctx) answers
  what `run/3` answers and is what a timer registered through the ctx fires
  into; `max_timers_per_entity/0` (10_000) bounds an Entity's pending set.

  Saga: `compensate/3` (the outputs the Entity released, the state, a ctx) is
  the undo a workflow operative runs, in reverse completion order, when it
  unwinds after partial progress. A procedure declaring none has nothing to
  undo. A crash-resumed unwind reconstructs completed stages with EMPTY
  outputs — a procedure's collected output was never durable — so a hook that
  records an undo must do so unconditionally rather than from what it is
  handed.
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
  @callback entity_idle_ttl_ms() :: pos_integer() | nil
  @callback apply_change(state(), change :: term()) :: state()
  @callback compact_after_bytes() :: pos_integer()
  @callback state_from_json(map()) :: state()
  @callback schema_version() :: pos_integer()
  @callback min_readable_schema_version() :: pos_integer()
  @callback migrate_state(from_version :: pos_integer(), old_state :: term()) ::
              {:ok, state()} | {:error, term()}
  @callback migrate_change(from_version :: pos_integer(), old_change :: term()) ::
              {:ok, term()} | {:error, term()}
  @callback clock() :: :wall | :logical | :event_time
  @callback event_time_of(input :: term()) :: non_neg_integer()
  @callback on_timer(
              fired_at :: non_neg_integer(),
              kind :: :event_time | :processing_time,
              state(),
              Ctx.t()
            ) :: run_result()
  @callback max_timers_per_entity() :: pos_integer()
  @callback compensate(outputs :: [term()], state(), Ctx.t()) :: run_result()

  @optional_callbacks compensate: 3,
                      entity_keys: 0,
                      entities_for: 1,
                      mailbox_capacity: 0,
                      max_entities: 0,
                      entity_idle_ttl_ms: 0,
                      apply_change: 2,
                      compact_after_bytes: 0,
                      state_from_json: 1,
                      schema_version: 0,
                      min_readable_schema_version: 0,
                      migrate_state: 2,
                      migrate_change: 2,
                      clock: 0,
                      event_time_of: 1,
                      on_timer: 4,
                      max_timers_per_entity: 0

  @default_mailbox_capacity 1024
  @max_mailbox_capacity 1_048_576
  @default_max_entities 100_000
  @env_mailbox_capacity "FLEZHA_MAILBOX_CAPACITY"
  # In lockstep with `flz_core::persistence::DEFAULT_COMPACT_AFTER_BYTES` and
  # the controller's `:default_compact_after_bytes`.
  @default_compact_after_bytes 8 * 1024 * 1024
  @default_schema_version 1
  # In lockstep with the reference's `DEFAULT_MAX_TIMERS_PER_ENTITY`.
  @default_max_timers_per_entity 10_000

  @doc """
  Whether this procedure declares a saga undo hook. A workflow that unwinds
  runs `compensate/3` on every Entity whose procedure completed forward; a
  procedure declaring none simply has nothing to undo.
  """
  @spec compensates?(module()) :: boolean()
  def compensates?(module), do: exports?(module, :compensate, 3)

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

  @doc "Idle window after which a dynamic Entity is evicted; nil (the default) never evicts."
  @spec entity_idle_ttl_ms(module()) :: pos_integer() | nil
  def entity_idle_ttl_ms(module) do
    if exports?(module, :entity_idle_ttl_ms, 0), do: module.entity_idle_ttl_ms(), else: nil
  end

  @doc "Whether the procedure journals — it exports `apply_change/2`."
  @spec changeful?(module()) :: boolean()
  def changeful?(module), do: exports?(module, :apply_change, 2)

  @doc "The schema version stamped on the procedure's records (1 unless declared)."
  @spec schema_version(module()) :: pos_integer()
  def schema_version(module) do
    if exports?(module, :schema_version, 0),
      do: module.schema_version(),
      else: @default_schema_version
  end

  @doc "The oldest schema version the procedure reads (its own unless declared)."
  @spec min_readable_schema_version(module()) :: pos_integer()
  def min_readable_schema_version(module) do
    if exports?(module, :min_readable_schema_version, 0),
      do: module.min_readable_schema_version(),
      else: schema_version(module)
  end

  @doc "A snapshot stamped `from` as the running version's state; `:undeclared` without a migration."
  @spec migrate_state(module(), pos_integer(), term()) ::
          {:ok, state()} | {:error, term()} | :undeclared
  def migrate_state(module, from, old) do
    if exports?(module, :migrate_state, 2), do: module.migrate_state(from, old), else: :undeclared
  end

  @doc "A change stamped `from` as the running version's change; `:undeclared` without a migration."
  @spec migrate_change(module(), pos_integer(), term()) ::
          {:ok, term()} | {:error, term()} | :undeclared
  def migrate_change(module, from, old) do
    if exports?(module, :migrate_change, 2),
      do: module.migrate_change(from, old),
      else: :undeclared
  end

  @doc "The declared clock (`:wall` unless declared)."
  @spec clock(module()) :: atom()
  def clock(module) do
    if exports?(module, :clock, 0), do: module.clock(), else: :wall
  end

  @doc "Whether the procedure handles a firing — it exports `on_timer/4`."
  @spec on_timer?(module()) :: boolean()
  def on_timer?(module), do: exports?(module, :on_timer, 4)

  @doc "Cap on an Entity's pending timers (10_000 unless declared)."
  @spec max_timers_per_entity(module()) :: pos_integer()
  def max_timers_per_entity(module) do
    if exports?(module, :max_timers_per_entity, 0),
      do: module.max_timers_per_entity(),
      else: @default_max_timers_per_entity
  end

  @doc "What the procedure declares about its timers, resolved once: the name a refusal reports, the cap, the clock."
  @spec timer_policy(module()) :: Flezha.SDK.Timers.policy()
  def timer_policy(module),
    do: %{procedure: module.name(), cap: max_timers_per_entity(module), clock: clock(module)}

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
