defmodule Flezha.SDK.Procedure do
  @moduledoc """
  Behaviour an Elixir procedure implements. Mandatory: `name/0`, `init_state/1`
  (the entity key or nil), `run/3` (input, state, ctx) returning
  `{:ok, new_state} | {:error, reason} | {:error, {:non_retryable, reason}}`.
  Optional, with defaults: `entity_keys/0` (static keyset, `[]`), `entities_for/1`,
  `mailbox_capacity/0` (1024, env `FLEZHA_MAILBOX_CAPACITY`, range 1..=1_048_576),
  `max_entities/0` (100_000). Dynamic typing collapses the Rust type tokens —
  Input/Output/State are plain terms.
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

  @optional_callbacks entity_keys: 0, entities_for: 1, mailbox_capacity: 0, max_entities: 0

  @default_mailbox_capacity 1024
  @max_mailbox_capacity 1_048_576
  @default_max_entities 100_000
  @env_mailbox_capacity "FLEZHA_MAILBOX_CAPACITY"

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
