defmodule Flezha.SDK.Ctx do
  @moduledoc """
  The invocation context handed to `Procedure.run/3`. `emit/2` and
  `emit_with_meta/3` accumulate outputs for this invocation; `apply/3` folds a
  change into the state through the procedure's `apply_change/2` and records
  it for the Entity's record stream — one fold, one push, the only recorded
  mutation path; `now/1` is a wall-clock millisecond stamp captured once at
  invocation entry; `entity_key/1` is the owning Entity's key (or nil for a
  Single Entity). Emits and changes are held in the running process's
  dictionary under a per-invocation ref (so `emit/2` can side-effect through an
  immutable struct) and are taken by the Entity only when `run/3` returns
  `{:ok, _}` — a failing invocation discards both. Reserved `flz.`-prefixed
  metadata keys are refused.
  """

  @enforce_keys [:module, :entity_key, :now_ms, :ref]
  defstruct [:module, :entity_key, :now_ms, :ref]

  @type t :: %__MODULE__{}

  @reserved_prefix "flz."

  @doc "A fresh ctx for one invocation of `module`. `now_ms` is captured once here."
  def new(module, entity_key, now_ms) do
    ref = make_ref()
    Process.put(key(ref), [])
    Process.put(changes_key(ref), [])
    %__MODULE__{module: module, entity_key: entity_key, now_ms: now_ms, ref: ref}
  end

  @doc """
  Fold `change` into `state` with the procedure's `apply_change/2` and record
  it, so the Entity ships it after the invocation succeeds. The returned state
  is what `run/3` returns in its `{:ok, _}`.
  """
  @spec apply(t(), term(), term()) :: term()
  def apply(%__MODULE__{module: module, ref: ref}, state, change) do
    Process.put(changes_key(ref), [change | Process.get(changes_key(ref), [])])
    module.apply_change(state, change)
  end

  @doc "Emit a value on the procedure's outgoing channels."
  @spec emit(t(), term()) :: :ok | {:error, term()}
  def emit(%__MODULE__{} = ctx, value), do: emit_with_meta(ctx, value, %{})

  @doc "Emit a value with metadata. Reserved `flz.`-prefixed keys are refused."
  @spec emit_with_meta(t(), term(), map()) :: :ok | {:error, term()}
  def emit_with_meta(%__MODULE__{ref: ref}, value, meta) when is_map(meta) do
    case reserved_key(meta) do
      nil ->
        Process.put(key(ref), [{value, meta} | Process.get(key(ref), [])])
        :ok

      k ->
        {:error, {:reserved_metadata_key, k}}
    end
  end

  @doc "Wall-clock milliseconds, evaluated once at invocation entry."
  def now(%__MODULE__{now_ms: ms}), do: ms

  @doc "The owning Entity's key, or nil for a Single Entity."
  def entity_key(%__MODULE__{entity_key: k}), do: k

  @doc false
  def take_emits(%__MODULE__{ref: ref}) do
    emits = Process.get(key(ref), []) |> Enum.reverse()
    Process.delete(key(ref))
    emits
  end

  @doc false
  def take_changes(%__MODULE__{ref: ref}) do
    changes = Process.get(changes_key(ref), []) |> Enum.reverse()
    Process.delete(changes_key(ref))
    changes
  end

  @doc false
  def discard(%__MODULE__{ref: ref}) do
    Process.delete(key(ref))
    Process.delete(changes_key(ref))
  end

  defp reserved_key(meta) do
    Enum.find_value(meta, fn {k, _v} ->
      ks = to_string(k)
      if String.starts_with?(ks, @reserved_prefix), do: ks
    end)
  end

  defp key(ref), do: {__MODULE__, ref}
  defp changes_key(ref), do: {__MODULE__, ref, :changes}
end
