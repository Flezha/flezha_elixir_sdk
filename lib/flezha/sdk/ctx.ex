defmodule Flezha.SDK.Ctx do
  @moduledoc """
  The invocation context handed to `Procedure.run/3`. `emit/2`,
  `emit_with_meta/3`, `emit_keyed/3` and `emit_keyed_with_meta/4` accumulate
  outputs for this invocation; `apply/3` folds a change into the state through
  the procedure's `apply_change/2` and records it for the Entity's record
  stream — one fold, one push, the only recorded mutation path; `now/1` is a
  wall-clock millisecond stamp captured once at invocation entry;
  `entity_key/1` is the owning Entity's key (or nil for a Single Entity).
  Emits and changes are held in the running process's dictionary under a
  per-invocation ref (so `emit/2` can side-effect through an immutable struct)
  and are taken by the Entity only when `run/3` returns `{:ok, _}` — a failing
  invocation discards both. Reserved `flz.`-prefixed metadata keys are refused.

  A key rides an emit only where the node's sink is declared `keyed/1`, and a
  keyless emit from an Entity with no key of its own on such a sink is refused
  here, at the call — the reference's `KeyRequiredOnKeyedChannel` — from two
  topology facts the Entity hands `new/4`.
  """

  @enforce_keys [:module, :entity_key, :now_ms, :ref]
  defstruct [
    :module,
    :entity_key,
    :now_ms,
    :ref,
    :channel,
    keyed_sink?: false,
    key_required?: false
  ]

  @type t :: %__MODULE__{}

  @reserved_prefix "flz."

  @doc """
  A fresh ctx for one invocation of `module`. `now_ms` is captured once here;
  `opts` carries the node's channel name, whether its sink is keyed
  (`keyed_sink?`) and whether a keyless emit has nothing to fall back on
  (`key_required?`).
  """
  def new(module, entity_key, now_ms, opts \\ []) do
    ref = make_ref()
    Process.put(key(ref), [])
    Process.put(changes_key(ref), [])

    %__MODULE__{
      module: module,
      entity_key: entity_key,
      now_ms: now_ms,
      ref: ref,
      channel: Keyword.get(opts, :channel),
      keyed_sink?: Keyword.get(opts, :keyed_sink?, false),
      key_required?: Keyword.get(opts, :key_required?, false)
    }
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
  def emit_with_meta(%__MODULE__{key_required?: true, channel: channel}, _value, _meta),
    do: {:error, {:key_required_on_keyed_channel, channel}}

  def emit_with_meta(%__MODULE__{} = ctx, value, meta) when is_map(meta),
    do: put_emit(ctx, value, meta, nil)

  @doc "Emit under the output's natural key — the sink must be declared `keyed/1`."
  @spec emit_keyed(t(), String.t(), term()) :: :ok | {:error, term()}
  def emit_keyed(%__MODULE__{} = ctx, key, value), do: emit_keyed_with_meta(ctx, key, value, %{})

  @doc "`emit_keyed/3` with metadata."
  @spec emit_keyed_with_meta(t(), String.t(), term(), map()) :: :ok | {:error, term()}
  def emit_keyed_with_meta(
        %__MODULE__{keyed_sink?: false, channel: channel},
        _key,
        _value,
        _meta
      ),
      do: {:error, {:key_not_allowed_on_non_keyed_channel, channel}}

  def emit_keyed_with_meta(%__MODULE__{} = ctx, key, value, meta)
      when is_binary(key) and is_map(meta),
      do: put_emit(ctx, value, meta, key)

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

  defp put_emit(%__MODULE__{ref: ref}, value, meta, key) do
    case reserved_key(meta) do
      nil ->
        Process.put(key(ref), [{value, meta, key} | Process.get(key(ref), [])])
        :ok

      k ->
        {:error, {:reserved_metadata_key, k}}
    end
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
