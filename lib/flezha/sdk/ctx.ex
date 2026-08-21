defmodule Flezha.SDK.Ctx do
  @moduledoc """
  The invocation context handed to `Procedure.run/3`. `emit/2` and
  `emit_with_meta/3` accumulate outputs for this invocation; `now/1` is a
  wall-clock millisecond stamp captured once at invocation entry; `entity_key/1`
  is the owning Entity's key (or nil for a Single Entity). Emits are held in the
  running process's dictionary under a per-invocation ref (so `emit/2` can
  side-effect through an immutable struct) and are flushed by the Entity only
  when `run/3` returns `{:ok, _}` — a failing invocation discards its partial
  emits. Reserved `flz.`-prefixed metadata keys are refused.
  """

  @enforce_keys [:entity_key, :now_ms, :ref]
  defstruct [:entity_key, :now_ms, :ref]

  @type t :: %__MODULE__{}

  @reserved_prefix "flz."

  @doc "A fresh ctx for one invocation. `now_ms` is captured once here."
  def new(entity_key, now_ms) do
    ref = make_ref()
    Process.put(key(ref), [])
    %__MODULE__{entity_key: entity_key, now_ms: now_ms, ref: ref}
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
  def discard(%__MODULE__{ref: ref}), do: Process.delete(key(ref))

  defp reserved_key(meta) do
    Enum.find_value(meta, fn {k, _v} ->
      ks = to_string(k)
      if String.starts_with?(ks, @reserved_prefix), do: ks
    end)
  end

  defp key(ref), do: {__MODULE__, ref}
end
