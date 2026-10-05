defmodule Flezha.SDK.Prefix do
  @moduledoc """
  An instance's processed prefix (`durable-input-log`): the highest input-log
  offset `P` at or below which every input the instance took has been
  processed and every record it produced has been written. The shared core
  keeps the same ledger in `prefix.rs`.

  The Relay holds `P` below an input as it pushes it into a Cell; the Cell
  releases it once the record the input settled into is on the wire, or at
  once when it records nothing. The Relay reports `P` behind those records
  on every stream the instance writes, and never through a hold.

  An ordered table the Relay and its Cells share: an offset's row counts the
  inputs of it still pending, and the atom rows (which order after every
  offset) carry the highest offset seen, the last `P` reported, the restore's
  catch-up gate and the freeze a lost record sets.
  """

  @type t :: :ets.tid()

  @spec new() :: t()
  def new, do: :ets.new(:flezha_sdk_prefix, [:ordered_set, :public])

  @doc "Hold `P` below `offset` for one input. Only the Relay holds."
  @spec hold(t(), non_neg_integer()) :: :ok
  def hold(ledger, offset) do
    :ets.update_counter(ledger, offset, {2, 1}, {offset, 0})

    if offset > value(ledger, :highest, -1), do: :ets.insert(ledger, {:highest, offset})

    # The re-forward a restore waits for has caught up once an input past
    # everything seen before arrives.
    if offset > value(ledger, :catch_up_above, offset), do: :ets.delete(ledger, :catch_up_above)
    :ok
  end

  @doc "Release one input's hold; `nil` is a Cell with no ledger."
  @spec release(t() | nil, non_neg_integer() | nil) :: :ok
  def release(nil, _offset), do: :ok
  def release(_ledger, nil), do: :ok

  def release(ledger, offset) do
    if :ets.update_counter(ledger, offset, {2, -1}, {offset, 1}) <= 0,
      do: :ets.select_delete(ledger, [{{offset, :"$1"}, [{:"=<", :"$1", 0}], [true]}])

    :ok
  rescue
    # The Relay that owned the ledger stopped, and every hold with it.
    ArgumentError -> :ok
  end

  @doc """
  A Cell of the instance was restored: the controller re-forwards what the
  restore discarded, and nothing is reported until that re-forward caught up.
  A freeze the restore repaired goes.
  """
  @spec restored(t() | nil) :: :ok
  def restored(nil), do: :ok

  def restored(ledger) do
    :ets.delete(ledger, :frozen)

    case :ets.lookup(ledger, :highest) do
      [{:highest, highest}] -> :ets.insert(ledger, {:catch_up_above, highest})
      [] -> :ok
    end

    :ok
  end

  @doc "A record of the instance was lost from a hold: no marker covers it until a restore."
  @spec freeze(t() | nil) :: :ok
  def freeze(nil), do: :ok

  def freeze(ledger) do
    :ets.insert(ledger, {:frozen, true})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Whether an input is still pending anywhere in the instance."
  @spec pending?(t()) :: boolean()
  def pending?(ledger), do: is_integer(:ets.first(ledger))

  @doc """
  The prefix to report now: past the last one reported, and neither frozen
  nor waiting for a re-forward; `nil` otherwise.
  """
  @spec candidate(t()) :: non_neg_integer() | nil
  def candidate(ledger) do
    prefix =
      case :ets.first(ledger) do
        low when is_integer(low) and low > 0 -> low - 1
        low when is_integer(low) -> nil
        _no_pending -> value(ledger, :highest, nil)
      end

    blocked? = :ets.member(ledger, :frozen) or :ets.member(ledger, :catch_up_above)

    if not blocked? and is_integer(prefix) and prefix > value(ledger, :reported, -1),
      do: prefix
  end

  @doc "`prefix` went out on every stream."
  @spec reported(t(), non_neg_integer()) :: :ok
  def reported(ledger, prefix) do
    :ets.insert(ledger, {:reported, prefix})
    :ok
  end

  defp value(ledger, key, default) do
    case :ets.lookup(ledger, key) do
      [{^key, value}] -> value
      [] -> default
    end
  end
end
