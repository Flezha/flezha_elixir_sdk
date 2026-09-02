defmodule Flezha.SDK.EntryInput do
  @moduledoc """
  When an entry channel last received a frame — the one fact only the SDK has,
  recorded as one atomic store per delivery into a cell resolved once when an
  Operative starts. One cell per channel NAME, however many classes declare
  it, so a registration made before the class is declared and the later
  topology resolve the same cell. Deciding whether the silence since then is
  anomalous is `Flezha.SDK.EntryIdle`'s.

  The cells live in `:persistent_term`, which needs no owner process — the
  accessor answers before any SDK process exists — and costs a global GC scan
  per NEW name, which is why interning happens where a runner resolves a
  topology or a registration and never on the delivery path.
  """

  @type cell :: :atomics.atomics_ref()

  @doc "The cell for `channel`, interned on first mention."
  @spec cell_for(String.t()) :: cell()
  def cell_for(channel) when is_binary(channel) do
    key = {__MODULE__, channel}

    case :persistent_term.get(key, nil) do
      nil ->
        cell = :atomics.new(1, signed: false)
        :persistent_term.put(key, cell)
        cell

      cell ->
        cell
    end
  end

  @doc "The cells of every entry in `entries`, by name — resolved once, cached on the Operative."
  @spec cells_for(%{String.t() => term()}) :: %{String.t() => cell()}
  def cells_for(entries), do: Map.new(entries, fn {name, _} -> {name, cell_for(name)} end)

  @doc "A frame reached the channel: one atomic store, no lock."
  @spec stamp(cell()) :: :ok
  def stamp(cell), do: :atomics.put(cell, 1, max(now_nanos(), 1))

  @doc false
  # `stamp/1` at a chosen instant, for tests; interns the name.
  @spec stamp_at(String.t(), integer()) :: :ok
  def stamp_at(channel, nanos), do: :atomics.put(cell_for(channel), 1, max(nanos, 1))

  @doc """
  When `channel` last received a frame, on `now_nanos/0`'s clock; nil before
  its first frame, and for a name nothing declares. A read never interns.
  """
  @spec last_delivery_nanos(String.t()) :: pos_integer() | nil
  def last_delivery_nanos(channel) when is_binary(channel) do
    case :persistent_term.get({__MODULE__, channel}, nil) do
      nil -> nil
      cell -> nonzero(:atomics.get(cell, 1))
    end
  end

  defp nonzero(0), do: nil
  defp nonzero(nanos), do: nanos

  @doc """
  The wall-clock instant a monotonic stamp names, derived from its gap to
  `now_nanos` so a clock step between the delivery and the read cannot skew it.
  """
  @spec instant(pos_integer(), non_neg_integer(), DateTime.t()) :: DateTime.t()
  def instant(last, now_nanos, wall_now),
    do: DateTime.add(wall_now, -div(max(now_nanos - last, 0), 1_000), :microsecond)

  @doc """
  The SDK's monotonic clock: nanoseconds since the runtime started. The BEAM's
  own monotonic time is negative, so it is anchored here for `0` to mean
  "never delivered", as it does in the shared core.
  """
  @spec now_nanos() :: non_neg_integer()
  def now_nanos do
    started = :erlang.convert_time_unit(:erlang.system_info(:start_time), :native, :nanosecond)
    max(System.monotonic_time(:nanosecond) - started, 0)
  end
end
