defmodule Flezha.SDK.Cell.Room do
  @moduledoc """
  A Cell's free queue slots, in one `:atomics` counter that its pusher and
  the Cell both reach. A pusher that takes a slot casts its item and never
  waits on a Cell that is mid-run; one that finds none calls, and the Cell
  parks it. Only the Cell gives slots back, and while a caller is parked it
  hands each freed slot to that caller instead, so the count stays at or
  below zero and no later pusher overtakes a parked one.

  Only an input holds a slot. A firing, a barrier or an undo is cast without
  one and rides beside the inputs, and an input the Cell puts back at the
  head of its queue takes its slot again, so the count may go below zero by
  those: the overshoot the queue already allows them.

  Not a `Flezha.SDK.CounterTable`: those only report, and this decides.
  """

  @type t :: :atomics.atomics_ref()

  @spec new(pos_integer()) :: t()
  def new(capacity) do
    room = :atomics.new(1, signed: true)
    :atomics.put(room, 1, capacity)
    room
  end

  @doc "Take one free slot; `false`, changing nothing, when none is free."
  @spec take(t()) :: boolean()
  def take(room), do: take(room, :atomics.get(room, 1))

  defp take(_room, free) when free < 1, do: false

  # Never below one, so a pusher cannot move a count the Cell reads to
  # decide who a freed slot goes to.
  defp take(room, free) do
    case :atomics.compare_exchange(room, 1, free, free - 1) do
      :ok -> true
      now -> take(room, now)
    end
  end

  @doc "Move the free count by `delta`."
  @spec add(t(), integer()) :: :ok
  def add(room, delta), do: :atomics.add(room, 1, delta)

  @spec free(t()) :: integer()
  def free(room), do: :atomics.get(room, 1)
end
