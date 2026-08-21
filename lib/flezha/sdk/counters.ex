defmodule Flezha.SDK.Counters do
  @moduledoc """
  Runner-wide liveness counters backed by `:atomics`, shared with every Entity so
  the heartbeat can report them without a message round trip. `dispatches` is
  bumped only on a COMPLETED dispatch — the key the controller's
  `runner_pool_wedged` verdict requires (a frozen value with the runner alive is
  what makes a wedged Entity population visible). `blocked_parks` counts episodes.
  """

  @dispatches 1
  @blocked_parks 2
  @size 2

  @type t :: :atomics.atomics_ref()

  @spec new() :: t()
  def new, do: :atomics.new(@size, signed: false)

  @spec inc_dispatches(t()) :: :ok
  def inc_dispatches(ref), do: :atomics.add(ref, @dispatches, 1)

  @spec inc_blocked_parks(t()) :: :ok
  def inc_blocked_parks(ref), do: :atomics.add(ref, @blocked_parks, 1)

  @spec dispatches(t()) :: non_neg_integer()
  def dispatches(ref), do: :atomics.get(ref, @dispatches)

  @spec blocked_parks(t()) :: non_neg_integer()
  def blocked_parks(ref), do: :atomics.get(ref, @blocked_parks)
end
