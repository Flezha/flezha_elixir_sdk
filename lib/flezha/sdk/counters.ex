defmodule Flezha.SDK.Counters do
  @moduledoc """
  Runner-wide liveness counters backed by `:atomics`, shared with every Entity so
  the heartbeat can report them without a message round trip. `dispatches` is
  bumped only on a COMPLETED dispatch — the key the controller's
  `runner_pool_wedged` verdict requires (a frozen value with the runner alive is
  what makes a wedged Entity population visible). `blocked_parks` counts blocked
  enqueues, and `drops_newest`/`drops_oldest` count what a shedding policy threw
  away — the same three keys every other SDK's heartbeat carries, so an operator
  reads one vocabulary whichever runtime the operative runs on.

  Handout liveness — how many dispatches are underway and how long the oldest has
  been — is the same self-report every other SDK carries, and it is what makes ONE
  wedged Entity visible while the runner as a whole keeps completing work. It
  rides a fixed table of start timestamps rather than an in-flight counter,
  because a counter cannot be un-leaked: an Entity killed untrappably would hold
  its increment for the life of the runner. A slot belongs to an Entity's
  identity instead, so the Operative clears it when that Entity dies. Two
  identities hashing to one slot means one of them runs unrecorded — the report
  under-states, which is the safe direction for a signal a rebind hangs off.

  Reporting only: nothing in the SDK changes behaviour from these values.
  """

  @dispatches 1
  @blocked_parks 2
  @drops_newest 3
  @drops_oldest 4
  # Start-time slots follow the counters; adding a counter moves the base with
  # it rather than leaving a second place to remember how many there are.
  @slot_base @drops_oldest
  @slots 64
  @size @slot_base + @slots

  @type t :: :atomics.atomics_ref()
  @typedoc "An Entity's identity, as the slot table hashes it."
  @type identity :: term()

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

  @spec inc_drops(t(), :drop_newest | :drop_oldest) :: :ok
  def inc_drops(ref, :drop_newest), do: :atomics.add(ref, @drops_newest, 1)
  def inc_drops(ref, :drop_oldest), do: :atomics.add(ref, @drops_oldest, 1)

  @spec drops_newest(t()) :: non_neg_integer()
  def drops_newest(ref), do: :atomics.get(ref, @drops_newest)

  @spec drops_oldest(t()) :: non_neg_integer()
  def drops_oldest(ref), do: :atomics.get(ref, @drops_oldest)

  @doc """
  Record that `identity` started a dispatch. A slot already held (by this Entity
  re-entering, or by a colliding identity) leaves the dispatch unrecorded.
  """
  @spec dispatch_started(t(), identity()) :: :ok
  def dispatch_started(ref, identity) do
    _ = :atomics.compare_exchange(ref, slot(identity), 0, now_ms())
    :ok
  end

  @doc "Release `identity`'s slot — its dispatch ended, or the Entity died."
  @spec dispatch_finished(t(), identity()) :: :ok
  def dispatch_finished(ref, identity), do: :atomics.put(ref, slot(identity), 0)

  @doc """
  Handout liveness: `{dispatches underway, age in ms of the oldest}`, `{0, 0}`
  when the runner is idle. One scan, so the pair cannot disagree — two would
  let the count come from before a dispatch ended and the age from after.
  """
  @spec handout_liveness(t()) :: {non_neg_integer(), non_neg_integer()}
  def handout_liveness(ref) do
    now = now_ms()

    Enum.reduce(1..@slots, {0, 0}, fn i, {count, oldest} = idle ->
      case :atomics.get(ref, @slot_base + i) do
        0 -> idle
        started -> {count + 1, max(oldest, max(now - started, 0))}
      end
    end)
  end

  defp slot(identity), do: @slot_base + 1 + :erlang.phash2(identity, @slots)

  # Wall-clock ms: unsigned atomics cannot hold a monotonic reading, and `0` is
  # the free marker, which a wall-clock millisecond never is.
  defp now_ms, do: System.system_time(:millisecond)
end
