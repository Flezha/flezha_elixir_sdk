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

  Deferred-push liveness (`pending_pushes` / `pending_push_oldest_ms`) rides a
  second block of the same table, for the same reason and read the same way: a
  push this SDK accepted but has not yet queued is invisible to `dispatches`
  and to the handout report, and without it backpressure that withholds work
  looks exactly like a runner doing nothing.

  `credit_parked` counts the credit window's park iterations — a sender waiting
  for the controller's grant — so a runner parked on an exhausted window reads
  as parked beside a frozen `dispatches`, not wedged.

  `restore_discarded_inputs` counts the input a restore threw away: an Entity
  re-seats on a past in which nothing staged for the open checkpoint interval
  was seen, so the interval's inputs and the buffered slice go with it. The
  controller re-forwards them from the rewound watermark, which is why the
  discard is a report and not a loss — and why a key that is silently absent
  and a key reported as zero are different answers to an operator.

  `timers_armed` is the processing-time timers the runner's Entities hold on
  the BEAM's timer service — one per armed deadline, released on its firing,
  its disarm and the Entity's own end; an Entity killed untrappably releases
  nothing, so the gauge over-states by its count until the runner restarts —
  and `timers_fired` the firings completed, the two timer keys every other
  SDK's heartbeat carries.

  Reporting only: nothing in the SDK changes behaviour from these values.
  """

  use Flezha.Process.Counters,
    table: :counter,
    backend: :atomics,
    new_opts: [signed: false],
    # Two blocks of start timestamps, addressed by an Entity's identity hash
    # rather than by name. Their bases follow the counters, so adding a counter
    # moves them rather than letting a new slot land inside one.
    blocks: [dispatch: 64, deferred: 64],
    slots: [
      [name: :dispatches, kind: :count],
      [name: :blocked_parks, kind: :count],
      [name: :drops_newest, kind: :count],
      [name: :drops_oldest, kind: :count],
      [name: :credit_parked, kind: :count],
      [name: :timers_fired, kind: :count],
      # A gauge: timers currently armed, released on firing and disarm.
      [name: :timers_armed, kind: :set],
      [name: :restore_discarded_inputs, kind: :count]
    ]

  @slots 64

  @type t :: :atomics.atomics_ref()
  @typedoc "An Entity's identity, as the slot table hashes it."
  @type identity :: term()

  @spec new() :: t()
  def new, do: counter_new()

  @spec inc_dispatches(t()) :: :ok
  def inc_dispatches(ref), do: counter_bump(ref, :dispatches)

  @spec inc_blocked_parks(t()) :: :ok
  def inc_blocked_parks(ref), do: counter_bump(ref, :blocked_parks)

  @spec dispatches(t()) :: non_neg_integer()
  def dispatches(ref), do: counter_get(ref, :dispatches)

  @doc "Count `n` inputs a restore discarded; `0` is a no-op, not a write."
  @spec add_restore_discarded_inputs(t(), non_neg_integer()) :: :ok
  def add_restore_discarded_inputs(_ref, 0), do: :ok

  def add_restore_discarded_inputs(ref, n) when n > 0,
    do: counter_add(ref, :restore_discarded_inputs, n)

  @spec restore_discarded_inputs(t()) :: non_neg_integer()
  def restore_discarded_inputs(ref), do: counter_get(ref, :restore_discarded_inputs)

  @spec blocked_parks(t()) :: non_neg_integer()
  def blocked_parks(ref), do: counter_get(ref, :blocked_parks)

  @spec inc_drops(t(), :drop_newest | :drop_oldest) :: :ok
  def inc_drops(ref, :drop_newest), do: counter_bump(ref, :drops_newest)
  def inc_drops(ref, :drop_oldest), do: counter_bump(ref, :drops_oldest)

  @spec drops_newest(t()) :: non_neg_integer()
  def drops_newest(ref), do: counter_get(ref, :drops_newest)

  @spec drops_oldest(t()) :: non_neg_integer()
  def drops_oldest(ref), do: counter_get(ref, :drops_oldest)

  @spec inc_credit_parked(t()) :: :ok
  def inc_credit_parked(ref), do: counter_bump(ref, :credit_parked)

  @spec credit_parked(t()) :: non_neg_integer()
  def credit_parked(ref), do: counter_get(ref, :credit_parked)

  @spec inc_timers_fired(t()) :: :ok
  def inc_timers_fired(ref), do: counter_bump(ref, :timers_fired)

  @spec timers_fired(t()) :: non_neg_integer()
  def timers_fired(ref), do: counter_get(ref, :timers_fired)

  @doc "Move the armed-timer gauge: one per processing-time timer armed, one less per firing or disarm."
  @spec add_timers_armed(t(), integer()) :: :ok
  def add_timers_armed(ref, delta) when delta >= 0, do: counter_add(ref, :timers_armed, delta)
  def add_timers_armed(ref, delta), do: counter_sub(ref, :timers_armed, -delta)

  @spec timers_armed(t()) :: non_neg_integer()
  def timers_armed(ref), do: counter_get(ref, :timers_armed)

  @typedoc "Which block of start-time slots — one Entity may hold one of each."
  @type block :: :dispatch | :deferred

  @doc """
  Record that `identity` started a dispatch. A slot already held (by this Entity
  re-entering, or by a colliding identity) leaves the dispatch unrecorded.
  """
  @spec dispatch_started(t(), identity()) :: :ok
  def dispatch_started(ref, identity), do: mark_started(ref, :dispatch, identity)

  @doc "Release `identity`'s slot — its dispatch ended, or the Entity died."
  @spec dispatch_finished(t(), identity()) :: :ok
  def dispatch_finished(ref, identity), do: mark_finished(ref, :dispatch, identity)

  @doc """
  Record that `identity` is holding at least one push it has not queued yet.
  Idempotent while it holds: the age reported is the OLDEST push's, so a later
  one must not reset the clock.
  """
  @spec deferred_started(t(), identity()) :: :ok
  def deferred_started(ref, identity), do: mark_started(ref, :deferred, identity)

  @doc "Release `identity`'s deferral slot — every held push has been queued."
  @spec deferred_finished(t(), identity()) :: :ok
  def deferred_finished(ref, identity), do: mark_finished(ref, :deferred, identity)

  @doc """
  Handout liveness: `{dispatches underway, age in ms of the oldest}`, `{0, 0}`
  when the runner is idle. One scan, so the pair cannot disagree — two would
  let the count come from before a dispatch ended and the age from after.
  """
  @spec handout_liveness(t()) :: {non_neg_integer(), non_neg_integer()}
  def handout_liveness(ref), do: liveness(ref, :dispatch)

  @doc """
  Deferred-push liveness: `{Entities holding an unqueued push, age in ms of the
  oldest}`. The same shape as `handout_liveness/1` because it answers the same
  question about the other half of the picture.
  """
  @spec pending_push_liveness(t()) :: {non_neg_integer(), non_neg_integer()}
  def pending_push_liveness(ref), do: liveness(ref, :deferred)

  defp mark_started(ref, block, identity) do
    _ = :atomics.compare_exchange(ref, slot(block, identity), 0, now_ms())
    :ok
  end

  defp mark_finished(ref, block, identity), do: :atomics.put(ref, slot(block, identity), 0)

  defp liveness(ref, block) do
    now = now_ms()
    base = counter_block_base(block)

    Enum.reduce(1..@slots, {0, 0}, fn i, {count, oldest} = idle ->
      case :atomics.get(ref, base + i) do
        0 -> idle
        started -> {count + 1, max(oldest, max(now - started, 0))}
      end
    end)
  end

  defp slot(block, identity),
    do: counter_block_base(block) + 1 + :erlang.phash2(identity, @slots)

  # Wall-clock ms: unsigned atomics cannot hold a monotonic reading, and `0` is
  # the free marker, which a wall-clock millisecond never is.
  defp now_ms, do: System.system_time(:millisecond)
end
