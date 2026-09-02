defmodule Flezha.SDK.Entity.Clock do
  @moduledoc """
  Everything an Entity does with its `timers` map: the clock its Procedure
  declared and the value that clock serves, the event-time high-water, and the
  BEAM timer refs a processing-time registration arms.

  Split out of `Flezha.SDK.Entity` by mutation (charter §7) — every function
  here writes `timers.clock`, `timers.refs` or `timers.high_water`, and the
  process half keeps its mailbox, its queue and its ladder. This module aliases
  `Counters`, `Procedure`, `Timers` and `State`, and **never names `Entity`**,
  so the split forms no cycle. Its functions run in the Entity's own process,
  which is what makes `self()` in `arm/2` the Entity.
  """

  alias Flezha.SDK.{Counters, Procedure, Timers}
  alias Flezha.SDK.Entity.State

  @doc "The `timers` map a fresh Entity starts from, over what its slot held."
  @spec seed(module(), map()) :: map()
  def seed(module, held) do
    %{
      pending: held.timers,
      clock: of(module),
      on_timer?: Procedure.on_timer?(module),
      refs: %{},
      high_water: nil
    }
  end

  @doc "The declared clock, with an event-time procedure's extractor captured."
  @spec of(module()) :: :wall | :logical | {:event_time, (term() -> non_neg_integer())}
  def of(module) do
    case Procedure.clock(module) do
      :event_time -> {:event_time, &module.event_time_of/1}
      clock -> clock
    end
  end

  @doc """
  The declared clock's value for `value`: the epoch millisecond, the durable
  seq this dispatch starts at, or the input's own event time. A firing's
  scheduled timestamp is the caller's answer under every clock.
  """
  @spec now(State.t(), term()) :: non_neg_integer()
  def now(%State{timers: %{clock: :wall}}, _value), do: System.system_time(:millisecond)
  def now(%State{timers: %{clock: :logical}, change_seq: seq}, _value), do: seq
  def now(%State{timers: %{clock: {:event_time, extract}}}, value), do: extract.(value)

  @doc "An event time, or `:error` when the procedure's extractor raises, throws or exits."
  @spec extract((term() -> non_neg_integer()), term()) :: {:ok, non_neg_integer()} | :error
  def extract(extractor, value) do
    {:ok, extractor.(value)}
  rescue
    _ -> :error
  catch
    _, _ -> :error
  end

  @doc "Raise the event-time high-water to `event_time`; it never falls, so a replay cannot lower it."
  @spec advance(State.t(), non_neg_integer()) :: State.t()
  def advance(%State{timers: timers} = s, event_time),
    do: %{s | timers: %{timers | high_water: max(timers.high_water || -1, event_time)}}

  @doc """
  Commit timer ops onto the BEAM's timer service — one `send_after` per
  processing-time registration, cancelled per removal — with the counters and
  the telemetry. A dispatch that touched no timer, the common one, pays nothing.
  """
  @spec reflect(State.t(), [map()], map()) :: State.t()
  def reflect(%State{} = s, [], %{registered: 0, fired: 0, cancelled: 0, refused: 0}), do: s

  def reflect(%State{} = s, ops, tally) do
    s =
      Enum.reduce(ops, s, fn
        %{kind: :processing_time, register: true, scheduled_at: at}, acc -> arm(acc, at)
        %{kind: :processing_time, register: false, scheduled_at: at}, acc -> disarm(acc, at)
        _event_time, acc -> acc
      end)

    if tally.fired > 0, do: Counters.inc_timers_fired(s.counters)
    Enum.each(tally, fn {event, n} -> if n > 0, do: event(s, event, n) end)
    s
  end

  @doc "Every armed ref cancelled: an eviction, or an Operative that has gone."
  @spec disarm_all(State.t()) :: State.t()
  def disarm_all(%State{timers: timers} = s),
    do: Enum.reduce(Map.keys(timers.refs), s, &disarm(&2, &1))

  @doc "What a restore folded or a slot seeded, armed afresh; an overdue one fires at once."
  @spec rearm(State.t()) :: State.t()
  def rearm(%State{} = s) do
    s = disarm_all(s)
    Enum.reduce(Timers.all(s.timers.pending, :processing_time), s, &arm(&2, &1))
  end

  defp arm(%State{} = s, at) do
    %State{timers: timers} = s = disarm(s, at)
    delay = max(at - System.system_time(:millisecond), 0)
    ref = Process.send_after(self(), {:timer_due, at}, delay)
    Counters.add_timers_armed(s.counters, 1)
    %{s | timers: %{timers | refs: Map.put(timers.refs, at, ref)}}
  end

  defp disarm(%State{timers: timers} = s, at) do
    case Map.pop(timers.refs, at) do
      {nil, _refs} ->
        s

      {ref, refs} ->
        Process.cancel_timer(ref)
        Counters.add_timers_armed(s.counters, -1)
        %{s | timers: %{timers | refs: refs}}
    end
  end

  defp event(%State{} = s, event, count) do
    :telemetry.execute([:flezha, :sdk, :timers], %{count: count}, %{
      operative_id: s.operative_id,
      procedure: s.module.name(),
      event: event
    })
  end
end
