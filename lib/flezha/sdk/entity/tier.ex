defmodule Flezha.SDK.Entity.Tier do
  @moduledoc """
  What an Entity does under the effectively-once tier — every function that
  moves the staged interval, the armed deadline, the owed FIFO or the replay
  slice, and the lateness classification a declared correction window adds. The process
  half (its mailbox, its ladder, its timers) is `Flezha.SDK.Entity`, which
  enters here from `complete/4`, `park/5` and the close item only; the durable
  half is `Flezha.SDK.Entity.Records`, which this module calls and never the
  other way round.

  One invocation becomes ONE record: its changes, the plugin outputs it
  captures here rather than letting the router ship them, the offset it
  consumed and its timer ops. The record is held only when the invocation also
  produced an inter-procedure output — never the Entity's queue, which the
  router may be waiting on at that moment.
  """

  alias Flezha.SDK.Entity.{Interval, Records, State}
  alias Flezha.SDK.Operative.Routing

  # The end of a replayed slice, taken from the Entity's own queue in mailbox
  # order behind the members it closes. Not the guarantee — that is
  # `Operative.push/6`'s synchronous `enqueue_block/2` — but the thing that
  # keeps the order right if that call ever becomes a cast, which would
  # otherwise misfold a slice with no error anywhere.
  defmodule SliceEnd do
    @moduledoc false
    defstruct []
  end

  @doc """
  Whether an input is dispatched at all, and whether it moves the event-time
  high-water — the twin of the core's `admit_and_track`. A late input inside
  the declared window is withheld HERE and asked for back; the replayed slice
  re-delivers it in place, so nothing is lost by not dispatching it.
  """
  @spec admit(State.t(), non_neg_integer(), atom() | nil) ::
          :advance | :dispatch_only | :withhold | {:buffer, non_neg_integer()}
  def admit(%State{tier: %{correction: nil}}, _event_time, _replay), do: :advance

  # A member of the ordered slice is in order by construction: it advances the
  # high-water and is never classified again.
  def admit(%State{}, _event_time, :ordered), do: :advance

  # A member still waiting for its slice to close: held, not classified. It is
  # behind the high-water by construction, and re-running it in arrival order
  # would recompute the very answer the correction exists to fix.
  def admit(%State{}, event_time, :member), do: {:buffer, event_time}

  def admit(%State{tier: %{correction: policy}} = s, event_time, _replay) do
    case classify(event_time, s.timers.high_water, policy.window_ms) do
      :on_time ->
        :advance

      # Requested, and dropped here: the slice brings it back. With no state
      # stream there is nobody to ask, so it falls back to the behaviour of a
      # class that declared no window at all.
      :correctable ->
        if s.secondary do
          Records.request_correction(s, event_time)
          :withhold
        else
          :advance
        end

      # The high-water deliberately does NOT advance for a beyond-window
      # arrival: a very old event must not drag the window backward and make
      # every input after it look late.
      :beyond_window ->
        case policy.beyond_window do
          :process_as_arrival -> :dispatch_only
          :drop -> :withhold
        end
    end
  end

  @doc """
  An input's lateness against the high-water, which is `nil` until something
  has been dispatched — so a first input is never late. The window is
  inclusive: exactly `window_ms` behind is still correctable.
  """
  @spec classify(non_neg_integer(), non_neg_integer() | nil, non_neg_integer()) ::
          :on_time | :correctable | :beyond_window
  def classify(_event_time, nil, _window_ms), do: :on_time

  def classify(event_time, high_water, window_ms) do
    cond do
      event_time >= high_water -> :on_time
      high_water - event_time <= window_ms -> :correctable
      true -> :beyond_window
    end
  end

  @doc "Hold a replayed slice member, with the event time the flush will order it by."
  @spec buffer(State.t(), struct(), non_neg_integer()) :: State.t()
  def buffer(%State{tier: tier} = s, member, event_time),
    do: %{s | tier: %{tier | slice: [{event_time, member} | tier.slice]}}

  @doc """
  Dispatch the held slice: ascending event time, ties broken by ascending
  input-log offset, back at the HEAD of the queue — so an input enqueued behind
  the end marker folds after every member, and each member goes through the
  ordinary drain, where an event-time timer it makes due fires between the two
  members that straddle it.
  """
  @spec flush_slice(State.t(), (State.t(), [struct()] -> State.t())) :: State.t()
  def flush_slice(%State{tier: %{slice: []}} = s, _requeue), do: s

  def flush_slice(%State{tier: tier} = s, requeue) do
    ordered =
      tier.slice
      |> Enum.sort_by(fn {event_time, member} -> {event_time, member.offset || 0} end)
      |> Enum.map(fn {_event_time, member} -> %{member | replay: :ordered} end)

    requeue.(%{s | tier: %{tier | slice: []}}, ordered)
  end

  @doc """
  Fold one invocation's effects into the interval and ship what the fold
  produced. Answers the state and the report the router owes back — `{pid,
  ref}` when a record waits on this invocation's inter-procedure pushes, nil
  when it shipped at once.
  """
  @spec settle(State.t(), map(), [map()], non_neg_integer() | nil) ::
          {State.t(), {pid(), reference()} | nil}
  def settle(%State{} = s, facts, ops, offset) do
    effects = %{
      changes: facts.changes,
      timers: ops,
      outputs: capture_outputs(s, facts.emits),
      offset: offset,
      event_time: facts.event_time,
      checkpointed: facts.checkpointed?,
      parked: false
    }

    # An invocation whose emits reach another procedure holds its record until
    # the router reports every push admitted; one that reaches only sinks has
    # its outputs IN the record and ships at once.
    hold_ref = if s.tier.edges? and facts.emits != [], do: make_ref()
    s = s |> Records.settle(effects, nil, hold_ref) |> arm_deadline()
    {s, hold_ref && {self(), hold_ref}}
  end

  @doc "A park under the tier: the entry rides the record beside the offset, and nothing is held."
  @spec park(State.t(), map(), [map()], non_neg_integer() | nil, non_neg_integer()) :: State.t()
  def park(%State{} = s, dlq, ops, offset, event_time) do
    effects = %{
      changes: [],
      timers: ops,
      outputs: [],
      offset: offset,
      event_time: event_time,
      checkpointed: false,
      parked: true
    }

    s |> Records.settle(effects, dlq) |> arm_deadline()
  end

  @doc """
  The deadline's close, for the interval it was armed for. A close that
  already happened leaves this one to find nothing open, which is what the
  spent entry the reference leaves armed does too.
  """
  @spec close(State.t(), non_neg_integer()) :: State.t()
  def close(%State{tier: %{staged: %{opened_at_ms: at}}} = s, at) when at != nil,
    do: s |> Records.settle_close() |> arm_deadline()

  def close(%State{} = s, _stale), do: s

  @doc "Cancel any armed deadline — a restore re-seats on a past the interval is not part of."
  @spec cancel_deadline(State.t()) :: State.t()
  def cancel_deadline(%State{tier: %{deadline: nil}} = s), do: s

  def cancel_deadline(%State{tier: %{deadline: {_opened, ref}} = tier} = s) do
    Process.cancel_timer(ref)
    %{s | tier: %{tier | deadline: nil}}
  end

  # The invocation's plugin outputs as the record carries them, through the
  # same key rule the router uses. No identity stamps: the controller derives
  # them at flush, and a host that also stamped would produce a second answer.
  defp capture_outputs(%State{tier: %{sink_routes: []}}, _emits), do: []
  defp capture_outputs(%State{}, []), do: []

  defp capture_outputs(%State{tier: %{sink_routes: routes}} = s, emits) do
    origin = %{entity_key: s.entity_key}

    for {value, meta, key} <- emits,
        route <- routes,
        emit = %{meta: meta, key: key, origin: origin},
        out <- route.transform.(value) do
      %{
        channel: route.channel,
        key: Routing.sink_key(route, out, emit) || "",
        metadata: meta |> Map.delete("channel") |> Enum.sort(),
        payload: route.serializer.(out)
      }
    end
  end

  # One `Process.send_after/3` per open interval — the same BEAM timer service
  # the Entity's own timers ride. A close that beats it leaves the message
  # armed, and it finds nothing open.
  defp arm_deadline(%State{tier: %{checkpoint: %{max_delay_ms: delay}} = tier} = s)
       when is_integer(delay) do
    opened = tier.staged.opened_at_ms

    cond do
      opened == nil -> cancel_deadline(s)
      match?({^opened, _ref}, tier.deadline) -> s
      true -> rearm_deadline(cancel_deadline(s), opened)
    end
  end

  defp arm_deadline(%State{} = s), do: s

  defp rearm_deadline(%State{tier: tier} = s, opened) do
    at = Interval.deadline_ms(tier.staged, tier.checkpoint)
    delay = max(at - System.system_time(:millisecond), 0)
    ref = Process.send_after(self(), {:close_interval, opened}, delay)
    %{s | tier: %{tier | deadline: {opened, ref}}}
  end
end
