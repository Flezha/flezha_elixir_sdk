defmodule Flezha.SDK.Entity.Tier do
  @moduledoc """
  What an Entity does under the effectively-once tier — every function that
  moves the staged interval, the armed deadline or the owed FIFO. The process
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
