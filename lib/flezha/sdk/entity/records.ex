defmodule Flezha.SDK.Entity.Records do
  @moduledoc """
  The durable half of an Entity — every function that moves `proc_state`,
  `change_seq`, the record stream or the slot: the records a dispatch ships
  (before its outputs leave), the one effect record the effectively-once tier
  ships in place of them, the restore fold, the compaction snapshot after
  the records, the eviction's delete, and the slot a replacement of an Entity
  killed untrappably seeds from. The process — its mailbox, its ladder, its
  park — is `Flezha.SDK.Entity`, which enters here from `dispatch/2` and
  `init/1` only.
  """

  require Logger

  alias Flezha.SDK.{Hold, Record, Timers}
  alias Flezha.SDK.Entity.{Interval, State}

  @doc """
  Rebuild the state from a `RestoreInitState`; `{:ok, state, terminal, migrated}`.
  The open interval is discarded — the Entity re-seats on a past in which none
  of it was applied — and the tier's consumed floor and the event-time
  high-water come back off the restored effect records.
  """
  @spec restore(State.t(), binary(), binary(), binary(), non_neg_integer()) ::
          {:ok, State.t(), non_neg_integer(), boolean()} | {:error, String.t()}
  def restore(%State{changeful?: true} = s, header, snapshot, log, hint) do
    case Record.fold(s.module, s.entity_key, header, snapshot, log, hint) do
      {:ok, restored, terminal, migrated} ->
        timers = %{s.timers | pending: restored.timers, high_water: restored.high_water}
        # The slice goes with the interval: a restore re-seats the Entity on a
        # past in which no buffered member was seen, and the controller
        # re-forwards the slice from the rewound watermark.
        tier = %{
          s.tier
          | staged: Interval.clear(s.tier.staged),
            floor: restored.floor,
            slice: []
        }

        s = %{
          s
          | proc_state: restored.state,
            timers: timers,
            tier: tier,
            change_seq: terminal + 1
        }

        {:ok, write_slot(s), terminal, migrated}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def restore(%State{} = s, _header, _snapshot, _log, _hint),
    do: {:error, "#{State.label(s)} persists nothing"}

  # The snapshot folds every seq already on the wire: nothing is pending, so
  # that is `change_seq - 1`, and the frame leaves behind the records on the
  # same stream from the same process. The pending timers are re-stated ABOVE
  # that seq afterwards: the snapshot's body holds the state alone, and the
  # truncation it authorises would otherwise take the timers with it.
  @doc """
  Ship the state as an `entity_state_snapshot`; `{:ok, state, snapshot_seq,
  body_size}`. A refused write is answered to the caller — the controller
  retries compaction — and the frame is still held for the next bound stream.
  """
  @spec snapshot(State.t()) ::
          {:ok, State.t(), non_neg_integer(), non_neg_integer()}
          | {:error, State.t(), String.t()}
  def snapshot(%State{changeful?: false} = s),
    do: {:error, s, "#{State.label(s)} has no state stream"}

  def snapshot(%State{} = s) do
    seq = max(s.change_seq - 1, 0)
    body = Record.encode_body(s.proc_state)
    frame = Record.frame(s.identity, seq, body, s.schema_version)
    {verdict, hold} = Hold.write("entity_state_snapshot", s.stream, {frame, %{}})
    # The re-statement follows the snapshot whichever answer the wire gave, so
    # a held pair replays in the order that keeps the timers above the seq the
    # truncation is authorised to.
    s = restate_timers(%{s | stream: hold})

    case verdict do
      :ok -> {:ok, s, seq, byte_size(body)}
      {:held, reason} -> {:error, s, inspect(reason)}
    end
  end

  # Destructive: the delete follows the records on the same stream from the
  # same process, and the slot goes so a successor starts from `init_state/1`
  # rather than from what this Entity last shipped. A hold that dies with the
  # process that owned it is not a hold, so what no stream would take — the
  # records still held, then the delete — passes to the Operative, which
  # outlives this Entity and replays it on the next bound stream.
  @doc "What an eviction leaves behind: the delete when persistent, no slot, and no lost frame."
  @spec delete(State.t()) :: :ok
  def delete(%State{} = s) do
    {items, _empty} = Hold.drain(s.stream)
    # The route goes with the Entity: the Operative writes what it takes over
    # on the stream this identity chooses, never on the one this hold used.
    held = Enum.map(items, fn {key, body, meta, _route} -> {key, body, meta} end)
    hand_off(s, held ++ owed_delete(s, held))
    if s.slots, do: :ets.delete(s.slots, s.identity)
    :ok
  end

  @doc "Ship a dispatch's changes, one record each, then its timer ops as one record; the seq advances past each."
  @spec ship(State.t(), [term()], [Timers.op()]) :: State.t()
  def ship(%State{changeful?: false} = s, _changes, _ops), do: s
  def ship(%State{} = s, [], []), do: s

  def ship(%State{} = s, changes, ops) do
    {s, seq} =
      Enum.reduce(changes, {s, s.change_seq}, fn change, {acc, seq} ->
        {ship_change(acc, seq, change), seq + 1}
      end)

    {s, seq} = ship_ops(s, seq, ops)
    write_slot(%{s | change_seq: seq})
  end

  defp ship_change(%State{persistent?: false} = s, _seq, _change), do: s

  defp ship_change(%State{} = s, seq, change),
    do: send_record(s, "entity_state_change", change_frame(s, seq, change))

  @doc """
  The tier's ship: fold one invocation into the staged interval, then — when
  the fold produced a record — one `entity_effect_record` at the next seq, then
  the slot. One frame where `ship/3` writes many, and the offset it carries is
  what makes a re-forward a completed dispatch rather than a re-run.
  """
  @spec settle(State.t(), Interval.effects(), map() | nil, reference() | nil) :: State.t()
  def settle(s, effects, dlq, hold_ref \\ nil)

  def settle(%State{} = s, effects, dlq, hold_ref) do
    {shipped, staged} =
      Interval.fold(
        s.tier.staged,
        s.tier.checkpoint,
        %{effects | parked: dlq != nil},
        interval_now(s)
      )

    s = %{s | tier: %{s.tier | staged: staged}}

    case shipped do
      nil -> write_slot(s)
      record -> ship_effect(s, record, dlq, hold_ref)
    end
  end

  @doc """
  Close the staged interval on its deadline: no invocation of its own, one
  record for what the interval accumulated, and nothing at all when a close
  already happened — the spent entry the reference leaves armed.
  """
  @spec settle_close(State.t()) :: State.t()
  def settle_close(%State{} = s) do
    {shipped, staged} = Interval.close_on_deadline(s.tier.staged)
    s = %{s | tier: %{s.tier | staged: staged}}

    case shipped do
      nil -> s
      record -> ship_effect(s, record, nil, nil)
    end
  end

  @doc """
  Ship the head of the owed FIFO: the record whose inter-procedure pushes the
  router has now delivered. The router reports in the order it was cast, so the
  head is always the reported one; anything else is a bug rather than a race,
  and it is said out loud.
  """
  @spec release(State.t(), reference()) :: State.t()
  def release(%State{tier: tier} = s, ref) do
    case :queue.out(tier.owed) do
      {{:value, {^ref, _seq, frame}}, rest} ->
        s = send_record(s, "entity_effect_record", frame)
        %{s | tier: %{s.tier | owed: rest}}

      _other ->
        Logger.warning(
          "[Flezha.SDK] #{State.label(s)} edge report #{inspect(ref)} is not the owed head"
        )

        s
    end
  end

  @doc """
  Ask the controller to correct behind `target_event_time`: one fire-and-forget
  frame on the Entity's own state stream. The controller coalesces duplicates
  and re-raises a lost one when the input is re-delivered, so there is no ack
  and nothing here to keep.
  """
  @spec request_correction(State.t(), non_neg_integer()) :: :ok
  def request_correction(%State{} = s, target_event_time) do
    frame = Record.correction_frame(s.identity, target_event_time)

    case Hold.write("correction_request", s.stream, {frame, %{}}) do
      {:ok, _hold} ->
        :ok

      {{:error, reason}, _hold} ->
        Logger.warning(
          "[Flezha.SDK] #{State.label(s)} correction request undelivered: #{inspect(reason)}"
        )
    end
  end

  defp ship_effect(%State{persistent?: false} = s, _record, _dlq, _hold_ref),
    do: write_slot(%{s | change_seq: s.change_seq + 1})

  defp ship_effect(%State{} = s, record, dlq, hold_ref) do
    seq = s.change_seq
    frame = Record.effect_frame(s.identity, seq, effect_body(record, dlq), s.schema_version)
    write_slot(hold_or_ship(%{s | change_seq: seq + 1}, seq, frame, hold_ref))
  end

  # An invocation with no inter-procedure output ships at once. One that has
  # them holds its RECORD until the router reports every push admitted, so an
  # offset is never durable ahead of the output it produced. Never its queue:
  # the router may be blocked pushing INTO this Entity, and an Entity that
  # stopped draining to wait for that router would be half of a cycle.
  defp hold_or_ship(%State{} = s, _seq, frame, nil),
    do: send_record(s, "entity_effect_record", frame)

  defp hold_or_ship(%State{tier: tier} = s, seq, frame, ref),
    do: %{s | tier: %{tier | owed: :queue.in({ref, seq, frame}, tier.owed)}}

  defp effect_body(record, dlq) do
    %{
      changes: Enum.map(record.changes, &Record.encode_body/1),
      outputs: record.outputs,
      consumed_offset: record.offset,
      dlq: dlq,
      event_time: record.event_time,
      timers: record.timers
    }
  end

  # The interval's clock is the wall, never the declared one — it is what the
  # deadline is measured against. A class with no checkpoint policy has no
  # interval to time, so it reads no clock at all.
  defp interval_now(%State{tier: %{checkpoint: nil}}), do: 0
  defp interval_now(%State{}), do: System.system_time(:millisecond)

  defp change_frame(%State{} = s, seq, change),
    do: Record.frame(s.identity, seq, Record.encode_body(change), s.schema_version)

  # The trailing timer-ops record, at the seq after the changes.
  defp ship_ops(s, seq, []), do: {s, seq}

  defp ship_ops(%State{persistent?: false} = s, seq, _ops), do: {s, seq + 1}

  defp ship_ops(%State{} = s, seq, ops) do
    frame = Record.timer_ops_frame(s.identity, seq, ops, s.schema_version)
    {send_record(s, "entity_state_change", frame), seq + 1}
  end

  defp restate_timers(%State{timers: %{pending: pending}} = s) do
    if Timers.empty?(pending) do
      s
    else
      {s, seq} = ship_ops(s, s.change_seq, Timers.register_ops(pending))
      write_slot(%{s | change_seq: seq})
    end
  end

  # Between a close and its reconnect there is no stream: the class holds the
  # record for the next bound one, and the drain replays it in the order the
  # changes were made. Parking on an exhausted credit window happens here, in
  # the Entity, and the router's push behind it waits — the emit-path park.
  defp send_record(%State{} = s, payload_id, frame) do
    {_verdict, hold} = Hold.write(payload_id, s.stream, {frame, %{}})
    %{s | stream: hold}
  end

  # Fire-and-forget, outside the credit window, as the shared core's own is. It
  # goes on the wire only when nothing is held ahead of it: a delete that
  # overtook this Entity's own records would tell the controller to forget
  # state it has not been told about yet.
  defp owed_delete(%State{persistent?: false}, _ahead), do: []

  defp owed_delete(%State{} = s, []) do
    case Hold.write("delete_entity_state", s.stream, {s.identity, %{}}) do
      {:ok, _hold} -> []
      {{:held, _reason}, _hold} -> [{"delete_entity_state", s.identity, %{}}]
    end
  end

  defp owed_delete(%State{} = s, _ahead), do: [{"delete_entity_state", s.identity, %{}}]

  # The identity rides with the frames: the Operative writes them on the stream
  # that identity chooses — the one a replacement Entity will bind to — so this
  # Entity's last frames stay ahead of its replacement's first.
  defp hand_off(%State{}, []), do: :ok
  defp hand_off(%State{} = s, frames), do: send(s.operative, {:handed, s.identity, frames})

  # A replacement for an Entity killed untrappably continues from what its
  # predecessor last shipped; a first Entity starts from `init_state/1`. The
  # tier's staged interval and consumed floor ride the slot too, so the
  # replacement re-holds what its predecessor had not yet made durable.
  def seed(nil, _identity, module, key), do: fresh(module, key)

  def seed(slots, identity, module, key) do
    case :ets.lookup(slots, identity) do
      [{^identity, held}] -> held
      [] -> fresh(module, key)
    end
  end

  defp fresh(module, key) do
    %{
      state: module.init_state(key),
      seq: 0,
      timers: Timers.new(),
      staged: %Interval{},
      floor: -1
    }
  end

  # The slot is what the WAL holds — written after the records are on the
  # wire, read by the replacement of an Entity killed untrappably.
  def write_slot(%State{changeful?: true, slots: slots} = s) when slots != nil do
    :ets.insert(
      slots,
      {s.identity,
       %{
         state: s.proc_state,
         seq: s.change_seq,
         timers: s.timers.pending,
         staged: s.tier.staged,
         floor: s.tier.floor
       }}
    )

    s
  end

  def write_slot(s), do: s

  @doc """
  Bind the Entity to one of `secondaries` and replay what it held while there
  was none. One stream per Entity, chosen by identity: its records and its
  snapshot stay in order because one process writes them to one stream.
  """
  @spec rebind(State.t(), [term()]) :: State.t()
  def rebind(%State{} = s, []), do: %{s | stream: Hold.bind(s.stream, [])}

  # A bound stream is what a held record was waiting for, and the replay is one
  # pass in the order the records were written, so `change_seq` is never
  # inverted.
  def rebind(%State{} = s, secondaries) do
    handle = Hold.secondary_for(secondaries, s.identity)
    %{s | stream: s.stream |> Hold.bind([handle]) |> Hold.replay()}
  end
end
