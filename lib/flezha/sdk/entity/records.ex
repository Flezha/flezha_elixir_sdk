defmodule Flezha.SDK.Entity.Records do
  @moduledoc """
  The durable half of an Entity — every function that moves `proc_state`,
  `change_seq`, the record stream or the slot: the records a dispatch ships
  (before its outputs leave), the restore fold, the compaction snapshot after
  the records, the eviction's delete, and the slot a replacement of an Entity
  killed untrappably seeds from. The process — its mailbox, its ladder, its
  park — is `Flezha.SDK.Entity`, which enters here from `dispatch/2` and
  `init/1` only.
  """

  require Logger

  alias Flezha.SDK.{Credit, Record, Timers}
  alias Flezha.SDK.Entity.State
  alias Flezha.Transport.Stream, as: TransportStream

  @doc "Rebuild the state from a `RestoreInitState`; `{:ok, state, terminal, migrated}`."
  @spec restore(State.t(), binary(), binary(), binary(), non_neg_integer()) ::
          {:ok, State.t(), non_neg_integer(), boolean()} | {:error, String.t()}
  def restore(%State{changeful?: true} = s, header, snapshot, log, hint) do
    case Record.fold(s.module, s.entity_key, header, snapshot, log, hint) do
      {:ok, state, pending, terminal, migrated} ->
        timers = %{s.timers | pending: pending, high_water: nil}
        s = %{s | proc_state: state, timers: timers, change_seq: terminal + 1}
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
  @doc "Ship the state as an `entity_state_snapshot`; `{:ok, state, snapshot_seq, body_size}`."
  @spec snapshot(State.t()) ::
          {:ok, State.t(), non_neg_integer(), non_neg_integer()} | {:error, String.t()}
  def snapshot(%State{changeful?: true, secondary: secondary} = s) when secondary != nil do
    seq = max(s.change_seq - 1, 0)
    body = Record.encode_body(s.proc_state)
    frame = Record.frame(s.identity, seq, body, s.schema_version)

    case TransportStream.send_frame(secondary, "entity_state_snapshot", frame, %{}) do
      :ok -> {:ok, restate_timers(s), seq, byte_size(body)}
      {:error, reason} -> {:error, inspect(reason)}
    end
  end

  def snapshot(%State{} = s), do: {:error, "#{State.label(s)} has no state stream"}

  # Destructive: the delete follows the records on the same stream from the
  # same process, and the slot goes so a successor starts from `init_state/1`
  # rather than from what this Entity last shipped.
  @doc "What an eviction leaves behind: the delete frame when persistent, no slot."
  @spec delete(State.t()) :: :ok
  def delete(%State{} = s) do
    if s.persistent?, do: send_delete(s)
    if s.slots, do: :ets.delete(s.slots, s.identity)
    :ok
  end

  @doc "Ship a dispatch's changes, one record each, then its timer ops as one record; the seq advances past each."
  @spec ship(State.t(), [term()], [Timers.op()]) :: State.t()
  def ship(%State{changeful?: false} = s, _changes, _ops), do: s
  def ship(%State{} = s, [], []), do: s

  def ship(%State{} = s, changes, ops) do
    seq =
      Enum.reduce(changes, s.change_seq, fn change, seq ->
        if s.persistent?, do: send_record(s, seq, change_frame(s, seq, change))
        seq + 1
      end)

    write_slot(%{s | change_seq: ship_ops(s, seq, ops)})
  end

  defp change_frame(%State{} = s, seq, change),
    do: Record.frame(s.identity, seq, Record.encode_body(change), s.schema_version)

  # The trailing timer-ops record, at the seq after the changes.
  defp ship_ops(_s, seq, []), do: seq

  defp ship_ops(%State{} = s, seq, ops) do
    if s.persistent?,
      do: send_record(s, seq, Record.timer_ops_frame(s.identity, seq, ops, s.schema_version))

    seq + 1
  end

  defp restate_timers(%State{timers: %{pending: pending}} = s) do
    if Timers.empty?(pending),
      do: s,
      else: write_slot(%{s | change_seq: ship_ops(s, s.change_seq, Timers.register_ops(pending))})
  end

  # Between a close and its reconnect there is no stream: the seq still
  # advances, so the gap it leaves is what makes the controller resync the
  # Entity from its own copy rather than a silently diverged one.
  defp send_record(%State{secondary: nil} = s, seq, _frame) do
    Logger.warning(
      "[Flezha.SDK] #{State.label(s)} record seq #{seq} had no state stream (reconnecting); " <>
        "the controller resyncs the gap"
    )
  end

  # Debit-on-send: parks here, in the Entity, when the window is exhausted —
  # and the router's push behind it waits, which is the emit-path park.
  defp send_record(%State{} = s, seq, frame) do
    Credit.acquire(s.credit, byte_size(frame))

    case TransportStream.send_frame(s.secondary, "entity_state_change", frame, %{}) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "[Flezha.SDK] #{State.label(s)} record seq #{seq} undelivered: #{inspect(reason)}"
        )
    end
  end

  # Fire-and-forget, outside the credit window, as the shared core's own is.
  defp send_delete(%State{secondary: nil} = s),
    do:
      Logger.warning("[Flezha.SDK] #{State.label(s)} evicted with no state stream (reconnecting)")

  defp send_delete(%State{} = s) do
    case TransportStream.send_frame(s.secondary, "delete_entity_state", s.identity, %{}) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("[Flezha.SDK] #{State.label(s)} delete undelivered: #{inspect(reason)}")
    end
  end

  # A replacement for an Entity killed untrappably continues from what its
  # predecessor last shipped; a first Entity starts from `init_state/1`.
  def seed(nil, _identity, module, key), do: {module.init_state(key), 0, Timers.new()}

  def seed(slots, identity, module, key) do
    case :ets.lookup(slots, identity) do
      [{^identity, state, seq, timers}] -> {state, seq, timers}
      [] -> {module.init_state(key), 0, Timers.new()}
    end
  end

  # The slot is what the WAL holds — written after the records are on the
  # wire, read by the replacement of an Entity killed untrappably.
  def write_slot(%State{changeful?: true, slots: slots} = s) when slots != nil do
    :ets.insert(slots, {s.identity, s.proc_state, s.change_seq, s.timers.pending})
    s
  end

  def write_slot(s), do: s

  # One stream per Entity, chosen by identity: its records and its snapshot
  # stay in order because one process writes them to one stream.
  def put_secondaries(%State{} = s, []), do: %{s | secondary: nil}

  def put_secondaries(%State{} = s, secondaries),
    do: %{
      s
      | secondary: Enum.at(secondaries, rem(:erlang.phash2(s.identity), length(secondaries)))
    }
end
