defmodule Flezha.SDK.LocalStream do
  @moduledoc """
  In-node ("native") transport stream: one lightweight process per stream with
  two sides — `:controller` and `:sdk`. Both peers hold a `%#{__MODULE__}{}`
  handle onto the same process, differing only in `:side`, and reach it exactly
  as they reach a socket stream handle.

  A frame sent on one side is delivered to the *other* side's current owner as
  `{:flz_data, id, payload_id, message, metadata}` — by term identity, with no
  framing and no serialization (so `message` may be any term, never assumed a
  binary). Frames that arrive while a side has no active owner are buffered and
  flushed, in order, when the side is activated. Ownership transfer and process
  death synthesize `{:flz_closed, id, reason}` (reason a binary, matching the
  NIF shape) to the surviving side. Stream ids come from
  `System.unique_integer([:positive, :monotonic])`.

  This module is same-node only by construction — a handle is a pid, which is
  not meaningfully transferable across BEAM nodes for this purpose. A runner in
  a node of its own uses the binary protocol over UDS/QUIC, not this transport.
  """

  alias Flezha.SDK.{Knob, SafeCall}

  use GenServer

  @enforce_keys [:pid, :id, :side]
  defstruct [:pid, :id, :side]

  @type side :: :controller | :sdk
  @type t :: %__MODULE__{pid: pid(), id: pos_integer(), side: side()}

  @doc """
  Open a native stream. Returns `{sdk_handle, controller_handle}` — two handles
  onto one process, differing only in `:side`. Both sides start passive with no
  owner; a peer takes its side with `controlling_process/2` then `set_active/1`.
  """
  @spec open() :: {t(), t()}
  def open do
    id = System.unique_integer([:positive, :monotonic])
    {:ok, pid} = GenServer.start(__MODULE__, id)

    {%__MODULE__{pid: pid, id: id, side: :sdk}, %__MODULE__{pid: pid, id: id, side: :controller}}
  end

  # --- per-stream ops ---

  @spec stream_id(t()) :: pos_integer()
  def stream_id(%__MODULE__{id: id}), do: id

  @spec send_frame(t(), binary(), term(), map()) :: :ok | {:error, :closed}
  def send_frame(%__MODULE__{pid: pid, side: side}, payload_id, message, metadata) do
    if Process.alive?(pid) do
      GenServer.cast(pid, {:frame, side, payload_id, message, metadata})
      :ok
    else
      {:error, :closed}
    end
  end

  @spec controlling_process(t(), pid()) :: :ok | {:error, :closed}
  def controlling_process(%__MODULE__{pid: pid, side: side}, new_owner) when is_pid(new_owner) do
    call_if_alive(pid, {:controlling_process, side, new_owner})
  end

  @spec set_active(t()) :: :ok | {:error, :closed}
  def set_active(%__MODULE__{pid: pid, side: side}) do
    call_if_alive(pid, {:set_active, side})
  end

  @spec close(t()) :: :ok
  def close(%__MODULE__{pid: pid, side: side}) do
    if Process.alive?(pid) do
      SafeCall.bounded(pid, {:close, side}, Knob.get(:local_stream_call_ms), on_error: :ok)
    end

    :ok
  end

  defp call_if_alive(pid, msg) do
    if Process.alive?(pid) do
      SafeCall.bounded(pid, msg, Knob.get(:local_stream_call_ms), on_error: {:error, :closed})
    else
      {:error, :closed}
    end
  end

  # --- server ---

  @impl true
  def init(id) do
    side = %{owner: nil, monitor: nil, active: false, buffer: []}
    {:ok, %{id: id, sides: %{controller: side, sdk: side}}}
  end

  @impl true
  def handle_cast({:frame, from_side, payload_id, message, metadata}, state) do
    to = other(from_side)
    frame = {:flz_data, state.id, payload_id, message, metadata}
    side = Map.fetch!(state.sides, to)

    side =
      if side.active and side.owner do
        send(side.owner, frame)
        side
      else
        %{side | buffer: [frame | side.buffer]}
      end

    {:noreply, put_in(state.sides[to], side)}
  end

  @impl true
  def handle_call({:controlling_process, s, new_owner}, _from, state) do
    side = Map.fetch!(state.sides, s)
    if side.monitor, do: Process.demonitor(side.monitor, [:flush])
    mref = Process.monitor(new_owner)
    side = maybe_flush(%{side | owner: new_owner, monitor: mref})
    {:reply, :ok, put_in(state.sides[s], side)}
  end

  def handle_call({:set_active, s}, _from, state) do
    side = maybe_flush(%{Map.fetch!(state.sides, s) | active: true})
    {:reply, :ok, put_in(state.sides[s], side)}
  end

  def handle_call({:close, s}, _from, state) do
    notify_close(state, other(s), "closed")
    {:stop, :normal, :ok, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    dead =
      Enum.find(~w(controller sdk)a, fn s -> state.sides[s].monitor == ref end)

    if dead, do: notify_close(state, other(dead), "peer_down")
    {:stop, :normal, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  # A crash (any non-normal reason) still tells both owners the wire is gone.
  # Normal stops route their close explicitly (`:close`/`:DOWN`), so skip them.
  def terminate(:normal, _state), do: :ok
  def terminate(:shutdown, _state), do: :ok

  def terminate(_reason, state) do
    for s <- [:controller, :sdk], owner = state.sides[s].owner, owner != nil do
      send(owner, {:flz_closed, state.id, "stream_crashed"})
    end

    :ok
  end

  defp maybe_flush(%{active: true, owner: owner, buffer: buf} = side) when owner != nil do
    Enum.each(Enum.reverse(buf), &send(owner, &1))
    %{side | buffer: []}
  end

  defp maybe_flush(side), do: side

  defp notify_close(state, s, reason) do
    case state.sides[s].owner do
      nil -> :ok
      owner -> send(owner, {:flz_closed, state.id, reason})
    end
  end

  defp other(:controller), do: :sdk
  defp other(:sdk), do: :controller
end
