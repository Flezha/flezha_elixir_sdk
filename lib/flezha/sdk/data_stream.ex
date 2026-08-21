defmodule Flezha.SDK.DataStream do
  @moduledoc """
  Owns one bound primary stream sdk-side handle and routes inbound
  `{:flz_data, _, operative_id, message, metadata}` to the owning operative — a
  per-stream reader, so primaries route concurrently rather than serializing
  through the runner. The operative registry is a shared ETS table the runner
  owns; a `{:flz_closed, ...}` is reported to the runner for reconnect.
  """

  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    state = %{
      handle: Keyword.fetch!(opts, :handle),
      registry: Keyword.fetch!(opts, :registry),
      runner: Keyword.fetch!(opts, :runner)
    }

    :ok = Flezha.Transport.Stream.controlling_process(state.handle, self())
    :ok = Flezha.Transport.Stream.set_active(state.handle)
    {:ok, state}
  end

  @impl true
  def handle_info({:flz_data, _id, operative_id, message, metadata}, state) do
    case :ets.lookup(state.registry, operative_id) do
      [{^operative_id, op_pid}] ->
        Flezha.SDK.Operative.input(op_pid, metadata["channel"], message, metadata)

      [] ->
        :ok
    end

    {:noreply, state}
  end

  def handle_info({:flz_closed, _id, reason}, state) do
    send(state.runner, {:data_stream_closed, self(), reason})
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}
end
