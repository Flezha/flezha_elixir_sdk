defmodule Flezha.SDK.DataStream do
  @moduledoc """
  Owns one bound primary stream sdk-side handle and routes inbound
  `{:flz_data, _, operator_id, message, metadata}` to the owning operator — a
  per-stream reader, so primaries route concurrently rather than serializing
  through the runner. The operator registry is a shared ETS table the runner
  owns; a `{:flz_closed, ...}` is reported to the runner for reconnect.
  """

  use GenServer

  alias Flezha.SDK.Operator.Inbox

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    state = %{
      handle: Keyword.fetch!(opts, :handle),
      registry: Keyword.fetch!(opts, :registry),
      runner: Keyword.fetch!(opts, :runner)
    }

    :ok = Flezha.SDK.Transport.controlling_process(state.handle, self())
    :ok = Flezha.SDK.Transport.set_active(state.handle)
    {:ok, state}
  end

  @impl true
  def handle_info({:flz_data, _id, operator_id, message, metadata}, state) do
    # A stepped scope's frame names its outer instance and carries the rest
    # of the scope's id, which no frame header has room for.
    operator_id = operator_id <> Flezha.SDK.Scope.suffix(metadata)

    case :ets.lookup(state.registry, operator_id) do
      [{^operator_id, op_pid}] ->
        Inbox.input(op_pid, metadata["channel"], message, metadata)

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
