# A stand-in for the runner: it answers every barrier the way a controller
# that journalled it would, and forwards what it was asked to ship so a test
# can read the wire rather than the driver's own state.
defmodule Flezha.SDK.Test.StubRunner do
  @moduledoc false
  use GenServer

  alias Flezha.SDK.Spawn

  def start_link(opts), do: GenServer.start_link(__MODULE__, Map.new(opts))
  def init(opts), do: {:ok, Map.put_new(opts, :error, nil)}

  def handle_cast({:barrier, driver, ref, command}, s) do
    send(s.test, {:shipped, command})
    send(driver, {:barrier_ack, ref, "", s.error})
    {:noreply, s}
  end

  def handle_cast({:notify, command}, s) do
    send(s.test, {:shipped, command})
    {:noreply, s}
  end

  # A park the entrance shipped, forwarded as the frame it would become.
  def handle_cast({:park, _cell_pid, identity, body}, s) do
    send(s.test, {:shipped, {:park_payload, Map.put(body, :identity, identity)}})
    {:noreply, s}
  end

  # A spawn request, answered as a controller that started the instance would.
  def handle_cast({:spawn, caller, spawn, payload, if_running}, s) do
    send(s.test, {:spawn_requested, payload, if_running})
    Spawn.answer(caller, %{spawn | claimed?: true}, {"spawned", spawn.instance_id}, nil)
    {:noreply, s}
  end

  def handle_cast(_other, s), do: {:noreply, s}
end
