defmodule Agentdesk.Application do
  @moduledoc """
  Starts the desk's runner: the process that connects to the controller and
  serves agentdesk's relays, in the `agents` namespace.
  """
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Flezha.SDK,
       namespace: "agents",
       relays: [Agentdesk.AgentRelay, Agentdesk.GatewayRelay],
       transport: :uds}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
