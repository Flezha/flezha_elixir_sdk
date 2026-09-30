defmodule Agentdesk.Application do
  @moduledoc """
  Starts the desk's runner: the process that connects to the controller and
  serves agentdesk's operators, in the `agents` namespace.
  """
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # `:uds` because this node is not the controller's.
      {Flezha.SDK,
       namespace: "agents",
       operators: [Agentdesk.AgentOperator, Agentdesk.GatewayOperator],
       transport: :uds}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
