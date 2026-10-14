defmodule Starter.Application do
  @moduledoc """
  The runner: connects to the controller and serves the starter relay. It is a
  child of this application's supervision tree, so a crash restarts it.
  """
  use Application

  @impl true
  def start(_type, _args) do
    # `mix run --no-halt -- my_app` names the namespace.
    namespace = List.first(System.argv(), "default")

    children = [{Flezha.SDK, namespace: namespace, relays: [Starter.Relay], transport: :uds}]
    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
