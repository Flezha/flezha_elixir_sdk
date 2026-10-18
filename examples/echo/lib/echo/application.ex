defmodule Echo.Application do
  @moduledoc """
  Starts a runner: a process that connects to the controller and serves your
  relays.

  The runner is a child of this application's supervision tree, so a crash
  restarts it and it reconnects under the same identity. Declare every relay
  you want served in `relays:`, however it runs: how it runs is read from
  the module, never named by the list.
  """
  use Application

  @impl true
  def start(_type, _args) do
    # `mix run --no-halt -- my_app` names the namespace.
    namespace = List.first(System.argv(), "default")

    children = [
      # connects to the controller and serves these relays
      {Flezha.SDK, namespace: namespace, relays: [Echo.Relay], transport: :uds}
    ]

    {:ok, supervisor} = Supervisor.start_link(children, strategy: :one_for_one)

    # Everything `flz relay` and `flz plugin` do is also a function in
    # `Flezha.SDK.Control`, and `Flezha.SDK.send/5` hands a message to an
    # relay. The runner is connected once its child has started, so
    # uncomment to drive this namespace from the application instead of from a
    # terminal:
    #
    # alias Flezha.SDK.Control
    # runner = Flezha.SDK.Runner
    # {:ok, %{id: "my_echo"}} = Control.start_relay(runner, "echo", id: "my_echo")
    #
    # {:ok, _} =
    #   Control.enable_plugin(runner, "writer", "relay_file_output",
    #     opts: %{"path" => "echo-out.txt"}
    #   )
    #
    # {:ok, _} = Control.bind_plugin(runner, "writer", "my_echo", hooks: [["on_output", %{}]])
    #
    # {:ok, _} = Flezha.SDK.send(runner, "my_echo", "hello world", "in", msg_id: "hello-1")
    #
    # docs/public/topics/core/sending_and_calling.md covers sending. An
    # enforced controller refuses all of it until
    # `flz role bind runner@default app default`.

    {:ok, supervisor}
  end
end
