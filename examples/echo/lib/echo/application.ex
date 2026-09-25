defmodule Echo.Application do
  @moduledoc """
  Starts a runner: a process that connects to the controller and serves your
  operators.

  The runner is a child of this application's supervision tree, so a crash
  restarts it and it reconnects under the same identity. Declare every operator
  you want served in `operators:`, whichever kind it is: the kind is read from
  the module, never named by the list.
  """
  use Application

  @impl true
  def start(_type, _args) do
    # `mix run --no-halt -- my_app` names the namespace.
    namespace = List.first(System.argv(), "default")

    children = [
      # `:uds` because this node is not the controller's. The default, `:native`,
      # is for code running inside the controller's own node.
      {Flezha.SDK, namespace: namespace, operators: [Echo.Operator], transport: :uds}
    ]

    {:ok, supervisor} = Supervisor.start_link(children, strategy: :one_for_one)

    # Everything `flz operator` and `flz plugin` do is also a function in
    # `Flezha.SDK.Admin`. The runner is connected once its child has started, so
    # uncomment to drive this namespace from the application instead of from a
    # terminal:
    #
    # alias Flezha.SDK.Admin
    # runner = Flezha.SDK.RunnerServer
    # {:ok, %{id: "my_echo"}} = Admin.start_operator(runner, "echo", id: "my_echo")
    #
    # {:ok, _} =
    #   Admin.enable_plugin(runner, "writer", %{
    #     "id" => "writer",
    #     "provider" => "operator_file_sink",
    #     "operator_id" => "my_echo",
    #     "opts" => %{"path" => "echo-out.txt"},
    #     "hooks" => [["on_output", %{}]]
    #   })
    #
    # {:ok, _} = Admin.send_to(runner, "my_echo", "hello world", "in")
    #
    # docs/sdk/admin.md has the whole surface. An enforced controller refuses
    # all of it until `flz role bind runner@default app default`.

    {:ok, supervisor}
  end
end
