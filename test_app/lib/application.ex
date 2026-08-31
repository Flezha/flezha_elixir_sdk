defmodule ElixirRunner.Application do
  @moduledoc """
  Boots the standalone Elixir SDK runner: flezha starts runner-only (config), then
  this app runs the SDK under its own supervision tree over the transport named by
  `FLEZHA_TRANSPORT`. The namespace is `argv[1]` (default `"default"`), matching
  rust_runner/java_runner. A stdin-EOF watcher halts the VM when the e2e harness
  closes the port (`Port.close/1`) — omitting it orphans every spawned runner.
  """
  use Application
  require Logger

  alias Mix.Tasks.Bench.ElixirFactories

  def start(_type, _args) do
    spawn_stdin_eof_watcher()

    namespace =
      case System.argv() do
        [ns | _] when is_binary(ns) and ns != "" -> ns
        _ -> "default"
      end

    transport = System.get_env("FLEZHA_TRANSPORT", "uds") |> String.to_atom()
    streams = ElixirFactories.all() ++ ElixirRunner.Factories.all()

    Logger.info("[elixir_runner] namespace=#{namespace} transport=#{transport}")

    children = [
      Flezha.SDK.child_spec(namespace: namespace, streams: streams, transport: transport)
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: ElixirRunner.Supervisor)
  end

  # Block reading stdin; EOF (the harness closing the port) → halt the VM. The
  # runner is otherwise kept alive by the SDK's await loop under the supervisor.
  defp spawn_stdin_eof_watcher do
    spawn(fn -> stdin_loop() end)
  end

  defp stdin_loop do
    case IO.read(:stdio, :line) do
      :eof ->
        System.halt(0)

      # A read error is NOT end-of-file: it means the group leader has gone
      # because the VM is already shutting down. Halting here races that
      # shutdown and swallows whatever the runner was about to report — a
      # failed boot then looks exactly like a clean exit.
      {:error, _} ->
        :ok

      _line ->
        stdin_loop()
    end
  end
end
