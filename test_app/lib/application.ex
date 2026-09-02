defmodule ElixirRunner.Application do
  @moduledoc """
  Boots the standalone Elixir SDK runner: flezha starts runner-only (config), then
  this app runs the SDK under its own supervision tree over the transport named by
  `FLEZHA_TRANSPORT`. The namespace is `argv[1]` (default `"default"`), matching
  rust_runner/java_runner. A standard-input watcher halts the VM when the e2e
  harness closes the port (`Port.close/1`) — omitting it orphans every spawned
  runner.
  """
  use Application
  require Logger

  alias Mix.Tasks.Bench.ElixirFactories

  def start(_type, _args) do
    watch_stdin()

    namespace =
      case System.argv() do
        [ns | _] when is_binary(ns) and ns != "" -> ns
        _ -> "default"
      end

    transport = System.get_env("FLEZHA_TRANSPORT", "uds") |> String.to_atom()
    streams = ElixirFactories.all() ++ ElixirRunner.Factories.all()
    # A runner-level registration, not a declaration: armed beside the streams.
    idle_observers = ElixirRunner.EntryIdleCounter.arm()

    Logger.info("[elixir_runner] namespace=#{namespace} transport=#{transport}")

    children = [
      Flezha.SDK.child_spec(
        namespace: namespace,
        streams: streams,
        transport: transport,
        idle_observers: idle_observers
      )
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: ElixirRunner.Supervisor)
  end

  # Block reading stdin; EOF (the harness closing the port) → halt the VM. The
  # runner is otherwise kept alive by the SDK's await loop under the supervisor.
  @doc false
  def watch_stdin do
    spawn(fn ->
      # Read the standard-io device itself, not `:stdio`. Under an application
      # `:stdio` is the application master, which relays to the device without
      # being it: `:io.request/2` monitors the master, so a device that dies
      # answers the relayed read nothing at all and the watcher hangs for ever.
      if device = Process.whereis(:user), do: Process.group_leader(self(), device)
      stdin_loop()
    end)
  end

  defp stdin_loop do
    case IO.read(:stdio, :line) do
      :eof ->
        System.halt(0)

      # A read error, never end-of-file, is what a dead device answers, and the
      # port going away breaks standard output before it closes standard input:
      # the write takes EPIPE, the device dies with it, and no end-of-file can
      # ever arrive. Halt — unless the VM is already starting or stopping, where
      # the device dies as part of that and halting would race it, swallowing
      # whatever the runner was about to report.
      {:error, _} ->
        if match?({:started, _}, :init.get_status()), do: System.halt(0)

      _line ->
        stdin_loop()
    end
  end
end
