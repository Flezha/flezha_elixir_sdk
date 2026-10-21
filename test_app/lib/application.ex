defmodule ElixirRunner do
  @moduledoc false

  @doc """
  Where a sleeping fixture keeps its work: locally, sleeping after the class
  default unless the drill or the bench driving it shortens the interval
  through `FLEZHA_FIXTURE_SLEEP_AFTER_MS`.
  """
  def fixture_durable do
    case Integer.parse(System.get_env("FLEZHA_FIXTURE_SLEEP_AFTER_MS", "")) do
      {ms, ""} when ms > 0 -> {:local, sleep_after: ms}
      _unset -> :local
    end
  end
end

defmodule ElixirRunner.Application do
  @moduledoc """
  Boots the standalone Elixir SDK runner: this app runs the SDK under its own
  supervision tree over the transport named by
  `FLEZHA_TRANSPORT`. The namespace is `argv[1]` (default `"default"`), matching
  rust_runner/java_runner. A standard-input watcher halts the VM when the e2e
  harness closes the port (`Port.close/1`) — omitting it orphans every spawned
  runner.

  Two arguments serve the refusal-table drill, both handled by
  `ElixirRunner.Conformance`: `--conformance-report <path>` writes the report and
  exits 0 without connecting, and `FLEZHA_REFUSAL_FIXTURE=<topology id>` narrows
  the declared set to that one row.
  """
  use Application
  require Logger

  alias ElixirRunner.BenchFactories

  def start(_type, _args) do
    # The report path rides the environment: this app launches through
    # `sh -c '<script>'`, where an appended word binds to `$0` inside the
    # script rather than reaching `System.argv/0`.
    args =
      parse_args(System.argv(), %{
        namespace: nil,
        report: System.get_env("FLEZHA_CONFORMANCE_REPORT")
      })

    case args.report do
      nil -> boot(args.namespace || "default")
      path -> report(path)
    end
  end

  defp report(path) do
    ElixirRunner.Conformance.write_report(path)
    System.halt(0)
  end

  defp boot(namespace) do
    # The declared set is settled before the watcher is armed: a refusal-fixture
    # row that the check refuses exits non-zero here, and an already-closed
    # standard input would otherwise halt the VM with 0 first.
    relays = declared_set()
    watch_stdin()
    transport = System.get_env("FLEZHA_TRANSPORT", "uds") |> String.to_atom()

    Logger.info("[elixir_runner] namespace=#{namespace} transport=#{transport}")

    children = [
      Flezha.SDK.child_spec(
        namespace: namespace,
        relays: relays,
        transport: transport
      )
    ]

    started =
      Supervisor.start_link(children, strategy: :one_for_one, name: ElixirRunner.Supervisor)

    local_sends(System.get_env("FLEZHA_LOCAL_SEND_SCRIPT"))
    started
  end

  # The local-send drill: a JSON array of `{relay, channel, message, when}`
  # sends this app's own code makes with `local: true`, each once the file
  # `when` names exists, printing `local_send <relay> ok` or
  # `local_send <relay> err <code>`. An entry with `local: false` sends
  # through the controller, printing `send <relay> ...`, and may carry `bytes`,
  # the base64 of a message that need not be text, in place of `message`. An
  # entry with `call: true` carries `value` instead and calls the channel with
  # `Flezha.SDK.call/4`, printing `call <relay> ok <answer JSON>` or
  # `call <relay> err <code>`.
  defp local_sends(nil), do: :ok

  defp local_sends(script) do
    spawn(fn ->
      for %{"relay" => relay, "channel" => channel, "when" => go} = entry <-
            Jason.decode!(script) do
        await_file(go)
        IO.puts(drill(entry, relay, channel))
      end
    end)
  end

  defp drill(%{"call" => true, "value" => value}, relay, channel) do
    case Flezha.SDK.call(Flezha.SDK.Runner, relay, channel, value) do
      {:ok, answer} -> "call #{relay} ok #{Jason.encode!(answer)}"
      {:error, %{code: code}} -> "call #{relay} err #{code}"
    end
  end

  defp drill(entry, relay, channel) do
    local = Map.get(entry, "local", true)
    verb = if local, do: "local_send", else: "send"

    case Flezha.SDK.send(Flezha.SDK.Runner, relay, message(entry), channel, local: local) do
      {:ok, _} -> "#{verb} #{relay} ok"
      {:error, %{code: code}} -> "#{verb} #{relay} err #{code}"
    end
  end

  defp message(%{"bytes" => bytes}), do: Base.decode64!(bytes)
  defp message(%{"message" => message}), do: message

  defp await_file(path) do
    unless File.exists?(path) do
      Process.sleep(50)
      await_file(path)
    end
  end

  # The classes this runner advertises: its whole fixture set, or the one
  # refusal-table row `FLEZHA_REFUSAL_FIXTURE` names and nothing else.
  defp declared_set do
    case System.get_env("FLEZHA_REFUSAL_FIXTURE") do
      nil ->
        BenchFactories.all() ++
          ElixirRunner.Factories.all() ++
          ElixirRunner.Factories.shorthand() ++
          ElixirRunner.Steps.all() ++
          ElixirRunner.StepsLoop.all() ++
          ElixirRunner.Call.all() ++
          ElixirRunner.RelayCall.all() ++
          ElixirRunner.Part.all()

      topology ->
        ElixirRunner.Conformance.boot_classes(topology)
    end
  end

  defp parse_args([], acc), do: acc

  defp parse_args(["--conformance-report", path | rest], acc),
    do: parse_args(rest, %{acc | report: path})

  defp parse_args([arg | rest], %{namespace: nil} = acc)
       when is_binary(arg) and arg != "" do
    if String.starts_with?(arg, "--"),
      do: parse_args(rest, acc),
      else: parse_args(rest, %{acc | namespace: arg})
  end

  defp parse_args([_arg | rest], acc), do: parse_args(rest, acc)

  # Block reading stdin; EOF (the harness closing the port) → halt the VM. The
  # runner is otherwise kept alive by the SDK's await loop under the supervisor.
  @doc false
  def watch_stdin do
    spawn(fn ->
      # Read the standard-io device itself, not `:stdio`. Under an application
      # `:stdio` is the application master, which forwards to the device without
      # being it: `:io.request/2` monitors the master, so a device that dies
      # answers the forwarded read nothing at all and the watcher hangs for ever.
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
