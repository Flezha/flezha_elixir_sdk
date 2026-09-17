defmodule Flezha.SDK.ElixirRunnerExitTest do
  @moduledoc """
  The standalone Elixir runner ends with the port the e2e harness manages it by.
  A `Port.close/1` normally reaches it as end of file on standard input, but the
  same disconnect logs, and a write to a pipe whose reader has gone takes EPIPE
  and takes the io device with it — after which the watcher's read answers an
  error and no end of file ever arrives. Both tests drive the real
  `ElixirRunner.Application.watch_stdin/0` in its own VM, with standard output
  piped to a reader that has already exited.
  """
  use ExUnit.Case, async: true

  @app_dir "test_app"

  # A booted VM halts in milliseconds; the budget covers only its start.
  @exit_budget_ms 30_000

  # Compiled here, never by the child: the compiler's own line on standard
  # output would take the EPIPE first and end the VM before the watcher does.
  setup_all do
    {_, 0} =
      System.cmd("mix", ["do", "deps.get", "+", "compile"],
        cd: Path.expand(@app_dir),
        env: [{"MIX_ENV", "dev"}],
        stderr_to_stdout: true
      )

    :ok
  end

  @tag :tmp_dir
  test "a runner whose io device dies while the VM runs ends itself", %{tmp_dir: dir} do
    assert "0" == run_watcher(dir, "")
  end

  @tag :tmp_dir
  test "a runner already stopping keeps its own exit code", %{tmp_dir: dir} do
    assert "3" == run_watcher(dir, "System.stop(3)\n")
  end

  # Boots the test app's VM with its standard output piped to a reader that has
  # already exited, arms the watcher, runs `prelude`, then writes until the write
  # raises — the EPIPE that kills the io device. Answers the VM's exit code.
  defp run_watcher(dir, prelude) do
    script = Path.join(dir, "watcher.exs")
    status = Path.join(dir, "status")
    pidfile = Path.join(dir, "pid")

    File.write!(script, """
    File.write!("#{pidfile}", System.pid())

    # The watcher is armed from an application's `start/2`, as the runner's is:
    # that is what makes its group leader the application master rather than the
    # standard-io device, and the whole defect lives in that difference.
    defmodule ProbeApp do
      @behaviour Application
      def start(_, _) do
        ElixirRunner.Application.watch_stdin()
        {:ok, spawn(fn -> receive do: (:never -> :ok) end)}
      end

      def stop(_), do: :ok
    end

    :application.load(
      {:application, :probe_app,
       [{:vsn, ~c"1"}, {:mod, {ProbeApp, []}}, {:applications, [:kernel, :stdlib]}]}
    )

    {:ok, _} = :application.ensure_all_started(:probe_app)

    spawn(fn ->
      # The port breaks under a runner that has finished booting, never under
      # one still starting, so the break waits for the system's own signal.
      case :init.notify_when_started(self()) do
        :started -> :ok
        :ok -> receive do: ({:init, :started} -> :ok)
      end

      #{prelude}
      Enum.reduce_while(1..1000, :whole, fn _, _ ->
        try do
          IO.write(:stdio, String.duplicate("x", 256))
          {:cont, :whole}
        rescue
          _ -> {:halt, :broken}
        catch
          _, _ -> {:halt, :broken}
        end
      end)
    end)
    """)

    command =
      "sh -c 'cd #{Path.expand(@app_dir)}; " <>
        "{ MIX_ENV=dev mix run --no-halt --no-compile --no-start #{script}; " <>
        "echo $? > #{status}; } | head -c 0'"

    # `stderr_to_stdout` so the broken VM's own EPIPE report lands in the port
    # rather than in the suite's output.
    port = Port.open({:spawn, command}, [:binary, :exit_status, :stderr_to_stdout])
    on_exit(fn -> kill_leftover(pidfile) end)

    assert_receive {^port, {:exit_status, _}}, @exit_budget_ms
    status |> File.read!() |> String.trim()
  end

  # A VM that does not halt outlives the port's shell, so a red run must not
  # leave one behind.
  defp kill_leftover(pidfile) do
    case File.read(pidfile) do
      {:ok, pid} -> System.cmd("kill", ["-9", String.trim(pid)], stderr_to_stdout: true)
      {:error, _} -> :ok
    end
  end
end
