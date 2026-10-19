defmodule Flezha.SDK.Runner do
  @moduledoc false
  # The process behind a started handle, registered under this module's name:
  # the registration is the process-wide runner slot. Once it holds the slot,
  # `connect/1` opens a core with every class declared, connects it with the
  # settings this node reads, and serves it (`Pool.open/2`). A refused connect
  # is that call's answer, and the process ends normally, so its caller is not
  # taken with it.

  use GenServer

  alias Flezha.SDK.{Call, Dispatch, Error, Knob, Native, Pool}

  # With the context's own wait for calls to leave, inside `child_spec/1`'s
  # shutdown budget.
  @drain_ms 4_000

  # Past the core's own start wait, which this call waits on.
  @call_slack_ms 5_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Declare and connect; the runner ends normally when it cannot."
  def connect(server) do
    wait = Knob.get(:runner_start_wait_ms)
    timeout = if wait == 0, do: :infinity, else: wait + @call_slack_ms
    Call.control(server, :connect, error(), timeout)
  end

  @doc "Declare one more class to the live core."
  def define(server, module), do: Call.control(server, {:define, module}, error())

  @doc "Stop serving a class; the core refuses while an instance of it is bound."
  def remove(server, factory_id), do: Call.control(server, {:remove, factory_id}, error())

  @doc "The relay instances bound on this runner."
  def relays(server), do: Call.control(server, :relays, error())

  @doc "The core context, for a call that waits on the caller's own process."
  def core(server), do: Call.control(server, :core, error())

  defp error,
    do: {:error, Error.new("runtime.runner_unavailable", reason: "the runner did not answer")}

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    Native.notice_sink()
    {:ok, %{opts: opts, core: nil, pool: nil, cells: nil}}
  end

  # The settings this node reads, handed to the core: the BEAM keeps its own
  # copy of the environment, which the core never sees. The start wait is one
  # of them, so the core waits what this call does.
  defp start_core(core, opts) do
    settings =
      %{
        transport: Atom.to_string(Keyword.fetch!(opts, :transport)),
        socket_files_path: Knob.get(:socket_files_path),
        quic_addresses: Knob.get(:quic_addresses),
        ca_cert_path: Knob.get(:ca_cert_path),
        quic_client_cert: Knob.get(:quic_client_cert),
        quic_client_key: Knob.get(:quic_client_key),
        namespace_secret: Keyword.get(opts, :namespace_secret) || Knob.get(:namespace_secret),
        start_wait_ms: Integer.to_string(Knob.get(:runner_start_wait_ms))
      }
      |> Map.reject(fn {_k, v} -> is_nil(v) end)

    Native.answered(Native.start(core, Keyword.fetch!(opts, :namespace), Jason.encode!(settings)))
  end

  @impl true
  def handle_call(:connect, _from, %{core: nil, opts: opts} = s) do
    case Pool.open(Keyword.fetch!(opts, :relays), &start_core(&1, opts)) do
      {:ok, served} -> {:reply, :ok, Map.merge(s, served)}
      refused -> {:stop, :normal, refused, s}
    end
  end

  def handle_call({:define, module}, _from, s), do: {:reply, Dispatch.declare(s.core, module), s}

  def handle_call({:remove, factory_id}, _from, s),
    do: {:reply, Native.answered(Native.undeclare_class(s.core, factory_id)), s}

  def handle_call(:relays, _from, s) do
    case Native.answered(Native.relays(s.core)) do
      {:error, _} = refused -> {:reply, refused, s}
      ids -> {:reply, {:ok, ids}, s}
    end
  end

  def handle_call(:core, _from, s), do: {:reply, {:ok, s.core}, s}

  # A crash report prints the state; the credential stays out of it.
  @impl true
  def format_status(status) do
    Map.update(status, :state, nil, fn
      %{opts: opts} = s -> %{s | opts: Keyword.replace(opts, :namespace_secret, :redacted)}
      other -> other
    end)
  end

  @impl true
  def handle_info({:EXIT, pool, reason}, %{pool: pool} = s), do: {:stop, reason, s}
  def handle_info(_message, s), do: {:noreply, s}

  # The core ships each relay's state, which the workers serve, then tells
  # every poller it is shut down and ends the session.
  @impl true
  def terminate(_reason, s), do: Pool.close(s, &Native.shutdown/1, @drain_ms, :settle)
end
