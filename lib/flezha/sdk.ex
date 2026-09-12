defmodule Flezha.SDK do
  @moduledoc """
  The Elixir runner SDK — the runner-as-handle surface, deployable two ways from
  one core: in-node over the native transport (`transport: :native`, the default)
  or as a standalone runner node over the binary protocol (`transport: :uds |
  :quic`). A handle accumulates its declared factory set with no connection or
  process; `start/1` claims the process-wide runner slot (a named singleton) and
  connects; `run/1` is `start/1` then `await_termination/1`. Authoring and
  connection failures are named error tuples returned at the call site (never a
  raise) and propagate through a pipe. `child_spec/1` wraps the same sequence for
  a host application's supervision tree.
  """

  alias Flezha.SDK.{RunnerServer, Topology, WorkflowOperative}
  alias Flezha.SDK.Workflow.Spec

  @enforce_keys [:namespace]
  defstruct namespace: nil, transport: :native, classes: [], server: nil

  @type t :: %__MODULE__{}
  @type result :: t() | {:error, term()}

  # --- runner-as-handle ---

  @doc "Create a runner handle. No connection, no slot claim, no processes."
  @spec create_runner(String.t(), keyword()) :: result()
  def create_runner(namespace, opts \\ []) do
    transport = Keyword.get(opts, :transport, :native)

    cond do
      not (is_binary(namespace) and namespace != "") ->
        {:error, {:invalid_namespace, namespace}}

      transport not in [:native, :uds, :quic] ->
        {:error, {:invalid_transport, transport}}

      true ->
        %__MODULE__{namespace: namespace, transport: transport}
    end
  end

  @doc "Declare a stream operative. Legal before and after `start/1`."
  @spec define_stream(result(), module()) :: result()
  def define_stream({:error, _} = e, _module), do: e

  def define_stream(%__MODULE__{} = h, module) do
    with :ok <- validate_operative(module),
         {:ok, topo} <- Topology.build(module),
         :ok <- not_duplicate(h, topo.factory_id) do
      if h.server do
        case RunnerServer.define(h.server, module, topo) do
          :ok -> %{h | classes: h.classes ++ [{module, topo}]}
          {:error, _} = e -> e
        end
      else
        %{h | classes: h.classes ++ [{module, topo}]}
      end
    end
  end

  @doc """
  Declare a workflow operative. Legal before and after `start/1`. A workflow
  class rides the same declared set a stream class does — the controller places
  by factory id and learns the kind from the `AddOperative` it sends, not from
  the advertisement — so the two share every downstream reader.
  """
  @spec define_workflow(result(), module()) :: result()
  def define_workflow({:error, _} = e, _module), do: e

  def define_workflow(%__MODULE__{} = h, module) do
    with :ok <- validate_workflow(module),
         {:ok, spec} <- Spec.build(module),
         :ok <- not_duplicate(h, spec.factory_id) do
      if h.server do
        case RunnerServer.define(h.server, module, spec) do
          :ok -> %{h | classes: h.classes ++ [{module, spec}]}
          {:error, _} = e -> e
        end
      else
        %{h | classes: h.classes ++ [{module, spec}]}
      end
    end
  end

  @doc "Remove a declared workflow operative."
  @spec remove_workflow(result(), module()) :: result()
  def remove_workflow(h, module), do: remove_stream(h, module)

  @doc "Remove a declared stream operative."
  @spec remove_stream(result(), module()) :: result()
  def remove_stream({:error, _} = e, _module), do: e

  def remove_stream(%__MODULE__{server: nil} = h, module) do
    fid = module.factory_id()

    if Enum.any?(h.classes, fn {_m, t} -> t.factory_id == fid end) do
      %{h | classes: Enum.reject(h.classes, fn {_m, t} -> t.factory_id == fid end)}
    else
      {:error, {:not_defined, fid}}
    end
  end

  def remove_stream(%__MODULE__{} = h, module) do
    case RunnerServer.remove(h.server, module) do
      :ok -> %{h | classes: Enum.reject(h.classes, fn {m, _t} -> m == module end)}
      {:error, _} = e -> e
    end
  end

  @doc "Claim the runner slot and connect. Loser gets `{:error, {:already_running, ns}}`."
  @spec start(result()) :: result()
  def start({:error, _} = e), do: e

  def start(%__MODULE__{} = h) do
    topologies = Map.new(h.classes, fn {_m, t} -> {t.factory_id, t} end)
    factories = Enum.map(h.classes, fn {m, t} -> {t.factory_id, description(m)} end)

    opts = [
      namespace: h.namespace,
      transport: h.transport,
      topologies: topologies,
      factories: factories
    ]

    case RunnerServer.start_link(opts) do
      {:ok, pid} -> %{h | server: pid}
      {:error, {:already_started, _pid}} -> {:error, {:already_running, h.namespace}}
      {:error, {:connect_failed, reason}} -> {:error, {:connect_failed, reason}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  The live runner process behind a handle: `{:error, :not_started}` before
  `start/1`, and a refused handle's own error unchanged.

  The one place a caller outside this module reads the handle's server, so the
  struct's shape stays this module's business.
  """
  @spec server(result()) :: {:ok, pid()} | {:error, term()}
  def server({:error, _reason} = e), do: e
  def server(%__MODULE__{server: nil}), do: {:error, :not_started}
  def server(%__MODULE__{server: pid}), do: {:ok, pid}

  @doc "Block until the runner terminates."
  @spec await_termination(result()) :: :ok | {:error, term()}
  def await_termination(handle) do
    with {:ok, pid} <- server(handle) do
      ref = Process.monitor(pid)

      receive do
        {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
      end
    end
  end

  @doc "`start/1` then `await_termination/1`."
  @spec run(result()) :: :ok | {:error, term()}
  def run({:error, _} = e), do: e

  def run(%__MODULE__{} = h) do
    case start(h) do
      %__MODULE__{} = started -> await_termination(started)
      {:error, _} = e -> e
    end
  end

  @doc "Disconnect and release the slot. A never-started handle is a no-op."
  @spec close(result()) :: :ok
  def close({:error, _}), do: :ok
  def close(%__MODULE__{server: nil}), do: :ok

  def close(%__MODULE__{server: pid}) do
    if Process.alive?(pid) do
      try do
        GenServer.stop(pid, :normal)
      catch
        :exit, _ -> :ok
      end
    end

    :ok
  end

  @spec namespace(t()) :: String.t()
  def namespace(%__MODULE__{namespace: ns}), do: ns

  # --- child_spec: create → define → start under a host supervisor ---

  @doc """
  A child spec wrapping create → define → start for the host application's
  supervision tree. The supervised process is the RunnerServer, so a crash
  restarts it and it reconnects with the same stable id (a binding swap).
  """
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :id, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      restart: Keyword.get(opts, :restart, :permanent),
      # A graceful stop serialises every operative and waits for the
      # controller's acks; the budget covers both inside a container's grace
      # period.
      shutdown: Keyword.get(opts, :shutdown, 10_000),
      type: :worker
    }
  end

  @doc false
  def start_link(opts) do
    namespace = Keyword.fetch!(opts, :namespace)
    transport = Keyword.get(opts, :transport, :native)

    handle =
      Enum.reduce(
        Keyword.get(opts, :streams, []),
        create_runner(namespace, transport: transport),
        fn module, acc ->
          define_stream(acc, module)
        end
      )

    handle =
      Enum.reduce(Keyword.get(opts, :workflows, []), handle, fn module, acc ->
        define_workflow(acc, module)
      end)

    case start(handle) do
      %__MODULE__{server: pid} -> {:ok, pid}
      {:error, reason} -> {:error, reason}
    end
  end

  # --- helpers ---

  defp validate_operative(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, :factory_id, 0) and
         function_exported?(module, :channels, 1) do
      :ok
    else
      {:error, {:not_a_stream_operative, module}}
    end
  end

  defp validate_workflow(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, :factory_id, 0) and
         (WorkflowOperative.structured?(module) or WorkflowOperative.run_sugar?(module)) do
      :ok
    else
      {:error, {:not_a_workflow_operative, module}}
    end
  end

  defp not_duplicate(%__MODULE__{classes: classes}, fid) do
    if Enum.any?(classes, fn {_m, t} -> t.factory_id == fid end),
      do: {:error, {:duplicate_factory_id, fid}},
      else: :ok
  end

  defp description(module) do
    if function_exported?(module, :description, 0), do: module.description(), else: nil
  end
end
