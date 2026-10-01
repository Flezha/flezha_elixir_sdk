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

  alias Flezha.SDK.{Codec, Error, Operator, RunnerServer, Topology}
  alias Flezha.SDK.Steps.Spec

  @enforce_keys [:namespace]
  defstruct namespace: nil, transport: :native, classes: [], server: nil

  @type t :: %__MODULE__{}
  @type result :: t() | {:error, Error.t()}

  # --- runner-as-handle ---

  @doc "Create a runner handle. No connection, no slot claim, no processes."
  @spec create_runner(String.t(), keyword()) :: result()
  def create_runner(namespace, opts \\ []) do
    transport = Keyword.get(opts, :transport, :native)

    cond do
      not (is_binary(namespace) and namespace != "") ->
        {:error, Error.new("namespace.invalid_name", namespace: namespace)}

      transport not in [:native, :uds, :quic] ->
        {:error, Error.new("runtime.invalid_transport", transport: transport)}

      true ->
        %__MODULE__{namespace: namespace, transport: transport}
    end
  end

  @doc """
  Declare an operator: a module of the `Flezha.SDK.Operator` behaviour. Legal
  before and after `start/1`.

  Its settings tree is read and checked here, and a class that runs in steps
  is compiled into its steps spec. The runner announces each entry's run with
  its factory id, so the controller knows it before it places.
  """
  @spec register_factory(result(), module()) :: result()
  def register_factory({:error, _} = e, _module), do: e

  def register_factory(%__MODULE__{} = h, module) do
    with :ok <- operator?(module),
         {:ok, class} <- build_class(module),
         :ok <- not_duplicate(h, class.factory_id) do
      if h.server do
        case RunnerServer.define(h.server, factory_row(module, class), class) do
          :ok -> %{h | classes: h.classes ++ [{module, class}]}
          {:error, _} = e -> e
        end
      else
        %{h | classes: h.classes ++ [{module, class}]}
      end
    end
  end

  @doc "Remove a declared operator — the inverse of `register_factory/2`, taking what it took."
  @spec unregister_factory(result(), module()) :: result()
  def unregister_factory({:error, _} = e, _module), do: e

  def unregister_factory(%__MODULE__{server: nil} = h, module) do
    fid = module.factory_id()

    if Enum.any?(h.classes, fn {_m, t} -> t.factory_id == fid end) do
      %{h | classes: Enum.reject(h.classes, fn {_m, t} -> t.factory_id == fid end)}
    else
      {:error, Error.new("runtime.class_not_registered", factory: fid)}
    end
  end

  def unregister_factory(%__MODULE__{} = h, module) do
    case RunnerServer.remove(h.server, module) do
      :ok -> %{h | classes: Enum.reject(h.classes, fn {m, _t} -> m == module end)}
      {:error, _} = e -> e
    end
  end

  @doc """
  Claim the runner slot and connect. The loser gets `runtime.runner_already_running`
  naming the namespace.
  """
  @spec start(result()) :: result()
  def start({:error, _} = e), do: e

  def start(%__MODULE__{} = h) do
    topologies = Map.new(h.classes, fn {_m, t} -> {t.factory_id, t} end)

    factories = Enum.map(h.classes, fn {m, t} -> factory_row(m, t) end)

    opts = [
      namespace: h.namespace,
      transport: h.transport,
      topologies: topologies,
      factories: factories
    ]

    case RunnerServer.start_link(opts) do
      {:ok, pid} ->
        %{h | server: pid}

      {:error, {:already_started, _pid}} ->
        {:error, Error.new("runtime.runner_already_running", namespace: h.namespace)}

      {:error, {:connect_failed, reason}} ->
        {:error, Error.new("runner.controller_unreachable", reason: reason)}

      {:error, reason} ->
        {:error, Error.new("runtime.runner_unavailable", reason: reason)}
    end
  end

  @doc """
  The live runner process behind a handle: `runtime.runner_not_started` before
  `start/1`, and a refused handle's own error unchanged.

  The one place a caller outside this module reads the handle's server, so the
  struct's shape stays this module's business.
  """
  @spec server(result()) :: {:ok, pid()} | {:error, Error.t()}
  def server({:error, _reason} = e), do: e
  def server(%__MODULE__{server: nil}), do: {:error, Error.new("runtime.runner_not_started")}
  def server(%__MODULE__{server: pid}), do: {:ok, pid}

  @doc "Block until the runner terminates."
  @spec await_termination(result()) :: :ok | {:error, Error.t()}
  def await_termination(handle) do
    with {:ok, pid} <- server(handle) do
      ref = Process.monitor(pid)

      receive do
        {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
      end
    end
  end

  @doc "`start/1` then `await_termination/1`."
  @spec run(result()) :: :ok | {:error, Error.t()}
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

  # --- child_spec: create → register → start under a host supervisor ---

  @doc """
  A child spec wrapping create → register → start for the host application's
  supervision tree. The supervised process is the RunnerServer, so a crash
  restarts it and it reconnects with the same stable id (a binding swap).
  """
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :id, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      restart: Keyword.get(opts, :restart, :permanent),
      # A graceful stop serialises every operator and waits for the
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
        Keyword.get(opts, :operators, []),
        create_runner(namespace, transport: transport),
        fn module, acc -> register_factory(acc, module) end
      )

    case start(handle) do
      %__MODULE__{server: pid} -> {:ok, pid}
      {:error, reason} -> {:error, reason}
    end
  end

  # --- helpers ---

  # The `@behaviour` the `use` macro sets and a hand-written module writes.
  defp operator?(module) do
    declared =
      if Code.ensure_loaded?(module),
        do: module.module_info(:attributes) |> Keyword.get_values(:behaviour) |> List.flatten(),
        else: []

    if Operator in declared,
      do: :ok,
      else: {:error, Error.new("topology.no_operator_exported", module: inspect(module))}
  end

  # The tree decides the class: a steps spec, or a flow topology.
  defp build_class(module) do
    case Operator.settings(module) do
      {:ok, %Operator{run: :steps} = settings} -> Spec.build(module, settings)
      {:ok, settings} -> Topology.build(module, settings)
      {:error, issue} -> {:error, Topology.refusal([issue])}
    end
  end

  defp not_duplicate(%__MODULE__{classes: classes}, fid) do
    if Enum.any?(classes, fn {_m, t} -> t.factory_id == fid end),
      do: {:error, Error.new("topology.duplicate_factory_id", factory_id: fid)},
      else: :ok
  end

  defp factory_row(module, class) do
    description =
      if function_exported?(module, :description, 0), do: module.description(), else: nil

    {class.factory_id, description, Codec.run(class)}
  end
end
