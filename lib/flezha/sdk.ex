defmodule Flezha.SDK do
  @moduledoc """
  The Elixir runner SDK — the runner-as-handle surface over `flz_core`, the
  runtime every Flezha SDK runs on. A runner reaches its controller one way,
  over UDS (`transport: :uds`, the default) or QUIC (`transport: :quic`),
  wherever the controller runs: in this BEAM, on this machine or on another.
  A handle accumulates its declared factory set with no connection or process;
  `start/1` claims the process-wide runner slot (a named singleton) and
  connects; `run/1` is `start/1` then `await_termination/1`. Authoring and
  connection failures are named error tuples returned at the call site (never a
  raise) and propagate through a pipe. `child_spec/1` wraps the same sequence for
  a host application's supervision tree.

  The connection settings are read from the OS environment first, then `config :flezha_sdk, ...`. A controller that enforces
  authorization asks every runner for a credential: pass it as
  `namespace_secret:`, or set `FLEZHA_NAMESPACE_SECRET`.
  """

  alias Flezha.SDK.{Declaration, Error, Native, Relay, Runner}

  @enforce_keys [:namespace]
  @derive {Inspect, except: [:namespace_secret]}
  defstruct namespace: nil, transport: :uds, namespace_secret: nil, modules: [], server: nil

  @type t :: %__MODULE__{}
  @type result :: t() | {:error, Error.t()}

  # --- runner-as-handle ---

  @doc "Create a runner handle. No connection, no slot claim, no processes."
  @spec create_runner(String.t(), keyword()) :: result()
  def create_runner(namespace, opts \\ []) do
    transport = Keyword.get(opts, :transport, :uds)

    cond do
      not (is_binary(namespace) and namespace != "") ->
        {:error, Error.new("namespace.invalid_name", namespace: namespace)}

      transport not in [:uds, :quic] ->
        {:error, Error.new("runtime.invalid_transport", transport: transport)}

      true ->
        %__MODULE__{
          namespace: namespace,
          transport: transport,
          namespace_secret: Keyword.get(opts, :namespace_secret)
        }
    end
  end

  @doc """
  Declare a relay: a module of the `Flezha.SDK.Relay` behaviour. Legal
  before and after `start/1`.

  Its declaration is judged here by `flz_core`, the check every SDK's builder
  is judged by, and declared to the live core when the runner is started.
  """
  @spec register_factory(result(), module()) :: result()
  def register_factory({:error, _} = e, _module), do: e

  def register_factory(%__MODULE__{} = h, module) do
    with :ok <- relay?(module),
         :ok <- not_duplicate(h, module.factory_id()),
         :ok <- judged(h, module) do
      %{h | modules: h.modules ++ [module]}
    end
  end

  # Before start the core answers the verdict with nothing declared; after it
  # the live core takes the declaration.
  defp judged(%__MODULE__{server: nil}, module) do
    with {:ok, _derived} <- Declaration.check(module), do: :ok
  end

  defp judged(%__MODULE__{server: server}, module), do: Runner.define(server, module)

  @doc "Remove a declared relay — the inverse of `register_factory/2`, taking what it took."
  @spec unregister_factory(result(), module()) :: result()
  def unregister_factory({:error, _} = e, _module), do: e

  def unregister_factory(%__MODULE__{} = h, module) do
    fid = module.factory_id()

    cond do
      module not in h.modules ->
        {:error, Error.new("runtime.class_not_registered", factory: fid)}

      h.server == nil ->
        %{h | modules: List.delete(h.modules, module)}

      true ->
        with :ok <- Runner.remove(h.server, fid),
             do: %{h | modules: List.delete(h.modules, module)}
    end
  end

  @doc """
  Claim the runner slot and connect. The loser gets `runtime.runner_already_running`
  naming the namespace.
  """
  @spec start(result()) :: result()
  def start({:error, _} = e), do: e

  def start(%__MODULE__{server: nil} = h) do
    opts = [
      namespace: h.namespace,
      transport: h.transport,
      namespace_secret: h.namespace_secret,
      relays: h.modules
    ]

    case Runner.start_link(opts) do
      {:ok, pid} ->
        with :ok <- Runner.connect(pid), do: %{h | server: pid}

      {:error, {:already_started, _pid}} ->
        {:error, Error.new("runtime.runner_already_running", namespace: h.namespace)}

      {:error, %Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error, Error.new("runtime.runner_unavailable", reason: inspect(reason))}
    end
  end

  def start(%__MODULE__{} = h),
    do: {:error, Error.new("runtime.runner_already_running", namespace: h.namespace)}

  @doc """
  The live runner process behind a handle: `runtime.runner_not_started` before
  `start/1`, and a refused handle's own error unchanged.

  The one place a caller outside this module reads the handle's server, so the
  struct's shape stays this module's business.
  """
  @spec server(result() | pid() | atom()) :: {:ok, pid() | atom()} | {:error, Error.t()}
  def server(server) when is_pid(server) or is_atom(server), do: {:ok, server}
  def server({:error, _reason} = e), do: e
  def server(%__MODULE__{server: nil}), do: {:error, Error.new("runtime.runner_not_started")}
  def server(%__MODULE__{server: pid}), do: {:ok, pid}

  @doc false
  # The core context behind a handle, for a call made from the caller's process.
  @spec core(result() | pid() | atom()) :: {:ok, Native.core()} | {:error, Error.t()}
  def core(runner), do: with({:ok, server} <- server(runner), do: Runner.core(server))

  @doc """
  Send `message` to the entry channel `channel` of `relay_id`.

  Through the controller by default, under the same role check as
  `Flezha.SDK.Control`, answering its verdict: `{:ok, %{"to" => relay,
  "verdict" => "delivered" | "absorbed" | "suspended"}}`. Options:

    * `msg_id:` the message's id, stamped as `flz.msg_id`: a relay whose
      input log still holds an input of this id absorbs the repeat.
    * `key:` the partition key whose member of a partition group takes it.
    * `local: true` in process, without the controller: the message is on no
      disk, is lost if the runner stops before handling it, and a relay's
      input log does not cover it. Answers `{:ok, nil}`; refused with
      `runtime.relay_not_local` for a relay this runner does not hold,
      and with `usage.options_conflict` beside `msg_id:` or `key:`.

  `runner` is a handle, or the runner server's pid or name.
  """
  @spec send(result() | pid() | atom(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, term()} | {:error, Error.t()}
  def send(runner, relay_id, message, channel, opts \\ []) do
    [msg_id, key] = Enum.map([:msg_id, :key], &to_string(opts[&1]))
    local = Keyword.get(opts, :local, false)

    with {:ok, core} <- core(runner),
         json when is_binary(json) <-
           Native.answered(
             Native.send_message(core, relay_id, channel, message, msg_id, key, local)
           ),
         do: {:ok, Jason.decode!(json)}
  end

  @doc """
  Call the entry channel `channel` of `relay_id` with `value`, through the
  controller under the same role check as `send/5`, answering what the relay
  answers: `{:ok, answer}` with a JSON value, a string for text, the bytes for
  the binary formats, or `%{"accepted" => true}` from a channel no output
  answers. `value` is encoded as an output with no serializer of its own
  encodes it.

  `runner` is a handle, or the runner server's pid or name.
  """
  @spec call(result() | pid() | atom(), String.t(), String.t(), term()) ::
          {:ok, term()} | {:error, Error.t()}
  def call(runner, relay_id, channel, value) do
    payload = value |> Flezha.SDK.ChannelBuilder.default_serialize() |> Base.encode64()
    args = [relay_id, channel, payload, %{"payload" => "base64"}]

    case Flezha.SDK.Control.call(runner, "Relay.call_channel", args) do
      {:ok, %{"accepted" => true}} -> {:ok, %{"accepted" => true}}
      {:ok, %{"answer" => answer, "answer_codec" => codec}} -> {:ok, answer(codec, answer)}
      other -> other
    end
  end

  defp answer(codec, answer) when codec in ["json", "text"], do: answer
  defp answer(_binary, answer), do: Base.decode64!(answer)

  @doc """
  The ids of the relay instances bound on this runner now, a partition
  member by its member id, read without the controller.
  """
  @spec relays(result() | pid() | atom()) :: {:ok, [String.t()]} | {:error, Error.t()}
  def relays(runner) do
    with {:ok, server} <- server(runner), do: Runner.relays(server)
  end

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

  @doc """
  Ship each relay's state, end the session and release the slot. A runner
  runs once per BEAM: a later start in this node is refused as
  `runtime.core_shut_down`. A never-started handle is a no-op.
  """
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
  supervision tree: `namespace:`, `relays:`, `transport:` and
  `namespace_secret:`. The supervised process is the runner. A runner runs
  once per BEAM: one that stops or crashes ends its session, and the restart
  that follows is refused, so the failure reaches
  the node, which is the unit to restart. The controller moves the relays
  meanwhile.
  """
  def child_spec(opts) do
    # The credential rides as a function, so a supervisor's report of the
    # child's start arguments prints none of it.
    opts =
      case Keyword.fetch(opts, :namespace_secret) do
        {:ok, secret} when is_binary(secret) ->
          Keyword.put(opts, :namespace_secret, fn -> secret end)

        _ ->
          opts
      end

    %{
      id: Keyword.get(opts, :id, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      restart: Keyword.get(opts, :restart, :permanent),
      # A graceful stop serialises every relay and waits for the
      # controller's acks; the budget covers both inside a container's grace
      # period.
      shutdown: Keyword.get(opts, :shutdown, 10_000),
      type: :worker
    }
  end

  @doc false
  def start_link(opts) do
    namespace = Keyword.fetch!(opts, :namespace)

    opts =
      Keyword.update(opts, :namespace_secret, nil, fn
        secret when is_function(secret, 0) -> secret.()
        secret -> secret
      end)

    handle =
      Enum.reduce(
        Keyword.get(opts, :relays, []),
        create_runner(namespace, Keyword.take(opts, [:transport, :namespace_secret])),
        fn module, acc -> register_factory(acc, module) end
      )

    case start(handle) do
      %__MODULE__{server: pid} -> {:ok, pid}
      {:error, reason} -> {:error, reason}
    end
  end

  # --- helpers ---

  # The `@behaviour` the `use` macro sets and a hand-written module writes.
  defp relay?(module) do
    declared =
      if Code.ensure_loaded?(module),
        do: module.module_info(:attributes) |> Keyword.get_values(:behaviour) |> List.flatten(),
        else: []

    if Relay in declared,
      do: :ok,
      else: {:error, Error.new("topology.no_relay_exported", module: inspect(module))}
  end

  defp not_duplicate(%__MODULE__{modules: modules}, fid) do
    if Enum.any?(modules, &(&1.factory_id() == fid)),
      do: {:error, Error.new("topology.duplicate_factory_id", factory_id: fid)},
      else: :ok
  end
end
