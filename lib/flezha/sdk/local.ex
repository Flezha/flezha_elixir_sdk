defmodule Flezha.SDK.Local do
  @moduledoc """
  Running a declared relay graph in this process, with no controller.

  `start/2` takes the `Flezha.SDK.Relay` module you deploy and runs the graph
  its `channels/1` already declares on `flz_core`'s local run, the same
  runtime a deployed relay runs on: entry decode, edge transforms and keying,
  one Cell per resolved key with its own mailbox, the retry ladder. Nothing
  about the class changes: no local-mode callback, no second declaration.

  What a local run has not got: durability of any kind, plugins, placement and
  migration, the dead-letter queue, observability export, authz and
  multi-runner scale. Every one of those needs a controller, and every run says
  so on stderr before it does any work. A class that runs in steps, or under
  the effectively-once tier, is refused as `runtime.local_needs_controller`.

  ## Running a graph

      {:ok, run} = Flezha.SDK.Local.start(WordCount)
      :ok = Flezha.SDK.Local.on_output(run, "counts", &IO.puts(&1.payload))
      :ok = Flezha.SDK.Local.feed(run, "lines", "The quick brown fox")
      :ok = Flezha.SDK.Local.feed(run, "lines", "the lazy dog")
      :ok = Flezha.SDK.Local.run_until_idle(run, 5_000)
      :ok = Flezha.SDK.Local.stop(run)
  """

  use GenServer

  alias Flezha.SDK.{Call, Channels, Error, Native, Pool}

  # How often a run with an output callback looks for released values.
  @drain_every_ms 20
  @stop_drain_ms 5_000

  defmodule Output do
    @moduledoc """
    One value a graph released on a declared output, carrying the bytes the
    output's serializer produced and the metadata a deployed run would have put
    on the wire: on a `keyed/1` output the `flz.cell`, `flz.seq` and `flz.key`
    stamps.
    """

    @enforce_keys [:channel, :payload, :metadata]
    defstruct [:channel, :payload, :metadata]

    @type t :: %__MODULE__{channel: String.t(), payload: binary(), metadata: map()}
  end

  @doc """
  Start `module`'s declared graph in this process. Refuses a declaration the
  core refuses, and one that asks for something only a controller can answer.
  `:relay_id` names the run (default `"<factory_id>-local"`).
  """
  @spec start(module(), keyword()) :: {:ok, pid()} | {:error, Error.t()}
  def start(module, opts \\ []) do
    {:ok, run} = GenServer.start_link(__MODULE__, {module, opts})

    # A refusal is the call's answer and the run ends normally, so the caller
    # is not taken with it.
    with :ok <- Call.control(run, :begin, {:error, silent()}), do: {:ok, run}
  end

  @doc """
  Deliver every value released on the output `channel` to `fun`, in release
  order. Refuses a name the declaration does not carry as an output.
  """
  @spec on_output(pid(), String.t(), (Output.t() -> any())) :: :ok | {:error, Error.t()}
  def on_output(run, channel, fun) when is_function(fun, 1),
    do: Call.control(run, {:on_output, channel, fun}, {:error, silent()})

  @doc """
  Feed one value to a declared entry channel: the bytes a controller would
  forward, decoded by that channel's own parser. A value that is not a binary
  is encoded as an output with no serializer of its own encodes it. Refuses a
  name the declaration does not carry as an entry.
  """
  @spec feed(pid(), String.t(), term()) :: :ok | {:error, Error.t()}
  def feed(run, channel, payload) do
    with {:ok, core} <- Call.control(run, :core, {:error, silent()}) do
      core |> Native.local_feed(channel, Channels.default_serialize(payload)) |> Native.answered()
    end
  end

  @doc """
  Block until the graph has nothing left to do and every value it released
  has been delivered, or `budget_ms` elapses — which is reported rather than
  passed off as done.
  """
  @spec run_until_idle(pid(), non_neg_integer()) :: :ok | {:error, Error.t()}
  def run_until_idle(run, budget_ms) do
    with {:ok, core} <- Call.control(run, :core, {:error, silent()}),
         :ok <- core |> Native.local_run_until_idle(budget_ms) |> Native.answered() do
      Call.control(run, :deliver, {:error, silent()})
    end
  end

  @doc """
  Stop the run, delivering what it already released. A run whose starter has
  already gone is stopped by that exit; asking again is answered, never raised.
  """
  @spec stop(pid()) :: :ok
  def stop(run) do
    GenServer.stop(run, :normal)
  catch
    :exit, _gone -> :ok
  end

  defp silent, do: Error.new("runtime.local_run_unanswered", budget_ms: 0)

  # --- server ---

  @impl true
  def init({module, opts}) do
    Process.flag(:trap_exit, true)
    {:ok, %{module: module, opts: opts, core: nil, pool: nil, sinks: %{}}}
  end

  @impl true
  def handle_call(:begin, _from, %{core: nil, module: module} = s) do
    relay_id = Keyword.get(s.opts, :relay_id, "#{module.factory_id()}-local")
    begin = &Native.answered(Native.local_start(&1, module.factory_id(), relay_id))

    case Pool.open([module], begin) do
      {:ok, served} -> {:reply, :ok, Map.merge(s, served)}
      refused -> {:stop, :normal, refused, s}
    end
  end

  def handle_call({:on_output, channel, fun}, _from, s) do
    case Native.answered(Native.local_on_sink(s.core, channel)) do
      :ok ->
        if s.sinks == %{}, do: Process.send_after(self(), :deliver, @drain_every_ms)
        {:reply, :ok, put_in(s.sinks[channel], fun)}

      error ->
        {:reply, error, s}
    end
  end

  def handle_call(:core, _from, s), do: {:reply, {:ok, s.core}, s}
  def handle_call(:deliver, _from, s), do: {:reply, deliver(s), s}

  @impl true
  def handle_info(:deliver, s) do
    deliver(s)
    Process.send_after(self(), :deliver, @drain_every_ms)
    {:noreply, s}
  end

  def handle_info({:EXIT, pool, reason}, %{pool: pool} = s), do: {:stop, reason, s}
  def handle_info(_msg, s), do: {:noreply, s}

  # The run first, then what it released and no callback has seen: ending the
  # run while a value sits in the queue is how an author loses output the graph
  # did produce.
  @impl true
  def terminate(_reason, s) do
    stop = fn core ->
      Native.local_stop(core)
      deliver(s)
    end

    Pool.close(s, stop, @stop_drain_ms, :stop)
  end

  # Every value released so far, in release order. Only this process reads the
  # queue, so the order is the graph's.
  defp deliver(s) do
    case Native.local_next_output(s.core, 0) do
      %{target: :plugin, plugin_channel: channel} = out ->
        if fun = s.sinks[channel] do
          fun.(%Output{channel: channel, payload: out.payload, metadata: Map.new(out.metadata)})
        end

        deliver(s)

      _nothing_or_refused ->
        :ok
    end
  end
end
