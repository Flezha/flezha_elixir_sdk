defmodule Flezha.SDK.Local do
  @moduledoc """
  Running a declared operative graph in this process, with no controller.

  `start/2` takes the `Flezha.SDK.StreamOperative` module you deploy, compiles
  the topology its `channels/1` already declares, and stands the operative up on
  an in-node stream pair this run owns. Nothing about the class changes: no
  local-mode callback, no second declaration. Entity lifecycle, key resolution,
  routing, mailboxes, overflow and the retry ladder are the deployed ones —
  `feed/3` enters through the same `Flezha.SDK.Operative.Inbox.input/4` a frame
  off a bound data stream enters, and `on_sink/3` takes output where production
  frames it, stamps and metadata included.

  What a local run has not got: durability of any kind (record frames are
  discarded at the outbound edge, so the record path still runs and still costs
  what it costs), plugins, placement and migration, the dead-letter queue,
  observability export, authz and multi-runner scale. Every one of those needs a
  controller, and every run says so on stderr before it does any work.

  ## Running a graph

      {:ok, run} = Flezha.SDK.Local.start(WordCount)
      :ok = Flezha.SDK.Local.on_sink(run, "counts", &IO.puts(&1.payload))
      :ok = Flezha.SDK.Local.feed(run, "lines", "The quick brown fox")
      :ok = Flezha.SDK.Local.feed(run, "lines", "the lazy dog")
      :ok = Flezha.SDK.Local.run_until_idle(run, 5_000)
      :ok = Flezha.SDK.Local.stop(run)

  A complete two-procedure `WordCount` — the entry chain, both procedures and
  the keyed sink — is in `docs/quickstart.md`.
  """

  use GenServer
  require Logger

  alias Flezha.SDK.{Counters, KeyGroup, Operative, Topology, WorkflowOperative}
  alias Flezha.SDK.Operative.Inbox
  alias Flezha.Transport.Local.Stream, as: LocalStream
  alias Flezha.Transport.Stream, as: TransportStream

  @notice "[flezha] LOCAL MODE: state is held in memory and NOTHING is durable — " <>
            "no write-ahead log, no snapshots, no dead-letter queue. Everything this run " <>
            "computes is gone when the process exits. Durability, plugins, placement and " <>
            "migration, the dead-letter queue, observability export, authz and multi-runner " <>
            "scale all need a controller."

  @counters_key {__MODULE__, :counters}
  @notices 1
  @unhandled 2
  # Consecutive idle probes needed before a run is called settled: an Entity
  # between its dispatch and the emit that follows it is momentarily idle by
  # every probe, and the first probe of a run establishes the dispatch total
  # the rest are compared against.
  @idle_observations 2

  defmodule Output do
    @moduledoc """
    One value a graph released on a declared sink, carrying the bytes the sink's
    serializer produced and the metadata a deployed run would have put on the
    wire — the channel name, and on a `keyed/1` sink the `flz.entity`, `flz.seq`
    and `flz.key` stamps.
    """

    @enforce_keys [:channel, :payload, :metadata]
    defstruct [:channel, :payload, :metadata]

    @type t :: %__MODULE__{channel: String.t(), payload: term(), metadata: map()}
  end

  # --- API ---

  @doc """
  Start `module`'s declared graph in this process. Refuses, before building
  anything, a declaration that asks for something only a controller can answer.
  `:operative_id` names the run (default `"<factory_id>-local"`).
  """
  @spec start(module(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start(module, opts \\ []) do
    with :ok <- refuse_controller_only(module),
         {:ok, topology} <- Topology.build(module),
         :ok <- refuse_topology_facts(topology) do
      GenServer.start_link(__MODULE__, {topology, opts})
    end
  end

  @doc """
  Deliver every value released on the sink `channel` to `fun`, in release order.
  Refuses a name the declaration does not carry as a sink.
  """
  @spec on_sink(pid(), String.t(), (Output.t() -> any())) :: :ok | {:error, String.t()}
  def on_sink(run, channel, fun) when is_function(fun, 1),
    do: GenServer.call(run, {:on_sink, channel, fun})

  @doc """
  Feed one value to a declared entry channel — the bytes a controller would
  forward, decoded by that channel's own parser and admitted as deployed.
  Refuses a name the declaration does not carry as an entry.
  """
  @spec feed(pid(), String.t(), term()) :: :ok | {:error, String.t()}
  def feed(run, channel, payload), do: GenServer.call(run, {:feed, channel, payload})

  @doc """
  Block until every Entity has drained and nothing is in flight, or `budget_ms`
  elapses — which is reported rather than passed off as done.
  """
  @spec run_until_idle(pid(), non_neg_integer()) :: :ok | {:error, String.t()}
  def run_until_idle(run, budget_ms) do
    case settle(run, System.monotonic_time(:millisecond) + budget_ms, {0, :unread}) do
      :ok -> :ok
      {:working, id} -> {:error, "operative `#{id}` was still working after #{budget_ms}ms"}
    end
  end

  @doc """
  Stop the run and every Entity it holds. A run whose starter has already gone
  is stopped by that exit; asking again is answered, never raised.
  """
  @spec stop(pid()) :: :ok
  def stop(run) do
    GenServer.stop(run, :normal)
  catch
    :exit, _gone -> :ok
  end

  @doc "The live operative, for a caller that wants to read its Entities."
  @spec operative(pid()) :: pid()
  def operative(run), do: GenServer.call(run, :operative)

  @doc "What every local run states before it does any work."
  @spec notice() :: String.t()
  def notice, do: @notice

  @doc "Non-durability notices emitted in this process; one per started run."
  @spec notices_emitted() :: non_neg_integer()
  def notices_emitted, do: :counters.get(counters(), @notices)

  @doc """
  Values released on a sink no callback claimed. Each is reported as it happens;
  the count is what a run can assert on afterwards.
  """
  @spec unhandled_sink_outputs() :: non_neg_integer()
  def unhandled_sink_outputs, do: :counters.get(counters(), @unhandled)

  # --- server ---

  @impl true
  def init({%Topology{} = topology, opts}) do
    Process.flag(:trap_exit, true)
    {sdk_side, controller_side} = LocalStream.open()
    :ok = TransportStream.controlling_process(controller_side, self())
    :ok = TransportStream.set_active(controller_side)
    counters = Counters.new()
    id = Keyword.get(opts, :operative_id, "#{topology.factory_id}-local")

    {:ok, op} =
      Operative.start_link(
        operative_id: id,
        topology: topology,
        range: KeyGroup.decode(0, 0, 0, 0),
        counters: counters,
        runner: self(),
        secondaries: [sdk_side],
        credit: nil
      )

    :counters.add(counters(), @notices, 1)
    IO.puts(:stderr, @notice)

    {:ok,
     %{
       op: op,
       id: id,
       topology: topology,
       counters: counters,
       stream: controller_side,
       sinks: %{}
     }}
  end

  @impl true
  def handle_call({:on_sink, channel, fun}, _from, s) do
    case declared(s, channel, :sink) do
      :ok -> {:reply, :ok, put_in(s.sinks[channel], fun)}
      refusal -> {:reply, refusal, s}
    end
  end

  def handle_call({:feed, channel, payload}, _from, s) do
    case declared(s, channel, :entry) do
      :ok ->
        Inbox.input(s.op, channel, payload, %{"channel" => channel})
        {:reply, :ok, s}

      refusal ->
        {:reply, refusal, s}
    end
  end

  # Answered between inbound frames, so a caller polling it cannot see "idle"
  # while a sink output this run has yet to deliver sits behind the question.
  # The completed-dispatch total rides along: a probe walks several processes
  # in turn and one of them can finish behind the walk, which the caller then
  # sees as a total that moved.
  def handle_call(:status, _from, s) do
    verdict = if idle?(s), do: :idle, else: :working
    {:reply, {verdict, s.id, Counters.dispatches(s.counters)}, s}
  end

  def handle_call(:operative, _from, s), do: {:reply, s.op, s}

  @impl true
  # The operative forwards a park here because local mode is its runner: an
  # entry parse that failed, or a value the partition guard refused. Nothing
  # acks it, so it is named rather than swallowed.
  def handle_cast({:park, _entity, _identity, body}, s) do
    Logger.warning(
      "[flezha] local mode: operative `#{s.id}` parked an input on channel " <>
        "`#{body.channel}`: #{body.error}"
    )

    {:noreply, s}
  end

  @impl true
  # A sink output: the router writes it under the operative's own payload id.
  def handle_info({:flz_data, _stream, id, payload, metadata}, %{id: id} = s) do
    channel = Map.get(metadata, "channel", id)

    case Map.fetch(s.sinks, channel) do
      {:ok, fun} ->
        fun.(%Output{channel: channel, payload: payload, metadata: metadata})

      :error ->
        :counters.add(counters(), @unhandled, 1)

        Logger.warning(
          "[flezha] local mode: a value was released on sink `#{channel}`, " <>
            "which has no registered callback"
        )
    end

    {:noreply, s}
  end

  # Every other frame on this stream is a durable record — a change, a timer-ops
  # or effect record, a snapshot, a delete. Durability is removed by discarding
  # them here at the outbound edge rather than by branching anything above: the
  # change log, the fold and the effect path all still run.
  def handle_info({:flz_data, _stream, _payload_id, _payload, _metadata}, s), do: {:noreply, s}

  def handle_info({:EXIT, op, reason}, %{op: op} = s), do: {:stop, reason, s}
  def handle_info(_msg, s), do: {:noreply, s}

  @impl true
  def terminate(_reason, s) do
    if Process.alive?(s.op), do: Operative.stop(s.op)
    TransportStream.close(s.stream)
    :ok
  end

  # --- refusals ---

  # What a declaration can ask for that only a controller answers. Each is
  # refused by name rather than run as a graph whose work would go nowhere.
  # Partitioning is absent from this list because it is absent from the
  # declaration: a member's key-group range arrives with `AddOperative`, so a
  # local run is unpartitioned by construction.
  defp refuse_controller_only(module) do
    if WorkflowOperative.structured?(module) or WorkflowOperative.run_sugar?(module) do
      needs_controller(
        inspect(module),
        "a workflow",
        "a workflow's steps are driven from the controller's inbox, and its commit " <>
          "barriers are durable"
      )
    else
      :ok
    end
  end

  defp refuse_topology_facts(%Topology{effectively_once?: true, factory_id: id}) do
    needs_controller(
      id,
      "effectively_once",
      "under the tier an output rides its effect record, and the controller releases it " <>
        "once that record is durable — local mode has no durable store to release from"
    )
  end

  defp refuse_topology_facts(%Topology{}), do: :ok

  defp needs_controller(class, capability, why),
    do: {:error, "class `#{class}` declares #{capability}, which requires a controller: #{why}"}

  # The declared channel of that kind; the refusal names the channel and the
  # operative, because a name nothing matches is otherwise a value that vanishes.
  defp declared(s, channel, kind) do
    names = channel_names(s.topology, kind)

    if channel in names,
      do: :ok,
      else:
        {:error,
         "operative `#{s.id}` has no #{kind} channel `#{channel}`; " <>
           "it declares #{inspect(names)}"}
  end

  defp channel_names(%Topology{entries: entries}, :entry), do: Map.keys(entries)

  # A sink channel takes the producing procedure's name unless `named/2` gave it
  # one; the compiled route already resolved which.
  defp channel_names(%Topology{routes: routes}, :sink),
    do: for({_source, rs} <- routes, %{to: {:sink, _}, channel: c} <- rs, uniq: true, do: c)

  # --- quiescence ---

  defp settle(run, deadline, {seen, dispatched}) do
    {state, id, now_dispatched} = GenServer.call(run, :status)
    seen = if state == :idle and now_dispatched == dispatched, do: seen + 1, else: 0

    cond do
      seen >= @idle_observations ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        {:working, id}

      true ->
        Process.sleep(1)
        settle(run, deadline, {seen, now_dispatched})
    end
  end

  # The Entities first, then the router every completion passes back through,
  # so a dispatch that ends behind the walk is caught by the router rather than
  # missed by both. `s.stream` is this run's own side of the in-node pair: a
  # frame the router wrote but that process has not forwarded yet is in flight.
  defp idle?(s) do
    {handouts, _age} = Counters.handout_liveness(s.counters)
    {deferred, _age} = Counters.pending_push_liveness(s.counters)

    handouts == 0 and deferred == 0 and
      Process.info(self(), :message_queue_len) == {:message_queue_len, 0} and
      Enum.all?(entity_pids(s.op) ++ [s.stream.pid, s.op], &settled?/1)
  end

  defp entity_pids(op) do
    for {_procedure, _mode, keyed} <- Operative.entities(op), {_key, pid} <- keyed, do: pid
  end

  # A process is settled when it is parked in its own receive with nothing
  # queued: a dispatch under way reads `:running` or `:runnable`, and an input
  # not yet taken is a message in the queue. A dead one owes nothing.
  defp settled?(pid) do
    case Process.info(pid, [:status, :message_queue_len]) do
      [status: status, message_queue_len: 0] -> status == :waiting
      _ -> true
    end
  end

  # --- process-wide counters ---

  defp counters do
    case :persistent_term.get(@counters_key, nil) do
      nil ->
        :persistent_term.put(@counters_key, :counters.new(2, [:atomics]))
        :persistent_term.get(@counters_key)

      ref ->
        ref
    end
  end
end
