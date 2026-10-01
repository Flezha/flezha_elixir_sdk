defmodule Flezha.SDK.Local do
  @moduledoc """
  Running a declared operator graph in this process, with no controller.

  `start/2` takes the `Flezha.SDK.Operator` module you deploy, compiles
  the topology its `channels/1` already declares, and stands the operator up on
  an in-node stream pair this run owns. Nothing about the class changes: no
  local-mode callback, no second declaration. Cell lifecycle, key resolution,
  routing, mailboxes, overflow and the retry ladder are the deployed ones —
  `feed/3` enters through the same `Flezha.SDK.Operator.Inbox.input/4` a frame
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

  A complete two-tasklet `WordCount` — the entry chain, both tasklets and
  the keyed sink — is in `docs/quickstart.md`.
  """

  use GenServer
  require Logger

  alias Flezha.SDK.{
    Call,
    Counters,
    Error,
    KeyGroup,
    Knob,
    LocalStream,
    Operator,
    Spawn,
    Topology,
    Transport
  }

  alias Flezha.SDK.Operator.Inbox

  @notice "[flezha] LOCAL MODE: state is held in memory and NOTHING is durable — " <>
            "no write-ahead log, no snapshots, no dead-letter queue. Everything this run " <>
            "computes is gone when the process exits. Durability, plugins, placement and " <>
            "migration, the dead-letter queue, observability export, authz and multi-runner " <>
            "scale all need a controller."

  @counters_key {__MODULE__, :counters}

  use Flezha.SDK.CounterTable,
    table: :counter,
    slots: [
      [name: :notices, kind: :count],
      [name: :unhandled, kind: :count]
    ]

  # Consecutive idle probes needed before a run is called settled: a Cell
  # between its dispatch and the emit that follows it is momentarily idle by
  # every probe, and the first probe of a run establishes the dispatch total
  # the rest are compared against.
  @idle_observations 2

  defmodule Output do
    @moduledoc """
    One value a graph released on a declared sink, carrying the bytes the sink's
    serializer produced and the metadata a deployed run would have put on the
    wire — the channel name, and on a `keyed/1` sink the `flz.cell`, `flz.seq`
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
  `:operator_id` names the run (default `"<factory_id>-local"`).
  """
  @spec start(module(), keyword()) :: {:ok, pid()} | {:error, Error.t()}
  def start(module, opts \\ []) do
    with :ok <- refuse_controller_only(module),
         {:ok, topology} <- Topology.build(module),
         :ok <- refuse_topology_facts(topology) do
      # Nothing would wake a sleeping local run.
      GenServer.start_link(__MODULE__, {%{topology | sleep_after_ms: nil}, opts})
    end
  end

  @doc """
  Deliver every value released on the sink `channel` to `fun`, in release order.
  Refuses a name the declaration does not carry as a sink.
  """
  @spec on_sink(pid(), String.t(), (Output.t() -> any())) :: :ok | {:error, Error.t()}
  def on_sink(run, channel, fun) when is_function(fun, 1),
    do: Call.control(run, {:on_sink, channel, fun}, {:error, silent()})

  @doc """
  Feed one value to a declared entry channel — the bytes a controller would
  forward, decoded by that channel's own parser and admitted as deployed.
  Refuses a name the declaration does not carry as an entry.
  """
  @spec feed(pid(), String.t(), term()) :: :ok | {:error, Error.t()}
  def feed(run, channel, payload),
    do: Call.control(run, {:feed, channel, payload}, {:error, silent()})

  @doc """
  Block until every Cell has drained and nothing is in flight, or `budget_ms`
  elapses — which is reported rather than passed off as done.
  """
  @spec run_until_idle(pid(), non_neg_integer()) :: :ok | {:error, Error.t()}
  def run_until_idle(run, budget_ms) do
    case settle(run, System.monotonic_time(:millisecond) + budget_ms, {0, :unread}) do
      :ok ->
        :ok

      {:working, nil} ->
        {:error, Error.new("runtime.local_run_unanswered", budget_ms: budget_ms)}

      {:working, id} ->
        {:error, Error.new("runtime.local_run_busy", operator: id, budget_ms: budget_ms)}
    end
  end

  @doc """
  Stop the run and every Cell it holds. A run whose starter has already gone
  is stopped by that exit; asking again is answered, never raised.
  """
  @spec stop(pid()) :: :ok
  def stop(run) do
    GenServer.stop(run, :normal)
  catch
    :exit, _gone -> :ok
  end

  @doc """
  The live operator, for a caller that wants to read its Cells, or nil from
  a run that will not answer.
  """
  @spec operator(pid()) :: pid() | nil
  def operator(run), do: Call.control(run, :operator, nil)

  @doc "What every local run states before it does any work."
  @spec notice() :: String.t()
  def notice, do: @notice

  @doc "Non-durability notices emitted in this process; one per started run."
  @spec notices_emitted() :: non_neg_integer()
  def notices_emitted, do: counter_get(counters(), :notices)

  @doc """
  Values released on a sink no callback claimed. Each is reported as it happens;
  the count is what a run can assert on afterwards.
  """
  @spec unhandled_sink_outputs() :: non_neg_integer()
  def unhandled_sink_outputs, do: counter_get(counters(), :unhandled)

  # --- server ---

  @impl true
  def init({%Topology{} = topology, opts}) do
    Process.flag(:trap_exit, true)
    {sdk_side, controller_side} = LocalStream.open()
    :ok = Transport.controlling_process(controller_side, self())
    :ok = Transport.set_active(controller_side)
    counters = Counters.new()
    id = Keyword.get(opts, :operator_id, "#{topology.factory_id}-local")

    {:ok, op} =
      Operator.Server.start_link(
        operator_id: id,
        topology: topology,
        range: KeyGroup.decode(0, 0, 0, 0),
        counters: counters,
        runner: self(),
        secondaries: [sdk_side],
        credit: nil
      )

    counter_bump(counters(), :notices)
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

  def handle_call(:operator, _from, s), do: {:reply, s.op, s}

  @impl true
  # The operator forwards a park here because local mode is its runner: an
  # entry parse that failed, or a value the partition guard refused. Nothing
  # acks it, so it is named rather than swallowed.
  def handle_cast({:park, _cell, _identity, body}, s) do
    Logger.warning(
      "[flezha] local mode: operator `#{s.id}` parked an input on channel " <>
        "`#{body.channel}`: #{body.error}"
    )

    {:noreply, s}
  end

  # A tasklet's spawn request. Local mode has no control plane, so it is
  # answered as a runner whose session is down answers it.
  def handle_cast({:spawn, caller, spawn, _payload, _if_running}, s) do
    Spawn.answer(caller, spawn, :unavailable, nil)
    {:noreply, s}
  end

  def handle_cast({:claim_spawn, _reference_id}, s), do: {:noreply, s}

  # An instance's end is the controller's to act on; a local run has none.
  def handle_cast({:notify, _command}, s), do: {:noreply, s}

  @impl true
  # A sink output: the router writes it under the operator's own payload id.
  def handle_info({:flz_data, _stream, id, payload, metadata}, %{id: id} = s) do
    channel = Map.get(metadata, "channel", id)

    case Map.fetch(s.sinks, channel) do
      {:ok, fun} ->
        fun.(%Output{channel: channel, payload: payload, metadata: metadata})

      :error ->
        counter_bump(counters(), :unhandled)

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
    if Process.alive?(s.op), do: Operator.Server.stop(s.op)
    Transport.close(s.stream)
    :ok
  end

  # --- refusals ---

  # What a declaration can ask for that only a controller answers. Each is
  # refused by name rather than run as a graph whose work would go nowhere.
  # Partitioning is absent from this list because it is absent from the
  # declaration: a member's key-group range arrives with `AddOperator`, so a
  # local run is unpartitioned by construction.
  defp refuse_controller_only(module) do
    case Flezha.SDK.Operator.settings(module) do
      {:ok, %{run: :steps}} ->
        needs_controller(
          inspect(module),
          "run: :steps",
          "a class that runs in steps is driven from the controller's inbox, and its commit " <>
            "barriers are durable"
        )

      _flow_or_refused ->
        :ok
    end
  end

  defp refuse_topology_facts(%Topology{effectively_once?: true, factory_id: id}) do
    needs_controller(
      id,
      "exactly: :effectively_once",
      "under the tier an output rides its effect record, and the controller releases it " <>
        "once that record is durable — local mode has no durable store to release from"
    )
  end

  defp refuse_topology_facts(%Topology{}), do: :ok

  defp needs_controller(class, capability, why) do
    {:error,
     Error.new("runtime.local_needs_controller", factory: class, capability: capability, why: why)}
  end

  defp silent,
    do: Error.new("runtime.local_run_unanswered", budget_ms: Knob.get(:sdk_control_call_ms))

  # The declared channel of that kind; the refusal names the channel and the
  # operator, because a name nothing matches is otherwise a value that vanishes.
  defp declared(s, channel, kind) do
    names = channel_names(s.topology, kind)

    if channel in names,
      do: :ok,
      else:
        {:error,
         Error.new("runtime.channel_undeclared",
           operator: s.id,
           channel: channel,
           kind: kind,
           channels: names
         )}
  end

  defp channel_names(%Topology{entries: entries}, :entry), do: Map.keys(entries)

  # A sink channel takes the producing tasklet's name unless `named/2` gave it
  # one; the compiled route already resolved which.
  defp channel_names(%Topology{routes: routes}, :sink),
    do: for({_source, rs} <- routes, %{to: {:sink, _}, channel: c} <- rs, uniq: true, do: c)

  # --- quiescence ---

  defp settle(run, deadline, {seen, dispatched}) do
    # A run that will not answer its own status is not idle, and it names no
    # operator: the budget is what ends the wait either way.
    {state, id, now_dispatched} = Call.control(run, :status, {:working, nil, :unread})

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

  # The Cells first, then the router every completion passes back through,
  # so a dispatch that ends behind the walk is caught by the router rather than
  # missed by both. `s.stream` is this run's own side of the in-node pair: a
  # frame the router wrote but that process has not forwarded yet is in flight.
  defp idle?(s) do
    {handouts, _age} = Counters.handout_liveness(s.counters)
    {deferred, _age} = Counters.pending_push_liveness(s.counters)

    handouts == 0 and deferred == 0 and
      Process.info(self(), :message_queue_len) == {:message_queue_len, 0} and
      settled_cells?(s)
  end

  # A router that will not answer cannot be walked, so the run is not idle.
  defp settled_cells?(%{op: op} = s) do
    case Operator.Server.cells(op) do
      {:error, _busy} ->
        false

      nodes ->
        pids = for {_tasklet, _mode, keyed} <- nodes, {_key, pid} <- keyed, do: pid
        Enum.all?(pids ++ [s.stream.pid, op], &settled?/1)
    end
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
        :persistent_term.put(@counters_key, counter_new())
        :persistent_term.get(@counters_key)

      ref ->
        ref
    end
  end
end
