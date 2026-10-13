defmodule Flezha.SDK.Relay.Server do
  @moduledoc """
  One process per relay instance: the routing core plus per-Cell
  supervision. An input on an entry channel (parsed, a parse failure riding the
  entry ladder) fans out along that entry's routes; a value a Cell emits fans
  out along its node's routes. Each route applies at most one fused transform and
  one key, then delivers to the owning Cell (Single/Static/Dynamic) or an output.
  The partition guard admits a dynamic key before any Cell state exists and
  filters static keys by ownership. Output outputs and park frames leave on the
  runner's secondary streams; an output frame is named by its producing node's
  channel and, on an output declared keyed, stamped with the Cell's identity,
  the seq its dispatch started at with the emit ordinal, and the key. Routing
  is serialized here (per-Cell ordering); Cells dispatch concurrently.

  Its Cells are the `Flezha.SDK.Relay.Cells` set, the one a steps relay
  driver holds too: minted per mode, capped, and swept by a `:reap` tick that
  runs in this process, so no push can be routed while it runs. What an
  evicted Cell could not ship — the records it still held, then its delete —
  it hands here on the way out, and a rebind replays those before it rebinds
  the live Cells or replays its own outputs, so a dead Cell's delete cannot
  arrive behind its replacement's first record.

  A persistent instance keeps a processed-prefix ledger (`Flezha.SDK.Prefix`):
  each push of an input with an offset holds it, its Cell releases it, and a
  tick reports the prefix behind everything written, on every stream.

  A class that sleeps (`sleep_after` under `durable`) arms one idle timer at
  bind. When it fires, the owed input tail still awaited or a Cell active
  inside the window re-arms it; with every Cell quiet past the window the
  instance ships `IterPark` at iteration `0`, and the controller evicts it.

  A tasklet's `complete/2`, or the class's `max_duration_ms`, ends the
  instance: once every Cell holds nothing it ships one `RelayCompleted`
  (`completed`, or `cancelled` for the wall-clock stop), and the controller
  stops it as it stops a finished steps relay.
  """

  use GenServer
  require Logger

  alias Flezha.SDK.{Call, Cell, Hold, Knob, Park, Prefix, Record}
  alias Flezha.SDK.Relay.{Cells, Inbox, Routing}
  alias Flezha.SDK.Steps.Commit

  # How long a report of the processed prefix waits for more releases.
  @prefix_tick_ms 5
  # How long an ending instance waits before it looks at its Cells again.
  @drain_recheck_ms 5

  # What a router that will not answer tells its caller.
  @busy {:error, :relay_busy}
  # What an entry input carries into routing: no metadata, no key, no Cell,
  # and the durable offset the controller stamped.
  @entry_emit %{meta: %{}, key: nil, origin: nil, offset: nil, call: nil, replay: nil}

  defmodule State do
    @moduledoc false
    defstruct [
      :relay_id,
      :topology,
      :runner,
      :cells,
      :prefix,
      handed: nil,
      prefix_tick?: false,
      # The sleep: the input tail the bind still owes, when it bound, and the
      # armed idle timer's ref (`nil` once it asked to sleep).
      owed: 0,
      bound_at: 0,
      idle: nil,
      # The end a tasklet or the wall clock asked for, `{status, result}`,
      # until it is reported.
      asked: nil,
      reported?: false
    ]
  end

  # --- API ---

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def stop(pid), do: GenServer.stop(pid, :normal)

  @doc """
  Whether an instance or Cell process ended cleanly: `:normal`, `:shutdown` or
  `{:shutdown, _}`. Any other reason is a crash.
  """
  @spec clean_exit?(term()) :: boolean()
  def clean_exit?(reason), do: reason in [:normal, :shutdown] or match?({:shutdown, _}, reason)

  @doc """
  The Cell of `tasklet` (its `name/0`) at `key` — `nil` for a Single node.
  `materialise: true` creates a dynamic key that has no Cell yet, which is
  what a restore of state the controller holds for it needs.
  """
  @spec lookup(GenServer.server(), String.t(), String.t() | nil, keyword()) ::
          {:ok, pid()}
          | {:error,
             :tasklet_not_found
             | :cell_not_found
             | :cell_evicted
             | :cell_capped
             | :relay_busy}
  def lookup(pid, tasklet, key, opts \\ []),
    do: Call.control(pid, {:lookup, tasklet, key, Keyword.get(opts, :materialise, false)}, @busy)

  @doc """
  Every node as `{tasklet_name, mode, [{key, cell_pid}]}` — the live-read
  walk's input — or `{:error, :relay_busy}` from a router that will not
  answer. The refusal is deliberately not an empty list: "no Cells" and "the
  router did not say" are different facts, and a walk that confused them would
  report a live relay as empty.
  """
  @spec cells(GenServer.server(), timeout()) ::
          [{String.t(), :single | :static | :dynamic, [{term(), pid()}]}]
          | {:error, :relay_busy}
  def cells(pid, ms \\ Knob.get(:sdk_control_call_ms)), do: Call.control(pid, :cells, @busy, ms)

  # --- server ---

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    counters = Keyword.fetch!(opts, :counters)
    # The reconnect cap, resolved once: this Relay's holds and every
    # Cell's are bounded by one number the caller can state, rather than by
    # what the process-wide variable said at each separate construction.
    hold_cap = Keyword.get(opts, :hold_cap) || Hold.cap()
    topology = Keyword.fetch!(opts, :topology)
    prefix = if topology.durable?, do: Prefix.new()

    cells =
      opts
      |> Keyword.take([
        :relay_id,
        :topology,
        :range,
        :counters,
        :runner,
        :effects,
        :secondaries,
        :credit
      ])
      |> Keyword.merge(run: :flow, hold_cap: hold_cap, prefix: prefix)
      |> Cells.new()
      |> Cells.schedule_reap()

    s = %State{
      relay_id: Keyword.fetch!(opts, :relay_id),
      topology: topology,
      runner: Keyword.fetch!(opts, :runner),
      cells: cells,
      prefix: prefix,
      # The frames a Cell that stopped handed on. The handoff cannot share
      # the Cells' output hold: it arrives after outputs it has to precede,
      # and one FIFO cannot say that.
      handed: Hold.new(cap: hold_cap, counters: counters),
      owed: Keyword.get(opts, :owed_inbox, 0),
      bound_at: now_ms()
    }

    after_ms = topology.sleep_after_ms
    max_duration_ms = topology.settings.max_duration_ms
    if max_duration_ms, do: Process.send_after(self(), :max_duration, max_duration_ms)
    {:ok, if(after_ms, do: arm_idle(s, after_ms), else: s)}
  end

  @impl true
  def handle_call({:lookup, tasklet, key, materialise?}, _from, %State{} = s) do
    {reply, cells} = Cells.lookup(s.cells, tasklet, key, materialise?)
    {:reply, reply, %{s | cells: cells}}
  end

  def handle_call(:cells, _from, %State{} = s), do: {:reply, Cells.listing(s.cells), s}

  # A restore frame landed. Its saga markers are a steps relay's; a flow relay has
  # no restore gate to count it toward.
  def handle_call({:restore_landed, _identity, _intent, _done}, _from, %State{} = s),
    do: {:reply, :ok, s}

  # The end of a replayed slice: a payload-less broadcast closing every Cell's
  # slice at once, because an input reaches ONE Cell by key and a marker
  # riding one would strand the slice every other Cell buffered. It names no
  # channel, so it is answered before any is resolved.
  @impl true
  def handle_cast({:input, _channel, _message, %{"flz.replay_end" => _}}, %State{} = s) do
    for pid <- Cells.pids(s.cells), do: send(pid, :end_replay_slice)
    {:noreply, s}
  end

  # A value a restore delivers again, named by the producer's record: to the
  # one consumer it names, whose floor drops it when it already took `origin`.
  def handle_cast({:input, _channel, message, %{"flz.edge" => edge} = metadata}, %State{} = s) do
    {offset, call, _replay} = frame_facts(metadata)
    {:noreply, redeliver_edge(s, edge, message, offset, call)}
  end

  def handle_cast({:input, channel, message, metadata}, %State{} = s) do
    s = if owed_arrival?(metadata), do: %{s | owed: max(s.owed - 1, 0)}, else: s

    {offset, call, replay} = frame_facts(metadata)

    case Inbox.enter(s.runner, s.relay_id, s.topology, channel, message, call) do
      {:ok, channel, value} ->
        emit = %{@entry_emit | offset: offset, call: call, replay: replay}
        # Held while it routes, so the prefix reaches it even when it reaches
        # no Cell.
        held = offset && s.prefix
        if held, do: Prefix.hold(held, offset)
        {s, _ordinal} = route_from(s, {:entry, channel}, value, emit, 0)
        if held, do: Prefix.release(held, offset)
        {:noreply, s}

      :parked ->
        {:noreply, s}
    end
  end

  # One invocation's emits: the ordinal counts its plugin outputs from zero.
  @impl true
  def handle_cast(
        {:emitted, node_ref, cell_key, emits, handout_seq, {offset, call}, report},
        %State{} = s
      ) do
    origin = %{node_ref: node_ref, cell_key: cell_key, seq: handout_seq}

    {s, _ordinal} =
      Enum.reduce(emits, {s, 0}, fn {value, meta, key}, {acc, ordinal} ->
        route_from(
          acc,
          {:node, node_ref},
          value,
          %{meta: meta, key: key, origin: origin, offset: offset, call: call},
          ordinal
        )
      end)

    Inbox.delivered(report)

    {:noreply, s}
  end

  # A tasklet asked to end the instance; a later completion replaces the result.
  @impl true
  def handle_cast({:complete, _node_ref, _cell_key, result}, %State{} = s),
    do: {:noreply, ask_end(s, "completed", result)}

  @impl true
  def handle_cast({:park, cell_pid, identity, body}, %State{} = s) do
    Park.ship(s.runner, cell_pid, identity, body)
    {:noreply, s}
  end

  # A Cell died — forget it; it is recreated on that key's next input. A
  # crash must not take the Relay down with it, and must not leave the
  # key unroutable; the log is what makes a key crashing on every input
  # visible instead of looking like silent inactivity. Monitored, not linked,
  # so this process's own exit — `:normal` on `RemRelay` — ends every Cell
  # through its monitor of this process. The Cell's `after` cannot run for an
  # untrappable kill, so its in-flight slot is released there too.
  @impl true
  def handle_info({:DOWN, _ref, :process, pid, reason}, %State{} = s),
    do: {:noreply, %{s | cells: Cells.down(s.cells, pid, reason)}}

  # What a stopped Cell handed on goes ahead of the Cells' own records and
  # outputs: its delete has to reach the controller before a replacement's
  # first record.
  def handle_info({:secondaries, secondaries}, %State{} = s) do
    s = %{s | handed: s.handed |> Hold.bind(secondaries) |> Hold.replay()}
    {:noreply, %{s | cells: Cells.rebind(s.cells, secondaries)}}
  end

  # A Cell that stopped in an orderly way hands on what no stream would
  # take: the records it still held, then the delete it owes. A hold that dies
  # with the process that owned it is not a hold, and this process outlives it.
  def handle_info({:handed, identity, frames}, %State{} = s) do
    handed =
      Enum.reduce(frames, wire(s, s.handed), fn {key, body, meta}, acc ->
        {_verdict, acc} = Hold.write(key, acc, {body, meta, identity})
        acc
      end)

    {:noreply, %{s | handed: handed}}
  end

  # The sweep runs here, so no push is routed while it runs: the drain gate.
  def handle_info(:reap, %State{} = s),
    do: {:noreply, %{s | cells: s.cells |> Cells.reap(MapSet.new()) |> Cells.schedule_reap()}}

  def handle_info({:idle, ref}, %State{idle: ref, asked: nil} = s), do: {:noreply, idle_due(s)}

  def handle_info(:max_duration, %State{} = s), do: {:noreply, ask_end(s, "cancelled", "")}
  def handle_info(:end_due, %State{} = s), do: {:noreply, end_due(s)}

  def handle_info(:prefix_tick, %State{} = s) do
    s = %{s | prefix_tick?: false}
    report = report_prefix(s)
    {:noreply, if(report == :owed or Prefix.pending?(s.prefix), do: arm_prefix(s), else: s)}
  end

  def handle_info(_msg, s), do: {:noreply, s}

  # A crash ends every Cell first and waits for each, so everything the dead
  # incarnation's Cells wrote is on the wire before the runner reads this
  # process's exit and reports it. A clean stop leaves them to end through
  # their monitor of this process.
  @impl true
  def terminate(reason, %State{} = s) do
    if not clean_exit?(reason), do: Cells.kill_all(s.cells)
    :ok
  end

  # --- ending from inside ---

  # The first end asked for arms the check; a completion replaces a
  # wall-clock stop that has not been reported yet, never the reverse.
  defp ask_end(%State{reported?: true} = s, _status, _result), do: s

  defp ask_end(%State{asked: nil} = s, status, result) do
    send(self(), :end_due)
    %{s | asked: {status, result}}
  end

  defp ask_end(%State{} = s, "completed", result), do: %{s | asked: {"completed", result}}
  defp ask_end(%State{} = s, _status, _result), do: s

  # What the Cells already hold runs to its outputs before the end is reported.
  defp end_due(%State{asked: {status, result}, reported?: false} = s) do
    if quiet_since(s) == :busy do
      Process.send_after(self(), :end_due, @drain_recheck_ms)
      s
    else
      Commit.completed(s.runner, s.relay_id, status, "", result)
      %{s | reported?: true}
    end
  end

  defp end_due(%State{} = s), do: s

  # --- sleeping ---

  defp arm_idle(%State{} = s, after_ms) do
    ref = make_ref()
    Process.send_after(self(), {:idle, ref}, max(after_ms, 1))
    %{s | idle: ref}
  end

  # The owed tail bounds the wait for itself; a Cell busy or active inside the
  # window re-arms; every Cell quiet past it sleeps the instance.
  defp idle_due(%State{} = s) do
    window = s.topology.sleep_after_ms
    now = now_ms()
    owed_left = Knob.get(:sdk_owed_inbox_wait_ms) - (now - s.bound_at)

    with :sleep <-
           if(s.owed > 0 and owed_left > 0, do: {:wait, min(window, owed_left)}, else: :sleep),
         {:quiet, latest} <- quiet_since(s),
         quiet_for = now - latest,
         :sleep <- if(quiet_for < window, do: {:wait, window - quiet_for}, else: :sleep) do
      Commit.park(s.runner, s.relay_id, 0, 0, nil)
      %{s | idle: nil}
    else
      {:wait, after_ms} -> arm_idle(s, after_ms)
      :busy -> arm_idle(s, window)
    end
  end

  # The latest input of any Cell, the bind when none came later, or `:busy`
  # when one still holds work.
  defp quiet_since(%State{cells: cells, bound_at: bound_at}) do
    probe_ms = Knob.get(:sdk_idle_probe_ms)

    Enum.reduce_while(Cells.pids(cells), {:quiet, bound_at}, fn pid, {:quiet, latest} ->
      case Cell.quiet_since(pid, probe_ms) do
        {:quiet, at} -> {:cont, {:quiet, max(latest, at)}}
        :busy -> {:halt, :busy}
      end
    end)
  end

  # --- the processed prefix ---

  defp arm_prefix(%State{prefix_tick?: true} = s), do: s

  defp arm_prefix(%State{} = s) do
    Process.send_after(self(), :prefix_tick, @prefix_tick_ms)
    %{s | prefix_tick?: true}
  end

  # The marker goes on every stream, behind everything this process wrote
  # there, and only when nothing it holds is still waiting for one: a marker
  # ahead of a held record would confirm what the controller never received.
  # `:owed` when a prefix could not go out yet, `:done` otherwise.
  defp report_prefix(%State{prefix: ledger, cells: cells} = s) do
    with p when is_integer(p) <- Prefix.candidate(ledger),
         true <- s.handed.n == 0 and cells.held.n == 0 and cells.secondaries != [] do
      identity = Park.identity(s.relay_id, "", "")
      frame = Record.prefix_frame(identity, p, length(cells.secondaries))

      sent? =
        Enum.all?(cells.secondaries, fn handle ->
          hold = Hold.new(streams: [handle])
          match?({:ok, _hold}, Hold.write("processed_prefix", hold, {frame, %{}}))
        end)

      if sent?, do: Prefix.reported(ledger, p)
      if sent?, do: :done, else: :owed
    else
      nil -> :done
      false -> :owed
    end
  end

  # --- routing ---

  # A journaled input the controller re-forwarded or forwarded: stamped with
  # its input-log offset.
  defp owed_arrival?(metadata), do: is_map(metadata) and Map.has_key?(metadata, "flz.offset")

  defp now_ms, do: System.monotonic_time(:millisecond)

  # Fan `value` out along `source`'s routes; `emit` is the producing emit's
  # metadata, key and origin, `ordinal` the invocation's plugin-output count.
  defp route_from(%State{topology: topo} = s, source, value, emit, ordinal) do
    routes = Map.get(topo.routes, source, [])

    Enum.reduce(routes, {s, ordinal}, fn route, {acc, ord} ->
      outputs = route.transform.(value)

      Enum.reduce(outputs, {acc, ord}, fn out, {a, o} ->
        deliver(a, route, source, out, emit, o)
      end)
    end)
  end

  # Under the effectively-once tier an output IS the Cell's effect
  # record: it never reaches the wire from here, and the ordinal advances as
  # it would have.
  defp deliver(
         %State{topology: %{effectively_once?: true}} = s,
         %{to: {:sink, _}},
         _source,
         _value,
         _emit,
         ordinal
       ),
       do: {s, ordinal + 1}

  # An output frame: the producing node's channel name over the user's metadata,
  # then — on a keyed output — the identity stamps, which nothing can clobber.
  # Every plugin output advances the ordinal, stamped or not.
  defp deliver(%State{} = s, %{to: {:sink, _}} = route, _source, value, emit, ordinal) do
    stamp_with = if route.keyed, do: {s.relay_id, s.topology.node_modules}
    {metadata, bytes} = Routing.sink_frame(route, value, emit, ordinal, stamp_with)
    {%{s | cells: Cells.emit_output(s.cells, metadata, bytes)}, ordinal + 1}
  end

  defp deliver(
         %State{} = s,
         %{to: {:node, node_ref}, key_by: key_by},
         _source,
         value,
         emit,
         ordinal
       ) do
    key = if key_by, do: key_by.(value), else: nil
    {deliver_to_node(s, node_ref, value, key, emit), ordinal}
  end

  # What the controller stamped on a forwarded input: its input-log offset,
  # the call waiting on it, and whether the frame is a member of a replayed
  # slice.
  defp frame_facts(metadata) when is_map(metadata),
    do:
      {Inbox.input_offset(metadata), Inbox.input_call(metadata),
       if(Map.has_key?(metadata, "flz.replay"), do: :member)}

  defp frame_facts(_metadata), do: {nil, nil, nil}

  # The queue item: an input with an offset or a call carries them, the replay
  # mark and whether the controller forwarded it, so the Cell can skip a
  # re-forward it already took, hold a slice member back and answer the call.
  # Anything else is the raw value.
  defp deliver_to_node(%State{topology: topo} = s, node_ref, value, key, emit) do
    item =
      if topo.effectively_once? or is_integer(emit.offset) or emit.call != nil,
        do: %Cell.Input{
          value: value,
          offset: emit.offset,
          call: emit.call,
          replay: Map.get(emit, :replay),
          forwarded?: emit.origin == nil
        },
        else: value

    s =
      case Cells.deliver(s.cells, node_ref, value, key, item) do
        {{:misroute, reason}, cells} ->
          park_misroute(%{s | cells: cells}, node_ref, value, reason, emit.call)

        {_keys, cells} ->
          %{s | cells: cells}
      end

    if s.prefix && is_integer(emit.offset), do: arm_prefix(s), else: s
  end

  # The edge names its producer and consumer by node seq, as the producer's
  # record wrote it; the value was already through the route's transform.
  defp redeliver_edge(%State{topology: topo} = s, edge, payload, offset, call) do
    with [producer, consumer] <- String.split(edge, ":"),
         {producer, ""} <- Integer.parse(producer),
         {consumer, ""} <- Integer.parse(consumer),
         %{key_by: key_by, to: {:node, consumer_ref}} <- edge_route(topo, producer, consumer),
         {:ok, value} <- Record.decode_body(payload) do
      key = if key_by, do: key_by.(value)
      emit = %{@entry_emit | origin: :redelivered, offset: offset, call: call}
      deliver_to_node(s, consumer_ref, value, key, emit)
    else
      _unknown ->
        Logger.warning("[Flezha.SDK] #{s.relay_id} re-delivered edge #{edge} matches no route")
        s
    end
  end

  defp edge_route(topo, producer, consumer) do
    refs = Map.new(topo.node_seq, fn {ref, seq} -> {seq, ref} end)

    with %{^producer => from, ^consumer => to} <- refs,
         do: topo.routes |> Map.get({:node, from}, []) |> Enum.find(&(&1.to == {:node, to}))
  end

  # --- parking (entry-parse + partition mis-route) ---

  defp park_misroute(%State{} = s, node_ref, value, reason, call) do
    label = node_ref_label(s, node_ref)
    Inbox.park_pre_dispatch(s.runner, s.relay_id, label, label, reason, value, reply_to: call)
    s
  end

  # The hold, on the streams bound right now. `secondaries` is the one source
  # of truth for what is bound, so no hold can hold a stale stream.
  defp wire(%State{cells: cells}, hold), do: Hold.bind(hold, cells.secondaries)

  defp node_ref_label(%State{topology: topo}, node_ref),
    do: topo.node_name[node_ref] || inspect(node_ref)
end
