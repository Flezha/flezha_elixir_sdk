defmodule Flezha.SDK.Operative do
  @moduledoc """
  One process per operative instance: the routing core plus per-Cell
  supervision. An input on an entry channel (parsed, a parse failure riding the
  entry ladder) fans out along that entry's routes; a value a Cell emits fans
  out along its node's routes. Each route applies at most one fused transform and
  one key, then delivers to the owning Cell (Single/Static/Dynamic) or a sink.
  The partition guard admits a dynamic key before any Cell state exists and
  filters static keys by ownership. Sink outputs and park frames leave on the
  runner's secondary streams; a sink frame is named by its producing node's
  channel and, on a sink declared keyed, stamped with the Cell's identity,
  the seq its dispatch started at with the emit ordinal, and the key. Routing
  is serialized here (per-Cell ordering); Cells dispatch concurrently.

  Its Cells are the `Flezha.SDK.Operative.Cells` set, the one a workflow
  driver holds too: minted per mode, capped, and swept by a `:reap` tick that
  runs in this process, so no push can be routed while it runs. What an
  evicted Cell could not ship — the records it still held, then its delete —
  it hands here on the way out, and a rebind replays those before it rebinds
  the live Cells or replays its own outputs, so a dead Cell's delete cannot
  arrive behind its replacement's first record.
  """

  use GenServer

  alias Flezha.SDK.{Call, Cell, Hold, Park}
  alias Flezha.SDK.Operative.{Cells, Inbox, Routing}

  # What a router that will not answer tells its caller.
  @busy {:error, :operative_busy}
  # What an entry input carries into routing: no metadata, no key, no Cell,
  # and the durable offset the controller stamped under the tier.
  @entry_emit %{meta: %{}, key: nil, origin: nil, offset: nil, replay: nil}

  defmodule State do
    @moduledoc false
    defstruct [
      :operative_id,
      :topology,
      :runner,
      :cells,
      handed: nil
    ]
  end

  # --- API ---

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def stop(pid), do: GenServer.stop(pid, :normal)

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
             | :operative_busy}
  def lookup(pid, tasklet, key, opts \\ []),
    do: Call.control(pid, {:lookup, tasklet, key, Keyword.get(opts, :materialise, false)}, @busy)

  @doc """
  Every node as `{tasklet_name, mode, [{key, cell_pid}]}` — the live-read
  walk's input — or `{:error, :operative_busy}` from a router that will not
  answer. The refusal is deliberately not an empty list: "no Cells" and "the
  router did not say" are different facts, and a walk that confused them would
  report a live operative as empty.
  """
  @spec cells(GenServer.server()) ::
          [{String.t(), :single | :static | :dynamic, [{term(), pid()}]}]
          | {:error, :operative_busy}
  def cells(pid), do: Call.control(pid, :cells, @busy)

  # --- server ---

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    counters = Keyword.fetch!(opts, :counters)
    # The reconnect cap, resolved once: this Operative's holds and every
    # Cell's are bounded by one number the caller can state, rather than by
    # what the process-wide variable said at each separate construction.
    hold_cap = Keyword.get(opts, :hold_cap) || Hold.cap()

    cells =
      opts
      |> Keyword.take([
        :operative_id,
        :topology,
        :range,
        :counters,
        :runner,
        :secondaries,
        :credit
      ])
      |> Keyword.merge(kind: :stream, hold_cap: hold_cap)
      |> Cells.new()
      |> Cells.schedule_reap()

    {:ok,
     %State{
       operative_id: Keyword.fetch!(opts, :operative_id),
       topology: Keyword.fetch!(opts, :topology),
       runner: Keyword.fetch!(opts, :runner),
       cells: cells,
       # The frames a Cell that stopped handed on. The handoff cannot share
       # the Cells' output hold: it arrives after outputs it has to precede,
       # and one FIFO cannot say that.
       handed: Hold.new(cap: hold_cap, counters: counters)
     }}
  end

  @impl true
  def handle_call({:lookup, tasklet, key, materialise?}, _from, %State{} = s) do
    {reply, cells} = Cells.lookup(s.cells, tasklet, key, materialise?)
    {:reply, reply, %{s | cells: cells}}
  end

  def handle_call(:cells, _from, %State{} = s), do: {:reply, Cells.listing(s.cells), s}

  # A restore frame landed. Its saga markers are a workflow's; a stream has
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

  def handle_cast({:input, channel, message, metadata}, %State{} = s) do
    case Inbox.enter(s.runner, s.operative_id, s.topology, channel, message) do
      {:ok, channel, value} ->
        {offset, replay} = frame_facts(s.topology, metadata)
        emit = %{@entry_emit | offset: offset, replay: replay}
        {s, _ordinal} = route_from(s, {:entry, channel}, value, emit, 0)
        {:noreply, s}

      :parked ->
        {:noreply, s}
    end
  end

  # One invocation's emits: the ordinal counts its plugin outputs from zero.
  @impl true
  def handle_cast(
        {:emitted, node_ref, cell_key, emits, handout_seq, offset, report},
        %State{} = s
      ) do
    origin = %{node_ref: node_ref, cell_key: cell_key, seq: handout_seq}

    {s, _ordinal} =
      Enum.reduce(emits, {s, 0}, fn {value, meta, key}, {acc, ordinal} ->
        route_from(
          acc,
          {:node, node_ref},
          value,
          %{meta: meta, key: key, origin: origin, offset: offset},
          ordinal
        )
      end)

    Inbox.delivered(report)

    {:noreply, s}
  end

  @impl true
  def handle_cast({:park, cell_pid, identity, body}, %State{} = s) do
    Park.ship(s.runner, cell_pid, identity, body)
    {:noreply, s}
  end

  # A Cell died — forget it; it is recreated on that key's next input. A
  # crash must not take the Operative down with it, and must not leave the
  # key unroutable; the log is what makes a key crashing on every input
  # visible instead of looking like silent inactivity. Monitored, not linked,
  # so this process's own exit — `:normal` on `RemOperative` — ends every Cell
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

  def handle_info(_msg, s), do: {:noreply, s}

  # --- routing ---

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

  # Under the effectively-once tier a sink output IS the Cell's effect
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

  # A sink frame: the producing node's channel name over the user's metadata,
  # then — on a keyed sink — the identity stamps, which nothing can clobber.
  # Every plugin output advances the ordinal, stamped or not.
  defp deliver(%State{} = s, %{to: {:sink, _}} = route, _source, value, emit, ordinal) do
    stamp_with = if route.keyed, do: {s.operative_id, s.topology.node_modules}
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

  # What the controller stamped on a forwarded input of a tier class: the
  # durable offset, and whether the frame is a member of a replayed slice.
  # Nothing off the tier, where no metadata is read at all.
  defp frame_facts(%{effectively_once?: true}, metadata) when is_map(metadata) do
    offset =
      case Integer.parse(Map.get(metadata, "flz.offset", "")) do
        {offset, _rest} -> offset
        :error -> nil
      end

    {offset, if(Map.has_key?(metadata, "flz.replay"), do: :member)}
  end

  defp frame_facts(_topo, _metadata), do: {nil, nil}

  # The queue item: under the tier the offset and the replay mark ride the
  # input, so the Cell can skip a re-forward it already made durable and hold
  # a slice member back. Off the tier the queue is the raw value it always was.
  defp deliver_to_node(%State{topology: topo} = s, node_ref, value, key, emit) do
    item =
      if topo.effectively_once?,
        do: %Cell.Input{value: value, offset: emit.offset, replay: Map.get(emit, :replay)},
        else: value

    case Cells.deliver(s.cells, node_ref, value, key, item) do
      {{:misroute, reason}, cells} -> park_misroute(%{s | cells: cells}, node_ref, value, reason)
      {_keys, cells} -> %{s | cells: cells}
    end
  end

  # --- parking (entry-parse + partition mis-route) ---

  defp park_misroute(%State{} = s, node_ref, value, reason) do
    label = node_ref_label(s, node_ref)
    Inbox.park_pre_dispatch(s.runner, s.operative_id, label, label, reason, value)
    s
  end

  # The hold, on the streams bound right now. `secondaries` is the one source
  # of truth for what is bound, so no hold can hold a stale stream.
  defp wire(%State{cells: cells}, hold), do: Hold.bind(hold, cells.secondaries)

  defp node_ref_label(%State{topology: topo}, node_ref),
    do: topo.node_name[node_ref] || inspect(node_ref)
end
