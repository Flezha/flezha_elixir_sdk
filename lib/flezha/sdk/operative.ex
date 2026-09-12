defmodule Flezha.SDK.Operative do
  @moduledoc """
  One process per operative instance: the routing core plus per-Entity
  supervision. An input on an entry channel (parsed, a parse failure riding the
  entry ladder) fans out along that entry's routes; a value an Entity emits fans
  out along its node's routes. Each route applies at most one fused transform and
  one key, then delivers to the owning Entity (Single/Static/Dynamic) or a sink.
  The partition guard admits a dynamic key before any Entity state exists and
  filters static keys by ownership. Sink outputs and park frames leave on the
  runner's secondary streams; a sink frame is named by its producing node's
  channel and, on a sink declared keyed, stamped with the Entity's identity,
  the seq its dispatch started at with the emit ordinal, and the key. Routing
  is serialized here (per-Entity ordering); Entities dispatch concurrently.

  A dynamic node's Entities are minted per key up to its `max_entities/0` and,
  under a declared `entity_idle_ttl_ms/0`, swept by one `:reap` tick here:
  the sweep runs in this process, so no push can be routed while it asks each
  Entity whether it is idle — the drain gate — and an idle one evicts itself
  destructively before the next push can mint its successor. Evicted keys are
  remembered, bounded, so a live read can say "evicted" rather than "never
  existed". What that Entity could not ship — the records it still held, then
  its delete — it hands here on the way out, and a rebind replays those before
  it rebinds the live Entities or replays its own outputs, so a dead Entity's
  delete cannot arrive behind its replacement's first record.
  """

  use GenServer
  require Logger

  alias Flezha.SDK.{
    Call,
    Counters,
    Entity,
    EntryInput,
    Hold,
    KeyGroup,
    Knob,
    Park,
    Procedure,
    Topology
  }

  alias Flezha.SDK.Operative.{Inbox, Routing}

  # What a router that will not answer tells its caller.
  @busy {:error, :operative_busy}
  @default_retry_max 3
  # What an entry input carries into routing: no metadata, no key, no Entity,
  # and the durable offset the controller stamped under the tier.
  @entry_emit %{meta: %{}, key: nil, origin: nil, offset: nil, replay: nil}

  # The identity slot a park with no resolved channel takes. A name off the
  # wire never becomes a path component: the controller makes this slot a
  # directory under the dead-letter root, and the name the frame asked for
  # rides the row's `channel` field. `flz_core` spells it the same way.
  @unresolved_procedure_id "_unresolved"
  # How long a sweep waits on one Entity's idle probe before moving on.
  # `FLEZHA_REAP_INTERVAL_MS`, else a quarter of the shortest TTL, at most this.
  # Evicted keys remembered; beyond it a key reads as never existed.
  @evicted_trace_cap 1_024
  # Evictions one sweep performs at most: the router is held while it runs,
  # so a burst of expiries drains over a few ticks rather than stalling it.
  @evict_budget 256

  defmodule State do
    @moduledoc false
    defstruct [
      :operative_id,
      :topology,
      :range,
      :counters,
      :hold_cap,
      :runner,
      :secondaries,
      :slots,
      :credit,
      cells: %{},
      entities: %{},
      dynamic: %{},
      evicted: nil,
      held: nil,
      handed: nil
    ]
  end

  # --- API ---

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def stop(pid), do: GenServer.stop(pid, :normal)

  @doc """
  The Entity of `procedure` (its `name/0`) at `key` — `nil` for a Single node.
  `materialise: true` creates a dynamic key that has no Entity yet, which is
  what a restore of state the controller holds for it needs.
  """
  @spec lookup(GenServer.server(), String.t(), String.t() | nil, keyword()) ::
          {:ok, pid()}
          | {:error,
             :procedure_not_found
             | :entity_not_found
             | :entity_evicted
             | :entity_capped
             | :operative_busy}
  def lookup(pid, procedure, key, opts \\ []),
    do:
      Call.control(pid, {:lookup, procedure, key, Keyword.get(opts, :materialise, false)}, @busy)

  @doc """
  Every node as `{procedure_name, mode, [{key, entity_pid}]}` — the live-read
  walk's input — or `{:error, :operative_busy}` from a router that will not
  answer. The refusal is deliberately not an empty list: "no Entities" and "the
  router did not say" are different facts, and a walk that confused them would
  report a live operative as empty.
  """
  @spec entities(GenServer.server()) ::
          [{String.t(), :single | :static | :dynamic, [{term(), pid()}]}]
          | {:error, :operative_busy}
  def entities(pid), do: Call.control(pid, :entities, @busy)

  # --- server ---

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    topology = Keyword.fetch!(opts, :topology)
    counters = Keyword.fetch!(opts, :counters)
    # The reconnect cap, resolved once: this Operative's holds and every
    # Entity's are bounded by one number the caller can state, rather than by
    # what the process-wide variable said at each separate construction.
    hold_cap = Keyword.get(opts, :hold_cap) || Hold.cap()

    state = %State{
      operative_id: Keyword.fetch!(opts, :operative_id),
      topology: topology,
      # The entry cells, resolved once: the RunnerServer interns them before
      # starting this process, so the delivery path below is one atomic store.
      cells: Keyword.get_lazy(opts, :cells, fn -> EntryInput.cells_for(topology.entries) end),
      range: Keyword.fetch!(opts, :range),
      counters: counters,
      hold_cap: hold_cap,
      runner: Keyword.fetch!(opts, :runner),
      secondaries: Keyword.fetch!(opts, :secondaries),
      credit: Keyword.get(opts, :credit),
      # The slot survives the Entity: what a persistent Entity last shipped,
      # for the replacement of one killed untrappably.
      slots: :ets.new(:flezha_sdk_slots, [:public, :set]),
      dynamic: dynamic_knobs(topology),
      # Outputs no stream will take, the frames an Entity that stopped handed
      # on, and the keys a sweep evicted: one structure, three holds. The
      # handoff cannot share `held`: it arrives after outputs it has to
      # precede, and one FIFO cannot say that.
      held: Hold.new(cap: hold_cap, counters: counters),
      handed: Hold.new(cap: hold_cap, counters: counters),
      evicted: Hold.new(cap: @evicted_trace_cap)
    }

    {:ok, state |> prewarm() |> schedule_reap()}
  end

  # The dynamic nodes' knobs, read once: the cap, and the TTL when declared.
  defp dynamic_knobs(topo) do
    for {node_ref, :dynamic} <- topo.node_mode, into: %{} do
      module = topo.node_modules[node_ref]

      {node_ref,
       %{cap: Procedure.max_entities(module), ttl_ms: Procedure.entity_idle_ttl_ms(module)}}
    end
  end

  # One tick for the whole operative, at the shortest TTL's cadence; no TTL, no tick.
  defp schedule_reap(%State{dynamic: dynamic} = s) do
    case for {_ref, %{ttl_ms: ttl}} <- dynamic, ttl != nil, do: ttl do
      [] -> s
      ttls -> Process.send_after(self(), :reap, reap_interval(Enum.min(ttls)))
    end

    s
  end

  defp reap_interval(ttl_ms),
    do:
      Knob.get(
        :reap_interval_ms,
        ttl_ms |> div(4) |> min(Knob.get(:sdk_max_reap_interval_ms)) |> max(1)
      )

  @impl true
  def handle_call({:lookup, procedure, key, materialise?}, _from, %State{topology: topo} = s) do
    with {:ok, node_ref} <- Topology.node_of(topo, procedure),
         {:ok, slot_key} <- slot_key(s, node_ref, key) do
      case {get_in(s.entities, [node_ref, slot_key]), materialise?} do
        {nil, false} -> {:reply, {:error, missing(s, node_ref, slot_key)}, s}
        {nil, true} -> materialise(s, node_ref, slot_key)
        {pid, _} -> {:reply, {:ok, pid}, s}
      end
    else
      {:error, _} = error -> {:reply, error, s}
    end
  end

  def handle_call(:entities, _from, %State{topology: topo} = s) do
    listing =
      Enum.map(topo.order, fn node_ref ->
        pids = s.entities |> Map.get(node_ref, %{}) |> Enum.sort()
        {topo.node_modules[node_ref].name(), topo.node_mode[node_ref], pids}
      end)

    {:reply, listing, s}
  end

  # A restore's case: mint the key it names, unless the node is at its cap.
  defp materialise(%State{} = s, node_ref, key) do
    case ensure_entity(s, node_ref, key) do
      {:capped, s} -> {:reply, {:error, :entity_capped}, s}
      {pid, s} -> {:reply, {:ok, pid}, s}
    end
  end

  # Evicted and never-existed serve the same absence; the diagnosis differs.
  defp missing(%State{topology: topo} = s, node_ref, key) do
    if topo.node_mode[node_ref] == :dynamic and Hold.member?(s.evicted, key),
      do: :entity_evicted,
      else: :entity_not_found
  end

  # Eagerly create Single (one) and owned Static entities; Dynamic ones are lazy.
  defp prewarm(%State{topology: topo} = state) do
    Enum.reduce(topo.node_mode, state, fn
      {node_ref, :single}, acc ->
        ensure_entity(acc, node_ref, nil) |> elem(1)

      {node_ref, :static}, acc ->
        module = topo.node_modules[node_ref]

        Procedure.entity_keys(module)
        |> Enum.filter(&KeyGroup.owns?(acc.range, &1))
        |> Enum.reduce(acc, fn key, a -> ensure_entity(a, node_ref, key) |> elem(1) end)

      {_node_ref, :dynamic}, acc ->
        acc
    end)
  end

  # The end of a replayed slice: a payload-less broadcast closing every Entity's
  # slice at once, because an input reaches ONE Entity by key and a marker
  # riding one would strand the slice every other Entity buffered. It names no
  # channel, so it is answered before any is resolved.
  @impl true
  def handle_cast({:input, _channel, _message, %{"flz.replay_end" => _}}, %State{} = s) do
    for {_node_ref, keyed} <- s.entities, {_key, pid} <- keyed, do: send(pid, :end_replay_slice)
    {:noreply, s}
  end

  def handle_cast({:input, channel, message, metadata}, %State{} = s) do
    case resolve_channel(s.topology, channel) do
      {:unresolved, requested} ->
        {:noreply, park_unresolved(s, requested, message)}

      channel ->
        # The frame counts before it can be declined: the stamp precedes the
        # parse. A frame that resolved no channel stamps none, having reached
        # none.
        EntryInput.stamp(Map.fetch!(s.cells, channel))

        case parse(s.topology, channel, message) do
          {:ok, value} ->
            {offset, replay} = frame_facts(s.topology, metadata)
            emit = %{@entry_emit | offset: offset, replay: replay}
            {s, _ordinal} = route_from(s, {:entry, channel}, value, emit, 0)
            {:noreply, s}

          {:error, reason} ->
            {:noreply, park_entry(s, channel, message, reason)}
        end
    end
  end

  # One invocation's emits: the ordinal counts its plugin outputs from zero.
  @impl true
  def handle_cast(
        {:emitted, node_ref, entity_key, emits, handout_seq, offset, report},
        %State{} = s
      ) do
    origin = %{node_ref: node_ref, entity_key: entity_key, seq: handout_seq}

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
  def handle_cast({:park, entity_pid, identity, body}, %State{} = s) do
    Park.ship(s.runner, entity_pid, identity, body)
    {:noreply, s}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, reason}, %State{} = s) do
    # An Entity died — forget it; it is recreated on that key's next input. A
    # crash must not take the Operative down with it, and must not leave the
    # key unroutable; the log is what makes a key crashing on every input
    # visible instead of looking like silent inactivity. Monitored, not
    # linked, so this process's own exit — `:normal` on `RemOperative` — ends
    # every Entity through its monitor of this process.
    dead = for {ref, keyed} <- s.entities, {key, ^pid} <- keyed, do: {ref, key}

    s =
      Enum.reduce(dead, s, fn {node_ref, key}, acc ->
        if reason not in [:normal, :shutdown] do
          Logger.warning(
            "[Flezha.SDK] entity crashed: operative=#{acc.operative_id} " <>
              "node=#{inspect(node_ref)} key=#{inspect(key)} reason=#{inspect(reason)}"
          )
        end

        # The Entity's `after` cannot run for an untrappable kill, so its
        # in-flight slot is released here or it would report a stall forever.
        acc |> forget_entity(node_ref, key) |> remint(node_ref, key, reason)
      end)

    {:noreply, s}
  end

  # Each Entity rebinds and replays its held records BEFORE this replays an
  # output of the same dispatch, which is why the rebind is a call: a send only
  # orders the mailbox, and the guarantee is about the order two processes
  # write to the wire. What a stopped Entity handed on goes ahead of both: its
  # delete has to reach the controller before a replacement's first record.
  def handle_info({:secondaries, secondaries}, %State{} = s) do
    s = %{s | secondaries: secondaries}
    s = %{s | handed: s |> wire(s.handed) |> Hold.replay()}
    for {_ref, keyed} <- s.entities, {_key, pid} <- keyed, do: Entity.rebind(pid, secondaries)

    {:noreply, %{s | held: s |> wire(s.held) |> Hold.replay()}}
  end

  # An Entity that stopped in an orderly way hands on what no stream would
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
  def handle_info(:reap, %State{} = s) do
    {s, _left} =
      Enum.reduce(s.dynamic, {s, @evict_budget}, fn
        {node_ref, %{ttl_ms: ttl}}, {acc, left} when ttl != nil ->
          reap_node(acc, node_ref, ttl, left)

        _no_ttl, acc ->
          acc
      end)

    {:noreply, schedule_reap(s)}
  end

  def handle_info(_msg, s), do: {:noreply, s}

  # A Single or Static Entity that crashed is minted again at once, as
  # `prewarm/1` minted it: what its slot holds — armed timers above all — must
  # not wait for an input. A dynamic one stays lazy, as its keys are.
  defp remint(%State{topology: topo} = s, node_ref, key, reason) do
    if reason in [:normal, :shutdown] or topo.node_mode[node_ref] == :dynamic,
      do: s,
      else: s |> ensure_entity(node_ref, key) |> elem(1)
  end

  # Two calls per Entity: a bounded probe that changes nothing, then — for one
  # proved idle — the eviction, synchronous, so its delete is on the wire
  # before any push can mint a successor on the same stream.
  defp reap_node(%State{} = s, node_ref, ttl_ms, budget) do
    s.entities
    |> Map.get(node_ref, %{})
    |> Enum.reduce({s, budget}, fn
      _entity, {acc, 0} ->
        {acc, 0}

      {key, pid}, {acc, left} ->
        if Entity.idle_for?(pid, ttl_ms, Knob.get(:sdk_idle_probe_ms)) and
             Entity.evict(pid) == :evicted,
           do: {note_evicted(forget_entity(acc, node_ref, key), key), left - 1},
           else: {acc, left}
    end)
  end

  defp note_evicted(%State{evicted: evicted} = s, key) do
    dynamic_event(s, :evicted)
    %{s | evicted: Hold.push(evicted, key)}
  end

  defp dynamic_event(%State{operative_id: oid}, event),
    do:
      :telemetry.execute([:flezha, :sdk, :dynamic], %{count: 1}, %{
        operative_id: oid,
        event: event
      })

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

  # Under the effectively-once tier a sink output IS the Entity's effect
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
    {emit_output(s, metadata, bytes), ordinal + 1}
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
  # input, so the Entity can skip a re-forward it already made durable and hold
  # a slice member back. Off the tier the queue is the raw value it always was.
  defp deliver_to_node(%State{topology: topo} = s, node_ref, value, key, emit) do
    item =
      if topo.effectively_once?,
        do: %Entity.Input{value: value, offset: emit.offset, replay: Map.get(emit, :replay)},
        else: value

    case Map.fetch!(topo.node_mode, node_ref) do
      :single ->
        enqueue(s, node_ref, nil, item)

      :static ->
        module = topo.node_modules[node_ref]

        Procedure.entities_for(module, value)
        |> Enum.filter(&KeyGroup.owns?(s.range, &1))
        |> Enum.reduce(s, fn k, acc -> enqueue(acc, node_ref, k, item) end)

      :dynamic ->
        case KeyGroup.admit(s.range, key || "") do
          :ok -> enqueue(s, node_ref, key, item)
          {:error, reason} -> park_misroute(s, node_ref, value, reason)
        end
    end
  end

  # A `:drop_nothing` enqueue is a `GenServer.call`, so an Entity that died between
  # the last input and this one takes the CALLER down — this Operative — unless
  # the exit is caught. The `:DOWN` clause cannot be relied on to have run first:
  # the input and the down message are two messages racing for the same mailbox.
  # Catching it here is what makes "the key is routable again on its next
  # input" true by construction rather than by ordering luck; the retry starts
  # a fresh Entity, which is what the `:DOWN` clause would have arranged.
  defp enqueue(%State{} = s, node_ref, key, value, retry? \\ true) do
    case ensure_entity(s, node_ref, key) do
      {:capped, s} -> s
      {pid, s} -> push(s, pid, node_ref, key, value, retry?)
    end
  end

  defp push(%State{} = s, pid, node_ref, key, value, retry?) do
    case knob(s.topology, node_ref, :on_full, :drop_nothing) do
      :drop_nothing -> Entity.enqueue_block(pid, value)
      other -> Entity.enqueue_drop(pid, value, other)
    end

    s
  catch
    :exit, _reason when retry? ->
      enqueue(forget_entity(s, node_ref, key), node_ref, key, value, false)
  end

  defp ensure_entity(%State{} = s, node_ref, key) do
    keyed = Map.get(s.entities, node_ref, %{})

    case Map.get(keyed, key) do
      nil -> mint_entity(s, node_ref, key, keyed)
      pid -> {pid, s}
    end
  end

  # A dynamic node at its cap mints nothing for a new key: the shared core's
  # best-effort cap — counted, never reported back to the controller.
  defp mint_entity(%State{dynamic: dynamic} = s, node_ref, key, keyed) do
    case Map.get(dynamic, node_ref) do
      %{cap: cap} when map_size(keyed) >= cap ->
        dynamic_event(s, :capped)
        {:capped, s}

      knobs ->
        {:ok, pid} = start_entity(s, node_ref, key)
        Process.monitor(pid)
        if knobs, do: dynamic_event(s, :created)
        {pid, %{s | entities: Map.put(s.entities, node_ref, Map.put(keyed, key, pid))}}
    end
  end

  # Drop a dead Entity and release its in-flight slot. Idempotent with the
  # `:DOWN` clause, which does the same for the down message that follows.
  defp forget_entity(%State{} = s, node_ref, key) do
    Counters.dispatch_finished(s.counters, Entity.identity(s.operative_id, node_ref, key))
    %{s | entities: Map.update(s.entities, node_ref, %{}, &Map.delete(&1, key))}
  end

  defp start_entity(%State{topology: topo} = s, node_ref, key) do
    module = topo.node_modules[node_ref]

    Entity.start(
      operative: self(),
      operative_id: s.operative_id,
      node_ref: node_ref,
      node_name: topo.node_name[node_ref],
      # The channel this Entity consumes from — what a park reports, matching
      # the reference SDK. v1 has no merge, so one source per node.
      input_channel: inbound_channel(topo, node_ref),
      module: module,
      entity_key: key,
      capacity: Procedure.mailbox_capacity(module),
      policy: knob(topo, node_ref, :on_full, :drop_nothing),
      retry_max: knob(topo, node_ref, :retry_max, @default_retry_max),
      retry_backoff_ms: knob(topo, node_ref, :retry_backoff_ms, Knob.ladder(:sdk_retry).base),
      counters: s.counters,
      hold_cap: s.hold_cap,
      secondaries: s.secondaries,
      slots: s.slots,
      credit: s.credit,
      persistence_enabled: topo.persistence_enabled?,
      tier: tier_facts(topo, node_ref),
      ctx_opts: ctx_opts(topo, node_ref, key)
    )
  end

  # What an Entity of this node needs to know about the tier: whether it is on,
  # the checkpoint policy, the sink routes it captures its own outputs through
  # — the router delivers a tier class's emits to nodes only — and whether it
  # has an inter-procedure edge at all.
  defp tier_facts(topo, node_ref) do
    routes = Map.get(topo.routes, {:node, node_ref}, [])

    %{
      on?: topo.effectively_once?,
      checkpoint: topo.checkpoint,
      sink_routes: for(%{to: {:sink, _}} = r <- routes, do: r),
      edges?: Enum.any?(routes, &match?(%{to: {:node, _}}, &1)),
      correction: topo.correction
    }
  end

  # The facts the ctx refuses on: this node's channel name, whether an output
  # reaches a keyed sink, and whether such a sink leaves a keyless Entity no
  # key to fall back on.
  defp ctx_opts(topo, node_ref, key) do
    keyed =
      for %{to: {:sink, _}, keyed: true} = r <- Map.get(topo.routes, {:node, node_ref}, []), do: r

    [
      channel: topo.node_channel[node_ref],
      keyed_sink?: keyed != [],
      key_required?: key == nil and Enum.any?(keyed, &is_nil(&1.key_by))
    ]
  end

  # The key an Entity is registered under for its node's mode: none for a
  # Single node, a declared and owned one for a Static node, any owned key for
  # a Dynamic node.
  defp slot_key(%State{topology: topo} = s, node_ref, key) do
    case Map.fetch!(topo.node_mode, node_ref) do
      :single when key in [nil, ""] ->
        {:ok, nil}

      :static ->
        if key in Procedure.entity_keys(topo.node_modules[node_ref]) and
             KeyGroup.owns?(s.range, key),
           do: {:ok, key},
           else: {:error, :entity_not_found}

      :dynamic when is_binary(key) ->
        if KeyGroup.owns?(s.range, key), do: {:ok, key}, else: {:error, :entity_not_found}

      _ ->
        {:error, :entity_not_found}
    end
  end

  # --- parking (entry-parse + partition mis-route) ---

  # The channel a node consumes from — an entry channel by its own name, an
  # inter-procedure edge by the producing node's output name. A static
  # topology fact: v1 has no merge, so a node has one source.
  defp inbound_channel(topo, node_ref) do
    Enum.find_value(topo.routes, fn {source, routes} ->
      if Enum.any?(routes, &(&1.to == {:node, node_ref})) do
        case source do
          {:entry, channel} -> channel
          {:node, producer} -> topo.node_name[producer] || inspect(producer)
        end
      end
    end)
  end

  defp park_entry(%State{} = s, channel, message, reason),
    do: park_pre_dispatch(s, channel, channel, "entry parse failed: #{inspect(reason)}", message)

  defp park_unresolved(%State{} = s, requested, message) do
    park_pre_dispatch(
      s,
      @unresolved_procedure_id,
      requested,
      unresolved_entry_error(s.topology, requested),
      message
    )
  end

  defp park_misroute(%State{} = s, node_ref, value, reason) do
    label = node_ref_label(s, node_ref)
    park_pre_dispatch(s, label, label, reason, value)
  end

  # A pre-dispatch park (entry parse, unresolved channel, partition mis-route),
  # forwarded to the runner which owns the ack loop. The park_id uses a fresh
  # unique integer since no Entity seq exists yet.
  #
  # `procedure_id` is the identity slot, which the controller turns into a
  # DIRECTORY under the dead-letter root, so it is always a value this side
  # chose. `channel` is the row's attribution and may be a name off the wire.
  defp park_pre_dispatch(%State{} = s, procedure_id, channel, error, payload) do
    park_id =
      "#{s.operative_id}/#{procedure_id}##{System.unique_integer([:positive, :monotonic])}"

    Park.ship(
      s.runner,
      self(),
      Park.identity(s.operative_id, procedure_id, ""),
      Park.body(park_id, channel, error, 1, payload)
    )

    s
  end

  # --- helpers ---

  defp parse(topo, channel, message) do
    case Map.get(topo.entries, channel) do
      %{parser: parser} -> safe_parse(parser, message)
      nil -> {:ok, message}
    end
  end

  defp safe_parse(parser, message) do
    parser.(message)
  rescue
    e -> {:error, Exception.message(e)}
  end

  # A name the operative declares resolves to it; a name it does not is
  # unresolved, and parks under the name it asked for rather than reaching
  # whichever entry channel happens to be first. A frame naming no channel
  # takes the first DECLARED entry — the source-plugin default, and the order
  # `entry_order` keeps, because `entries` is a map and its key order is the
  # term order, which is not the author's — and is unresolved only when there
  # is none to take.
  #
  # The answer is asymmetric on purpose: this runs once per input frame, so
  # the resolved arm returns the name itself and allocates nothing, and only
  # the cold arm builds a tuple. A declared name is a binary and can never
  # match `{:unresolved, _}`.
  defp resolve_channel(topo, channel) when is_binary(channel) do
    if Map.has_key?(topo.entries, channel), do: channel, else: {:unresolved, channel}
  end

  defp resolve_channel(topo, _channel) do
    case topo.entry_order do
      [first | _] -> first
      [] -> {:unresolved, ""}
    end
  end

  # Why the frame did not enter: the name it asked for against the set the
  # operative declares, or that it declares none at all. `flz_core`'s
  # `unresolved_entry_error` answers in the same words, over the same sorted
  # set, so an operator reading a DLQ row cannot tell which core wrote it.
  defp unresolved_entry_error(topo, requested) do
    case Enum.sort(topo.entry_order) do
      [] ->
        "the operative declares no entry channel"

      declared ->
        "no entry channel `#{requested}`; the operative declares: #{Enum.join(declared, ", ")}"
    end
  end

  # Sink outputs ride a secondary stream directly (no router hop). The controller
  # routes io_out by payload-id = operative id, so any secondary carries it.
  # An output no stream will take is held for the next bound one rather than
  # lost: the caller cannot learn of a send-side loss, so nothing here may
  # discard a frame in silence.
  # A client stream signals congestion by withholding its write's reply, so the
  # buffer never turns a frame away here; what does reach the class's held arm
  # is a transport that is gone. The runner's close detection reconnects, and
  # the rebind replays what was held.
  defp emit_output(%State{operative_id: oid} = s, meta, bytes) do
    {_verdict, held} = Hold.write({:output, oid}, wire(s, s.held), {bytes, meta})
    %{s | held: held}
  end

  # The hold, on the streams bound right now. `secondaries` is the one source
  # of truth for what is bound, so no hold can hold a stale stream.
  defp wire(%State{secondaries: secondaries}, hold), do: Hold.bind(hold, secondaries)

  defp knob(topo, node_ref, key, default),
    do: get_in(topo.node_knobs, [node_ref, key]) || default

  defp node_ref_label(%State{topology: topo}, node_ref),
    do: topo.node_name[node_ref] || inspect(node_ref)
end
