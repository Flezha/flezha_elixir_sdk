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
  existed".
  """

  use GenServer
  require Logger

  alias Flezha.SDK.{Channels, Counters, Entity, EntryInput, KeyGroup, Park, Procedure}

  @default_retry_max 3
  @default_retry_backoff_ms 50
  # What an entry input carries into routing: no metadata, no key, no Entity.
  @entry_emit %{meta: %{}, key: nil, origin: nil}
  # How long a sweep waits on one Entity's idle probe before moving on.
  @idle_probe_ms 50
  # `FLEZHA_REAP_INTERVAL_MS`, else a quarter of the shortest TTL, at most this.
  @reap_interval_env "FLEZHA_REAP_INTERVAL_MS"
  @max_reap_interval_ms 30_000
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
      :runner,
      :secondaries,
      :slots,
      :credit,
      cells: %{},
      entities: %{},
      dynamic: %{},
      evicted: {:queue.new(), 0},
      monitors: %{}
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
          | {:error, :procedure_not_found | :entity_not_found | :entity_evicted | :entity_capped}
  def lookup(pid, procedure, key, opts \\ []),
    do: GenServer.call(pid, {:lookup, procedure, key, Keyword.get(opts, :materialise, false)})

  @doc "Every node as `{procedure_name, mode, [{key, entity_pid}]}` — the live-read walk's input."
  @spec entities(GenServer.server()) :: [
          {String.t(), :single | :static | :dynamic, [{term(), pid()}]}
        ]
  def entities(pid), do: GenServer.call(pid, :entities)

  # --- server ---

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    topology = Keyword.fetch!(opts, :topology)

    state = %State{
      operative_id: Keyword.fetch!(opts, :operative_id),
      topology: topology,
      # The entry cells, resolved once: the RunnerServer interns them before
      # starting this process, so the delivery path below is one atomic store.
      cells: Keyword.get_lazy(opts, :cells, fn -> EntryInput.cells_for(topology.entries) end),
      range: Keyword.fetch!(opts, :range),
      counters: Keyword.fetch!(opts, :counters),
      runner: Keyword.fetch!(opts, :runner),
      secondaries: Keyword.fetch!(opts, :secondaries),
      credit: Keyword.get(opts, :credit),
      # The slot survives the Entity: what a persistent Entity last shipped,
      # for the replacement of one killed untrappably.
      slots: :ets.new(:flezha_sdk_slots, [:public, :set]),
      dynamic: dynamic_knobs(topology)
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

  defp reap_interval(ttl_ms) do
    with val when is_binary(val) <- System.get_env(@reap_interval_env),
         {ms, _} <- Integer.parse(val) do
      max(ms, 1)
    else
      _ -> ttl_ms |> div(4) |> min(@max_reap_interval_ms) |> max(1)
    end
  end

  @impl true
  def handle_call({:lookup, procedure, key, materialise?}, _from, %State{topology: topo} = s) do
    with {:ok, node_ref} <- node_of(topo, procedure),
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
  defp missing(%State{topology: topo, evicted: {trace, _n}}, node_ref, key) do
    if topo.node_mode[node_ref] == :dynamic and :queue.member(key, trace),
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

  @impl true
  def handle_cast({:input, channel, message, _metadata}, %State{} = s) do
    channel = resolve_channel(s.topology, channel)
    # The frame counts before it can be declined: the stamp precedes the parse.
    EntryInput.stamp(Map.fetch!(s.cells, channel))

    case parse(s.topology, channel, message) do
      {:ok, value} ->
        {s, _ordinal} = route_from(s, {:entry, channel}, value, @entry_emit, 0)
        {:noreply, s}

      {:error, reason} ->
        {:noreply, park_entry(s, channel, message, reason)}
    end
  end

  # One invocation's emits: the ordinal counts its plugin outputs from zero.
  @impl true
  def handle_cast({:emitted, node_ref, entity_key, emits, handout_seq}, %State{} = s) do
    origin = %{node_ref: node_ref, entity_key: entity_key, seq: handout_seq}

    {s, _ordinal} =
      Enum.reduce(emits, {s, 0}, fn {value, meta, key}, {acc, ordinal} ->
        route_from(
          acc,
          {:node, node_ref},
          value,
          %{meta: meta, key: key, origin: origin},
          ordinal
        )
      end)

    {:noreply, s}
  end

  @impl true
  def handle_cast({:park, entity_pid, identity, body}, %State{} = s) do
    Park.ship(s.runner, entity_pid, identity, body)
    {:noreply, s}
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, %State{} = s) do
    # An Entity died — forget it; it is recreated on that key's next input. A
    # crash must not take the Operative down with it, and must not leave the
    # key unroutable; the log is what makes a key crashing on every input
    # visible instead of looking like silent inactivity.
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
        forget_entity(acc, node_ref, key)
      end)

    {:noreply, s}
  end

  def handle_info({:secondaries, secondaries}, %State{} = s) do
    for {_ref, keyed} <- s.entities,
        {_key, pid} <- keyed,
        do: send(pid, {:secondaries, secondaries})

    {:noreply, %{s | secondaries: secondaries}}
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
        if Entity.idle_for?(pid, ttl_ms, @idle_probe_ms) and Entity.evict(pid) == :evicted,
          do: {note_evicted(forget_entity(acc, node_ref, key), key), left - 1},
          else: {acc, left}
    end)
  end

  defp note_evicted(%State{evicted: {trace, n}} = s, key) do
    dynamic_event(s, :evicted)
    {trace, n} = if n == @evicted_trace_cap, do: {:queue.drop(trace), n - 1}, else: {trace, n}
    %{s | evicted: {:queue.in(key, trace), n + 1}}
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

  # A sink frame: the producing node's channel name over the user's metadata,
  # then — on a keyed sink — the identity stamps, which nothing can clobber.
  # Every plugin output advances the ordinal, stamped or not.
  defp deliver(%State{} = s, %{to: {:sink, plugin}} = route, source, value, emit, ordinal) do
    serializer = Map.get(s.topology.sink_serializers, plugin, &Channels.default_serialize/1)
    metadata = emit.meta |> Map.delete("channel") |> Map.put("channel", channel_name(s, source))

    metadata =
      if route.keyed,
        do: stamp(metadata, s, emit.origin, ordinal, sink_key(route, value, emit)),
        else: metadata

    emit_output(s, metadata, serializer.(value))
    {s, ordinal + 1}
  end

  defp deliver(
         %State{} = s,
         %{to: {:node, node_ref}, key_by: key_by},
         _source,
         value,
         _emit,
         ordinal
       ) do
    key = if key_by, do: key_by.(value), else: nil
    {deliver_to_node(s, node_ref, value, key), ordinal}
  end

  # The emit's own key, else the sink route's, else the emitting Entity's —
  # the reference's `emit_keyed` then the entity-key default.
  defp sink_key(route, value, emit) do
    cond do
      emit.key -> emit.key
      route.key_by -> route.key_by.(value)
      emit.origin -> emit.origin.entity_key
      true -> nil
    end
  end

  defp stamp(metadata, %State{operative_id: oid, topology: topo}, origin, ordinal, key) do
    %{node_ref: node_ref, entity_key: entity_key, seq: seq} = origin

    metadata
    |> Map.put("flz.entity", "#{oid}/#{topo.node_modules[node_ref].name()}/#{entity_key || ""}")
    |> Map.put("flz.seq", "#{seq}.#{ordinal}")
    |> Map.put("flz.key", key)
  end

  defp channel_name(_s, {:entry, name}), do: name
  defp channel_name(%State{topology: topo}, {:node, node_ref}), do: topo.node_channel[node_ref]

  defp deliver_to_node(%State{topology: topo} = s, node_ref, value, key) do
    case Map.fetch!(topo.node_mode, node_ref) do
      :single ->
        enqueue(s, node_ref, nil, value)

      :static ->
        module = topo.node_modules[node_ref]

        Procedure.entities_for(module, value)
        |> Enum.filter(&KeyGroup.owns?(s.range, &1))
        |> Enum.reduce(s, fn k, acc -> enqueue(acc, node_ref, k, value) end)

      :dynamic ->
        case KeyGroup.admit(s.range, key || "") do
          :ok -> enqueue(s, node_ref, key, value)
          {:error, reason} -> park_misroute(s, node_ref, value, reason)
        end
    end
  end

  # A `:drop_nothing` enqueue is a `GenServer.call`, so an Entity that died between
  # the last input and this one takes the CALLER down — this Operative — unless
  # the exit is caught. The `:EXIT` clause cannot be relied on to have run first:
  # the input and the exit signal are two messages racing for the same mailbox.
  # Catching it here is what makes "the key is routable again on its next
  # input" true by construction rather than by ordering luck; the retry starts
  # a fresh Entity, which is what the `:EXIT` clause would have arranged.
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
        if knobs, do: dynamic_event(s, :created)
        {pid, %{s | entities: Map.put(s.entities, node_ref, Map.put(keyed, key, pid))}}
    end
  end

  # Drop a dead Entity and release its in-flight slot. Idempotent with the
  # `:EXIT` clause, which does the same for the exit signal that follows.
  defp forget_entity(%State{} = s, node_ref, key) do
    Counters.dispatch_finished(s.counters, Entity.identity(s.operative_id, node_ref, key))
    %{s | entities: Map.update(s.entities, node_ref, %{}, &Map.delete(&1, key))}
  end

  defp start_entity(%State{topology: topo} = s, node_ref, key) do
    module = topo.node_modules[node_ref]

    Entity.start_link(
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
      retry_backoff_ms: knob(topo, node_ref, :retry_backoff_ms, @default_retry_backoff_ms),
      counters: s.counters,
      secondaries: s.secondaries,
      slots: s.slots,
      credit: s.credit,
      persistence_enabled: topo.persistence_enabled?,
      ctx_opts: ctx_opts(topo, node_ref, key)
    )
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

  defp node_of(topo, procedure) do
    case Enum.find(topo.node_modules, fn {_ref, m} -> m.name() == procedure end) do
      {ref, _module} -> {:ok, ref}
      nil -> {:error, :procedure_not_found}
    end
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
    do: park_pre_dispatch(s, channel, "entry parse failed: #{inspect(reason)}", message)

  defp park_misroute(%State{} = s, node_ref, value, reason),
    do: park_pre_dispatch(s, node_ref_label(s, node_ref), reason, value)

  # A pre-dispatch park (entry-parse or partition mis-route), forwarded to the
  # runner which owns the ack loop. The park_id uses a fresh unique integer since
  # no Entity seq exists yet.
  defp park_pre_dispatch(%State{} = s, label, error, payload) do
    park_id = "#{s.operative_id}/#{label}##{System.unique_integer([:positive, :monotonic])}"

    Park.ship(
      s.runner,
      self(),
      Park.identity(s.operative_id, label, ""),
      Park.body(park_id, label, error, 1, payload)
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

  # An unknown/absent channel falls back to the first declared entry.
  defp resolve_channel(topo, channel) do
    if is_binary(channel) and Map.has_key?(topo.entries, channel),
      do: channel,
      else: topo.entries |> Map.keys() |> List.first()
  end

  # Sink outputs ride a secondary stream directly (no router hop). The controller
  # routes io_out by payload-id = operative id, so any secondary carries it.
  defp emit_output(%State{secondaries: []}, _meta, _bytes), do: :ok

  defp emit_output(%State{secondaries: secondaries, operative_id: oid}, meta, bytes) do
    handle = Enum.at(secondaries, rem(:erlang.phash2(oid), length(secondaries)))

    case Flezha.Transport.Stream.send_frame(handle, oid, bytes, meta) do
      :ok ->
        :ok

      # Discarding this answer is how a send-side loss becomes invisible. A
      # client stream signals congestion by withholding its write's reply, so
      # the buffer never turns a frame away here; what does reach this arm is a
      # transport that is gone, and the frame with it. The runner's close
      # detection reconnects — this is what says a frame did not make it.
      {:error, reason} ->
        Logger.warning(
          "[operative] output frame undelivered operative=#{oid} " <>
            "channel=#{inspect(meta["channel"])} reason=#{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  defp knob(topo, node_ref, key, default),
    do: get_in(topo.node_knobs, [node_ref, key]) || default

  defp node_ref_label(%State{topology: topo}, node_ref),
    do: topo.node_name[node_ref] || inspect(node_ref)
end
