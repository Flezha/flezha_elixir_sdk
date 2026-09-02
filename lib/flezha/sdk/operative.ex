defmodule Flezha.SDK.Operative do
  @moduledoc """
  One process per operative instance: the routing core plus per-Entity
  supervision. An input on an entry channel (parsed, a parse failure riding the
  entry ladder) fans out along that entry's routes; a value an Entity emits fans
  out along its node's routes. Each route applies at most one fused transform and
  one key, then delivers to the owning Entity (Single/Static/Dynamic) or a sink.
  The partition guard admits a dynamic key before any Entity state exists and
  filters static keys by ownership. Sink outputs and park frames leave on the
  runner's secondary streams. Routing is serialized here (per-Entity ordering);
  Entities dispatch concurrently.
  """

  use GenServer
  require Logger

  alias Flezha.SDK.{Channels, Counters, Entity, EntryInput, KeyGroup, Park, Procedure}

  @default_retry_max 3
  @default_retry_backoff_ms 50

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
          {:ok, pid()} | {:error, :procedure_not_found | :entity_not_found}
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
      slots: :ets.new(:flezha_sdk_slots, [:public, :set])
    }

    {:ok, prewarm(state)}
  end

  @impl true
  def handle_call({:lookup, procedure, key, materialise?}, _from, %State{topology: topo} = s) do
    with {:ok, node_ref} <- node_of(topo, procedure),
         {:ok, slot_key} <- slot_key(s, node_ref, key) do
      case {Map.get(s.entities, {node_ref, slot_key}), materialise?} do
        {nil, false} ->
          {:reply, {:error, :entity_not_found}, s}

        {nil, true} ->
          {pid, s} = ensure_entity(s, node_ref, slot_key)
          {:reply, {:ok, pid}, s}

        {pid, _} ->
          {:reply, {:ok, pid}, s}
      end
    else
      {:error, _} = error -> {:reply, error, s}
    end
  end

  def handle_call(:entities, _from, %State{topology: topo} = s) do
    listing =
      Enum.map(topo.order, fn node_ref ->
        pids = for {{^node_ref, key}, pid} <- s.entities, do: {key, pid}
        {topo.node_modules[node_ref].name(), topo.node_mode[node_ref], Enum.sort(pids)}
      end)

    {:reply, listing, s}
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
  def handle_cast({:input, channel, message, metadata}, %State{} = s) do
    channel = resolve_channel(s.topology, channel)
    # The frame counts before it can be declined: the stamp precedes the parse.
    EntryInput.stamp(Map.fetch!(s.cells, channel))

    case parse(s.topology, channel, message) do
      {:ok, value} ->
        {:noreply, route_from(s, {:entry, channel}, value, metadata)}

      {:error, reason} ->
        {:noreply, park_entry(s, channel, message, reason)}
    end
  end

  @impl true
  def handle_cast({:emitted, node_ref, _entity_key, values}, %State{} = s) do
    s =
      Enum.reduce(values, s, fn value, acc -> route_from(acc, {:node, node_ref}, value, %{}) end)

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
    dead = for {ref, ^pid} <- s.entities, do: ref

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
    Enum.each(s.entities, fn {_ref, pid} -> send(pid, {:secondaries, secondaries}) end)
    {:noreply, %{s | secondaries: secondaries}}
  end

  def handle_info(_msg, s), do: {:noreply, s}

  # --- routing ---

  defp route_from(%State{topology: topo} = s, source, value, _metadata) do
    routes = Map.get(topo.routes, source, [])

    Enum.reduce(routes, s, fn route, acc ->
      outputs = route.transform.(value)

      Enum.reduce(outputs, acc, fn out, a ->
        key = if route.key_by, do: route.key_by.(out), else: nil
        deliver(a, route.to, out, key)
      end)
    end)
  end

  defp deliver(%State{} = s, {:sink, plugin}, value, _key) do
    serializer = Map.get(s.topology.sink_serializers, plugin, &Channels.default_serialize/1)
    bytes = serializer.(value)
    emit_output(s, %{"channel" => plugin}, bytes)
    s
  end

  defp deliver(%State{topology: topo} = s, {:node, node_ref}, value, key) do
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
    {pid, s} = ensure_entity(s, node_ref, key)
    policy = knob(s.topology, node_ref, :on_full, :drop_nothing)

    try do
      case policy do
        :drop_nothing -> Entity.enqueue_block(pid, value)
        other -> Entity.enqueue_drop(pid, value, other)
      end

      s
    catch
      :exit, _reason when retry? ->
        enqueue(forget_entity(s, node_ref, key), node_ref, key, value, false)
    end
  end

  defp ensure_entity(%State{} = s, node_ref, key) do
    case Map.get(s.entities, {node_ref, key}) do
      nil ->
        {:ok, pid} = start_entity(s, node_ref, key)
        {pid, %{s | entities: Map.put(s.entities, {node_ref, key}, pid)}}

      pid ->
        {pid, s}
    end
  end

  # Drop a dead Entity and release its in-flight slot. Idempotent with the
  # `:EXIT` clause, which does the same for the exit signal that follows.
  defp forget_entity(%State{} = s, node_ref, key) do
    Counters.dispatch_finished(s.counters, Entity.identity(s.operative_id, node_ref, key))
    %{s | entities: Map.delete(s.entities, {node_ref, key})}
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
      persistence_enabled: topo.persistence_enabled?
    )
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
