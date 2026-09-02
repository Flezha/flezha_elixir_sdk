defmodule Flezha.SDK.RunnerServer do
  @moduledoc """
  One GenServer per started runner (a process-wide singleton — the slot is
  claimed by its registered name). Owns the connection lifecycle through the
  client-transport seam: the handshake (namespace, factories, stable id),
  `AssignRunnerId`, `ConnectSocket`, per-command dispatch, the control keepalive,
  data heartbeats, `UpdateFactories` on post-start set changes, the park-ack
  loop, and reconnect-with-the-same-stable-id (a binding swap; dead tags 20/21
  are not implemented). Persistence-family commands are answered with a safe
  empty success and a warn; a `"workflow"` `AddOperative` is refused.
  """

  use GenServer
  require Logger

  alias Flezha.SDK.{
    Codec,
    Counters,
    DataStream,
    Entity,
    EntryIdle,
    EntryInput,
    Jitter,
    KeyGroup,
    Operative,
    Park,
    PersistentId,
    Transport
  }

  alias Flezha.Transport.Server, as: TransportServer

  @name __MODULE__
  @hb_default_ms 10_000
  @control_keepalive_ms 15_000
  @park_resend_ms 2_000
  @park_warn_every 5
  @reconnect_cap_ms 30_000
  # Exit status of a terminally-disconnected runner. Any non-zero value makes
  # the orchestrator restart the process; a distinct one lets an operator tell
  # "the controller proved me wedged" apart from a crash. Matches
  # `flz_core::shutdown::TERMINAL_EXIT_CODE`.
  @terminal_exit_status 70

  defmodule State do
    @moduledoc false
    defstruct [
      :namespace,
      :transport,
      :stable_id,
      :registry,
      :counters,
      :payload,
      session_token: nil,
      control_handle: nil,
      conn: nil,
      topologies: %{},
      factories: [],
      primaries: [],
      secondaries: [],
      operatives: %{},
      parks: %{},
      idle: nil,
      reconnect_attempt: 0,
      disconnecting?: false
    ]
  end

  # --- API ---

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: @name)

  def whereis, do: Process.whereis(@name)

  @doc "Post-start declared-set mutations (ships UpdateFactories)."
  def define(server, module, topology), do: GenServer.call(server, {:define, module, topology})
  def remove(server, module), do: GenServer.call(server, {:remove, module})
  def namespace(server), do: GenServer.call(server, :namespace)

  @doc "Register an idle observer on the live runner (see `Flezha.SDK.on_entry_idle/4`)."
  def on_entry_idle(server, channel, threshold_ms, observer),
    do: GenServer.call(server, {:on_entry_idle, channel, threshold_ms, observer})

  # --- server ---

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    namespace = Keyword.fetch!(opts, :namespace)
    transport = Keyword.fetch!(opts, :transport)
    topologies = Keyword.fetch!(opts, :topologies)
    factories = Keyword.fetch!(opts, :factories)
    idle_observers = Keyword.get(opts, :idle_observers, [])

    if transport == :native, do: TransportServer.ensure_started(:native)

    stable_id = PersistentId.stable_id(namespace, Enum.map(factories, &elem(&1, 0)))

    state = %State{
      namespace: namespace,
      transport: transport,
      stable_id: stable_id,
      registry: :ets.new(:flezha_sdk_operatives, [:public, :set, {:read_concurrency, true}]),
      counters: Counters.new(),
      topologies: topologies,
      factories: factories,
      payload: handshake_payload(namespace, factories, stable_id)
    }

    case do_connect(state) do
      {:ok, state} ->
        schedule_heartbeat()
        schedule_keepalive()

        {:ok,
         Enum.reduce(idle_observers, state, fn {channel, threshold_ms, observer}, acc ->
           register_idle(acc, channel, threshold_ms, observer)
         end)}

      {:error, reason} ->
        # The one diagnosis, before this runner gives up: which of the four
        # situations the socket directory is in and what fixes it — the same
        # text a Rust runner logs and `flz runner check` prints.
        Logger.error(
          "[Flezha.SDK] cannot reach a controller: #{inspect(reason)}. " <>
            Transport.diagnosis(state.transport)
        )

        # The VM is about to come down with this process, and Logger is async:
        # without the flush the one line that explains the failure is dropped
        # on the way out, which is exactly the silence this diagnosis replaces.
        Logger.flush()

        {:stop, {:connect_failed, reason}}
    end
  end

  @impl true
  def handle_call({:define, _module, topology}, _from, %State{} = s) do
    fid = topology.factory_id

    if Map.has_key?(s.topologies, fid) do
      {:reply, {:error, {:duplicate_factory_id, fid}}, s}
    else
      s = %{
        s
        | topologies: Map.put(s.topologies, fid, topology),
          factories: s.factories ++ [{fid, nil}]
      }

      ship_update_factories(s)
      {:reply, :ok, s}
    end
  end

  def handle_call({:remove, module}, _from, %State{} = s) do
    fid = module.factory_id()
    live = Enum.count(s.operatives, fn {_oid, {_pid, f}} -> f == fid end)

    cond do
      not Map.has_key?(s.topologies, fid) ->
        {:reply, {:error, {:not_defined, fid}}, s}

      live > 0 ->
        {:reply, {:error, {:operative_type_in_use, fid, live}}, s}

      true ->
        s = %{
          s
          | topologies: Map.delete(s.topologies, fid),
            factories: Enum.reject(s.factories, fn {f, _} -> f == fid end)
        }

        ship_update_factories(s)
        {:reply, :ok, s}
    end
  end

  def handle_call(:namespace, _from, %State{} = s), do: {:reply, s.namespace, s}

  def handle_call({:on_entry_idle, channel, threshold_ms, observer}, _from, %State{} = s),
    do: {:reply, :ok, register_idle(s, channel, threshold_ms, observer)}

  # --- control commands ---

  @impl true
  def handle_info({:flz_data, _id, "control_command", bin, _meta}, %State{} = s) do
    case Codec.decode_command(bin) do
      {:ok, %{reference_id: ref, command: command}} ->
        dispatch(command, ref, s)

      {:error, reason} ->
        Logger.warning("[Flezha.SDK] bad control command: #{inspect(reason)}")
        {:noreply, s}
    end
  end

  # The control keepalive echo from the controller — liveness only, never re-echoed.
  def handle_info({:flz_data, _id, "control_ping", _body, _meta}, %State{} = s), do: {:noreply, s}

  def handle_info({:flz_data, _id, _payload_id, _msg, _meta}, %State{} = s), do: {:noreply, s}

  # --- liveness timers ---

  def handle_info(:data_heartbeat, %State{} = s) do
    # Handout liveness, in lockstep with the other SDKs: one wedged Entity is
    # invisible to `dispatches` while the rest of the runner keeps completing.
    {inflight, oldest_ms} = Counters.handout_liveness(s.counters)
    # Backpressure that withholds work rather than shedding it: an Entity
    # holding a push it has not queued yet holds no handout either, so
    # `inflight` cannot see it.
    {pending, pending_oldest_ms} = Counters.pending_push_liveness(s.counters)

    meta = %{
      "dispatches" => Integer.to_string(Counters.dispatches(s.counters)),
      "blocked_parks" => Integer.to_string(Counters.blocked_parks(s.counters)),
      "drops_newest" => Integer.to_string(Counters.drops_newest(s.counters)),
      "drops_oldest" => Integer.to_string(Counters.drops_oldest(s.counters)),
      "inflight" => Integer.to_string(inflight),
      "inflight_oldest_ms" => Integer.to_string(oldest_ms),
      "pending_pushes" => Integer.to_string(pending),
      "pending_push_oldest_ms" => Integer.to_string(pending_oldest_ms)
    }

    Enum.each(s.secondaries, fn h ->
      Flezha.Transport.Stream.send_frame(h, "data_heartbeat", <<>>, meta)
    end)

    schedule_heartbeat()
    {:noreply, s}
  end

  def handle_info(:control_keepalive, %State{control_handle: nil} = s) do
    schedule_keepalive()
    {:noreply, s}
  end

  def handle_info(:control_keepalive, %State{} = s) do
    Flezha.Transport.Stream.send_frame(s.control_handle, "control_ping", <<>>, %{})
    schedule_keepalive()
    {:noreply, s}
  end

  # --- park resend loop ---

  def handle_info({:park_resend, park_id}, %State{} = s) do
    case Map.get(s.parks, park_id) do
      nil ->
        {:noreply, s}

      %{identity: identity, body: body, resends: n} = park ->
        send_park_frame(s, identity, body)

        if rem(n + 1, @park_warn_every) == 0,
          do: Logger.warning("[Flezha.SDK] park #{park_id} unacked after #{n + 1} resends")

        ref = Process.send_after(self(), {:park_resend, park_id}, @park_resend_ms)
        {:noreply, %{s | parks: Map.put(s.parks, park_id, %{park | resends: n + 1, timer: ref})}}
    end
  end

  # --- closes → reconnect ---

  def handle_info({:flz_closed, _id, reason}, %State{disconnecting?: true} = s) do
    Logger.debug("[Flezha.SDK] stream closed during disconnect: #{reason}")
    {:noreply, s}
  end

  def handle_info({:flz_closed, _id, reason}, %State{} = s), do: reconnect(s, reason)
  def handle_info({:data_stream_closed, _pid, reason}, %State{} = s), do: reconnect(s, reason)

  def handle_info({:EXIT, _pid, _reason}, %State{} = s), do: {:noreply, s}
  def handle_info(_msg, %State{} = s), do: {:noreply, s}

  @impl true
  def handle_cast({:park, entity_pid, identity, body}, %State{} = s) do
    send_park_frame(s, identity, body)
    ref = Process.send_after(self(), {:park_resend, body.park_id}, @park_resend_ms)

    park = %{entity: entity_pid, identity: identity, body: body, resends: 0, timer: ref}
    {:noreply, %{s | parks: Map.put(s.parks, body.park_id, park)}}
  end

  # --- command dispatch ---

  defp dispatch({:assign_runner_id, %{runner_id: rid}}, ref, s) do
    reply(s, ref, "success")
    {:noreply, %{s | session_token: rid}}
  end

  defp dispatch({:connect_socket, %{sockets_count: n}}, ref, s) do
    case establish_sockets(s, n) do
      {:ok, s} ->
        reply(s, ref, "success")
        {:noreply, s}

      {:error, reason} ->
        Logger.error("[Flezha.SDK] ConnectSocket failed: #{inspect(reason)}")
        reply(s, ref, "error", [inspect(reason)])
        {:noreply, s}
    end
  end

  defp dispatch({:add_operative, add}, ref, s) do
    case add_operative(s, add) do
      {:ok, s, metadata} ->
        reply(s, ref, "success", [], Codec.encode_add_operative_response(metadata))
        {:noreply, s}

      {:error, reason, s} ->
        reply(s, ref, "error", [reason])
        {:noreply, s}
    end
  end

  defp dispatch({:rem_operative, %{operative_id: oid}}, ref, s) do
    reply_ok(teardown_operative(s, oid), ref)
  end

  defp dispatch({:ping, %{nonce: nonce}}, ref, s) do
    reply(s, ref, "success", [], Codec.encode_ping_response(nonce))
    {:noreply, s}
  end

  defp dispatch({:workflow_stop, %{operative_id: oid}}, ref, s) do
    Logger.warning("[Flezha.SDK] WorkflowStop for #{oid} — this runner advertises no workflow")
    reply_ok(s, ref)
  end

  # A terminal disconnect is the rung of the controller's convergence ladder
  # that ends a wedged process: nothing else clears it, and only the
  # orchestrator brings it back — for a FAILURE, never for a clean stop, hence
  # the non-zero status. The ack goes out first: the controller must see it
  # before the runtime starts stopping.
  defp dispatch({:runner_disconnect, %{terminal: true} = cmd}, ref, s) do
    reply(s, ref, "success")

    Logger.error(
      "[Flezha.SDK] controller ordered a terminal disconnect " <>
        "(reason=#{inspect(Map.get(cmd, :reason, ""))})"
    )

    if s.transport == :native do
      # In-node mode: this SDK shares the controller's VM, so halting it would
      # take the controller down with it. Stop the runner and leave the node's
      # lifecycle to whoever owns the controller.
      Logger.error("[Flezha.SDK] in-node runner: stopping the runner instead of the VM")
      {:stop, :normal, %{s | disconnecting?: true}}
    else
      # `System.stop/1` runs the application shutdown — this server, its
      # Operatives and their Entities — and then ends the VM with the status.
      # The server stays up until then: a stopped one is restarted by its
      # supervisor and re-registers the process the controller just ended.
      System.stop(@terminal_exit_status)
      {:noreply, %{s | disconnecting?: true}}
    end
  end

  defp dispatch({:runner_disconnect, _}, ref, s) do
    reply(s, ref, "success")
    {:stop, :normal, %{s | disconnecting?: true}}
  end

  defp dispatch({:park_payload_ack, %{park_id: park_id}}, _ref, s) do
    {:noreply, release_park(s, park_id)}
  end

  # Persistence-family and dead tags: a persistence-free runner never triggers
  # these, but answer each safely rather than crash.
  defp dispatch({tag, _payload}, ref, s)
       when tag in [
              :entity_state,
              :entity_apply_state,
              :restore_init_state,
              :compact_entity,
              :migration_serialization,
              :migration_deserialization,
              :entity_mailbox_status,
              :credit_window_init,
              :credit_grant
            ] do
    Logger.warning("[Flezha.SDK] ignoring #{tag} — persistence is deferred for this runner")
    reply_ok(s, ref)
  end

  defp dispatch(other, ref, s) do
    Logger.debug("[Flezha.SDK] unhandled control command: #{inspect(other)}")
    reply_ok(s, ref)
  end

  # --- operative lifecycle ---

  defp add_operative(%State{} = s, add) do
    kind = if add.kind in [nil, ""], do: "stream", else: add.kind

    range =
      KeyGroup.decode(
        add.partition_index || 0,
        add.partition_count || 0,
        add.key_group_from || 0,
        add.key_group_to || 0
      )

    topo = Map.get(s.topologies, add.factory_id)

    cond do
      kind == "workflow" ->
        {:error, "workflow kind is not supported by the Elixir stream SDK", s}

      is_nil(topo) ->
        {:error, "unknown factory #{add.factory_id}", s}

      KeyGroup.partitioned?(range) and topo.keyless? ->
        {:error, "PartitionsWithoutKeyedProcedure: a partition_count >= 2 class needs a key_by",
         s}

      true ->
        {:ok, op_pid} =
          Operative.start_link(
            operative_id: add.operative_id,
            topology: topo,
            cells: EntryInput.cells_for(topo.entries),
            range: range,
            counters: s.counters,
            runner: self(),
            secondaries: s.secondaries
          )

        :ets.insert(s.registry, {add.operative_id, op_pid})
        metadata = if topo.keyless?, do: %{"keyed" => "false"}, else: %{}

        {:ok,
         %{s | operatives: Map.put(s.operatives, add.operative_id, {op_pid, add.factory_id})},
         metadata}
    end
  end

  defp teardown_operative(%State{} = s, oid) do
    case Map.pop(s.operatives, oid) do
      {nil, _} ->
        s

      {{pid, _fid}, operatives} ->
        :ets.delete(s.registry, oid)
        if Process.alive?(pid), do: Operative.stop(pid)
        %{s | operatives: operatives}
    end
  end

  # --- sockets ---

  defp establish_sockets(%State{} = s, count) do
    s = teardown_streams(s)

    case Transport.connect_sockets(s.transport, s.session_token, count, self(), s.conn) do
      {:ok, primaries, secondaries} ->
        # Own the secondaries for close detection; primaries get reader processes.
        Enum.each(secondaries, fn h ->
          Flezha.Transport.Stream.controlling_process(h, self())
          Flezha.Transport.Stream.set_active(h)
        end)

        readers =
          Enum.map(primaries, fn h ->
            {:ok, pid} =
              DataStream.start_link(handle: h, registry: s.registry, runner: self())

            pid
          end)

        # Push the fresh secondaries to any operatives that survived a reconnect.
        Enum.each(s.operatives, fn {_oid, {pid, _f}} ->
          if Process.alive?(pid), do: send(pid, {:secondaries, secondaries})
        end)

        {:ok, %{s | primaries: readers, secondaries: secondaries}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp teardown_streams(%State{} = s) do
    Enum.each(s.primaries, fn pid -> if Process.alive?(pid), do: GenServer.stop(pid, :normal) end)
    Enum.each(s.secondaries, &Flezha.Transport.Stream.close/1)
    %{s | primaries: [], secondaries: []}
  end

  # --- parking ---

  defp send_park_frame(%State{secondaries: []}, _identity, _body), do: :ok

  defp send_park_frame(%State{secondaries: secondaries}, identity, body) do
    handle = Enum.at(secondaries, rem(:erlang.phash2(body.park_id), length(secondaries)))
    frame = Park.frame(identity, 0, body)
    Flezha.Transport.Stream.send_frame(handle, "park_payload", frame, %{})
  end

  defp release_park(%State{} = s, park_id) do
    case Map.pop(s.parks, park_id) do
      {nil, _} ->
        s

      {%{entity: entity, timer: timer}, parks} ->
        if timer, do: Process.cancel_timer(timer)
        if Process.alive?(entity), do: Entity.release_park(entity, park_id)
        %{s | parks: parks}
    end
  end

  # --- reconnect ---

  defp reconnect(%State{reconnect_attempt: n} = s, reason) do
    Logger.warning("[Flezha.SDK] connection lost (#{reason}); reconnecting (attempt #{n + 1})")
    s = teardown_streams(s)
    Process.sleep(reconnect_backoff(n))

    case do_connect(%{s | reconnect_attempt: n + 1, control_handle: nil, conn: nil}) do
      {:ok, s} -> {:noreply, s}
      {:error, r} -> reconnect(s, "retry: #{inspect(r)}")
    end
  end

  defp reconnect_backoff(n) do
    Jitter.jitter(min(100 * Bitwise.bsl(1, min(n, 8)), @reconnect_cap_ms))
  end

  # --- connect + helpers ---

  defp do_connect(%State{} = s) do
    case Transport.connect(s.transport, s.payload, self()) do
      {:ok, control_handle, conn} ->
        {:ok, %{s | control_handle: control_handle, conn: conn, reconnect_attempt: 0}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handshake_payload(namespace, factories, stable_id) do
    %{
      "namespace" => namespace,
      "factory_ids" => Enum.map(factories, &elem(&1, 0)),
      "factories" =>
        Enum.map(factories, fn {id, desc} ->
          %{"id" => id, "description" => desc, "sdk_lang" => Codec.sdk_lang()}
        end),
      "stable_id" => stable_id
    }
  end

  defp ship_update_factories(%State{control_handle: nil}), do: :ok

  defp ship_update_factories(%State{} = s) do
    ref = "uf-" <> Integer.to_string(System.unique_integer([:positive]))
    command = Codec.update_factories_command(s.factories)
    bin = Codec.encode_request(ref, command)
    Flezha.Transport.Stream.send_frame(s.control_handle, "control_command", bin, %{})
  end

  defp reply_ok(s, ref) do
    reply(s, ref, "success")
    {:noreply, s}
  end

  defp reply(s, ref, status, errors \\ [], raw \\ <<>>)

  defp reply(%State{control_handle: nil}, _ref, _status, _errors, _raw), do: :ok

  defp reply(%State{} = s, ref, status, errors, raw) do
    bin = Codec.encode_response(ref, status, errors, raw)
    Flezha.Transport.Stream.send_frame(s.control_handle, "control_command", bin, %{})
  end

  # The idle process starts with the first registration and is linked here, so
  # it ends with the runner. Interning happens in this process, one at a time.
  defp register_idle(%State{} = s, channel, threshold_ms, observer) do
    s =
      case s.idle do
        nil ->
          {:ok, pid} = EntryIdle.start_link()
          %{s | idle: pid}

        _pid ->
          s
      end

    :ok =
      EntryIdle.register(s.idle, channel, EntryInput.cell_for(channel), threshold_ms, observer)

    s
  end

  defp schedule_heartbeat do
    base = String.to_integer(System.get_env("FLEZHA_DATA_HEARTBEAT_MS") || "#{@hb_default_ms}")
    # Jitter into [base/2, base] (the cross-SDK heartbeat contract).
    Process.send_after(self(), :data_heartbeat, Jitter.jitter_within(base))
  end

  defp schedule_keepalive,
    do: Process.send_after(self(), :control_keepalive, @control_keepalive_ms)
end
