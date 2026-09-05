defmodule Flezha.SDK.RunnerServer do
  @moduledoc """
  One GenServer per started runner (a process-wide singleton — the slot is
  claimed by its registered name). Owns the connection lifecycle through the
  client-transport seam: the handshake (namespace, factories, stable id),
  `AssignRunnerId`, `ConnectSocket`, per-command dispatch, the control keepalive,
  data heartbeats, `UpdateFactories` on post-start set changes, the park-ack
  loop, and reconnect-with-the-same-stable-id (a binding swap; dead tags 20/21
  are not implemented). A non-terminal `RunnerDisconnect` stops the runner in
  place — the reference's `Stopped`: the controller tears the session down,
  nothing reconnects, and the process stays, so a `:permanent` supervisor has
  nothing to restart into a second registration. The persistence family — restore, compaction, the live
  read, the JSON apply, the credit window — is answered by `Flezha.SDK.Commands`
  off this loop, and a graceful stop ships every operative's state before the
  connection closes; a `"workflow"` `AddOperative` is refused.
  """

  use GenServer
  require Logger

  alias Flezha.Config.Durations
  alias Flezha.Config.Backoff

  alias Flezha.SDK.{
    Codec,
    Commands,
    Counters,
    Credit,
    DataStream,
    Entity,
    EntryIdle,
    EntryInput,
    Hold,
    Jitter,
    KeyGroup,
    Operative,
    Park,
    PersistentId,
    Transport
  }

  alias Flezha.SDK.Declarations
  alias Flezha.SDK.Workflow.{Driver, Spec}
  alias Flezha.Transport.Server, as: TransportServer

  @name __MODULE__
  @park_warn_every 5
  @persistence_commands [
    :restore_init_state,
    :compact_entity,
    :entity_state,
    :entity_apply_state,
    :migration_serialization,
    :migration_deserialization,
    :entity_mailbox_status,
    :credit_window_init,
    :credit_grant
  ]
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
      :credit,
      session_token: nil,
      control_handle: nil,
      conn: nil,
      topologies: %{},
      factories: [],
      primaries: [],
      secondaries: [],
      operatives: %{},
      parks: %{},
      barriers: %{},
      idle: nil,
      held: nil,
      reconnect_attempt: 0,
      disconnecting?: false,
      stopped?: false
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
    state = initial_state(opts)
    if state.transport == :native, do: TransportServer.ensure_started(:native)

    case do_connect(state) do
      {:ok, state} ->
        schedule_heartbeat()
        schedule_keepalive()

        {:ok,
         Enum.reduce(Keyword.get(opts, :idle_observers, []), state, fn
           {channel, threshold_ms, observer}, acc ->
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

  # Everything the runner owns before it connects: its identity, the registry
  # and counters its Operatives share, and the credit window linked here.
  defp initial_state(opts) do
    namespace = Keyword.fetch!(opts, :namespace)
    factories = Keyword.fetch!(opts, :factories)
    stable_id = PersistentId.stable_id(namespace, Enum.map(factories, &elem(&1, 0)))
    counters = Counters.new()
    {:ok, credit} = Credit.start_link(counters: counters)

    %State{
      namespace: namespace,
      transport: Keyword.fetch!(opts, :transport),
      stable_id: stable_id,
      registry: :ets.new(:flezha_sdk_operatives, [:public, :set, {:read_concurrency, true}]),
      counters: counters,
      credit: credit,
      topologies: Keyword.fetch!(opts, :topologies),
      factories: factories,
      payload: handshake_payload(namespace, factories, stable_id),
      # The workflow driver's writes that no stream would take: this process
      # owns the wire they were meant for, so it is what keeps them.
      held: Hold.new()
    }
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
      "credit_parked" => Integer.to_string(Counters.credit_parked(s.counters)),
      "timers_armed" => Integer.to_string(Counters.timers_armed(s.counters)),
      "timers_fired" => Integer.to_string(Counters.timers_fired(s.counters)),
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

        ref =
          Process.send_after(
            self(),
            {:park_resend, park_id},
            Backoff.arm(:park_resend, n + 1, park_id)
          )

        {:noreply, %{s | parks: Map.put(s.parks, park_id, %{park | resends: n + 1, timer: ref})}}
    end
  end

  # --- closes → reconnect ---

  # A stopped or disconnecting runner reconnects into nothing: the closes are
  # the controller taking the session down, as ordered.
  def handle_info({closed, _id, reason}, %State{} = s)
      when closed in [:flz_closed, :data_stream_closed] and (s.disconnecting? or s.stopped?) do
    Logger.debug("[Flezha.SDK] stream closed during disconnect: #{reason}")
    {:noreply, s}
  end

  def handle_info({:flz_closed, _id, reason}, %State{} = s), do: reconnect(s, reason)
  def handle_info({:data_stream_closed, _pid, reason}, %State{} = s), do: reconnect(s, reason)

  def handle_info({:EXIT, _pid, _reason}, %State{} = s), do: {:noreply, s}
  def handle_info(_msg, %State{} = s), do: {:noreply, s}

  @impl true
  # The three casts a workflow driver sends upward, all of them built by
  # `Flezha.SDK.Workflow.Commit` so the driver never names this module.
  def handle_cast({:barrier, driver, ref, command}, %State{} = s) do
    reference_id = "wf-" <> Integer.to_string(System.unique_integer([:positive]))
    ship_request(s, reference_id, command)
    {:noreply, %{s | barriers: Map.put(s.barriers, reference_id, {driver, ref})}}
  end

  def handle_cast({:emit_output, _oid, _meta, _bytes} = write, %State{} = s),
    do: {:noreply, ship_write(s, write)}

  def handle_cast({:record, _payload_id, _frame} = write, %State{} = s),
    do: {:noreply, ship_write(s, write)}

  def handle_cast({:notify, command}, %State{} = s) do
    ship_request(s, "wf-" <> Integer.to_string(System.unique_integer([:positive])), command)
    {:noreply, s}
  end

  def handle_cast({:park, entity_pid, identity, body}, %State{} = s) do
    send_park_frame(s, identity, body)

    ref =
      Process.send_after(
        self(),
        {:park_resend, body.park_id},
        Backoff.arm(:park_resend, 1, body.park_id)
      )

    park = %{entity: entity_pid, identity: identity, body: body, resends: 0, timer: ref}
    {:noreply, %{s | parks: Map.put(s.parks, body.park_id, park)}}
  end

  # A graceful stop — the supervisor's shutdown, `Flezha.SDK.close/1`, a
  # `RunnerDisconnect` — ships every operative's state before the connection
  # goes, so a reconnect-resume is loss-free.
  @impl true
  def terminate(_reason, %State{} = s),
    do: Commands.shutdown(s, Durations.get(:sdk_shutdown_ack_ms))

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
    case Map.get(s.operatives, oid) do
      {pid, _fid} -> Driver.stop_workflow(pid)
      nil -> Logger.warning("[Flezha.SDK] WorkflowStop for unknown operative #{oid}")
    end

    reply_ok(s, ref)
  end

  # A barrier ack rides the reference_id of the commit it answers, which is
  # what names the driver that shipped it. Nothing is replied to: the ack is
  # itself a reply.
  defp dispatch({tag, %{error: error}}, ref, %State{} = s)
       when tag in [:proc_commit_response, :iter_commit_response] do
    case Map.pop(s.barriers, ref) do
      {{driver, barrier_ref}, barriers} ->
        send(driver, {:barrier_ack, barrier_ref, error})
        {:noreply, %{s | barriers: barriers}}

      {nil, _} ->
        Logger.debug("[Flezha.SDK] barrier ack for an unknown reference #{ref}")
        {:noreply, s}
    end
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

  # Stopped in place, never exited: the ack goes out, then this side closes
  # its streams (the controller waits for that close), and the process stays.
  # An exit here would be a `:normal` one a `:permanent` supervisor restarts,
  # and the restart registers again into the session being torn down.
  defp dispatch({:runner_disconnect, _}, ref, s) do
    reply(s, ref, "success")
    s = teardown_streams(s)
    Flezha.Transport.Stream.close(s.control_handle)
    {:noreply, %{s | control_handle: nil, stopped?: true}}
  end

  defp dispatch({:park_payload_ack, %{park_id: park_id}}, _ref, s) do
    {:noreply, release_park(s, park_id)}
  end

  defp dispatch({tag, payload}, ref, s) when tag in @persistence_commands do
    Commands.dispatch(tag, payload, &reply(s, ref, &1, &2, &3, &4), s)
    {:noreply, s}
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
      is_nil(topo) ->
        {:error, "unknown factory #{add.factory_id}", s}

      # The kind the controller placed under and the class registered under
      # this id must agree: a mismatch would drive a stream topology through
      # the workflow driver, or the reverse, and neither has the other's shape.
      kind == "workflow" ->
        start_workflow(s, add, topo)

      match?(%Spec{}, topo) ->
        {:error, "factory #{add.factory_id} is a workflow class; kind was #{inspect(kind)}", s}

      KeyGroup.partitioned?(range) and topo.keyless? ->
        {:error, "PartitionsWithoutKeyedProcedure: a partition_count >= 2 class needs a key_by",
         s}

      true ->
        start_stream(s, add, topo, range)
    end
  end

  defp start_stream(%State{} = s, add, topo, range) do
    {:ok, op_pid} =
      Operative.start_link(
        operative_id: add.operative_id,
        topology: topo,
        cells: EntryInput.cells_for(topo.entries),
        range: range,
        counters: s.counters,
        runner: self(),
        secondaries: s.secondaries,
        credit: s.credit
      )

    :ets.insert(s.registry, {add.operative_id, op_pid})
    seed_init_values(op_pid, add)

    {:ok, %{s | operatives: Map.put(s.operatives, add.operative_id, {op_pid, add.factory_id})},
     Declarations.for(topo)}
  end

  # What the class declares to the controller: partitionability only when
  # refusable, the compaction threshold only when it differs from the default,
  defp start_workflow(%State{} = s, add, %Spec{} = spec) do
    {:ok, pid} =
      Driver.start_link(
        operative_id: add.operative_id,
        spec: spec,
        args: add.init_values || "",
        journal: Codec.journal_facts(add.proc_journal),
        owed_inbox: add.inbox_tail_count || 0,
        resume_entity_count: add.resume_entity_count || 0,
        counters: s.counters,
        runner: self(),
        secondaries: s.secondaries,
        credit: s.credit
      )

    :ets.insert(s.registry, {add.operative_id, pid})

    {:ok, %{s | operatives: Map.put(s.operatives, add.operative_id, {pid, add.factory_id})},
     Declarations.for(spec)}
  end

  defp start_workflow(%State{} = s, add, _topology),
    do: {:error, "factory #{add.factory_id} is a stream class; kind was \"workflow\"", s}

  defp seed_init_values(op_pid, add) do
    case Commands.apply_json(op_pid, add.operative_id, add.init_values) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("[Flezha.SDK] #{add.operative_id} init_values: #{reason}")
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

        # What this process held goes out before the operatives are told the
        # wire is back, so a staged workflow output cannot be overtaken by the
        # frames a resumed operative writes.
        s = replay_held(%{s | primaries: readers, secondaries: secondaries})

        # Push the fresh secondaries to any operatives that survived a reconnect.
        Enum.each(s.operatives, fn {_oid, {pid, _f}} ->
          if Process.alive?(pid), do: send(pid, {:secondaries, secondaries})
        end)

        {:ok, s}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp teardown_streams(%State{} = s) do
    Enum.each(s.primaries, fn pid -> if Process.alive?(pid), do: GenServer.stop(pid, :normal) end)
    Enum.each(s.secondaries, &Flezha.Transport.Stream.close/1)
    %{s | primaries: [], secondaries: []}
  end

  # --- the workflow driver's writes ---

  # The driver has no wire of its own: it casts its outputs and its records
  # here, so a frame no stream will take is held here too and replayed on the
  # next bound one. Nothing about a write reaches the driver, so a drop here
  # would be a loss no caller could learn of.
  defp ship_write(%State{secondaries: []} = s, write), do: hold_write(s, write)

  defp ship_write(%State{} = s, write) do
    case write_frame(s, write) do
      :ok -> s
      {:error, _reason} -> hold_write(s, write)
    end
  end

  defp write_frame(%State{secondaries: secondaries}, {:emit_output, oid, meta, bytes}) do
    handle = Transport.secondary_for(secondaries, oid)
    Flezha.Transport.Stream.send_frame(handle, oid, bytes, meta)
  end

  defp write_frame(%State{secondaries: secondaries}, {:record, payload_id, frame}),
    do: Flezha.Transport.Stream.send_frame(Enum.at(secondaries, 0), payload_id, frame, %{})

  defp hold_write(%State{held: held} = s, write),
    do: %{s | held: Hold.keep(held, write, s.counters)}

  # In the order the driver wrote them; one refused again re-enters the hold
  # behind the rest, as every replay on this wire does.
  defp replay_held(%State{held: held} = s) do
    {items, empty} = Hold.drain(held)
    Enum.reduce(items, %{s | held: empty}, &ship_write(&2, &1))
  end

  # --- parking ---

  defp send_park_frame(%State{secondaries: []}, _identity, _body), do: :ok

  defp send_park_frame(%State{secondaries: secondaries}, identity, body) do
    handle = Transport.secondary_for(secondaries, body.park_id)
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
    # The credit session died with the stream; the controller re-offers one.
    Credit.disarm(s.credit)
    s = teardown_streams(s)
    Backoff.wait(:sdk_reconnect, n + 1, s.stable_id)

    case do_connect(%{s | reconnect_attempt: n + 1, control_handle: nil, conn: nil}) do
      {:ok, s} -> {:noreply, %{s | reconnect_attempt: Backoff.reset(:sdk_reconnect, s.stable_id)}}
      {:error, r} -> reconnect(s, "retry: #{inspect(r)}")
    end
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
    ship_request(s, ref, Codec.update_factories_command(s.factories))
  end

  defp ship_request(%State{control_handle: nil}, _ref, _command), do: :ok

  defp ship_request(%State{} = s, ref, command) do
    bin = Codec.encode_request(ref, command)
    Flezha.Transport.Stream.send_frame(s.control_handle, "control_command", bin, %{})
  end

  defp reply_ok(s, ref) do
    reply(s, ref, "success")
    {:noreply, s}
  end

  # `metadata` rides the reply frame: the live read's `flz.change_seq`.
  defp reply(s, ref, status, errors \\ [], raw \\ <<>>, metadata \\ %{})

  defp reply(%State{control_handle: nil}, _ref, _status, _errors, _raw, _metadata), do: :ok

  defp reply(%State{} = s, ref, status, errors, raw, metadata) do
    bin = Codec.encode_response(ref, status, errors, raw)
    Flezha.Transport.Stream.send_frame(s.control_handle, "control_command", bin, metadata)
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
    # Jitter into [base/2, base] (the cross-SDK heartbeat contract).
    interval = Jitter.jitter_within(Durations.env(:data_heartbeat_ms))
    Process.send_after(self(), :data_heartbeat, interval)
  end

  defp schedule_keepalive,
    do: Process.send_after(self(), :control_keepalive, Durations.get(:control_keepalive_ms))
end
