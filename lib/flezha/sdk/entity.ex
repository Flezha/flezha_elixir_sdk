defmodule Flezha.SDK.Entity do
  @moduledoc """
  One process per Entity: a bounded input queue, the retry-then-park dispatch
  ladder, and per-Entity ordering (arrival order out of the queue). Inputs under
  `:drop_nothing` arrive as a `GenServer.call` the Entity answers only when
  there is room — never dropped, never refused (the caller, the Operative
  router, waits, which is the documented co-tenancy cost);
  `:drop_newest`/`:drop_oldest` arrive as casts. A completed dispatch bumps `dispatches`; an exhausted ladder parks
  the input and HOLDS the Entity until the controller's `ParkPayloadAck`.

  A changeful Entity (its procedure exports `apply_change/2`) is also the
  writer of its own record stream: after a successful dispatch it ships one
  `entity_state_change` frame per recorded change at the next seq on one
  secondary stream, before the dispatch's outputs leave, then writes its
  `{state, seq}` into the Operative's slot table so a replacement started
  after an untrappable kill continues on the same seq line. It answers a
  restore (fold, re-arm), a compaction (the snapshot after its records), a
  live read and a state apply, each in mailbox order so no dispatch
  interleaves. With the class's persistence off the seq advances and nothing
  ships. Its outputs reach the Operative with their metadata, their keys and
  the seq the dispatch started at — what a keyed sink's stamps read.
  """

  use GenServer
  require Logger

  alias Flezha.SDK.{Counters, Credit, Ctx, Jitter, Park, Procedure, Record}
  alias Flezha.SDK.Operative.Inbox
  alias Flezha.Transport.Stream, as: TransportStream

  @retry_backoff_cap_ms 5_000

  defmodule State do
    @moduledoc false
    defstruct [
      :operative,
      :operative_id,
      :node_ref,
      :node_name,
      :input_channel,
      :module,
      :entity_key,
      :capacity,
      :policy,
      :retry_max,
      :retry_backoff_ms,
      :counters,
      :identity,
      :slots,
      :credit,
      ctx_opts: [],
      changeful?: false,
      persistent?: false,
      schema_version: 1,
      change_seq: 0,
      last_input_ms: 0,
      secondary: nil,
      proc_state: nil,
      queue: :queue.new(),
      size: 0,
      processing?: false,
      held?: false,
      park_id: nil,
      park_seq: 0,
      waiters: :queue.new()
    ]
  end

  # --- API (used by the Operative router) ---

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc "Enqueue under `:drop_nothing` — the caller waits until there is room."
  def enqueue_block(pid, value), do: GenServer.call(pid, {:enqueue, value}, :infinity)

  @doc "Enqueue under a drop policy — non-blocking."
  def enqueue_drop(pid, value, policy), do: GenServer.cast(pid, {:enqueue_drop, value, policy})

  @doc "Release a held (parked) Entity after its `ParkPayloadAck`."
  def release_park(pid, park_id), do: send(pid, {:park_released, park_id})

  @doc "Rebuild the state from a `RestoreInitState` payload; `{:ok, terminal_seq, migrated?}`."
  @spec restore(pid(), binary(), binary(), binary(), non_neg_integer()) ::
          {:ok, non_neg_integer(), boolean()} | {:error, String.t()}
  def restore(pid, snapshot_header, snapshot_bytes, change_log_bytes, terminal_hint),
    do:
      GenServer.call(
        pid,
        {:restore, snapshot_header, snapshot_bytes, change_log_bytes, terminal_hint},
        :infinity
      )

  @doc "Ship the state as an `entity_state_snapshot`; `{:ok, snapshot_seq, body_size}`."
  @spec snapshot(pid()) :: {:ok, non_neg_integer(), non_neg_integer()} | {:error, String.t()}
  def snapshot(pid), do: GenServer.call(pid, :snapshot, :infinity)

  @doc """
  The state and the seq it was read at; `{:error, :busy}` when a dispatch holds
  the Entity past `timeout` — the reference's serialize gate.
  """
  @spec read(pid(), timeout()) :: {:ok, term(), non_neg_integer()} | {:error, :busy | :gone}
  def read(pid, timeout) do
    GenServer.call(pid, :read, timeout)
  catch
    :exit, {:timeout, _} -> {:error, :busy}
    :exit, _gone -> {:error, :gone}
  end

  @doc "Replace the state with a JSON object through the procedure's `state_from_json/1`."
  @spec apply_state(pid(), map()) :: :ok | {:error, String.t()}
  def apply_state(pid, json), do: GenServer.call(pid, {:apply_state, json}, :infinity)

  @doc """
  Whether the Entity has been idle past `ttl_ms` holding no work at all — an
  empty queue, no dispatch in flight, no park held, no deferred push. A probe
  that times out (a dispatch in flight) is `false`, and its late reply is
  inert to the caller.
  """
  @spec idle_for?(pid(), pos_integer(), timeout()) :: boolean()
  def idle_for?(pid, ttl_ms, timeout) do
    GenServer.call(pid, {:idle?, ttl_ms}, timeout)
  catch
    :exit, _ -> false
  end

  @doc """
  Evict an Entity the caller proved idle: it ships `delete_entity_state` when
  it persists, drops its slot so a successor is a cold start, and stops.
  """
  @spec evict(pid()) :: :evicted | :gone
  def evict(pid) do
    GenServer.call(pid, :evict, :infinity)
  catch
    :exit, _ -> :gone
  end

  @doc """
  An Entity's slot identity in `Flezha.SDK.Counters`. Its Operative computes the
  same value to release the slot of an Entity that died mid-dispatch — the one
  unwind an `after` inside the Entity cannot reach.
  """
  @spec identity(binary(), term(), term()) :: term()
  def identity(operative_id, node_ref, entity_key),
    do: {operative_id, node_ref, entity_key}

  # --- server ---

  @impl true
  def init(opts) do
    module = Keyword.fetch!(opts, :module)
    entity_key = Keyword.get(opts, :entity_key)
    operative_id = Keyword.fetch!(opts, :operative_id)
    identity = Park.identity(operative_id, module.name(), entity_key || "")
    changeful? = Procedure.changeful?(module)
    slots = Keyword.get(opts, :slots)
    {proc_state, change_seq} = seed(slots, identity, module, entity_key)

    state = %State{
      operative: Keyword.fetch!(opts, :operative),
      operative_id: operative_id,
      node_ref: Keyword.fetch!(opts, :node_ref),
      node_name: Keyword.get(opts, :node_name),
      input_channel: Keyword.get(opts, :input_channel),
      module: module,
      entity_key: entity_key,
      capacity: Keyword.fetch!(opts, :capacity),
      policy: Keyword.fetch!(opts, :policy),
      retry_max: Keyword.fetch!(opts, :retry_max),
      retry_backoff_ms: Keyword.fetch!(opts, :retry_backoff_ms),
      counters: Keyword.fetch!(opts, :counters),
      identity: identity,
      slots: slots,
      credit: Keyword.get(opts, :credit),
      ctx_opts: Keyword.get(opts, :ctx_opts, []),
      changeful?: changeful?,
      persistent?: changeful? and Keyword.get(opts, :persistence_enabled, true),
      schema_version: Procedure.schema_version(module),
      proc_state: proc_state,
      change_seq: change_seq,
      last_input_ms: now_ms()
    }

    {:ok, put_secondaries(state, Keyword.get(opts, :secondaries, []))}
  end

  @impl true
  def handle_call({:enqueue, value}, from, %State{} = s) do
    if s.size < s.capacity do
      # Room now — accept immediately (the caller does not block).
      {:reply, :ok, kick(enqueue(s, value))}
    else
      # No room — defer the reply until a dispatch frees a slot (block policy).
      Counters.inc_blocked_parks(s.counters)
      # An accepted-but-unqueued push is invisible to `dispatches` and to the
      # handout report, so it carries its own: the age reported is the oldest
      # one's, which is why marking is idempotent while any is held.
      Counters.deferred_started(s.counters, identity(s.operative_id, s.node_ref, s.entity_key))
      {:noreply, %{s | waiters: :queue.in({from, value}, s.waiters)}}
    end
  end

  def handle_call({:restore, header, snapshot, log, hint}, _from, %State{changeful?: true} = s) do
    case Record.fold(s.module, s.entity_key, header, snapshot, log, hint) do
      {:ok, state, terminal, migrated} ->
        s = write_slot(%{s | proc_state: state, change_seq: terminal + 1})
        {:reply, {:ok, terminal, migrated}, s}

      {:error, reason} ->
        {:reply, {:error, reason}, s}
    end
  end

  def handle_call({:restore, _h, _s, _l, _hint}, _from, %State{} = s),
    do: {:reply, {:error, "#{label(s)} persists nothing"}, s}

  # The snapshot folds every seq already on the wire: nothing is pending, so
  # that is `change_seq - 1`, and the frame leaves behind the records on the
  # same stream from the same process.
  def handle_call(:snapshot, _from, %State{changeful?: true, secondary: secondary} = s)
      when secondary != nil do
    seq = max(s.change_seq - 1, 0)
    body = Record.encode_body(s.proc_state)
    frame = Record.frame(s.identity, seq, body, s.schema_version)

    case TransportStream.send_frame(secondary, "entity_state_snapshot", frame, %{}) do
      :ok -> {:reply, {:ok, seq, byte_size(body)}, s}
      {:error, reason} -> {:reply, {:error, inspect(reason)}, s}
    end
  end

  def handle_call(:snapshot, _from, %State{} = s),
    do: {:reply, {:error, "#{label(s)} has no state stream"}, s}

  def handle_call(:read, _from, %State{} = s),
    do: {:reply, {:ok, s.proc_state, s.change_seq}, s}

  def handle_call({:idle?, ttl_ms}, _from, %State{} = s) do
    idle? =
      s.size == 0 and not s.processing? and not s.held? and :queue.is_empty(s.waiters) and
        now_ms() - s.last_input_ms > ttl_ms

    {:reply, idle?, s}
  end

  # Destructive: the delete follows the records on the same stream from the
  # same process, the slot goes so a successor starts from `init_state/1`
  # rather than from what this Entity last shipped, then the process ends.
  def handle_call(:evict, _from, %State{} = s) do
    if s.persistent?, do: send_delete(s)
    if s.slots, do: :ets.delete(s.slots, s.identity)
    {:stop, :normal, :evicted, s}
  end

  def handle_call({:apply_state, json}, _from, %State{} = s) do
    {:reply, :ok, write_slot(%{s | proc_state: Procedure.state_from_json(s.module, json)})}
  rescue
    e -> {:reply, {:error, "apply_state: #{Exception.message(e)}"}, s}
  end

  @impl true
  def handle_cast({:enqueue_drop, value, policy}, %State{} = s) do
    cond do
      s.size < s.capacity ->
        {:noreply, kick(enqueue(s, value))}

      policy == :drop_oldest ->
        {{:value, _dropped}, q} = :queue.out(s.queue)
        record_drop(s, :drop_oldest)
        {:noreply, kick(enqueue(%{s | queue: q, size: s.size - 1}, value))}

      true ->
        record_drop(s, :drop_newest)
        {:noreply, s}
    end
  end

  @impl true
  def handle_info(:process, %State{} = s), do: {:noreply, process_next(s)}

  def handle_info({:secondaries, secondaries}, %State{} = s),
    do: {:noreply, put_secondaries(s, secondaries)}

  def handle_info({:park_released, park_id}, %State{park_id: park_id} = s) do
    {:noreply, process_next(%{s | held?: false, park_id: nil})}
  end

  def handle_info({:park_released, _stale}, s), do: {:noreply, s}
  def handle_info(_msg, s), do: {:noreply, s}

  # --- internals ---

  defp enqueue(%State{} = s, value),
    do: %{s | queue: :queue.in(value, s.queue), size: s.size + 1, last_input_ms: now_ms()}

  defp kick(%State{processing?: false, held?: false, size: size} = s) when size > 0 do
    send(self(), :process)
    %{s | processing?: true}
  end

  defp kick(s), do: s

  defp process_next(%State{held?: true} = s), do: s

  defp process_next(%State{} = s) do
    case :queue.out(s.queue) do
      {:empty, _} ->
        admit_waiter(%{s | processing?: false})

      {{:value, value}, q} ->
        s = %{s | queue: q, size: s.size - 1}
        s = dispatch(s, value)
        s = admit_waiter(s)

        if s.held? do
          %{s | processing?: false}
        else
          # keep draining
          send(self(), :process)
          %{s | processing?: true}
        end
    end
  end

  # A freed slot admits the next blocked caller (reply :ok, enqueue its value).
  defp admit_waiter(%State{} = s) do
    if s.size < s.capacity do
      case :queue.out(s.waiters) do
        {:empty, _} ->
          note_no_deferral(s)

        {{:value, {from, value}}, waiters} ->
          GenServer.reply(from, :ok)
          s = %{s | waiters: waiters} |> enqueue(value)
          if :queue.is_empty(waiters), do: note_no_deferral(s), else: s
      end
    else
      s
    end
  end

  # Release this Entity's deferred-push slot: nothing is held any more.
  # Idempotent, so the drain need not know whether one was ever taken.
  defp note_no_deferral(%State{} = s) do
    Counters.deferred_finished(s.counters, identity(s.operative_id, s.node_ref, s.entity_key))
    s
  end

  # `after` is the contract, not politeness: the in-flight report is what makes
  # ONE wedged Entity visible to the controller, so a dispatch that unwinds
  # (raise, throw, exit) must release its slot on the way out or the runner
  # would report a stall that healed long ago.
  defp dispatch(%State{} = s, value) do
    identity = identity(s.operative_id, s.node_ref, s.entity_key)
    Counters.dispatch_started(s.counters, identity)

    try do
      case run_ladder(s, value, 1) do
        {:ok, new_proc_state, emits, changes} ->
          Counters.inc_dispatches(s.counters)
          # The outputs are stamped with the seq the dispatch started at, not
          # the one its own records advance to: the records leave before the
          # outputs (flush-before-emit).
          handout_seq = s.change_seq
          s = ship(%{s | proc_state: new_proc_state}, changes)
          flush_emits(s, emits, handout_seq)
          s

        {:park, error, attempts} ->
          park(s, value, error, attempts)
      end
    after
      Counters.dispatch_finished(s.counters, identity)
    end
  end

  defp run_ladder(%State{} = s, value, attempt) do
    ctx = Ctx.new(s.module, s.entity_key, System.system_time(:millisecond), s.ctx_opts)

    outcome =
      try do
        {:result, s.module.run(value, s.proc_state, ctx)}
      rescue
        e -> {:raised, Exception.message(e)}
      catch
        kind, reason -> {:raised, "#{kind}: #{inspect(reason)}"}
      end

    case outcome do
      {:result, {:ok, new_state}} ->
        {:ok, new_state, Ctx.take_emits(ctx), Ctx.take_changes(ctx)}

      {:result, {:error, {:non_retryable, reason}}} ->
        Ctx.discard(ctx)
        {:park, format_error(reason), attempt}

      {:result, {:error, reason}} ->
        Ctx.discard(ctx)
        retry_or_park(s, value, attempt, format_error(reason))

      {:raised, message} ->
        Ctx.discard(ctx)
        retry_or_park(s, value, attempt, message)
    end
  end

  defp retry_or_park(%State{retry_max: max}, _value, attempt, error) when attempt >= max,
    do: {:park, error, attempt}

  defp retry_or_park(%State{} = s, value, attempt, error) do
    Logger.debug(
      "[Flezha.SDK] #{label(s)} attempt #{attempt}/#{s.retry_max} failed: #{error}; retrying"
    )

    :telemetry.execute([:flezha, :sdk, :retry], %{count: 1}, %{
      operative_id: s.operative_id,
      attempt: attempt
    })

    Process.sleep(backoff(s.retry_backoff_ms, attempt))
    run_ladder(s, value, attempt + 1)
  end

  # exp = base * 2^(attempt-1), capped at 5s, jittered uniform in [exp/2, exp*1.5).
  defp backoff(base_ms, attempt) do
    exp = min(base_ms * Bitwise.bsl(1, min(attempt - 1, 16)), @retry_backoff_cap_ms)
    min(Jitter.jitter(exp), @retry_backoff_cap_ms)
  end

  defp park(%State{} = s, value, error, attempts) do
    seq = s.park_seq
    park_id = "#{label(s)}##{seq}"

    channel = s.input_channel || s.node_name || s.operative_id
    body = Park.body(park_id, channel, error, attempts, value)

    Park.ship(
      s.operative,
      self(),
      Park.identity(s.operative_id, s.module.name(), s.entity_key),
      body
    )

    :telemetry.execute([:flezha, :sdk, :parked], %{count: 1}, %{operative_id: s.operative_id})

    %{s | held?: true, park_id: park_id, park_seq: seq + 1}
  end

  # --- the record stream ---

  defp ship(%State{changeful?: false} = s, _changes), do: s
  defp ship(%State{} = s, []), do: s

  defp ship(%State{} = s, changes) do
    seq =
      Enum.reduce(changes, s.change_seq, fn change, seq ->
        if s.persistent?, do: send_record(s, seq, Record.encode_body(change))
        seq + 1
      end)

    write_slot(%{s | change_seq: seq})
  end

  # Between a close and its reconnect there is no stream: the seq still
  # advances, so the gap it leaves is what makes the controller resync the
  # Entity from its own copy rather than a silently diverged one.
  defp send_record(%State{secondary: nil} = s, seq, _body) do
    Logger.warning(
      "[Flezha.SDK] #{label(s)} record seq #{seq} had no state stream (reconnecting); " <>
        "the controller resyncs the gap"
    )
  end

  # Debit-on-send: parks here, in the Entity, when the window is exhausted —
  # and the router's push behind it waits, which is the emit-path park.
  defp send_record(%State{} = s, seq, body) do
    frame = Record.frame(s.identity, seq, body, s.schema_version)
    Credit.acquire(s.credit, byte_size(frame))

    case TransportStream.send_frame(s.secondary, "entity_state_change", frame, %{}) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "[Flezha.SDK] #{label(s)} record seq #{seq} undelivered: #{inspect(reason)}"
        )
    end
  end

  # Fire-and-forget, outside the credit window, as the shared core's own is.
  defp send_delete(%State{secondary: nil} = s),
    do: Logger.warning("[Flezha.SDK] #{label(s)} evicted with no state stream (reconnecting)")

  defp send_delete(%State{} = s) do
    case TransportStream.send_frame(s.secondary, "delete_entity_state", s.identity, %{}) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("[Flezha.SDK] #{label(s)} delete undelivered: #{inspect(reason)}")
    end
  end

  # A replacement for an Entity killed untrappably continues from what its
  # predecessor last shipped; a first Entity starts from `init_state/1`.
  defp seed(nil, _identity, module, key), do: {module.init_state(key), 0}

  defp seed(slots, identity, module, key) do
    case :ets.lookup(slots, identity) do
      [{^identity, state, seq}] -> {state, seq}
      [] -> {module.init_state(key), 0}
    end
  end

  # The slot is what the WAL holds — written after the records are on the
  # wire, read by the replacement of an Entity killed untrappably.
  defp write_slot(%State{changeful?: true, slots: slots} = s) when slots != nil do
    :ets.insert(slots, {s.identity, s.proc_state, s.change_seq})
    s
  end

  defp write_slot(s), do: s

  # One stream per Entity, chosen by identity: its records and its snapshot
  # stay in order because one process writes them to one stream.
  defp put_secondaries(%State{} = s, []), do: %{s | secondary: nil}

  defp put_secondaries(%State{} = s, secondaries),
    do: %{
      s
      | secondary: Enum.at(secondaries, rem(:erlang.phash2(s.identity), length(secondaries)))
    }

  defp flush_emits(_s, [], _handout_seq), do: :ok

  defp flush_emits(%State{} = s, emits, handout_seq),
    do: Inbox.emitted(s.operative, s.node_ref, s.entity_key, emits, handout_seq)

  # Two readers, one event: telemetry is the in-node handle, the counter is what
  # leaves the runner on the heartbeat — a drop nobody can see from outside the
  # process is the defect this closes.
  defp record_drop(%State{} = s, policy) do
    Counters.inc_drops(s.counters, policy)

    :telemetry.execute([:flezha, :sdk, :mailbox], %{count: 1}, %{
      operative_id: s.operative_id,
      policy: policy
    })
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  defp format_error(reason) when is_binary(reason), do: reason
  defp format_error(reason), do: inspect(reason)

  defp label(%State{operative_id: oid, node_name: name, entity_key: key}) do
    "#{oid}/#{name || "node"}/#{key || "single"}"
  end
end
