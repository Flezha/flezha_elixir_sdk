defmodule Flezha.SDK.Entity do
  @moduledoc """
  One process per Entity: a bounded input queue, the retry-then-park dispatch
  ladder, and per-Entity ordering (arrival order out of the queue). Inputs under
  `:drop_nothing` arrive as a `GenServer.call` the Entity answers only when
  there is room — never dropped, never refused (the caller, the Operative
  router, waits, which is the documented co-tenancy cost);
  `:drop_newest`/`:drop_oldest` arrive as casts. A completed dispatch bumps `dispatches`; an exhausted ladder parks
  the input and HOLDS the Entity until the controller's `ParkPayloadAck`.
  """

  use GenServer
  require Logger

  alias Flezha.SDK.{Ctx, Counters, Park, Jitter}

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

    state = %State{
      operative: Keyword.fetch!(opts, :operative),
      operative_id: Keyword.fetch!(opts, :operative_id),
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
      proc_state: module.init_state(entity_key)
    }

    {:ok, state}
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

  def handle_info({:park_released, park_id}, %State{park_id: park_id} = s) do
    {:noreply, process_next(%{s | held?: false, park_id: nil})}
  end

  def handle_info({:park_released, _stale}, s), do: {:noreply, s}
  def handle_info(_msg, s), do: {:noreply, s}

  # --- internals ---

  defp enqueue(%State{} = s, value), do: %{s | queue: :queue.in(value, s.queue), size: s.size + 1}

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
        {:ok, new_proc_state, emits} ->
          Counters.inc_dispatches(s.counters)
          flush_emits(s, emits)
          %{s | proc_state: new_proc_state}

        {:park, error, attempts} ->
          park(s, value, error, attempts)
      end
    after
      Counters.dispatch_finished(s.counters, identity)
    end
  end

  defp run_ladder(%State{} = s, value, attempt) do
    ctx = Ctx.new(s.entity_key, System.system_time(:millisecond))

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
        {:ok, new_state, Ctx.take_emits(ctx)}

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

    Flezha.SDK.Operative.park(
      s.operative,
      self(),
      Park.identity(s.operative_id, s.module.name(), s.entity_key),
      body
    )

    :telemetry.execute([:flezha, :sdk, :parked], %{count: 1}, %{operative_id: s.operative_id})

    %{s | held?: true, park_id: park_id, park_seq: seq + 1}
  end

  defp flush_emits(%State{} = s, emits) do
    values = Enum.map(emits, fn {value, _meta} -> value end)

    if values != [],
      do: Flezha.SDK.Operative.emitted(s.operative, s.node_ref, s.entity_key, values)
  end

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

  defp format_error(reason) when is_binary(reason), do: reason
  defp format_error(reason), do: inspect(reason)

  defp label(%State{operative_id: oid, node_name: name, entity_key: key}) do
    "#{oid}/#{name || "node"}/#{key || "single"}"
  end
end
