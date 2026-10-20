defmodule Flezha.SDK.Pool do
  @moduledoc """
  The workers that answer one core's activations. Each worker is a process
  paired with a thread the native library owns: the thread waits in the
  core's poll and hands the activation over as a message, the worker runs the
  tasklet code and completes it, and only then does the thread poll again. No
  wait for an activation holds a BEAM scheduler.

  The core's pool governor sizes it, over the schedulers online as this
  runtime's base: it asks for the base workers, for one more each time the
  core reports starvation (every worker held by a tasklet that waits), and for
  a replacement when the pool is short, and a grown worker's thread stops it
  when idle. An activation a crashed worker held is failed back to the core's
  retry ladder.
  """

  use GenServer
  require Logger

  alias Flezha.SDK.{Dispatch, Error, Native}

  @doc """
  A new core context with `modules` declared, begun by `begin` (a connect or a
  local run), and a pool serving it: `{:ok, %{core:, cells:, pool:}}`. A
  refusal frees the context and is the answer.
  """
  @spec open([module()], (Native.core() -> :ok | {:error, Error.t()})) ::
          {:ok, %{core: Native.core(), cells: :ets.tid(), pool: pid()}} | {:error, Error.t()}
  def open(modules, begin) do
    core = Native.context_new(Native.schema_version())

    with :ok <- declare_all(core, modules),
         :ok <- begin.(core) do
      cells = Dispatch.cells()
      {:ok, pool} = start_link(core: core, cells: cells)
      {:ok, %{core: core, cells: cells, pool: pool}}
    else
      refused ->
        Native.context_free(core)
        refused
    end
  end

  @doc """
  Stop serving what `open/2` started: `stop` ends the core's work, then the
  pool drains as `drain/3` says. The context is freed only when no worker is
  left running: one the drain gave up on may yet call into it.
  """
  @spec close(map(), (Native.core() -> any()), non_neg_integer(), :stop | :settle) :: :ok
  def close(%{pool: nil}, _stop, _wait_ms, _how), do: :ok

  def close(%{core: core, pool: pool}, stop, wait_ms, how) do
    stop.(core)
    # A pool already gone has no worker left to wait for.
    if not Process.alive?(pool) or drain(pool, wait_ms, how) == 0, do: Native.context_free(core)
    :ok
  end

  defp declare_all(core, modules) do
    Enum.reduce_while(modules, :ok, fn module, :ok ->
      case Dispatch.declare(core, module) do
        :ok -> {:cont, :ok}
        refused -> {:halt, refused}
      end
    end)
  end

  @doc """
  Start a pool over `core`; `cells` is the Cell table its workers share, and
  `:retire_ms` a test's short retire time for a grown worker.
  """
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc """
  Stop the pool: every worker finishes the activation it holds, then its
  thread stops. Answers how many workers were still running after `wait_ms`
  and were killed; the core context is safe to free only when that is 0.

  `:settle`, after the core was told to shut down: the workers keep serving
  until the core says it is shut down, because shipping each relay's state
  is itself served by them; only the ones still running then are stopped.
  """
  @spec drain(pid(), non_neg_integer(), :stop | :settle) :: non_neg_integer()
  def drain(pool, wait_ms, how \\ :stop) do
    undrained = GenServer.call(pool, {:drain, wait_ms, how}, wait_ms + 5_000)
    GenServer.stop(pool)
    undrained
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    core = Keyword.fetch!(opts, :core)
    env = %{core: core, cells: Keyword.fetch!(opts, :cells)}
    retire_ms = Keyword.get(opts, :retire_ms, 0)
    governor = Native.pool_open(core, System.schedulers_online(), retire_ms, self())
    {:ok, %{core: core, env: env, governor: governor, workers: %{}}}
  end

  @impl true
  # The threads first, so none hands out another activation; then each worker
  # finishes the one it holds; then whatever a gone worker held is failed back.
  def handle_call({:drain, wait_ms, how}, _from, s) do
    Native.pool_stop(s.governor)
    deadline = System.monotonic_time(:millisecond) + wait_ms
    running = if how == :settle, do: await_exits(s.workers, deadline), else: s.workers

    for {pid, thread} <- running do
      Native.stop_thread(thread)
      send(pid, :stop)
    end

    running = await_exits(running, deadline)

    for {pid, thread} <- s.workers do
      if Map.has_key?(running, pid), do: Process.exit(pid, :kill)
      Native.release_thread(s.core, thread)
    end

    {:reply, map_size(running), %{s | workers: %{}}}
  end

  @impl true
  def handle_info({:flz_grow, grown}, s), do: {:noreply, add_worker(s, grown)}

  def handle_info({:EXIT, pid, reason}, s) do
    case Map.pop(s.workers, pid) do
      {nil, _} ->
        {:stop, reason, s}

      # The thread's stop gives its seat back; the governor asks for a
      # replacement when the pool is short.
      {thread, workers} ->
        Native.release_thread(s.core, thread)
        {:noreply, %{s | workers: workers}}
    end
  end

  @impl true
  def terminate(_reason, %{workers: workers} = s) when map_size(workers) > 0 do
    Native.pool_stop(s.governor)

    for {pid, thread} <- workers do
      Process.exit(pid, :kill)
      Native.release_thread(s.core, thread)
    end

    :ok
  end

  def terminate(_reason, _s), do: :ok

  defp await_exits(running, _deadline) when map_size(running) == 0, do: running

  defp await_exits(running, deadline) do
    receive do
      {:EXIT, pid, _reason} -> await_exits(Map.delete(running, pid), deadline)
    after
      max(deadline - System.monotonic_time(:millisecond), 0) -> running
    end
  end

  defp add_worker(s, grown) do
    worker = spawn_link(fn -> work(s.env) end)
    thread = Native.poller(s.core, s.governor, worker, grown)
    %{s | workers: Map.put(s.workers, worker, thread)}
  end

  # The worker takes its thread from the first activation the thread sends:
  # each `complete` lets that same thread poll again.
  defp work(env) do
    receive do
      {:flz_activation, thread, act} ->
        Native.complete(env.core, thread, Dispatch.answer(act, env))
        work(env)

      :flz_shutting_down ->
        :ok

      :stop ->
        :ok

      {:flz_poll_failed, document} ->
        Logger.error("[Flezha.SDK] a poller stopped: #{document}")
    end
  end
end
