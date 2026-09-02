defmodule Flezha.SDK.Credit do
  @moduledoc """
  The runner-wide credit window for the state stream, the shared core's rules
  in Elixir: the controller offers a session and a byte budget
  (`CreditWindowInit`), every shipped record debits its frame size, and a
  grant the controller mints on fsync replenishes it (`CreditGrant`). A sender
  out of credit parks — never drops — until a grant covers its frame, in
  arrival order; a grant for another session is stale and dropped; a reconnect
  disarms the session and parked senders proceed un-metered until the next
  offer. `arm`, `grant` and `disarm` are casts and never block; only
  `acquire/2` waits, in the calling Entity. Every park iteration — a park, and
  each grant that wakes a parked sender without covering it — bumps
  `credit_parked` on the runner's counters, which the heartbeat carries.

  One process per runner, linked to its RunnerServer, which hands the pid down
  to every Entity it starts; a test starts its own.
  """

  use GenServer

  alias Flezha.SDK.Counters

  defstruct session: nil, available: 0, waiters: :queue.new(), counters: nil

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))

  @doc "Adopt an offered window, replacing any session. Never blocks."
  @spec arm(GenServer.server(), String.t(), non_neg_integer()) :: :ok
  def arm(server, session, window_bytes),
    do: GenServer.cast(server, {:arm, session, window_bytes})

  @doc "Drop the session; parked senders proceed un-metered until the next `arm/3`."
  @spec disarm(GenServer.server()) :: :ok
  def disarm(server), do: GenServer.cast(server, :disarm)

  @doc "Apply a grant; one for another session is stale and dropped. Never blocks."
  @spec grant(GenServer.server(), String.t(), non_neg_integer()) :: :ok
  def grant(server, session, bytes), do: GenServer.cast(server, {:grant, session, bytes})

  @doc """
  Debit `bytes`, parking until enough credit is available. `nil`, or a window
  that is gone, is a runner without one and returns at once.
  """
  @spec acquire(GenServer.server() | nil, non_neg_integer()) :: :ok
  def acquire(nil, _bytes), do: :ok

  def acquire(server, bytes) do
    GenServer.call(server, {:acquire, bytes}, :infinity)
  catch
    :exit, _gone -> :ok
  end

  @doc "The adopted session, or nil."
  @spec session(GenServer.server()) :: String.t() | nil
  def session(server), do: GenServer.call(server, :session)

  # --- server ---

  @impl true
  def init(opts), do: {:ok, %__MODULE__{counters: Keyword.get(opts, :counters)}}

  @impl true
  def handle_call({:acquire, _bytes}, _from, %__MODULE__{session: nil} = s),
    do: {:reply, :ok, s}

  # A newcomer never overtakes a parked sender: arrival order is the window's.
  def handle_call({:acquire, bytes}, from, %__MODULE__{} = s) do
    if bytes <= s.available and :queue.is_empty(s.waiters) do
      {:reply, :ok, %{s | available: s.available - bytes}}
    else
      parked(s)
      {:noreply, %{s | waiters: :queue.in({from, bytes}, s.waiters)}}
    end
  end

  def handle_call(:session, _from, %__MODULE__{} = s), do: {:reply, s.session, s}

  @impl true
  def handle_cast({:arm, session, window}, %__MODULE__{} = s),
    do: {:noreply, admit(%{s | session: session, available: window})}

  def handle_cast(:disarm, %__MODULE__{} = s),
    do: {:noreply, release(%{s | session: nil, available: 0})}

  def handle_cast({:grant, session, bytes}, %__MODULE__{session: session} = s),
    do: {:noreply, admit(%{s | available: s.available + bytes})}

  def handle_cast({:grant, _stale, _bytes}, %__MODULE__{} = s), do: {:noreply, s}

  # Answer parked senders in arrival order while the window covers them; the
  # first that does not fit stays at the head and counts another iteration.
  defp admit(%__MODULE__{} = s) do
    case :queue.out(s.waiters) do
      {:empty, _} ->
        s

      {{:value, {from, bytes}}, rest} when bytes <= s.available ->
        GenServer.reply(from, :ok)
        admit(%{s | available: s.available - bytes, waiters: rest})

      {{:value, _}, _} ->
        parked(s)
        s
    end
  end

  defp release(%__MODULE__{} = s) do
    Enum.each(:queue.to_list(s.waiters), fn {from, _bytes} -> GenServer.reply(from, :ok) end)
    %{s | waiters: :queue.new()}
  end

  defp parked(%__MODULE__{counters: nil}), do: :ok
  defp parked(%__MODULE__{counters: counters}), do: Counters.inc_credit_parked(counters)
end
