defmodule Flezha.SDK.EntryIdle do
  @moduledoc """
  Idle observation for entry channels: the policy half of the input-liveness
  signal `Flezha.SDK.EntryInput` records.

  The SDK knows when a frame last arrived; only the application knows whether
  the silence since then is anomalous — a quiet overnight market is not a dead
  feed. So the SDK ships no default threshold and no default action:
  `Flezha.SDK.on_entry_idle/4` takes both from the caller and reports episodes.

  One process per runner, linked to its RunnerServer, holding the
  registrations. The episode latch is `evaluate/2`, pure — the state machine
  every SDK runs over its delivery stamp; the tick is a `:tick` message at a
  quarter of the shortest threshold, clamped to `[250 ms, 1 s]`, which the BEAM
  delivers on time whatever an Entity is doing; delivery runs under
  `try/rescue/catch`, so a raising observer becomes one warning and the tick
  goes on.
  """

  use GenServer
  require Logger

  alias Flezha.SDK.EntryInput

  @tick_min_ms 250
  @tick_max_ms 1_000

  defmodule Watch do
    @moduledoc false
    # `fired_at` is the delivery stamp the current episode fired against, nil
    # while armed. Comparing stamps, not clock readings, is what makes a resume
    # exact: the stamp moves if and only if a frame arrived.
    defstruct [:channel, :cell, :threshold_ns, :observer, fired_at: nil]

    @type t :: %__MODULE__{}
  end

  @type observer :: (non_neg_integer() -> any()) | module()
  @type episode :: {:idle, non_neg_integer()} | {:resumed, non_neg_integer()}

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts)

  @doc "Register `observer` against `channel`; `cell` is the channel's interned cell."
  @spec register(GenServer.server(), String.t(), EntryInput.cell(), pos_integer(), observer()) ::
          :ok
  def register(server, channel, cell, threshold_ms, observer),
    do: GenServer.call(server, {:register, watch(channel, cell, threshold_ms, observer)})

  @doc "A watch over `cell`, armed."
  @spec watch(String.t(), EntryInput.cell(), pos_integer(), observer()) :: Watch.t()
  def watch(channel, cell, threshold_ms, observer) do
    %Watch{
      channel: channel,
      cell: cell,
      threshold_ns: threshold_ms * 1_000_000,
      observer: observer
    }
  end

  @doc """
  The tick a threshold asks for: a quarter of it, so an episode is reported
  within 25% of the threshold, bounded at both ends.
  """
  @spec tick_period_ms(pos_integer()) :: pos_integer()
  def tick_period_ms(threshold_ms),
    do: threshold_ms |> div(4) |> max(@tick_min_ms) |> min(@tick_max_ms)

  @doc """
  Advance a watch's latch to `now_nanos`: the watch, and the boundary it
  crossed if any. A cold start — no frame ever — is never an episode.
  """
  @spec evaluate(Watch.t(), non_neg_integer()) :: {Watch.t(), episode() | nil}
  def evaluate(%Watch{} = w, now_nanos) do
    case {:atomics.get(w.cell, 1), w.fired_at} do
      {0, _} -> {w, nil}
      {last, nil} -> arm_or_fire(w, last, max(now_nanos - last, 0))
      {last, last} -> {w, nil}
      {last, fired_at} -> {%{w | fired_at: nil}, {:resumed, max(last - fired_at, 0)}}
    end
  end

  defp arm_or_fire(%Watch{threshold_ns: threshold} = w, _last, gap) when gap <= threshold,
    do: {w, nil}

  defp arm_or_fire(w, last, gap), do: {%{w | fired_at: last}, {:idle, gap}}

  # --- server ---

  @impl true
  def init(_opts), do: {:ok, %{watches: [], period_ms: nil}}

  @impl true
  def handle_call({:register, %Watch{} = watch}, _from, s) do
    wanted = tick_period_ms(div(watch.threshold_ns, 1_000_000))
    # The first registration starts the tick; a later one only shortens it.
    if s.period_ms == nil, do: schedule(wanted)
    period = if s.period_ms, do: min(s.period_ms, wanted), else: wanted
    {:reply, :ok, %{s | watches: s.watches ++ [watch], period_ms: period}}
  end

  @impl true
  def handle_info(:tick, s) do
    now = EntryInput.now_nanos()

    {watches, found} =
      Enum.map_reduce(s.watches, [], fn watch, found ->
        case evaluate(watch, now) do
          {w, nil} -> {w, found}
          {w, episode} -> {w, [{w, episode} | found]}
        end
      end)

    found |> Enum.reverse() |> Enum.each(fn {w, episode} -> deliver(w, episode) end)
    schedule(s.period_ms)
    {:noreply, %{s | watches: watches}}
  end

  defp schedule(period_ms), do: Process.send_after(self(), :tick, period_ms)

  # Invoke the observer, contained: application code runs in the SDK's own
  # process, so a raise must become a log line rather than a dead ticker.
  defp deliver(%Watch{} = w, {kind, gap_ns}) do
    invoke(w.observer, kind, div(gap_ns, 1_000_000))
  rescue
    e -> warn(w, kind, Exception.message(e))
  catch
    class, reason -> warn(w, kind, "#{class} #{inspect(reason)}")
  end

  defp invoke(fun, :idle, gap_ms) when is_function(fun, 1), do: fun.(gap_ms)
  defp invoke(fun, :resumed, _gap_ms) when is_function(fun, 1), do: :ok
  defp invoke(module, :idle, gap_ms) when is_atom(module), do: module.idle(gap_ms)

  defp invoke(module, :resumed, gap_ms) when is_atom(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, :resumed, 1),
      do: module.resumed(gap_ms),
      else: :ok
  end

  defp warn(%Watch{channel: channel}, kind, reason) do
    Logger.warning(
      "[Flezha.SDK] entry-idle observer raised on the #{kind} episode of channel " <>
        "'#{channel}': #{reason}"
    )
  end
end
