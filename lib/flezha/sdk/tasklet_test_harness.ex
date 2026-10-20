defmodule Flezha.SDK.TaskletTestHarness do
  @moduledoc """
  Run one tasklet as the dispatcher runs it, with no controller and no relay.

      h = TaskletTestHarness.new(Counter)
      {%{result: :ok, outputs: ["a=3"]}, h} = TaskletTestHarness.feed(h, "a:3")
      assert TaskletTestHarness.state(h) == %{value: 3}
      assert TaskletTestHarness.replay_equals(h) == :ok

  Each call takes the harness and answers the next one beside what the
  invocation produced: a `Flezha.SDK.TaskletTestHarness.Outcome`. An invocation
  that fails changes nothing, as a live one is discarded.

  `now/1` is `feed/3`'s `now:` when given, else what the declared clock reads:
  the input's `event_time/1`, the change count under `logical_clock`, `0`
  otherwise; inside a firing it is the scheduled time. The harness keeps the
  pending timers as one set per kind, as the core does; it does not judge them
  (the cap and the clock rule are the core's, when an item settles). An
  `effect/4` name is judged by the core: an over-long one answers
  `effect.bad_name`; a well-formed one answers `effect.outside_invocation`,
  since keeping an outside call's result needs a live activation
  (`Flezha.SDK.Local` runs one).
  """

  alias Flezha.SDK.{Ctx, Dispatch, Native, Tasklet, TimerFiring}

  defmodule Outcome do
    @moduledoc """
    What one invocation produced: `result` (`:ok`, or the `{:error, reason}`
    `run/3` or `on_timer/3` answered or raised), the values it emitted, the
    changes it recorded and the timer operations it made, in call order.
    """

    defstruct result: :ok, outputs: [], changes: [], timers: []

    @type t :: %__MODULE__{
            result: :ok | {:error, term()},
            outputs: [term()],
            changes: [term()],
            timers: [
              %{kind: TimerFiring.kind(), scheduled_at: non_neg_integer(), register: boolean()}
            ]
          }
  end

  @enforce_keys [:module, :cell_key, :initial, :state]
  defstruct [:module, :cell_key, :initial, :state, log: [], timers: MapSet.new()]

  @type t :: %__MODULE__{}

  @doc """
  A harness for the tasklet `module` on one Cell. Options: `cell_key:` (default
  nil, a Single Cell) and `state:` (default `init_state/1` of the key).
  """
  @spec new(module(), keyword()) :: t()
  def new(module, opts \\ []) do
    unless Tasklet.exports?(module, :run, 3),
      do: raise(ArgumentError, "#{inspect(module)} is not a tasklet: it exports no run/3")

    key = Keyword.get(opts, :cell_key)
    state = Keyword.get_lazy(opts, :state, fn -> module.init_state(key) end)
    %__MODULE__{module: module, cell_key: key, initial: state, state: state}
  end

  @doc "The Cell's state, as the recorded invocations left it."
  @spec state(t()) :: term()
  def state(%__MODULE__{state: state}), do: state

  @doc """
  Run `run/3` against `value`. Options: `now:` (the clock's value for this
  input) and `replay: true` (the input is replayed).
  """
  @spec feed(t(), term(), keyword()) :: {Outcome.t(), t()}
  def feed(%__MODULE__{module: module} = h, value, opts \\ []) do
    now = Keyword.get_lazy(opts, :now, fn -> clock(h, value) end)
    run(h, now, Keyword.get(opts, :replay, false), &module.run(value, &1, &2))
  end

  @doc """
  Fire the `kind` timer scheduled at `at`, pending or not, through
  `on_timer/3`; a tasklet without it answers the non-retryable error a live
  firing parks with.
  """
  @spec fire_timer(t(), TimerFiring.kind(), non_neg_integer()) :: {Outcome.t(), t()}
  def fire_timer(%__MODULE__{module: module} = h, kind, at)
      when kind in [:processing_time, :event_time] do
    h = %{h | timers: MapSet.delete(h.timers, {kind, at})}
    firing = %TimerFiring{kind: kind, scheduled_at: at}

    if Tasklet.on_timer?(module),
      do: run(h, at, false, &module.on_timer(firing, &1, &2)),
      else: {%Outcome{result: Dispatch.unhandled_firing(module)}, h}
  end

  @doc """
  Fire each pending event-time timer at or before `to`, earliest first; what a
  firing registers or cancels counts, as it does live.
  """
  @spec advance_event_time(t(), non_neg_integer()) :: {[Outcome.t()], t()}
  def advance_event_time(%__MODULE__{} = h, to), do: advance(h, to, [])

  defp advance(h, to, outcomes) do
    due = for {:event_time, at} <- h.timers, at <= to, do: at

    case due do
      [] ->
        {Enum.reverse(outcomes), h}

      _ ->
        {outcome, h} = fire_timer(h, :event_time, Enum.min(due))
        advance(h, to, [outcome | outcomes])
    end
  end

  @doc """
  Fold every recorded change through `apply_change/2` from the initial state:
  `:ok` when it rebuilds the live state, else `{:mismatch, live, replayed}`.
  A mismatch is an `apply_change/2` that is not a pure fold, or a state
  changed without a recorded change.
  """
  @spec replay_equals(t()) :: :ok | {:mismatch, term(), term()}
  def replay_equals(%__MODULE__{module: module, initial: initial, state: live} = h) do
    replayed =
      if Tasklet.changeful?(module),
        do: h.log |> Enum.reverse() |> Enum.reduce(initial, &module.apply_change(&2, &1)),
        else: initial

    if replayed == live, do: :ok, else: {:mismatch, live, replayed}
  end

  defp clock(%__MODULE__{module: module} = h, value) do
    case Tasklet.clock(module) do
      :event_time -> module.event_time(value)
      :logical -> length(h.log)
      :wall -> 0
    end
  end

  defp run(%__MODULE__{module: module} = h, now, replay?, body) do
    ctx =
      Ctx.new(module, h.cell_key, now,
        core: core(),
        token: 0,
        seq: length(h.log),
        replay: replay?,
        identity: "harness/#{module.name()}/#{h.cell_key}"
      )

    case Dispatch.invoke(fn -> body.(h.state, ctx) end) do
      {:ok, state} ->
        outcome = %Outcome{
          outputs: for({value, _meta, _key} <- Ctx.take_emits(ctx), do: value),
          changes: Ctx.take_changes(ctx),
          timers: Ctx.take_timers(ctx)
        }

        Ctx.discard(ctx)
        timers = Enum.reduce(outcome.timers, h.timers, &pending/2)
        {outcome, %{h | state: state, log: Enum.reverse(outcome.changes, h.log), timers: timers}}

      error ->
        Ctx.discard(ctx)
        {%Outcome{result: error}, h}
    end
  end

  defp pending(%{kind: kind, scheduled_at: at, register: true}, set),
    do: MapSet.put(set, {kind, at})

  defp pending(%{kind: kind, scheduled_at: at, register: false}, set),
    do: MapSet.delete(set, {kind, at})

  # One core context per BEAM, never begun: it holds no run, and its
  # `effect_begin` judges an effect's name before it looks for the activation.
  defp core do
    with nil <- :persistent_term.get({__MODULE__, :core}, nil) do
      core = Native.context_new(Native.schema_version())
      :persistent_term.put({__MODULE__, :core}, core)
      core
    end
  end
end
