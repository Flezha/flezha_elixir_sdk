defmodule Flezha.SDK.BoundedOperator do
  @moduledoc """
  A bounded operator, declared with `use` (see `__using__/1`), which defines
  this behaviour's zero-arity callbacks from its options and imports the channel
  builder. The author writes `channels/1`, as an unbounded operator does: a DAG
  of Tasklets, Channels and Cells authored on the same builder token. A
  tasklet ends its instance with `complete/2`, which `use Flezha.SDK.Tasklet`
  imports.

  Every knob below is optional and reads its default through this module's
  reader functions, so a class states only what it departs from. The values are
  frozen at registration: `Flezha.SDK.Bounded.Spec.build/1` reads them once,
  refuses the combinations that cannot run, and hands the driver a settled
  fact.
  """

  alias Flezha.SDK.Tasklet

  # The scalar settings `use` takes, each named after the callback it defines.
  @use_options [
    :description,
    :max_duration_ms,
    :self_terminating?,
    :loop_mode,
    :rollback_mode,
    :persistence_enabled,
    :sleep_when_idle,
    :sync_durability
  ]

  # The values the two mode options take; any other is refused where it is written.
  @modes [
    loop_mode: [:once, :per_input, :chained],
    rollback_mode: [:all_or_nothing, :keep_progress]
  ]

  @doc """
  Write a bounded operator as a declaration rather than as one-line callbacks.

      use Flezha.SDK.BoundedOperator, factory_id: "checkout", loop_mode: :chained

      @impl true
      def channels(c) do
        c |> input("in") |> into(Reserve) |> into(Charge) |> sink("receipts")
      end

  `factory_id` is required; every knob above is optional, and **only the
  options written define anything**, so a default stays where the reader
  functions resolve it. `channels/1` takes an argument and stays a `def` the
  author writes. An unknown option is a `CompileError` naming it, and so is a
  `loop_mode` or `rollback_mode` outside the values their types name. A retry
  ladder is not a class setting: it is the edge's `retry_max/2` and
  `retry_backoff_ms/2`, as on an unbounded operator.

  `Flezha.SDK.Channels` is imported.

  A module that writes its one tasklet's `run/3` instead of `channels/1`
  is the §5 short form, a `once` bounded operator that runs its tasklet on its first
  message unless it writes `loop_mode:`, the one class setting a short form
  takes; see `Flezha.SDK.UnboundedOperator.__using__/1`:

      use Flezha.SDK.BoundedOperator, factory_id: "place", sink: "placed", input: "order"

      def run(order, state, ctx) do
        emit(ctx, order)
        {:ok, state}
      end
  """
  defmacro __using__(opts),
    do: Tasklet.__operator_using__(opts, @use_options, __MODULE__, @modes)

  @type loop_mode :: :once | :per_input | :chained
  @type rollback_mode :: :all_or_nothing | :keep_progress

  @doc "Stable, controller-facing factory identifier."
  @callback factory_id() :: String.t()

  @doc """
  Declare a static DAG on the channel builder. Each entry channel roots a
  chain, and a message runs only the chain of the channel it arrived on. A
  tasklet becomes runnable once every producer feeding it in that chain has
  committed, so a fan-out interleaves independently and a fan-in is dispatched
  once with the accumulated batch of those producers.
  """
  @callback channels(Flezha.SDK.Channels.t()) :: any()

  @doc "Optional human description."
  @callback description() :: String.t()

  @doc "Wall-clock backstop in milliseconds; `nil` for no duration bound."
  @callback max_duration_ms() :: pos_integer() | nil

  @doc """
  Whether the class ends on its own. A class whose only possible exit is an
  external stop declares `false`, and is then refused unless it also declares
  `max_duration_ms/0`.
  """
  @callback self_terminating?() :: boolean()

  @doc "Loop shape of a structured chain."
  @callback loop_mode() :: loop_mode()

  @doc "Saga rollback scope when the operator terminates abnormally after partial progress."
  @callback rollback_mode() :: rollback_mode()

  @doc "Class-level persistence toggle; `false` keeps the instance in memory only."
  @callback persistence_enabled() :: boolean()

  @doc """
  Whether an instance waiting at a committed boundary for `FLEZHA_PARK_IDLE_MS`
  sleeps: parks, is evicted and wakes on its next message or at the earliest
  processing-time timer its tasklets registered. Defaults to the
  class's `persistence_enabled/0`; declaring it without persistence is refused
  as `SleepWithoutPersistence`.
  """
  @callback sleep_when_idle() :: boolean()

  @doc """
  Whether the controller completes the remote ship of each commit barrier
  before acknowledging it. Inert without a durability backend.
  """
  @callback sync_durability() :: boolean()

  @optional_callbacks description: 0,
                      max_duration_ms: 0,
                      self_terminating?: 0,
                      loop_mode: 0,
                      rollback_mode: 0,
                      persistence_enabled: 0,
                      sleep_when_idle: 0,
                      sync_durability: 0

  @defaults %{
    max_duration_ms: nil,
    self_terminating?: true,
    loop_mode: :once,
    rollback_mode: :all_or_nothing,
    persistence_enabled: true,
    sync_durability: false
  }

  @doc """
  The declared value of `knob`, or its default when the class is silent;
  `:sleep_when_idle` defaults to the class's persistence switch.
  """
  @spec knob(module(), atom()) :: term()
  def knob(module, :sleep_when_idle) do
    if Tasklet.exports?(module, :sleep_when_idle, 0),
      do: module.sleep_when_idle(),
      else: knob(module, :persistence_enabled)
  end

  def knob(module, knob) do
    default = Map.fetch!(@defaults, knob)
    if Tasklet.exports?(module, knob, 0), do: apply(module, knob, []), else: default
  end

  @doc "Every knob's declared value, as the spec freezes them."
  @spec knobs(module()) :: map()
  def knobs(module) do
    @defaults
    |> Map.new(fn {k, _} -> {k, knob(module, k)} end)
    |> Map.put(:sleep_when_idle, knob(module, :sleep_when_idle))
  end

  @doc false
  # The mode values, for the registration check a hand-written callback meets.
  def modes, do: @modes
end
