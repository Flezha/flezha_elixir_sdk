defmodule Flezha.SDK.WorkflowOperative do
  @moduledoc """
  A workflow operative, declared with `use` (see `__using__/1`), which defines
  this behaviour's zero-arity callbacks from its options and imports the channel
  builder. The author writes `tasklets/1`: a DAG of Tasklets, Channels and
  Cells authored on the same builder token a stream operative uses. A
  tasklet ends its instance with `Flezha.SDK.Ctx.complete/2`.

  Every knob below is optional and reads its default through this module's
  reader functions, so a class states only what it departs from. The values are
  frozen at registration: `Flezha.SDK.Workflow.Spec.build/1` reads them once,
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
    :wake_after_ms,
    :retry_attempts,
    :retry_backoff_ms,
    :barrier_retry_attempts,
    :rollback_mode,
    :persistence_enabled,
    :sync_durability
  ]

  @doc """
  Write a workflow operative as a declaration rather than as one-line callbacks.

      use Flezha.SDK.WorkflowOperative, factory_id: "checkout", loop_mode: :chained

      @impl true
      def tasklets(c) do
        c |> input("in") |> into(Reserve) |> into(Charge) |> sink("receipts")
      end

  `factory_id` is required; every knob above is optional, and **only the
  options written define anything**, so a default stays where the reader
  functions resolve it. `tasklets/1` takes an argument and stays a `def` the
  author writes. An unknown option is a `CompileError` naming it.

  `Flezha.SDK.Channels` is imported.

  A module that writes its one tasklet's `run/3` instead of `tasklets/1`
  is the §5 short form, a `once` workflow whose entry channel receives the
  start values; see `Flezha.SDK.StreamOperative.__using__/1`:

      use Flezha.SDK.WorkflowOperative, factory_id: "place", sink: "placed", input: "order"

      def run(order, state, ctx) do
        emit(ctx, order)
        {:ok, state}
      end
  """
  defmacro __using__(opts), do: Tasklet.__operative_using__(opts, @use_options, __MODULE__)

  @type loop_mode :: :once | :per_input | :chained
  @type rollback_mode :: :all_or_nothing | :keep_progress

  @doc "Stable, controller-facing factory identifier."
  @callback factory_id() :: String.t()

  @doc """
  Declare a static DAG on the channel builder. A tasklet becomes runnable
  only once every producer feeding it has committed, so a fan-out interleaves
  independently and a fan-in is dispatched once with the accumulated batch of
  all its producers.
  """
  @callback tasklets(Flezha.SDK.Channels.t()) :: any()

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

  @doc """
  Durable wake deadline for a `:per_input` loop: how long after parking the
  instance is rehydrated with an empty entry payload even with no input.
  """
  @callback wake_after_ms() :: pos_integer() | nil

  @doc "Total attempts a structured tasklet's cell gets, the first included."
  @callback retry_attempts() :: non_neg_integer()

  @doc "Backoff base of the retry ladder; attempt n waits base × 2^(n-1), jittered."
  @callback retry_backoff_ms() :: non_neg_integer()

  @doc "Sends of one commit barrier, the first included, before an unacked commit fails the run."
  @callback barrier_retry_attempts() :: non_neg_integer()

  @doc "Saga rollback scope when the operative terminates abnormally after partial progress."
  @callback rollback_mode() :: rollback_mode()

  @doc "Class-level persistence toggle; `false` keeps the instance in memory only."
  @callback persistence_enabled() :: boolean()

  @doc """
  Whether the controller completes the remote ship of each commit barrier
  before acknowledging it. Inert without a durability backend.
  """
  @callback sync_durability() :: boolean()

  @optional_callbacks description: 0,
                      max_duration_ms: 0,
                      self_terminating?: 0,
                      loop_mode: 0,
                      wake_after_ms: 0,
                      retry_attempts: 0,
                      retry_backoff_ms: 0,
                      barrier_retry_attempts: 0,
                      rollback_mode: 0,
                      persistence_enabled: 0,
                      sync_durability: 0

  @defaults %{
    max_duration_ms: nil,
    self_terminating?: true,
    loop_mode: :once,
    wake_after_ms: nil,
    retry_attempts: 3,
    retry_backoff_ms: 500,
    barrier_retry_attempts: 3,
    rollback_mode: :all_or_nothing,
    persistence_enabled: true,
    sync_durability: false
  }

  @doc "The declared value of `knob`, or its default when the class is silent."
  @spec knob(module(), atom()) :: term()
  def knob(module, knob) do
    default = Map.fetch!(@defaults, knob)
    if Tasklet.exports?(module, knob, 0), do: apply(module, knob, []), else: default
  end

  @doc "Whether the class declares its tasklet DAG."
  @spec structured?(module()) :: boolean()
  def structured?(module), do: Tasklet.exports?(module, :tasklets, 1)

  @doc "Every knob's declared value, as the spec freezes them."
  @spec knobs(module()) :: map()
  def knobs(module), do: Map.new(@defaults, fn {k, _} -> {k, knob(module, k)} end)
end
