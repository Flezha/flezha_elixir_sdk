defmodule Flezha.SDK.WorkflowOperative do
  @moduledoc """
  Behaviour an Elixir workflow operative implements. The bare `run/2` form is
  the trivial single-Procedure, single-Entity case; declaring `procedures/1`
  instead grows the class into an explicit DAG of Procedures, Channels and
  Entities authored on the same builder token a stream operative uses. A class
  declares one or the other, never both.

  Every knob below is optional and reads its default through this module's
  reader functions, so a class states only what it departs from. The values are
  frozen at registration: `Flezha.SDK.Workflow.Spec.build/1` reads them once,
  refuses the combinations that cannot run, and hands the driver a settled
  fact — a class already written against this vocabulary keeps working.
  """

  alias Flezha.SDK.Procedure

  @type loop_mode :: :once | :per_input | :chained
  @type rollback_mode :: :all_or_nothing | :keep_progress

  @doc "Stable, controller-facing factory identifier."
  @callback factory_id() :: String.t()

  @doc """
  Run-sugar body, invoked once per started instance with the spawn `args`.
  Emit through the ctx and return `:ok`; `{:error, reason}` is a terminal
  failure. A structured class never has this invoked.
  """
  @callback run(ctx :: term(), args :: String.t()) :: :ok | {:error, term()}

  @doc """
  Structured form: declare a static DAG on the channel builder. A procedure
  becomes runnable only once every producer feeding it has committed, so a
  fan-out interleaves independently and a fan-in is dispatched once with the
  accumulated batch of all its producers.
  """
  @callback procedures(Flezha.SDK.Channels.t()) :: any()

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

  @doc "Total attempts a structured procedure's entity gets, the first included."
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

  @optional_callbacks run: 2,
                      procedures: 1,
                      description: 0,
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
    if Procedure.exports?(module, knob, 0), do: apply(module, knob, []), else: default
  end

  @doc "Whether the class declares a structured procedure DAG."
  @spec structured?(module()) :: boolean()
  def structured?(module), do: Procedure.exports?(module, :procedures, 1)

  @doc "Whether the class declares a run-sugar body."
  @spec run_sugar?(module()) :: boolean()
  def run_sugar?(module), do: Procedure.exports?(module, :run, 2)

  @doc "Every knob's declared value, as the spec freezes them."
  @spec knobs(module()) :: map()
  def knobs(module), do: Map.new(@defaults, fn {k, _} -> {k, knob(module, k)} end)
end
