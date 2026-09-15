defmodule Flezha.SDK.Workflow.Ctx do
  @moduledoc """
  The context a run-sugar workflow body is handed: what it emits, the result
  it completes with, and the workflow instances it asks for.

  A structured class has none of this — its procedures emit through the
  ordinary `Flezha.SDK.Ctx`, and what they release is staged and journalled at
  each stage's barrier. The run-sugar form has no procedures and no stages, so
  its output goes straight out on the runner's secondary stream, which is the
  same wire a stream operative's sink output leaves on.
  """

  alias Flezha.SDK.Spawn

  @enforce_keys [:driver]
  defstruct [:driver, :operative_id, :runner, channel: "out"]

  @type t :: %__MODULE__{}

  @doc "Emit one output. Bytes go as they are; any other term is serialized."
  @spec emit(t(), term()) :: :ok
  def emit(%__MODULE__{driver: driver} = ctx, value),
    do: GenServer.cast(driver, {:sugar_emit, ctx.channel, value})

  @doc "Emit one output on a named channel."
  @spec emit_on(t(), String.t(), term()) :: :ok
  def emit_on(%__MODULE__{driver: driver}, channel, value),
    do: GenServer.cast(driver, {:sugar_emit, channel, value})

  @doc """
  The result the instance completes with. Optional — a body that returns `:ok`
  without calling this completes with an empty result.
  """
  @spec complete(t(), binary()) :: :ok
  def complete(%__MODULE__{driver: driver}, result) when is_binary(result),
    do: GenServer.cast(driver, {:sugar_result, result})

  @doc """
  Request spawn-or-deliver of a workflow instance, as
  `Flezha.SDK.Ctx.spawn_workflow/5` does from a procedure. A body is never a
  replay, so the request is always made.
  """
  @spec spawn_workflow(t(), String.t(), String.t(), binary(), Spawn.if_running()) :: Spawn.t()
  def spawn_workflow(%__MODULE__{runner: runner}, instance_id, factory_id, payload, if_running),
    do: Spawn.request(runner, false, instance_id, factory_id, payload, if_running)
end
