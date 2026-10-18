defmodule Flezha.SDK.Spawn do
  @moduledoc """
  Tasklet-requested spawn of a steps instance: the request a tasklet makes
  with `Flezha.SDK.Ctx.spawn_steps/4`, the handle it gets back at once, and
  what the controller's answer resolves to.

  The request is `flz_core`'s, made from the invocation's live activation, and
  the handle is the core's: it is read inside the invocation that made it, and
  released when that invocation's activation completes. A failure nobody read
  is logged once by the core. Nothing here faults the invocation: every
  refusal resolves the handle. The contract is `docs/public/topics/core/workflows.md`.
  """

  alias Flezha.SDK.{Ctx, Error, Knob, Native}

  @enforce_keys [:factory_id, :instance_id]
  defstruct [:factory_id, :instance_id, :core, :handle, :result]

  @typedoc """
  A request's handle: the core's handle number, or the `result` known without
  asking (a refusal at the call).
  """
  @type t :: %__MODULE__{}

  @typedoc "What a running instance id gets: the payload, or `already_running`."
  @type if_running :: :deliver | :error

  @typedoc """
  `id:` names the instance; without it the controller generates one and the
  request always starts a new instance. `if_running:` needs an `id:` and
  defaults to `:deliver`.
  """
  @type option :: {:id, String.t()} | {:if_running, if_running()}

  @typedoc """
  What a handle resolves to: the instance reached, or the refusal, carrying
  the controller's own code (`relay.already_running`, `relay.factory_unknown`,
  ...) or the core's (`spawn.timeout`, `spawn.unavailable`).
  """
  @type result ::
          {:ok, %{id: String.t(), outcome: :spawned | :delivered | :skipped_on_replay}}
          | {:error, Error.t()}

  @doc false
  @spec request(Ctx.t(), String.t(), binary(), [option()]) :: t()
  def request(%Ctx{} = ctx, factory_id, payload, opts)
      when is_binary(factory_id) and is_binary(payload) and is_list(opts) do
    {instance_id, if_running} = options!(opts)
    handle = %__MODULE__{factory_id: factory_id, instance_id: instance_id, core: ctx.core}

    if self() != ctx.owner do
      %{handle | result: {:error, Error.new("spawn.outside_invocation", factory: factory_id)}}
    else
      ctx.core
      |> Native.spawn_request(
        ctx.token,
        factory_id,
        instance_id,
        payload,
        Atom.to_string(if_running),
        ctx.replay?,
        ctx.offset
      )
      |> Native.answered()
      |> case do
        {:error, _} = refused -> %{handle | result: refused}
        number -> %{handle | handle: number}
      end
    end
  end

  # An empty instance id is the wire's request for a generated one. A policy is
  # refused without an id, because a request without one never finds a running
  # instance to apply it to.
  defp options!(opts) do
    opts = Keyword.validate!(opts, [:id, :if_running])

    case {opts[:id], opts[:if_running]} do
      {nil, nil} ->
        {"", :deliver}

      {nil, _policy} ->
        raise ArgumentError,
              "spawn_steps: `if_running` needs an `id`; without one every spawn starts a new instance"

      {id, _policy} when not is_binary(id) or id == "" ->
        raise ArgumentError,
              "spawn_steps: `id` must be a non-empty string, got: #{inspect(id)}"

      {id, policy} when policy in [nil, :deliver, :error] ->
        {id, policy || :deliver}

      {_id, policy} ->
        raise ArgumentError,
              "spawn_steps: `if_running` must be :deliver or :error, got: #{inspect(policy)}"
    end
  end

  @doc """
  Wait for the answer, up to `timeout` milliseconds; `spawn.timeout` when
  nothing arrived.
  """
  @spec await(t(), non_neg_integer()) :: result()
  def await(handle, timeout \\ Knob.get(:sdk_spawn_reply_ms))

  def await(%__MODULE__{result: nil} = handle, timeout)
      when is_integer(timeout) and timeout >= 0 do
    case read(handle, timeout) do
      :pending ->
        {:error,
         Error.new("spawn.timeout", seconds: div(timeout, 1000), factory: handle.factory_id)}

      answer ->
        answer
    end
  end

  def await(%__MODULE__{result: result}, _timeout), do: result

  @doc "The answer if it has arrived, else `:pending`, without waiting."
  @spec try_result(t()) :: result() | :pending
  def try_result(%__MODULE__{result: nil} = handle), do: read(handle, 0)
  def try_result(%__MODULE__{result: result}), do: result

  defp read(handle, timeout) do
    case Native.answered(Native.spawn_result(handle.core, handle.handle, timeout)) do
      nil -> :pending
      {:error, _} = refused -> refused
      json -> json |> Jason.decode!() |> resolved()
    end
  end

  defp resolved(%{"id" => id, "outcome" => "spawned"}), do: {:ok, %{id: id, outcome: :spawned}}

  defp resolved(%{"id" => id, "outcome" => "delivered"}),
    do: {:ok, %{id: id, outcome: :delivered}}

  defp resolved(%{"id" => id, "outcome" => "skipped_on_replay"}),
    do: {:ok, %{id: id, outcome: :skipped_on_replay}}

  defp resolved(other),
    do: {:error, Error.new("runtime.unreadable_reply", reply: "spawn answer #{inspect(other)}")}
end
