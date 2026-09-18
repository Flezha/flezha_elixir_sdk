defmodule Flezha.SDK.Spawn do
  @moduledoc """
  Procedure-requested spawn of a workflow instance: the request a procedure
  makes with `Flezha.SDK.Ctx.spawn_workflow/4`, the handle it gets back at
  once, and what the controller's answer resolves to.

  The request is a cast to the runner, which ships `SpawnWorkflowRequest` and
  keeps the caller as a waiter until `SpawnWorkflowResponse` arrives or the
  request's deadline passes. The answer reaches the caller as a message, so a
  handle is read once, and only inside the invocation that made it: after
  `run/3` returns the Entity drops what arrives. A failure nobody read is
  logged once, as a warning, by the process that answered it.

  Nothing here faults the invocation: every refusal resolves the handle. The
  contract is `docs/sdk/procedure_requested_spawn.md`.
  """

  require Logger

  alias Flezha.SDK.Knob

  @enforce_keys [:reference_id, :instance_id, :factory_id]
  defstruct [
    :reference_id,
    :instance_id,
    :factory_id,
    :runner,
    :deadline,
    :result,
    claimed?: false
  ]

  @typedoc """
  A request's handle. `instance_id` is the caller's id, or `""` when the
  controller is to generate one. `result` is set when the answer was known
  without asking: a replay, or no runner. `deadline` is a monotonic millisecond
  shared with the runner, which settles the request there. `claimed?` is read
  on the runner's copy: whether a reader asked for the answer before it arrived.
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

  @type result ::
          {:ok, %{id: String.t(), outcome: :spawned | :delivered | :skipped_on_replay}}
          | {:error,
             {:already_running, String.t()}
             | {:factory_unknown, String.t()}
             | {:refused, String.t()}
             | :timeout
             | :unavailable}

  @doc false
  @spec request(pid() | nil, boolean(), String.t(), binary(), [option()]) :: t()
  def request(runner, replay?, factory_id, payload, opts)
      when is_binary(factory_id) and is_binary(payload) and is_list(opts) do
    {instance_id, if_running} = options!(opts)

    handle = %__MODULE__{
      reference_id: "spawn-workflow-#{instance_id}-#{System.unique_integer([:positive])}",
      instance_id: instance_id,
      factory_id: factory_id
    }

    cond do
      replay? ->
        %{handle | result: {:ok, %{id: instance_id, outcome: :skipped_on_replay}}}

      runner == nil ->
        %{handle | result: {:error, :unavailable}}

      true ->
        deadline = System.monotonic_time(:millisecond) + Knob.get(:sdk_spawn_reply_ms)
        handle = %{handle | runner: runner, deadline: deadline}
        GenServer.cast(runner, {:spawn, self(), handle, payload, if_running})
        handle
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
              "spawn_workflow: `if_running` needs an `id`; without one every spawn starts a new instance"

      {id, _policy} when not is_binary(id) or id == "" ->
        raise ArgumentError,
              "spawn_workflow: `id` must be a non-empty string, got: #{inspect(id)}"

      {id, policy} when policy in [nil, :deliver, :error] ->
        {id, policy || :deliver}

      {_id, policy} ->
        raise ArgumentError,
              "spawn_workflow: `if_running` must be :deliver or :error, got: #{inspect(policy)}"
    end
  end

  @doc """
  Wait for the answer, up to `timeout` milliseconds and never past the
  request's deadline; `{:error, :timeout}` when nothing arrived. The answer is
  taken from the caller's mailbox, so a second read waits out what is left of
  the deadline.
  """
  @spec await(t(), non_neg_integer()) :: result()
  def await(handle, timeout \\ Knob.get(:sdk_spawn_reply_ms))

  def await(%__MODULE__{result: nil} = handle, timeout)
      when is_integer(timeout) and timeout >= 0 do
    left = max(handle.deadline - System.monotonic_time(:millisecond), 0)
    take(handle, min(timeout, left), {:error, :timeout})
  end

  def await(%__MODULE__{result: result}, _timeout), do: result

  @doc "The answer if it has arrived, else `:pending`, without waiting."
  @spec try_result(t()) :: result() | :pending
  def try_result(%__MODULE__{result: nil} = handle), do: take(handle, 0, :pending)
  def try_result(%__MODULE__{result: result}), do: result

  @doc false
  # The one place an answer is sent, so the reply and the unread warning cannot
  # disagree: the runner server's settle, its refusal with no control plane and
  # local mode all answer through here. A controller's reply is its outcome word
  # and the instance id the request resolved to.
  @spec answer(pid(), t(), {String.t(), String.t()} | :timeout | :unavailable, String.t()) ::
          :ok
  def answer(caller, %__MODULE__{} = spawn, reply, error) do
    send(caller, {:spawn_reply, spawn.reference_id, reply, error})

    with false <- spawn.claimed?,
         {:error, reason} <- interpret(spawn, reply, error) do
      Logger.warning(
        "[Flezha.SDK] spawn_workflow(factory `#{spawn.factory_id}`, " <>
          "id `#{spawn.instance_id}`) failed: " <> inspect(reason)
      )
    end

    :ok
  end

  # The claim goes first, so the runner does not also log a failure this reader
  # is about to see.
  defp take(%__MODULE__{reference_id: id} = handle, wait, nothing) do
    GenServer.cast(handle.runner, {:claim_spawn, id})

    receive do
      {:spawn_reply, ^id, reply, error} -> interpret(handle, reply, error)
    after
      wait -> nothing
    end
  end

  # As the reference maps a reply: an error is a refusal whatever the outcome
  # says, a success carries the id the controller resolved, `already_running`
  # and `factory_unknown` name the id they are about, and a word this SDK does
  # not know is a refusal naming it.
  defp interpret(_spawn, reply, _error) when reply in [:timeout, :unavailable],
    do: {:error, reply}

  defp interpret(_spawn, _reply, error) when error != "", do: {:error, {:refused, error}}
  defp interpret(_spawn, {"spawned", id}, _error), do: {:ok, %{id: id, outcome: :spawned}}
  defp interpret(_spawn, {"delivered", id}, _error), do: {:ok, %{id: id, outcome: :delivered}}

  defp interpret(spawn, {"already_running", _id}, _error),
    do: {:error, {:already_running, spawn.instance_id}}

  defp interpret(spawn, {"factory_unknown", _id}, _error),
    do: {:error, {:factory_unknown, spawn.factory_id}}

  defp interpret(_spawn, {other, _id}, _error),
    do: {:error, {:refused, "unknown outcome `#{other}`"}}
end
