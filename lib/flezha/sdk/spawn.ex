defmodule Flezha.SDK.Spawn do
  @moduledoc """
  Tasklet-requested spawn of a steps instance: the request a tasklet
  makes with `Flezha.SDK.Ctx.spawn_steps/4`, the handle it gets back at
  once, and what the controller's answer resolves to.

  The request is a cast to the runner, which ships `SpawnStepsRequest` and
  keeps the caller as a waiter until `SpawnStepsResponse` arrives or the
  request's deadline passes. The answer reaches the caller as a message, so a
  handle is read once, and only inside the invocation that made it: after
  `run/3` returns the Cell drops what arrives. A failure nobody read is
  logged once, as a warning, by the process that answered it.

  Nothing here faults the invocation: every refusal resolves the handle. The
  contract is `docs/sdk/tasklet_requested_spawn.md`.
  """

  require Logger

  alias Flezha.SDK.Error
  alias Flezha.SDK.Knob

  @enforce_keys [:reference_id, :instance_id, :factory_id]
  defstruct [
    :reference_id,
    :instance_id,
    :factory_id,
    :runner,
    :deadline,
    :result,
    identity: "",
    claimed?: false
  ]

  @typedoc """
  A request's handle. `instance_id` is the caller's id, or `""` when the
  controller is to generate one; `identity` names the request across a re-run
  of the input that asked, `""` for none. `result` is set when the answer was known
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

  @typedoc """
  What a handle resolves to: the instance reached, or the refusal, carrying
  the controller's own code (`operator.already_running`,
  `operator.factory_unknown`, `operator.factory_not_steps`, ...) or one the
  runner decides itself: `spawn.timeout`, `spawn.unavailable`.
  """
  @type result ::
          {:ok, %{id: String.t(), outcome: :spawned | :delivered | :skipped_on_replay}}
          | {:error, Error.t()}

  @doc false
  @spec request(pid() | nil, boolean(), String.t(), binary(), [option()], String.t()) :: t()
  def request(runner, replay?, factory_id, payload, opts, identity)
      when is_binary(factory_id) and is_binary(payload) and is_list(opts) do
    {instance_id, if_running} = options!(opts)

    handle = %__MODULE__{
      reference_id: "spawn-steps-#{instance_id}-#{System.unique_integer([:positive])}",
      instance_id: instance_id,
      factory_id: factory_id,
      identity: identity
    }

    cond do
      replay? ->
        %{handle | result: {:ok, %{id: instance_id, outcome: :skipped_on_replay}}}

      runner == nil ->
        %{handle | result: {:error, refused(handle, "spawn.unavailable")}}

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
  Wait for the answer, up to `timeout` milliseconds and never past the
  request's deadline; `spawn.timeout` when nothing arrived. The answer is
  taken from the caller's mailbox, so a second read waits out what is left of
  the deadline.
  """
  @spec await(t(), non_neg_integer()) :: result()
  def await(handle, timeout \\ Knob.get(:sdk_spawn_reply_ms))

  def await(%__MODULE__{result: nil} = handle, timeout)
      when is_integer(timeout) and timeout >= 0 do
    left = max(handle.deadline - System.monotonic_time(:millisecond), 0)
    take(handle, min(timeout, left), {:error, refused(handle, "spawn.timeout")})
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
  @spec answer(pid(), t(), {String.t(), String.t()} | :timeout | :unavailable, Error.t() | nil) ::
          :ok
  def answer(caller, %__MODULE__{} = spawn, reply, error) do
    send(caller, {:spawn_reply, spawn.reference_id, reply, error})

    with false <- spawn.claimed?,
         {:error, refusal} <- interpret(spawn, reply, error) do
      Logger.warning(
        "[Flezha.SDK] spawn_steps(factory `#{spawn.factory_id}`, " <>
          "id `#{spawn.instance_id}`) failed: #{refusal.message} (#{refusal.code})"
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

  # As the reference maps a reply: the controller's error is the refusal
  # whatever the outcome says, a success carries the id the controller
  # resolved, and a word this SDK does not know is an unreadable reply.
  defp interpret(spawn, :timeout, _error), do: {:error, refused(spawn, "spawn.timeout")}
  defp interpret(spawn, :unavailable, _error), do: {:error, refused(spawn, "spawn.unavailable")}
  defp interpret(_spawn, _reply, %Error{} = error), do: {:error, error}
  defp interpret(_spawn, {"spawned", id}, nil), do: {:ok, %{id: id, outcome: :spawned}}
  defp interpret(_spawn, {"delivered", id}, nil), do: {:ok, %{id: id, outcome: :delivered}}

  defp interpret(_spawn, {other, _id}, nil),
    do: {:error, Error.new("runtime.unreadable_reply", reply: "spawn outcome '#{other}'")}

  # The two refusals the runner decides itself, naming the request.
  defp refused(spawn, code) do
    Error.new(code,
      factory: spawn.factory_id,
      instance: spawn.instance_id,
      seconds: div(Knob.get(:sdk_spawn_reply_ms), 1000)
    )
  end
end
