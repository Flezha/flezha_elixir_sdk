defmodule Flezha.SDK.Call do
  @moduledoc """
  One call into one of the runner's own processes.

  A runner runs inside a host application, so a wedged Entity, Operative,
  RunnerServer, local run or entry-idle watcher must refuse its caller rather
  than exit it. Every such call goes through `Flezha.SafeCall` with a bound one
  of the declared rows names and the caller's refusal declared as data — and
  through this module, so the packaging boundary crosses in one file
  (`scripts/check-sdk-boundary.sh`).

  **A refusal is not a cancellation.** The request stays in the target's mailbox
  and the work still runs, so a bound that fires turns "slow" into "reported
  failed, then done anyway". That is the right trade for a process that is
  wedged and the wrong one for a healthy one that is merely taking its time, so
  a call whose duration grows with the data waits a row derived from the
  Controller surface that asked, never the flat control bound.

  The two calls that wait with no bound at all are not here: the `:drop_nothing`
  enqueue and the credit window's acquire are protocol waits, where the target
  is deliberately not answering until a condition holds.
  """

  alias Flezha.SafeCall
  alias Flezha.SDK.Knob

  # What an Entity that will not answer tells its caller: the token the
  # Controller matches as a prefix.
  @silent {:error, "entity_busy: the Entity did not answer"}

  @doc """
  Call one of the runner's own control processes, answering `refusal` when it
  does not reply within `sdk_control_call_ms`. None of them runs author code on
  the answering clause, so a wait here is a wedged process, not a busy one.
  """
  @spec control(GenServer.server(), term(), term()) :: term()
  def control(target, request, refusal),
    do: SafeCall.bounded(target, request, Knob.get(:sdk_control_call_ms), on_error: refusal)

  @doc """
  Call an Entity, which may be mid-dispatch.

  `:timeout` names the bound for a call whose duration grows with the state
  rather than with the queue in front of it; every other option is
  `Flezha.SafeCall.bounded/4`'s, and the refusal defaults to the `entity_busy`
  token.
  """
  @spec entity(pid(), term(), keyword()) :: term()
  def entity(pid, request, opts \\ []) do
    {ms, opts} = Keyword.pop_lazy(opts, :timeout, fn -> Knob.get(:sdk_entity_call_ms) end)
    SafeCall.bounded(pid, request, ms, Keyword.put_new(opts, :on_error, @silent))
  end
end
