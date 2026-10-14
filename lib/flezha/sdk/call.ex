defmodule Flezha.SDK.Call do
  @moduledoc """
  One call into one of the runner's own processes.

  A runner runs inside a host application, so a wedged runner or local run
  must refuse its caller rather than exit it. Every such call goes through
  `Flezha.SDK.SafeCall`, through this module, with a bound one of the declared
  rows names and the caller's refusal declared as data.

  **A refusal is not a cancellation.** The request stays in the target's mailbox
  and the work still runs, so a bound that fires turns "slow" into "reported
  failed, then done anyway". That is the right trade for a process that is
  wedged and the wrong one for a healthy one that is merely taking its time, so
  a call whose duration grows with the data waits a row derived from the
  Controller surface that asked, never the flat control bound.
  """

  alias Flezha.SDK.SafeCall
  alias Flezha.SDK.Knob

  @doc """
  Call one of the runner's own control processes, answering `refusal` when it
  does not reply within `sdk_control_call_ms`, or the caller's shorter `ms`.
  None of them runs author code on the answering clause, so a wait here is a
  wedged process, not a busy one.
  """
  @spec control(GenServer.server(), term(), term(), timeout()) :: term()
  def control(target, request, refusal, ms \\ Knob.get(:sdk_control_call_ms)),
    do: SafeCall.bounded(target, request, ms, on_error: refusal)
end
