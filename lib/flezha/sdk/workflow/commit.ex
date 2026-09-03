defmodule Flezha.SDK.Workflow.Commit do
  @moduledoc """
  Everything a driver sends upward: the two durable barriers and their acks,
  the park record, the terminal frame, and a run-sugar body's output.

  It sits beside the driver rather than on the runner for the reason
  `Flezha.SDK.Operative.Inbox` does: a driver reaching upward names this module
  and not the runner, so the two do not name each other. The runner answers
  every one of these in `handle_cast/2`.

  `proc/4` and `iter/4` are journal-then-ack: the runner ships the barrier and
  the controller replies only once the record is durable, so the driver starts
  the next procedure on the reply and a crash between them re-runs the
  uncommitted one. A refused ack rides back in the reply's `error`.
  """

  alias Flezha.SDK.Codec

  @doc "Ship `ProcCommit` for one completed stage; the ack reaches `driver` as `{:barrier_ack, ref, error}`."
  @spec proc(pid(), pid(), reference(), keyword()) :: tuple()
  def proc(runner, driver, ref, fields) do
    command =
      Codec.proc_commit_command(
        Keyword.fetch!(fields, :operative_id),
        Keyword.fetch!(fields, :proc_idx),
        Keyword.fetch!(fields, :iter),
        Keyword.fetch!(fields, :entities_ran),
        Keyword.fetch!(fields, :handoff),
        Keyword.get(fields, :outbox, []),
        Keyword.get(fields, :compensating, false)
      )

    barrier(runner, driver, ref, command)
    command
  end

  @doc "Ship `IterCommit` for one finished iteration."
  @spec iter(pid(), pid(), reference(), keyword()) :: tuple()
  def iter(runner, driver, ref, fields) do
    command =
      Codec.iter_commit_command(
        Keyword.fetch!(fields, :operative_id),
        Keyword.fetch!(fields, :iter),
        Keyword.fetch!(fields, :consumed_offset),
        Keyword.fetch!(fields, :handoff)
      )

    barrier(runner, driver, ref, command)
    command
  end

  @doc """
  Re-send a barrier the controller never acked, byte for byte and under the
  same reference: it journals one by identity, so a re-send is a no-op on a
  record that did land.
  """
  @spec resend(pid(), pid(), reference(), tuple()) :: :ok
  def resend(runner, driver, ref, command), do: barrier(runner, driver, ref, command)

  @doc """
  Ship `IterPark`: the loop found its inbox empty at a committed boundary and
  tears its run down WITHOUT a terminal frame, which is not an outcome.
  """
  @spec park(pid(), String.t(), non_neg_integer(), non_neg_integer()) :: :ok
  def park(runner, operative_id, iter, wake_deadline_ms),
    do: notify(runner, Codec.iter_park_command(operative_id, iter, wake_deadline_ms))

  @doc "Ship `WorkflowCompleted`: the run reached an exit."
  @spec completed(pid(), String.t(), String.t(), String.t(), binary()) :: :ok
  def completed(runner, operative_id, status, error, result),
    do: notify(runner, Codec.workflow_completed_command(operative_id, status, error, result))

  @doc """
  Ship one saga marker on the runner's record wire. Presence is the signal:
  the controller decodes the identity and persists a per-Entity marker file,
  which is what a rehydrating bind reads back onto its restore frames.
  """
  @spec record(pid(), String.t(), binary()) :: :ok
  def record(runner, payload_id, frame),
    do: GenServer.cast(runner, {:record, payload_id, frame})

  @doc "Ship one output frame of a run-sugar body, which has no stage to stage it at."
  @spec output(pid(), String.t(), map(), binary()) :: :ok
  def output(runner, operative_id, metadata, bytes),
    do: GenServer.cast(runner, {:emit_output, operative_id, metadata, bytes})

  defp barrier(runner, driver, ref, command),
    do: GenServer.cast(runner, {:barrier, driver, ref, command})

  defp notify(runner, command), do: GenServer.cast(runner, {:notify, command})
end
