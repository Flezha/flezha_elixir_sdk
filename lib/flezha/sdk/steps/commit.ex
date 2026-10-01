defmodule Flezha.SDK.Steps.Commit do
  @moduledoc """
  Everything a driver sends upward: the two durable barriers and their acks,
  the park record and the terminal frame.

  It sits beside the driver rather than on the runner for the reason
  `Flezha.SDK.Operator.Inbox` does: a driver reaching upward names this module
  and not the runner, so the two do not name each other. The runner answers
  every one of these in `handle_cast/2`.

  `task/4` and `iter/4` are journal-then-ack: the runner ships the barrier and
  the controller replies only once the record is durable, so the driver starts
  the next tasklet on the reply and a crash between them re-runs the
  uncommitted one. A refused ack rides back in the reply's `error`.
  """

  alias Flezha.SDK.Codec

  @doc """
  The Cells a stage (or its undo) ran, by identity, each with the effect
  records it wrote there as its barriers reported them (`written`, per stage
  and Cell); a Cell that wrote none is named with no records.
  """
  @spec cells_ran(map(), reference(), [binary()]) :: [map()]
  def cells_ran(written, node_ref, identities) do
    by_cell = Map.get(written, node_ref, %{})

    Enum.map(identities, fn id ->
      Map.get(by_cell, id, %{cell_id: id, first_seq: nil, seq: nil, channels: []})
    end)
  end

  @doc "Ship `TaskCommit` for one completed stage; the ack reaches `driver` as `{:barrier_ack, ref, result, error}`."
  @spec task(pid(), pid(), reference(), keyword()) :: tuple()
  def task(runner, driver, ref, fields) do
    command =
      Codec.task_commit_command(
        Keyword.fetch!(fields, :operator_id),
        Keyword.fetch!(fields, :task_idx),
        Keyword.fetch!(fields, :iter),
        Keyword.fetch!(fields, :cells_ran),
        Keyword.fetch!(fields, :handoff),
        Keyword.get(fields, :compensating, false),
        Keyword.get(fields, :completes, false),
        Keyword.get(fields, :result, <<>>),
        Keyword.get(fields, :trigger)
      )

    barrier(runner, driver, ref, command)
    command
  end

  @doc """
  Ship `IterCommit` for one finished iteration. `entry_channel` names the
  chain the iteration ran, which a resumed chained loop re-enters;
  `completes` marks the iteration a tasklet ended the instance in, and
  `result` is what it ends with, so a resume after this commit ends the
  instance rather than waiting. `dead_letter` (`tasklet`, `attempts`,
  `error`, `cell_key`) marks an iteration whose turn failed: the controller
  parks its message before it commits it.
  """
  @spec iter(pid(), pid(), reference(), keyword()) :: tuple()
  def iter(runner, driver, ref, fields) do
    command =
      Codec.iter_commit_command(
        Keyword.fetch!(fields, :operator_id),
        Keyword.fetch!(fields, :iter),
        Keyword.fetch!(fields, :consumed_offset),
        Keyword.fetch!(fields, :handoff),
        Keyword.get(fields, :completes, false),
        Keyword.get(fields, :result, <<>>),
        Keyword.get(fields, :entry_channel, ""),
        Keyword.get(fields, :dead_letter)
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
  Ship `IterPark`: an instance of either run asks to sleep (a loop that found
  its inbox empty at a committed boundary, a flow operator at iteration `0`) WITHOUT a
  terminal frame, which is not an outcome. The controller releases every
  input-log offset up to `consumed_offset`, the last message the run parked
  unread (`nil` when it parked none).
  """
  @spec park(pid(), String.t(), non_neg_integer(), non_neg_integer(), non_neg_integer() | nil) ::
          :ok
  def park(runner, operator_id, iter, wake_deadline_ms, consumed_offset),
    do:
      notify(
        runner,
        Codec.iter_park_command(operator_id, iter, wake_deadline_ms, consumed_offset)
      )

  @doc "Ship `OperatorCompleted`: the run reached an exit."
  @spec completed(pid(), String.t(), String.t(), String.t(), binary()) :: :ok
  def completed(runner, operator_id, status, error, result),
    do: notify(runner, Codec.operator_completed_command(operator_id, status, error, result))

  @doc """
  Ship one saga marker on the runner's record wire. Presence is the signal:
  the controller decodes the identity and persists a per-Cell marker file,
  which is what a rehydrating bind reads back onto its restore frames.
  """
  @spec record(pid(), String.t(), binary()) :: :ok
  def record(runner, payload_id, frame),
    do: GenServer.cast(runner, {:record, payload_id, frame})

  defp barrier(runner, driver, ref, command),
    do: GenServer.cast(runner, {:barrier, driver, ref, command})

  defp notify(runner, command), do: GenServer.cast(runner, {:notify, command})
end
