defmodule Flezha.SDK.Operative.Inbox do
  @moduledoc """
  The messages an operative is sent from below: a raw frame off a bound
  primary stream, the values a Cell produced for its node's outputs, and a
  workflow tasklet's request to end its instance. They live beside the
  operative rather than on it so that a `DataStream` or a `Cell` reaching
  upward names this module and not the router itself.

  The first two are answered by `Flezha.SDK.Operative.handle_cast/2`, all three
  by `Flezha.SDK.Workflow.Driver.handle_cast/2`.
  """

  @doc "Route a raw input frame arriving on an entry channel."
  def input(pid, channel, message, metadata),
    do: GenServer.cast(pid, {:input, channel, message, metadata})

  @doc """
  A Cell finished a dispatch: its emits as `{value, metadata, key}`, the
  change seq the dispatch started at — what a keyed sink's stamps read — and
  the durable offset of the input that produced them, which an
  inter-tasklet push inherits under the effectively-once tier.

  `report` is `{cell_pid, ref}` when the Cell is holding that invocation's
  effect record until every push lands, and nil when it owes nothing.
  """
  def emitted(pid, node_ref, cell_key, emits, handout_seq, offset, report),
    do: GenServer.cast(pid, {:emitted, node_ref, cell_key, emits, handout_seq, offset, report})

  @doc """
  A Cell's invocation called `Flezha.SDK.Ctx.complete/2`. Sent after its
  emits and before its stage barrier answers, so the driver holds it by the
  time the stage settles.
  """
  def completed(pid, node_ref, cell_key, result),
    do: GenServer.cast(pid, {:complete, node_ref, cell_key, result})

  @doc """
  Release the effect record a Cell is holding for the invocation just
  routed: every push it produced has been admitted, so the record may become
  durable. A `nil` report is an invocation that owed nothing.
  """
  def delivered(nil), do: :ok
  def delivered({cell, ref}), do: send(cell, {:edges_delivered, ref})
end
