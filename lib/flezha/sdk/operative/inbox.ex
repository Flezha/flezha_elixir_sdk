defmodule Flezha.SDK.Operative.Inbox do
  @moduledoc """
  The two messages an operative is sent from below: a raw frame off a bound
  primary stream, and the values an Entity produced for its node's outputs.
  They live beside the operative rather than on it so that a `DataStream` or an
  `Entity` reaching upward names this module and not the router itself.

  Both casts are answered by `Flezha.SDK.Operative.handle_cast/2`.
  """

  @doc "Route a raw input frame arriving on an entry channel."
  def input(pid, channel, message, metadata),
    do: GenServer.cast(pid, {:input, channel, message, metadata})

  @doc """
  An Entity finished a dispatch: its emits as `{value, metadata, key}`, the
  change seq the dispatch started at — what a keyed sink's stamps read — and
  the durable offset of the input that produced them, which an
  inter-procedure push inherits under the effectively-once tier.

  `report` is `{entity_pid, ref}` when the Entity is holding that invocation's
  effect record until every push lands, and nil when it owes nothing.
  """
  def emitted(pid, node_ref, entity_key, emits, handout_seq, offset, report),
    do: GenServer.cast(pid, {:emitted, node_ref, entity_key, emits, handout_seq, offset, report})

  @doc """
  Release the effect record an Entity is holding for the invocation just
  routed: every push it produced has been admitted, so the record may become
  durable. A `nil` report is an invocation that owed nothing.
  """
  def delivered(nil), do: :ok
  def delivered({entity, ref}), do: send(entity, {:edges_delivered, ref})
end
