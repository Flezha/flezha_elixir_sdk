defmodule Flezha.SDK.Relay.Inbox do
  @moduledoc """
  The messages a relay is sent from below: a raw frame off a bound
  primary stream, the values a Cell produced for its node's outputs, and a
  steps tasklet's request to end its instance. They live beside the
  relay rather than on it so that a `DataStream` or a `Cell` reaching
  upward names this module and not the router itself.

  The first two are answered by `Flezha.SDK.Relay.handle_cast/2`, all three
  by `Flezha.SDK.Steps.Driver.handle_cast/2`.

  The entrance is here too: how a raw frame becomes a value on an entry
  channel (`enter/5`) is one rule for both runs, so a steps relay's message is
  resolved, parsed and parked exactly where a flow relay's frame is.
  """

  alias Flezha.SDK.Park

  # The identity slot a frame naming an undeclared channel parks under: a
  # value this side chose, never a name off the wire.
  @unresolved_tasklet_id "_unresolved"

  @doc """
  The input-log offset the controller stamped on a journaled input, `nil` on
  one it did not journal.
  """
  @spec input_offset(map()) :: non_neg_integer() | nil
  def input_offset(metadata) do
    case Integer.parse(Map.get(metadata, "flz.offset", "")) do
      {offset, _rest} -> offset
      :error -> nil
    end
  end

  @doc """
  The token of the outside call waiting on an input (`flz.reply_to`), `nil`
  when nobody waits on it.
  """
  @spec input_call(map()) :: binary() | nil
  def input_call(metadata) do
    case Map.get(metadata, "flz.reply_to") do
      token when is_binary(token) and token != "" -> token
      _none -> nil
    end
  end

  @doc "Route a raw input frame arriving on an entry channel."
  def input(pid, channel, message, metadata),
    do: GenServer.cast(pid, {:input, channel, message, metadata})

  @doc """
  A Cell finished a dispatch: its emits as `{value, metadata, key}`, the
  change seq the dispatch started at — what a keyed sink's stamps read — the
  durable offset of the input that produced them, which an inter-tasklet push
  inherits under the effectively-once tier, and the call waiting on that
  input, which every push inherits and a `response()` output answers.

  `report` is `{cell_pid, ref}` when the Cell is holding that invocation's
  effect record until every push lands, and nil when it owes nothing.
  """
  def emitted(pid, node_ref, cell_key, emits, handout_seq, {offset, call}, report),
    do:
      GenServer.cast(
        pid,
        {:emitted, node_ref, cell_key, emits, handout_seq, {offset, call}, report}
      )

  @doc """
  A steps stage's Cell has a processing-time timer due: the driver decides
  whether its firing opens a pass, joins the running stage or waits its turn.
  """
  def firing_due(pid, node_ref, cell_key, fire),
    do: send(pid, {:firing_due, node_ref, cell_key, fire})

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

  # --- the entrance ---

  @doc """
  One frame through the entrance: `{:ok, channel, value}` once the name
  resolved and the channel's parser read the bytes, or `:parked` when the
  frame named a channel `topo` does not declare or the parser rejected it,
  parked under that name, on `runner`, with `relay_id`'s identity and the
  `call` waiting on the frame.
  """
  @spec enter(
          pid(),
          String.t(),
          Flezha.SDK.Topology.t(),
          String.t() | nil,
          binary(),
          binary() | nil
        ) :: {:ok, String.t(), term()} | :parked
  def enter(runner, relay_id, topo, channel, message, call) do
    case resolve_channel(topo, channel) do
      {:unresolved, requested} ->
        park_pre_dispatch(
          runner,
          relay_id,
          @unresolved_tasklet_id,
          requested,
          unresolved_entry_error(topo, requested),
          message,
          reply_to: call
        )

        :parked

      channel ->
        case parse(topo, channel, message) do
          {:ok, value} ->
            {:ok, channel, value}

          {:error, reason} ->
            park_pre_dispatch(
              runner,
              relay_id,
              channel,
              channel,
              "entry parse failed: #{inspect(reason)}",
              message,
              reply_to: call
            )

            :parked
        end
    end
  end

  # A name the relay declares resolves to it; a name it does not is
  # unresolved, and parks under the name it asked for rather than reaching
  # whichever entry channel happens to be first. A frame naming no channel
  # takes the first DECLARED entry — the source-plugin default, and the order
  # `entry_order` keeps, because `entries` is a map and its key order is the
  # term order, which is not the author's — and is unresolved only when there
  # is none to take.
  #
  # The answer is asymmetric on purpose: this runs once per input frame, so
  # the resolved arm returns the name itself and allocates nothing, and only
  # the cold arm builds a tuple. A declared name is a binary and can never
  # match `{:unresolved, _}`.
  defp resolve_channel(topo, channel) when is_binary(channel) do
    if Map.has_key?(topo.entries, channel), do: channel, else: {:unresolved, channel}
  end

  defp resolve_channel(topo, _channel) do
    case topo.entry_order do
      [first | _] -> first
      [] -> {:unresolved, ""}
    end
  end

  # The bytes through the entry channel's parser; a channel without one passes
  # them as they are.
  defp parse(topo, channel, message) do
    case Map.get(topo.entries, channel) do
      %{parser: parser} -> safe_parse(parser, message)
      nil -> {:ok, message}
    end
  end

  defp safe_parse(parser, message) do
    parser.(message)
  rescue
    e -> {:error, Exception.message(e)}
  end

  # Why the frame did not enter: the name it asked for against the set the
  # relay declares, or that it declares none at all. `flz_core`'s
  # `unresolved_entry_error` answers in the same words, over the same sorted
  # set, so a user reading a DLQ row cannot tell which core wrote it.
  defp unresolved_entry_error(topo, requested) do
    case Enum.sort(topo.entry_order) do
      [] ->
        "the relay declares no entry channel"

      declared ->
        "no entry channel `#{requested}`; the relay declares: #{Enum.join(declared, ", ")}"
    end
  end

  @doc """
  A pre-dispatch park (entry parse, unresolved channel, partition mis-route),
  forwarded to the runner which owns the ack loop. The park_id uses a fresh
  unique integer since no Cell seq exists yet.

  `tasklet_id` is the identity slot, which the controller turns into a
  DIRECTORY under the dead-letter root, so it is always a value this side
  chose. `channel` is the row's attribution and may be a name off the wire.
  A steps driver parks a failed turn here too, under its `_turn` slot:
  `opts` then name the `cell_key` and the `attempts` it failed after.
  `opts[:reply_to]` is the call waiting on the parked input, if any.
  """
  def park_pre_dispatch(runner, relay_id, tasklet_id, channel, error, payload, opts \\ []) do
    park_id = "#{relay_id}/#{tasklet_id}##{System.unique_integer([:positive, :monotonic])}"

    Park.ship(
      runner,
      self(),
      Park.identity(relay_id, tasklet_id, Keyword.get(opts, :cell_key, "")),
      Park.body(
        park_id,
        channel,
        error,
        Keyword.get(opts, :attempts, 1),
        payload,
        Keyword.get(opts, :reply_to)
      )
    )

    :ok
  end
end
