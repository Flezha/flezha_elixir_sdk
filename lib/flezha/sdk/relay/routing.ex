defmodule Flezha.SDK.Relay.Routing do
  @moduledoc """
  The sink half of the routing seam: what one plugin output looks like, shared
  by the two callers that build one. The router builds a frame for the wire
  (`sink_frame/5`, with the identity stamps on a keyed sink); a Cell under
  the effectively-once tier builds the same output into its effect record and
  never stamps — the controller derives the stamps at flush.

  `sink_key/3` is why this is one module rather than two: the key precedence —
  the emit's own, else the route's `key_by`, else the emitting Cell's — has
  to answer the same thing for the frame and for the record, or a re-drive
  would overwrite under a different natural key.

  It sits beside `Inbox` because a Cell already reaches upward through this
  namespace, and it names `Topology` for nothing at all: everything it needs
  is on the route the topology compiled.
  """

  @typedoc "The identity facts a keyed sink stamps with, or nil for a capture that never stamps."
  @type stamp_with :: {binary(), map()} | nil

  @end_keys ~w(flz.reply_to flz.call flz.call_create flz.call_response flz.deadline_at
               flz.call_channel flz.call_answer_channel flz.key)

  @doc """
  One plugin output's frame: its metadata (channel, the call a sink output
  answers or a call end's stamps, then the identity stamps) and its bytes.
  """
  @spec sink_frame(map(), term(), map(), non_neg_integer(), stamp_with()) :: {map(), binary()}
  def sink_frame(route, value, emit, ordinal, stamp_with) do
    metadata = emit.meta |> Map.put("channel", route.channel) |> answer(route, emit, value)

    metadata =
      case stamp_with do
        nil ->
          metadata

        {oid, modules} ->
          stamp(metadata, oid, modules, emit, ordinal, sink_key(route, value, emit))
      end

    {metadata, route.serializer.(value)}
  end

  @doc """
  The keys an output's end stamps, and only that end: a sink output's
  `flz.reply_to`, the call its input carried, when one did; a call end's
  callee, its Response channel, its deadline (wall clock at the emit), its
  entry and answer channels and the chain's key. Every other output drops
  them.
  """
  @spec answer(map(), map(), map(), term()) :: map()
  def answer(metadata, %{call: %{} = call} = route, _emit, value),
    do: metadata |> Map.drop(@end_keys) |> call_stamps(call, route.key_by && route.key_by.(value))

  def answer(metadata, _sink, %{call: token}, _value) when is_binary(token),
    do: metadata |> Map.drop(@end_keys) |> Map.put("flz.reply_to", token)

  def answer(metadata, _sink, _emit, _value), do: Map.drop(metadata, @end_keys)

  defp call_stamps(metadata, call, key) do
    [
      {"flz.call", call.target},
      {"flz.call_create", if(call.create, do: nil, else: "false")},
      {"flz.call_response", call.response},
      {"flz.deadline_at", Integer.to_string(System.system_time(:millisecond) + call.deadline_ms)},
      {"flz.call_channel", call.channel},
      {"flz.call_answer_channel", call.answer_channel},
      {"flz.key", key}
    ]
    |> Enum.reject(&match?({_key, nil}, &1))
    |> Enum.into(metadata)
  end

  @doc """
  The emit's own key, else the sink route's `key_by`, else the emitting
  Cell's — the reference's `emit_keyed` and then the cell-key default. A call
  end's key is its chain's alone, or none.
  """
  @spec sink_key(map(), term(), map()) :: binary() | nil
  def sink_key(%{call: %{}} = route, value, _emit), do: route.key_by && route.key_by.(value)

  def sink_key(route, value, emit) do
    cond do
      emit.key -> emit.key
      route.key_by -> route.key_by.(value)
      emit.origin -> emit.origin.cell_key
      true -> nil
    end
  end

  defp stamp(metadata, oid, modules, emit, ordinal, key) do
    %{node_ref: node_ref, cell_key: cell_key, seq: seq} = emit.origin

    metadata
    |> Map.put("flz.cell", "#{oid}/#{modules[node_ref].name()}/#{cell_key || ""}")
    |> Map.put("flz.seq", "#{seq}.#{ordinal}")
    |> Map.put("flz.key", key)
  end
end
