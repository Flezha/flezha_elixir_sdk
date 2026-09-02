defmodule Flezha.SDK.Operative.Routing do
  @moduledoc """
  The sink half of the routing seam: what one plugin output looks like, shared
  by the two callers that build one. The router builds a frame for the wire
  (`sink_frame/5`, with the identity stamps on a keyed sink); an Entity under
  the effectively-once tier builds the same output into its effect record and
  never stamps — the controller derives the stamps at flush.

  `sink_key/3` is why this is one module rather than two: the key precedence —
  the emit's own, else the route's `key_by`, else the emitting Entity's — has
  to answer the same thing for the frame and for the record, or a re-drive
  would overwrite under a different natural key.

  It sits beside `Inbox` because an Entity already reaches upward through this
  namespace, and it names `Topology` for nothing at all: everything it needs
  is on the route the topology compiled.
  """

  @typedoc "The identity facts a keyed sink stamps with, or nil for a capture that never stamps."
  @type stamp_with :: {binary(), map()} | nil

  @doc "One plugin output's frame: its metadata (channel, then the stamps) and its bytes."
  @spec sink_frame(map(), term(), map(), non_neg_integer(), stamp_with()) :: {map(), binary()}
  def sink_frame(route, value, emit, ordinal, stamp_with) do
    metadata = Map.put(emit.meta, "channel", route.channel)

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
  The emit's own key, else the sink route's `key_by`, else the emitting
  Entity's — the reference's `emit_keyed` and then the entity-key default.
  """
  @spec sink_key(map(), term(), map()) :: binary() | nil
  def sink_key(route, value, emit) do
    cond do
      emit.key -> emit.key
      route.key_by -> route.key_by.(value)
      emit.origin -> emit.origin.entity_key
      true -> nil
    end
  end

  defp stamp(metadata, oid, modules, emit, ordinal, key) do
    %{node_ref: node_ref, entity_key: entity_key, seq: seq} = emit.origin

    metadata
    |> Map.put("flz.entity", "#{oid}/#{modules[node_ref].name()}/#{entity_key || ""}")
    |> Map.put("flz.seq", "#{seq}.#{ordinal}")
    |> Map.put("flz.key", key)
  end
end
