defmodule Flezha.SDK.Declarations do
  @moduledoc """
  What a class tells the controller about itself in its `AddOperative` reply:
  a map of string keys that becomes `AddOperativeResponse.metadata`, empty when
  the class declares nothing.

  Both kinds live here, as they do in the reference's `operative_start.rs`,
  because they answer one question and the controller decodes them all in one
  place of its own. A new declared fact is added here and there, and nowhere
  else — the SDK names no controller module, so the pair is kept by the
  reader rather than by a reference.

  A stream class declares the tier and its granularity when declared, and both
  correction keys whenever a window is — an undeclared beyond-window fate
  reaches the controller as the default rather than as an absence, as it does
  in every other SDK. A workflow class declares the tier, synchronous
  durability, the compensable positions (a rewind may only unwind those) and
  the leaves, since the operative is done when every leaf has committed. The
  cadence, the outputs posture and the delay stay SDK-side.
  """

  alias Flezha.SDK.{Procedure, Topology}
  alias Flezha.SDK.Workflow.Spec

  @doc "The metadata `class` declares — a `Topology` or a workflow `Spec`."
  @spec for(Topology.t() | Spec.t()) :: %{String.t() => String.t()}
  def for(%Topology{} = topo) do
    keyed = if topo.keyless?, do: %{"keyed" => "false"}, else: %{}
    threshold = topo.compact_after_bytes

    compacted =
      if threshold && threshold != Procedure.default_compact_after_bytes(),
        do: Map.put(keyed, "compact_after_bytes", Integer.to_string(threshold)),
        else: keyed

    tiered =
      if topo.effectively_once?,
        do: Map.put(compacted, "effectively_once", "true"),
        else: compacted

    acked = if topo.checkpoint, do: Map.put(tiered, "ack_granularity", "checkpoint"), else: tiered

    corrected(acked, topo.correction)
  end

  def for(%Spec{} = spec) do
    %{}
    |> put_when(spec.knobs.sync_durability, "durability", "sync_remote")
    |> put_when(spec.topology && spec.topology.effectively_once?, "effectively_once", "true")
    |> put_procs("compensable_procs", spec, &Procedure.compensates?/1)
    |> put_procs("leaf_procs", spec, nil)
  end

  defp corrected(declarations, nil), do: declarations

  defp corrected(declarations, %{window_ms: window_ms, beyond_window: beyond_window}) do
    declarations
    |> Map.put("correction_window_ms", Integer.to_string(window_ms))
    |> Map.put("beyond_window_policy", Atom.to_string(beyond_window))
  end

  defp put_when(map, true, key, value), do: Map.put(map, key, value)
  defp put_when(map, _false_or_nil, _key, _value), do: map

  # The positions of `spec.dag.order` satisfying `pred` on their procedure, or
  # — with no predicate — the leaves. An empty result is still declared: "no
  # position qualifies" and "the class said nothing" are different answers.
  defp put_procs(map, _key, %Spec{dag: %{order: []}}, _pred), do: map

  defp put_procs(map, key, %Spec{dag: dag} = spec, pred) do
    procs =
      dag.order
      |> Enum.with_index()
      |> Enum.filter(fn {node_ref, _idx} ->
        if pred,
          do: pred.(Map.fetch!(spec.topology.node_modules, node_ref)),
          else: Map.fetch!(dag.children, node_ref) == []
      end)
      |> Enum.map_join(",", fn {_ref, idx} -> Integer.to_string(idx) end)

    Map.put(map, key, procs)
  end
end
