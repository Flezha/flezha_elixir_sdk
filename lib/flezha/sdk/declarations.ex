defmodule Flezha.SDK.Declarations do
  @moduledoc """
  What a class tells the controller about itself in its `AddRelay` reply:
  a map of string keys that becomes `AddRelayResponse.metadata`.

  Both runs live here, as they do in the reference's `relay_start.rs`,
  because they answer one question and the controller decodes them all in one
  place of its own. A new declared fact is added here and there, and nowhere
  else: the SDK names no controller module, so the pair is kept by the
  reader rather than by a reference.

  Every class declares its `run`, its `durable` level (`memory`, `local` or
  `remote`), its `sleep_after_ms` when it sleeps and its `max_duration_ms`
  when it has one; a member rides only when its parent is chosen. A flow
  class declares `exactly` when it is effectively once, `ack` when it
  acknowledges at checkpoints, and `correction_ms` with `beyond_window`
  whenever a window is declared, and that it is keyless and its compaction
  threshold when they depart from the default. A steps class declares its
  `each` (only a `per_input` loop partitions) and `on_failure`, the
  compensable positions (a rewind may only unwind those) and the leaves,
  since the relay is done when every leaf has committed. The checkpoint
  cadence, the outputs posture and the delay stay SDK-side.
  """

  alias Flezha.SDK.{Relay, Tasklet, Topology}
  alias Flezha.SDK.Steps.Spec

  @doc "The metadata `class` declares: a flow `Topology` or a steps `Spec`."
  @spec for(Topology.t() | Spec.t()) :: %{String.t() => String.t()}
  def for(%Topology{} = topo) do
    threshold = topo.compact_after_bytes

    topo.settings
    |> settled()
    |> Map.put("run", "flow")
    |> put_when(topo.keyless?, "keyed", "false")
    |> put_when(
      threshold != nil and threshold != Tasklet.default_compact_after_bytes(),
      "compact_after_bytes",
      threshold
    )
    |> put_when(topo.effectively_once?, "exactly", "effectively_once")
    |> put_when(topo.checkpoint != nil, "ack", "checkpoint")
    |> corrected(topo.correction)
  end

  def for(%Spec{settings: settings} = spec) do
    settings
    |> settled()
    |> Map.merge(%{
      "run" => "steps",
      "each" => Atom.to_string(settings.each),
      "on_failure" => Atom.to_string(settings.on_failure)
    })
    |> put_tasks("compensable_tasks", spec, &Tasklet.compensates?/1)
    |> put_tasks("leaf_tasks", spec, nil)
  end

  # What either run declares: where its work is kept, when it sleeps and when
  # it stops.
  defp settled(%Relay{} = settings) do
    %{"durable" => Atom.to_string(settings.durable)}
    |> put_when(settings.sleep_after_ms != nil, "sleep_after_ms", settings.sleep_after_ms)
    |> put_when(settings.max_duration_ms != nil, "max_duration_ms", settings.max_duration_ms)
  end

  defp corrected(declarations, nil), do: declarations

  defp corrected(declarations, %{window_ms: window_ms, beyond_window: beyond_window}) do
    declarations
    |> Map.put("correction_ms", Integer.to_string(window_ms))
    |> Map.put("beyond_window", Atom.to_string(beyond_window))
  end

  defp put_when(map, true, key, value), do: Map.put(map, key, to_string(value))
  defp put_when(map, _false_or_nil, _key, _value), do: map

  # The positions of `spec.dag.order` satisfying `pred` on their tasklet, or
  # — with no predicate — the leaves. An empty result is still declared: "no
  # position qualifies" and "the class said nothing" are different answers.
  defp put_tasks(map, _key, %Spec{dag: %{order: []}}, _pred), do: map

  defp put_tasks(map, key, %Spec{dag: dag} = spec, pred) do
    tasks =
      dag.order
      |> Enum.with_index()
      |> Enum.filter(fn {node_ref, _idx} ->
        if pred,
          do: pred.(Map.fetch!(spec.topology.node_modules, node_ref)),
          else: Map.fetch!(dag.children, node_ref) == []
      end)
      |> Enum.map_join(",", fn {_ref, idx} -> Integer.to_string(idx) end)

    Map.put(map, key, tasks)
  end
end
