defmodule Flezha.SDK.Topology do
  @moduledoc """
  Compiles a `StreamOperative` into a routing topology, accumulating every
  refusal into one `{:error, {:invalid_topology, issues}}` (never a raise). An
  `UndefinedFunctionError` inside `channels/1` — a deferred builder surface —
  is caught and reported as a registration error naming the module.

  Routing model (v1, keyless channels deferred): an input on an entry channel,
  or a value a node emits, fans out along the source's routes; each route fuses
  one transform closure and at most one key. Entity mode per node is derived
  from the wiring exactly as in the Rust SDK: non-empty `entity_keys/0` →
  Static; a keyed incoming route into a keyless procedure → Dynamic; else Single.
  """

  alias Flezha.SDK.{Channels, Procedure}

  @enforce_keys [:factory_id]
  defstruct factory_id: nil,
            entries: %{},
            node_modules: %{},
            node_mode: %{},
            node_name: %{},
            node_knobs: %{},
            routes: %{},
            order: [],
            sink_serializers: %{},
            keyless?: false

  @type t :: %__MODULE__{}

  @spec build(module()) :: {:ok, t()} | {:error, {:invalid_topology, [term()]}}
  def build(module) do
    c = Channels.new()

    try do
      _ = module.channels(c)
      acc = Channels.take(c)
      compile(module, acc)
    rescue
      e in UndefinedFunctionError ->
        _ = Channels.take(c)

        {:error, {:invalid_topology, [{:DeferredBuilderSurface, module, Exception.message(e)}]}}
    end
  end

  defp compile(module, acc) do
    routes = build_routes(acc)
    node_mode = derive_modes(acc, routes)
    issues = collect_issues(acc, routes, node_mode)

    if issues == [] do
      {:ok,
       %__MODULE__{
         factory_id: module.factory_id(),
         entries: acc.entries,
         node_modules: node_modules(acc),
         node_mode: node_mode,
         node_name: node_names(acc),
         node_knobs: acc.knobs,
         routes: routes,
         order: Map.keys(acc.nodes),
         sink_serializers: sink_serializers(acc),
         keyless?: keyless?(acc, node_mode)
       }}
    else
      {:error, {:invalid_topology, Enum.reverse(issues)}}
    end
  end

  # routes: %{source => [%{to: {:node, ref} | {:sink, plugin}, transform, key_by, serializer}]}
  defp build_routes(acc) do
    node_routes =
      Enum.reduce(acc.edges, %{}, fn e, m ->
        route = %{to: {:node, e.to}, transform: e.transform, key_by: e.key_by, serializer: nil}
        Map.update(m, e.from, [route], &[route | &1])
      end)

    Enum.reduce(acc.sinks, node_routes, fn s, m ->
      route = %{
        to: {:sink, s.plugin},
        transform: s.transform,
        key_by: s.key_by,
        serializer: s.serializer
      }

      Map.update(m, s.from, [route], &[route | &1])
    end)
  end

  defp derive_modes(acc, routes) do
    incoming = incoming_by_node(routes)

    Map.new(acc.nodes, fn {ref, %{module: module}} ->
      keys = Procedure.entity_keys(module)
      keyed_in? = Enum.any?(Map.get(incoming, ref, []), & &1.keyed)

      mode =
        cond do
          keys != [] -> :static
          keyed_in? -> :dynamic
          true -> :single
        end

      {ref, mode}
    end)
  end

  defp incoming_by_node(routes) do
    Enum.reduce(routes, %{}, fn {_from, rs}, m ->
      Enum.reduce(rs, m, fn
        %{to: {:node, ref}, key_by: kb}, acc ->
          Map.update(acc, ref, [%{keyed: kb != nil}], &[%{keyed: kb != nil} | &1])

        _sink, acc ->
          acc
      end)
    end)
  end

  defp collect_issues(acc, routes, node_mode) do
    incoming = incoming_by_node(routes)

    issues = acc.issues

    # dangling transform chains never consumed by into/sink
    issues =
      Enum.reduce(acc.chains, issues, fn {_cid, src}, is ->
        [{:DanglingTransformChain, src} | is]
      end)

    # per-node checks
    issues =
      Enum.reduce(acc.nodes, issues, fn {ref, %{module: module}}, is ->
        mode = Map.fetch!(node_mode, ref)
        in_routes = Map.get(incoming, ref, [])

        is
        |> check_unreachable(ref, in_routes, acc)
        |> check_static_keying(module, mode, in_routes)
        |> check_dynamic_keying(module, mode, in_routes)
        |> check_entities_for(module)
        |> check_mailbox(module)
      end)

    # cycles among nodes
    issues = check_cycles(acc, routes, issues)

    issues
  end

  defp check_unreachable(is, _ref, in_routes, _acc) when in_routes != [], do: is

  defp check_unreachable(is, ref, _in_routes, acc) do
    name = get_in(acc.nodes, [ref, :name]) || module_of(acc, ref)
    [{:UnreachableProcedure, name} | is]
  end

  defp check_static_keying(is, module, :static, in_routes) do
    if Enum.any?(in_routes, & &1.keyed),
      do: [{:KeyByOnUnkeyableTarget, mod_name(module)} | is],
      else: is
  end

  defp check_static_keying(is, _module, _mode, _in), do: is

  defp check_dynamic_keying(is, module, :dynamic, in_routes) do
    if Enum.any?(in_routes, &(not &1.keyed)),
      do: [{:UnkeyedEdgeIntoDynamicProcedure, mod_name(module)} | is],
      else: is
  end

  defp check_dynamic_keying(is, _module, _mode, _in), do: is

  defp check_entities_for(is, module) do
    if Procedure.exports?(module, :entities_for, 1) and Procedure.entity_keys(module) == [],
      do: [{:EntitiesForMissing, mod_name(module)} | is],
      else: is
  end

  defp check_mailbox(is, module) do
    if Procedure.exports?(module, :mailbox_capacity, 0) do
      case Procedure.validate_capacity(module.mailbox_capacity(), module) do
        :ok -> is
        {issue, _} -> [{issue, mod_name(module)} | is]
      end
    else
      is
    end
  end

  # A cycle among procedure nodes (entry sources are roots).
  defp check_cycles(acc, routes, is) do
    adj =
      Enum.reduce(routes, %{}, fn
        {{:node, from}, rs}, m ->
          tos = for %{to: {:node, to}} <- rs, do: to
          Map.put(m, from, tos)

        {_entry, _rs}, m ->
          m
      end)

    nodes = Map.keys(acc.nodes)

    if Enum.any?(nodes, &cyclic?(&1, adj, MapSet.new(), MapSet.new())) do
      [{:Cycle, "procedure graph"} | is]
    else
      is
    end
  end

  defp cyclic?(node, adj, visiting, _done) do
    if MapSet.member?(visiting, node) do
      true
    else
      visiting = MapSet.put(visiting, node)
      Enum.any?(Map.get(adj, node, []), &cyclic?(&1, adj, visiting, MapSet.new()))
    end
  end

  defp sink_serializers(acc),
    do: Map.new(acc.sinks, fn s -> {s.plugin, s.serializer} end)

  defp node_modules(acc), do: Map.new(acc.nodes, fn {ref, %{module: m}} -> {ref, m} end)
  defp node_names(acc), do: Map.new(acc.nodes, fn {ref, %{name: n}} -> {ref, n} end)
  defp module_of(acc, ref), do: mod_name(get_in(acc.nodes, [ref, :module]))
  defp mod_name(module), do: inspect(module)

  # A topology with no procedure that would create a keyed Entity is "keyless"
  # for the controller's partitioned-start preflight.
  defp keyless?(acc, node_mode) do
    acc.nodes != %{} and
      Enum.all?(node_mode, fn {_ref, mode} -> mode == :single end)
  end
end
