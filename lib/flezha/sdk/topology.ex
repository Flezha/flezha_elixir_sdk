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

  alias Flezha.SDK.{Channels, Graph, Procedure, StreamOperative}

  @enforce_keys [:factory_id]
  defstruct factory_id: nil,
            entries: %{},
            node_modules: %{},
            node_mode: %{},
            node_name: %{},
            node_channel: %{},
            node_seq: %{},
            node_knobs: %{},
            routes: %{},
            order: [],
            keyless?: false,
            persistence_enabled?: true,
            compact_after_bytes: nil,
            effectively_once?: false,
            checkpoint: nil,
            correction: nil

  @type t :: %__MODULE__{}

  @doc """
  Compile `module`'s declaration into a topology. `declare` names the callback
  that authors it: `:channels` for a stream class, `:procedures` for a workflow
  one — the two surfaces author on the same builder, so they compile through
  the same function.
  """
  @spec build(module(), atom()) :: {:ok, t()} | {:error, {:invalid_topology, [term()]}}
  def build(module, declare \\ :channels) do
    c = Channels.new()

    try do
      _ = apply(module, declare, [c])
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
    issues = collect_issues(module, acc, routes, node_mode)

    if issues == [] do
      {:ok,
       %__MODULE__{
         factory_id: module.factory_id(),
         entries: acc.entries,
         node_modules: node_modules(acc),
         node_mode: node_mode,
         node_name: node_names(acc),
         node_channel: node_channels(acc),
         node_seq: node_seqs(acc),
         node_knobs: acc.knobs,
         routes: routes,
         order: Map.keys(acc.nodes),
         keyless?: keyless?(acc, node_mode),
         persistence_enabled?: StreamOperative.persistence_enabled?(module),
         compact_after_bytes: compact_after_bytes(acc),
         effectively_once?: acc.tier.effectively_once,
         checkpoint: acc.tier.checkpoint,
         correction: acc.tier.correction
       }}
    else
      {:error, {:invalid_topology, Enum.reverse(issues)}}
    end
  end

  # routes: %{source => [%{to: {:node, ref} | {:sink, plugin}, transform, key_by, serializer, keyed}]}
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
        serializer: s.serializer,
        keyed: s.keyed,
        # The channel a frame off this route is named by: a static property of
        # the route's source, resolved once here rather than per output.
        channel: Channels.channel_name(acc, s.from)
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

  defp collect_issues(op_module, acc, routes, node_mode) do
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
        |> check_clock(module)
      end)

    # cycles among nodes
    issues = check_cycles(acc, routes, issues)

    issues = check_tier(op_module, acc, issues)
    issues = check_correction(op_module, acc, issues)

    check_changeful_identity(acc, issues)
  end

  # What the effectively-once tier refuses, the reference's names
  # (`builder.rs` `refuse_class_facts` / `refuse_channel`, plus the core's
  # zero-delay refusal at `register.rs`): a checkpoint policy without the
  # tier, a cadence or a delay of zero, a plugin wire the tier cannot
  # deduplicate by identity, and a shedding policy under a contract that says
  # nothing is lost.
  defp check_tier(op_module, acc, issues) do
    %{effectively_once: on?, checkpoint: policy} = acc.tier

    issues
    |> class_facts(on?, policy, mod_name(op_module))
    |> unkeyed_sinks(on?, acc)
    |> shedding_edges(on?, acc)
  end

  defp class_facts(is, on?, policy, name) do
    is = if policy != nil and not on?, do: [{:CheckpointAckWithoutTier, name} | is], else: is
    is = if policy[:every] == 0, do: [{:CheckpointEveryZero, name} | is], else: is
    if policy[:max_delay_ms] == 0, do: [{:CheckpointMaxDelayZero, name} | is], else: is
  end

  # What a correction window refuses, the reference's names
  # (`sdk/rust/src/channel/builder/issue.rs` `CorrectionWithoutTier` /
  # `CorrectionWithoutEventTime` / `CorrectionWindowZero`, plus the core's
  # "at least one event_time-clocked procedure" at `register.rs`). A shedding
  # policy needs no clause of its own: a window requires the tier, and
  # `shedding_edges/3` already refuses every shedding edge under it.
  defp check_correction(_op_module, %{tier: %{correction: nil}}, issues), do: issues

  defp check_correction(op_module, acc, issues) do
    %{effectively_once: on?, correction: %{window_ms: window_ms}} = acc.tier
    name = mod_name(op_module)

    issues = if on?, do: issues, else: [{:CorrectionWithoutTier, name} | issues]
    issues = if window_ms == 0, do: [{:CorrectionWindowZero, name} | issues], else: issues
    event_time_clock(issues, acc)
  end

  # Sorted by procedure name so the refusal names the same procedure on every
  # run: node refs are `make_ref/0` and their map order is not declaration order.
  defp event_time_clock(issues, acc) do
    clocks =
      acc.nodes
      |> Enum.map(fn {_ref, %{module: m}} -> {m.name(), Procedure.clock(m)} end)
      |> Enum.sort()

    if Enum.any?(clocks, fn {_name, clock} -> clock == :event_time end) do
      issues
    else
      case clocks do
        [{name, clock} | _] -> [{:CorrectionWithoutEventTime, name, clock} | issues]
        [] -> issues
      end
    end
  end

  defp unkeyed_sinks(is, false, _acc), do: is

  defp unkeyed_sinks(is, true, acc) do
    Enum.reduce(acc.sinks, is, fn sink, is ->
      if sink.keyed,
        do: is,
        else: [{:UnkeyedChannelUnderEffectivelyOnce, Channels.channel_name(acc, sink.from)} | is]
    end)
  end

  defp shedding_edges(is, false, _acc), do: is

  defp shedding_edges(is, true, acc) do
    Enum.reduce(acc.edges, is, fn edge, is ->
      case get_in(acc.knobs, [edge.to, :on_full]) do
        policy when policy in [:drop_newest, :drop_oldest] ->
          [
            {:DroppingPolicyUnderEffectivelyOnce, Channels.channel_name(acc, edge.from), policy}
            | is
          ]

        _ ->
          is
      end
    end)
  end

  # A changeful procedure's records are keyed by its `name/0`, so two nodes of
  # one changeful module in a class would share a WAL. The reference gives a
  # named node its own identity; here `named/2` also names the channel and the
  # scenarios pin the procedure name, so the shape is refused instead.
  defp check_changeful_identity(acc, issues) do
    acc.nodes
    |> Enum.map(fn {_ref, %{module: m}} -> m end)
    |> Enum.filter(&Procedure.changeful?/1)
    |> Enum.frequencies()
    |> Enum.reduce(issues, fn
      {module, n}, is when n > 1 -> [{:DuplicatePersistentProcedure, module.name()} | is]
      _, is -> is
    end)
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

  # A clock the runtime cannot serve: the reference's `UnservableClock` (a
  # logical clock reads a change seq no changeless procedure has), and the
  # two a dynamic declaration can get wrong.
  defp check_clock(is, module) do
    case Procedure.clock(module) do
      :wall ->
        is

      :logical ->
        if Procedure.changeful?(module),
          do: is,
          else: [{:UnservableClock, mod_name(module)} | is]

      :event_time ->
        if Procedure.exports?(module, :event_time_of, 1),
          do: is,
          else: [{:EventTimeClockWithoutExtractor, mod_name(module)} | is]

      _other ->
        [{:UnknownClock, mod_name(module)} | is]
    end
  end

  # A cycle among procedure nodes (entry sources are roots).
  defp check_cycles(acc, routes, is) do
    walk = Graph.walk(node_seqs(acc), node_edges(routes))
    if walk.on_cycle, do: [{:Cycle, "procedure graph"} | is], else: is
  end

  @doc "The node running the procedure `name/0` answers, or why there is none."
  @spec node_of(t(), String.t()) :: {:ok, reference()} | {:error, :procedure_not_found}
  def node_of(%__MODULE__{node_modules: modules}, procedure) do
    case Enum.find(modules, fn {_ref, m} -> m.name() == procedure end) do
      {ref, _module} -> {:ok, ref}
      nil -> {:error, :procedure_not_found}
    end
  end

  @doc false
  # The producer -> consumer pairs among nodes; a route into a sink is not one.
  def node_edges(routes) do
    for {{:node, from}, rs} <- routes, %{to: {:node, to}} <- rs, do: {from, to}
  end

  # The smallest threshold across the class's changeful procedures — what the
  # class declares when it differs from the default; nil when nothing journals.
  defp compact_after_bytes(acc) do
    acc.nodes
    |> Enum.map(fn {_ref, %{module: m}} -> m end)
    |> Enum.filter(&Procedure.changeful?/1)
    |> Enum.map(&Procedure.compact_after_bytes/1)
    |> Enum.min(fn -> nil end)
  end

  defp node_modules(acc), do: Map.new(acc.nodes, fn {ref, %{module: m}} -> {ref, m} end)
  defp node_seqs(acc), do: Map.new(acc.nodes, fn {ref, %{seq: n}} -> {ref, n} end)
  defp node_names(acc), do: Map.new(acc.nodes, fn {ref, %{name: n}} -> {ref, n} end)

  # The channel a node's outputs are named by on the wire: `named/2`'s name,
  # else the procedure's own — the auto-derived channel name every SDK ships.
  defp node_channels(acc),
    do: Map.new(acc.nodes, fn {ref, %{module: m, name: n}} -> {ref, n || m.name()} end)

  defp module_of(acc, ref), do: mod_name(get_in(acc.nodes, [ref, :module]))
  defp mod_name(module), do: inspect(module)

  # A topology with no procedure that would create a keyed Entity is "keyless"
  # for the controller's partitioned-start preflight.
  defp keyless?(acc, node_mode) do
    acc.nodes != %{} and
      Enum.all?(node_mode, fn {_ref, mode} -> mode == :single end)
  end
end
