defmodule Flezha.SDK.Topology do
  @moduledoc """
  Judges the record `Flezha.SDK.Channels` wrote and compiles it into a routing
  topology, accumulating every refusal into one `{:error, {:invalid_topology,
  issues}}` (never a raise). An `UndefinedFunctionError` inside the declaration
  body — a deferred builder surface — is caught and reported as a registration
  error naming the module.

  The judgement is the three stages `flz_core::host::register::check` judges
  its own record in, re-expressed here because the Elixir SDK is its own core
  and inherits nothing from `flz_core`:

    1. the chain rules, per channel and left to right — a link after a plugin
       sink, a transform after a key, a second key, a chain nothing closes, a
       repeated plugin wire, an edge re-declared where either side carries a
       link;
    2. the derivation those chains determine — the consumer edges in chain
       order, the plugin wire, the channel's `named/2` name applied once for
       every terminator, and the checkpoint granularity `Channels` implies
       from any checkpoint key;
    3. the topology and class facts over the derived shape.

  `test/fixtures/reference_topologies.txt` is the contract both this and the
  core are measured against. Three of its rows name a surface the v1 subset
  does not carry (`branch`, `merge`), so `BranchTooFewArms`,
  `MergeTooFewSources` and `DuplicateMergeSource` are absent here by design
  rather than missing. Two more of its class-fact rows this SDK does not judge:
  a workflow class here DOES declare the effectively-once tier — the
  `commit_block_chain` twin of that same file is one — and the checkpoint-hold
  flag cannot be written without its delay, because `outputs_at_checkpoint/2`
  takes both.

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
            entry_order: [],
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

  @transform_links [:map, :filter, :flat_map]

  # A channel name rides every frame the channel produces, as the `channel`
  # metadata value under the metadata region's one-byte length prefix. The core
  # spells the same number as `MAX_METADATA_FIELD_LEN`; the two are held together
  # by the `ChannelNameTooLong` row of `test/fixtures/reference_topologies.txt`,
  # which every runtime answers.
  @max_channel_name_bytes 255

  @doc """
  Compile `module`'s declaration into a topology. `declare` names the callback
  that authors it: `:channels` for a stream class, `:procedures` for a workflow
  one — the two surfaces author on the same builder, so they compile through
  the same function, and the kind is what decides which class facts apply.
  """
  @spec build(module(), atom()) :: {:ok, t()} | {:error, {:invalid_topology, [term()]}}
  def build(module, declare \\ :channels) do
    c = Channels.new()

    case declare_on(module, declare, c) do
      :ok ->
        compile(module, declare, Channels.take(c))

      {:deferred, message} ->
        _ = Channels.take(c)
        {:error, {:invalid_topology, [{:DeferredBuilderSurface, module, message}]}}
    end
  end

  defp declare_on(module, declare, c) do
    _ = apply(module, declare, [c])
    :ok
  rescue
    e in UndefinedFunctionError -> {:deferred, Exception.message(e)}
  end

  defp compile(module, declare, acc) do
    {channels, chain_issues} = derive(acc)
    routes = routes(channels)
    incoming = incoming(channels)
    node_mode = derive_modes(acc, incoming)
    shape = %{channels: channels, routes: routes, incoming: incoming, node_mode: node_mode}

    issues = acc.issues ++ chain_issues ++ facts(module, declare, acc, shape)

    if issues == [] do
      {:ok,
       %__MODULE__{
         factory_id: module.factory_id(),
         entries: acc.entries,
         # Declaration order, which `entries` cannot carry: a map's key order
         # is the term order. The first of these is where a frame naming no
         # channel goes, and `flz_core::first_of_kind` answers the same way.
         entry_order: Enum.uniq(acc.entry_order),
         node_modules: node_modules(acc),
         node_mode: node_mode,
         node_name: node_names(acc),
         node_channel: node_channels(acc),
         node_seq: node_seqs(acc),
         node_knobs: node_knobs(channels),
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
      {:error, {:invalid_topology, issues}}
    end
  end

  # --- stages one and two: the chain rules, and what they derive ---

  # One walk per channel: the chain rules judged left to right while the
  # consumers, the plugin wire and the knobs they determine are read off.
  defp derive(acc) do
    by_channel = Enum.group_by(acc.chains, & &1.channel)

    acc
    |> sources()
    |> Enum.map_reduce([], fn source, issues ->
      {channel, found} = derive_channel(acc, source, Map.get(by_channel, source, []))
      {channel, issues ++ found}
    end)
  end

  # Every channel of the class in declaration order: the entry channels the
  # author opened, then the output channel of every node an `into` created.
  defp sources(acc) do
    entries = acc.entry_order |> Enum.uniq() |> Enum.map(&{:entry, &1})
    nodes = Enum.map(nodes_in_order(acc), fn {ref, _node} -> {:node, ref} end)
    entries ++ nodes
  end

  defp derive_channel(acc, source, chains) do
    name = Channels.channel_name(acc, source)
    seed = %{name: name, sinks: [], consumers: [], issues: []}
    state = Enum.reduce(chains, seed, &walk_chain(acc, &1, &2))

    channel = %{
      source: source,
      name: name,
      knobs: Enum.reduce(chains, %{}, &Map.merge(&1.knobs, &2)),
      sinks: Enum.reverse(state.sinks),
      consumers: Enum.reverse(state.consumers)
    }

    {channel, Enum.reverse(state.issues)}
  end

  # A chain's own links, left to right. `wired?` is the chain's own plugin
  # wire, `state.sinks` every wire the channel has so far: a transform is
  # refused after any of them, and output identity is declared on one.
  defp walk_chain(acc, chain, state) do
    {state, _steps, walk} =
      Enum.reduce(chain.links, {state, [], new_walk()}, &walk_link(acc, chain, &1, &2))

    state
    |> dangling(walk)
    |> unwired_identity(walk)
  end

  defp new_walk,
    do: %{transform?: false, key?: false, keyed?: false, wired?: false, closed?: false}

  defp dangling(state, %{closed?: false, transform?: t?, key?: k?}) when t? or k?,
    do: issue(state, {:DanglingTransformChain, state.name})

  defp dangling(state, _walk), do: state

  # Output identity stamps only exist on plugin-bound frames, so a chain that
  # declares `keyed()` and reaches no `sink(...)` asked for a stamp nothing can
  # carry — whether it closed on a procedure or closed on nothing at all.
  defp unwired_identity(state, %{keyed?: true, wired?: false}),
    do: issue(state, {:KeyedChannelWithoutPluginWire, state.name})

  defp unwired_identity(state, _walk), do: state

  defp walk_link(_acc, _chain, {kind, _f} = step, {state, steps, walk})
       when kind in @transform_links do
    cond do
      terminated?(state, walk) ->
        {issue(state, {:TransformAfterTerminal, state.name}), steps, walk}

      walk.key? ->
        {issue(state, {:TransformAfterKeyBy, state.name}), [step | steps],
         %{walk | transform?: true}}

      true ->
        {state, [step | steps], %{walk | transform?: true}}
    end
  end

  defp walk_link(_acc, _chain, {:key, _f} = step, {state, steps, walk}) do
    cond do
      terminated?(state, walk) ->
        {issue(state, {:TransformAfterTerminal, state.name}), steps, walk}

      walk.key? ->
        {issue(state, {:DuplicateKeyBy, state.name}), steps, %{walk | key?: true}}

      true ->
        {state, [step | steps], %{walk | key?: true}}
    end
  end

  defp walk_link(_acc, _chain, :keyed, {state, steps, walk}) do
    {state, steps, %{walk | keyed?: true}}
  end

  defp walk_link(_acc, _chain, {:sink, plugin, serializer}, {state, steps, walk}) do
    {terminate_sink(state, plugin, serializer, steps, walk.keyed?), steps,
     %{walk | wired?: true, closed?: true}}
  end

  defp walk_link(acc, chain, {:into, node_ref}, {state, steps, walk}) do
    edge = %{to: node_ref, declares: walk.transform? or walk.key?, knobs: chain.knobs}
    {wire(acc, state, edge, steps), steps, %{walk | closed?: true}}
  end

  defp terminated?(state, walk), do: walk.closed? or state.sinks != []

  # A channel may still carry more than one plugin wire — the keyed wire beside
  # the plain one, which stamps and does not. What it may not carry is two
  # wires nothing tells apart: since a sink names the CHANNEL, the stamping
  # mode is the whole of a wire's identity, and two wires sharing it are two
  # identical routes with every value delivered twice.
  defp terminate_sink(state, plugin, serializer, steps, keyed?) do
    if Enum.any?(state.sinks, &(&1.keyed == keyed?)) do
      issue(state, {:DuplicatePluginSink, state.name, plugin})
    else
      {transform, key_by} = Channels.fuse(Enum.reverse(steps))

      wire = %{
        plugin: plugin,
        serializer: serializer,
        transform: transform,
        key_by: key_by,
        keyed: keyed?
      }

      %{state | sinks: [wire | state.sinks]}
    end
  end

  # One consumer edge per (channel, procedure). Re-declaring one where either
  # side carries a transform or a key is refused: the edge keeps a single fused
  # closure, so the second declaration would otherwise be dropped and every
  # value delivered under the first one's transform and key. Carrying neither
  # is the repeated-`into` fan-in, which leaves the one edge as it stands.
  defp wire(acc, state, edge, steps) do
    case Enum.find(state.consumers, &(&1.to == edge.to)) do
      nil ->
        {transform, key_by} = Channels.fuse(Enum.reverse(steps))
        settled = edge |> Map.put(:transform, transform) |> Map.put(:key_by, key_by)
        %{state | consumers: [settled | state.consumers]}

      %{declares: existing} ->
        if existing or edge.declares,
          do: issue(state, {:EdgeAlreadyWired, state.name, node_label(acc, edge.to)}),
          else: state
    end
  end

  defp issue(state, found), do: %{state | issues: [found | state.issues]}

  # The routes an emit fans out along: the channel's plugin wires first, then
  # its consumer edges, both in declaration order. A channel with neither is
  # absent, so a reader asking for one gets `[]` rather than an empty entry.
  defp routes(channels) do
    channels
    |> Enum.map(&{&1.source, channel_routes(&1)})
    |> Enum.reject(fn {_source, routes} -> routes == [] end)
    |> Map.new()
  end

  defp channel_routes(channel) do
    sinks =
      for wire <- channel.sinks,
          do: %{
            to: {:sink, wire.plugin},
            transform: wire.transform,
            key_by: wire.key_by,
            serializer: wire.serializer,
            keyed: wire.keyed,
            # The channel a frame off this route is named by: a static
            # property of the route's source, resolved once here.
            channel: channel.name
          }

    edges =
      for edge <- channel.consumers,
          do: %{
            to: {:node, edge.to},
            transform: edge.transform,
            key_by: edge.key_by,
            serializer: nil
          }

    sinks ++ edges
  end

  # A consumer edge's mailbox and ladder knobs attach to the target node. A
  # node reached by several edges keeps the first non-nil value of each.
  defp node_knobs(channels) do
    for channel <- channels, edge <- channel.consumers, reduce: %{} do
      knobs -> Map.update(knobs, edge.to, edge.knobs, &Map.merge(edge.knobs, &1))
    end
  end

  defp derive_modes(acc, incoming) do
    Map.new(acc.nodes, fn {ref, %{module: module}} ->
      keys = Procedure.entity_keys(module)
      keyed_in? = Enum.any?(Map.get(incoming, ref, []), &(&1.key_by != nil))

      mode =
        cond do
          keys != [] -> :static
          keyed_in? -> :dynamic
          true -> :single
        end

      {ref, mode}
    end)
  end

  defp incoming(channels) do
    for channel <- channels, edge <- channel.consumers, reduce: %{} do
      m -> Map.update(m, edge.to, [edge], &[edge | &1])
    end
  end

  # --- stage three: the topology and class facts over the derived shape ---

  defp facts(module, declare, acc, shape) do
    names(acc, shape.channels) ++
      Enum.flat_map(nodes_in_order(acc), &procedure_facts(&1, shape)) ++
      Enum.flat_map(shape.channels, &channel_facts(acc, &1)) ++
      class_facts(module, declare, acc) ++
      cycles(acc, shape.routes) ++
      changeful_identity(acc)
  end

  # One namespace per operative for channels and one for node identities: a
  # derived name, a `named/2` override and an entry name all collide here. A
  # name also has to fit the frame it rides, and this is where the author can
  # still be told.
  defp names(acc, channels) do
    channel_names = acc.entry_order ++ for(%{source: {:node, _}} = c <- channels, do: c.name)
    node_ids = for {_ref, node} <- nodes_in_order(acc), do: node.name || node.module.name()
    # Per name, how many nodes took their channel name from their own `sink`:
    # two of those is a shape that looks nothing like a naming collision to the
    # author who wrote it, so it is refused under its own name.
    from_sink =
      Enum.frequencies(for {_ref, %{name: nil, sink_names: [name | _]}} <- acc.nodes, do: name)

    duplicates(channel_names, &channel_collision(&1, from_sink)) ++
      duplicates(node_ids, &{:NodeIdentityCollision, &1}) ++
      over_long_names(channel_names) ++
      named_twice(acc)
  end

  defp channel_collision(name, from_sink) do
    if Map.get(from_sink, name, 0) >= 2,
      do: {:DuplicateSinkName, name},
      else: {:DuplicateChannel, name}
  end

  # A channel is named once. `named/2` beside a `sink(name)` is two
  # declarations even when they agree — permitting the agreeing case would
  # leave a redundant pair with a rule nobody can remember. Two sinks spelling
  # ONE name are the duplicate wire instead, which `terminate_sink` refuses.
  defp named_twice(acc) do
    for {_ref, node} <- nodes_in_order(acc),
        [first, second | _] <- [naming_declarations(node)],
        do: {:ChannelNamedTwice, node.module.name(), first, second}
  end

  defp naming_declarations(%{name: nil, sink_names: sinks}), do: Enum.uniq(sinks)
  defp naming_declarations(%{name: name, sink_names: sinks}), do: [name | Enum.uniq(sinks)]

  defp over_long_names(names) do
    for name <- Enum.uniq(names),
        byte_size(name) > @max_channel_name_bytes,
        do: {:ChannelNameTooLong, name}
  end

  defp duplicates(names, issue) when is_function(issue, 1) do
    names
    |> Enum.frequencies()
    |> Enum.sort()
    |> Enum.flat_map(fn
      {name, n} when n > 1 -> [issue.(name)]
      _once -> []
    end)
  end

  # What one procedure's declaration may claim about its Entities, its clock
  # and its mailbox, and what the edges reaching it may claim about its mode.
  defp procedure_facts({ref, node}, shape) do
    module = node.module
    mode = Map.fetch!(shape.node_mode, ref)
    in_edges = Map.get(shape.incoming, ref, [])

    []
    |> check_unreachable(node, in_edges)
    |> check_static_keying(module, mode, in_edges)
    |> check_dynamic_keying(module, mode, in_edges)
    |> check_entities_for(module)
    |> check_mailbox(module)
    |> check_clock(module)
    |> Enum.reverse()
  end

  defp check_unreachable(is, _node, in_edges) when in_edges != [], do: is

  defp check_unreachable(is, node, _in_edges),
    do: [{:UnreachableProcedure, node.name || mod_name(node.module)} | is]

  defp check_static_keying(is, module, :static, in_edges) do
    if Enum.any?(in_edges, &(&1.key_by != nil)),
      do: [{:KeyByOnUnkeyableTarget, mod_name(module)} | is],
      else: is
  end

  defp check_static_keying(is, _module, _mode, _in), do: is

  defp check_dynamic_keying(is, module, :dynamic, in_edges) do
    if Enum.any?(in_edges, &(&1.key_by == nil)),
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
        {found, _} -> [{found, mod_name(module)} | is]
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

  # What a channel may claim about its plugin wires, its attempt ceiling and
  # what it discards. The effectively-once tier deduplicates by output
  # identity, so every plugin-bound wire of a tier class must opt in with
  # `keyed()`.
  defp channel_facts(acc, channel) do
    tier? = acc.tier.effectively_once

    []
    |> check_wires_under_tier(channel, tier?)
    |> check_retry_max(channel)
    |> check_shedding(channel, tier?)
    |> Enum.reverse()
  end

  defp check_wires_under_tier(is, channel, true) do
    Enum.reduce(channel.sinks, is, fn
      %{keyed: true}, is -> is
      %{keyed: false}, is -> [{:UnkeyedChannelUnderEffectivelyOnce, channel.name} | is]
    end)
  end

  defp check_wires_under_tier(is, _channel, _tier?), do: is

  defp check_retry_max(is, channel) do
    if channel.knobs[:retry_max] == 0, do: [{:RetryMaxZero, channel.name} | is], else: is
  end

  # Promising effectively-once and asking to shed is a contradiction, not a
  # trade-off: a dropped input's offset is advanced past by a later item's
  # watermark. A correction window needs the tier, so this one arm covers it.
  defp check_shedding(is, channel, true) do
    case channel.knobs[:on_full] do
      policy when policy in [:drop_newest, :drop_oldest] ->
        [{:DroppingPolicyUnderEffectivelyOnce, channel.name, policy} | is]

      _kept ->
        is
    end
  end

  defp check_shedding(is, _channel, _tier?), do: is

  defp class_facts(module, declare, acc) do
    name = mod_name(module)

    kind_facts(module, declare, name) ++
      checkpoint_facts(acc, name) ++ correction_facts(acc, name)
  end

  # What a class of one kind may not declare on the other kind's builder.
  # `sync_durability` is a workflow knob: a stream class acknowledges at input
  # rates, where a remote round trip per acknowledgement is not a guarantee it
  # can keep, and this SDK's stream behaviour has no such callback — declaring
  # one is an authoring error, not a knob nothing reads.
  defp kind_facts(_module, :procedures, _name), do: []

  defp kind_facts(module, _stream, name) do
    if Procedure.exports?(module, :sync_durability, 0) and module.sync_durability(),
      do: [{:SyncDurabilityOnStream, name}],
      else: []
  end

  # What the checkpoint policy refuses: a policy without the tier it holds the
  # offset back for, and a cadence or a delay of zero.
  defp checkpoint_facts(%{tier: %{checkpoint: nil}}, _name), do: []

  defp checkpoint_facts(%{tier: %{effectively_once: tier?, checkpoint: policy}}, name) do
    without_tier = if tier?, do: [], else: [{:CheckpointAckWithoutTier, name}]
    every = if policy.every == 0, do: [{:CheckpointEveryZero, name}], else: []
    delay = if policy.max_delay_ms == 0, do: [{:CheckpointMaxDelayZero, name}], else: []

    without_tier ++ every ++ delay
  end

  # What a correction window refuses: the tier it replays the input log
  # against, a window of zero, and an event-time clock to order the replayed
  # slice by. A shedding policy needs no clause of its own: a window requires
  # the tier, and `check_shedding/3` already refuses every shedding channel
  # under it.
  defp correction_facts(%{tier: %{correction: nil}}, _name), do: []

  defp correction_facts(acc, name) do
    %{effectively_once: tier?, correction: %{window_ms: window_ms}} = acc.tier

    without_tier = if tier?, do: [], else: [{:CorrectionWithoutTier, name}]
    zero = if window_ms == 0, do: [{:CorrectionWindowZero, name}], else: []

    without_tier ++ zero ++ event_time_clock(acc)
  end

  # Sorted by procedure name so the refusal names the same procedure on every
  # run: node refs are `make_ref/0` and their map order is not declaration order.
  defp event_time_clock(acc) do
    clocks =
      acc.nodes
      |> Enum.map(fn {_ref, %{module: m}} -> {m.name(), Procedure.clock(m)} end)
      |> Enum.sort()

    if Enum.any?(clocks, fn {_name, clock} -> clock == :event_time end) do
      []
    else
      case clocks do
        [{name, clock} | _] -> [{:CorrectionWithoutEventTime, name, clock}]
        [] -> []
      end
    end
  end

  # A cycle among procedure nodes (entry sources are roots).
  defp cycles(acc, routes) do
    if Graph.walk(node_seqs(acc), node_edges(routes)).on_cycle,
      do: [{:Cycle, "procedure graph"}],
      else: []
  end

  # A changeful procedure's records are keyed by its `name/0`, so two nodes of
  # one changeful module in a class would share a WAL. The reference gives a
  # named node its own identity; here `named/2` also names the channel and the
  # scenarios pin the procedure name, so the shape is refused instead.
  defp changeful_identity(acc) do
    acc.nodes
    |> Enum.map(fn {_ref, %{module: m}} -> m end)
    |> Enum.filter(&Procedure.changeful?/1)
    |> Enum.frequencies()
    |> Enum.sort_by(fn {module, _n} -> module end)
    |> Enum.flat_map(fn
      {module, n} when n > 1 -> [{:DuplicatePersistentProcedure, module.name()}]
      _once -> []
    end)
  end

  # --- projections ---

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

  defp nodes_in_order(acc), do: Enum.sort_by(acc.nodes, fn {_ref, node} -> node.seq end)

  defp node_modules(acc), do: Map.new(acc.nodes, fn {ref, %{module: m}} -> {ref, m} end)
  defp node_seqs(acc), do: Map.new(acc.nodes, fn {ref, %{seq: n}} -> {ref, n} end)
  defp node_names(acc), do: Map.new(acc.nodes, fn {ref, %{name: n}} -> {ref, n} end)

  # The channel a node's outputs are named by on the wire — the one derivation,
  # read off `Channels` so the routes and the check cannot answer differently.
  defp node_channels(acc),
    do: Map.new(acc.nodes, fn {ref, node} -> {ref, Channels.node_channel_name(node)} end)

  # How a refusal names a node: the name the author gave it, else its module.
  defp node_label(acc, node_ref) do
    case Map.get(acc.nodes, node_ref) do
      %{name: name} when is_binary(name) -> name
      %{module: module} -> mod_name(module)
    end
  end

  defp mod_name(module), do: inspect(module)

  # A topology with no procedure that would create a keyed Entity is "keyless"
  # for the controller's partitioned-start preflight.
  defp keyless?(acc, node_mode) do
    acc.nodes != %{} and
      Enum.all?(node_mode, fn {_ref, mode} -> mode == :single end)
  end
end
