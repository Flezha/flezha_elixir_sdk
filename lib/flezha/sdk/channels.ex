defmodule Flezha.SDK.Channels do
  @moduledoc """
  The graph-builder token handed to `channels/1`. Openers (`input`/`input_with`),
  edge transforms (`map`/`filter`/`flat_map`/`key_by`), consumers
  (`into`/`into_named`/`sink`/`sink_with`/`sealed`), per-edge knobs
  (`named`/`on_full`/`retry_max`/`retry_backoff_ms`) and `keyed` (output
  identity on the sink the handle reaches). Handles carry their pending
  transform chain functionally; consumers write to a build accumulator kept in
  the process dictionary for the duration of `channels/1` (the twin of the Rust
  builder's `pending`). Fan-out is handle reuse. Refusals accumulate — nothing
  raises; `Flezha.SDK.Topology.build/1` returns `{:error, {:invalid_topology, _}}`.
  """

  alias Flezha.SDK.Stream

  @enforce_keys [:ref]
  defstruct [:ref]

  @type t :: %__MODULE__{ref: reference()}

  # --- build lifecycle (used by Topology.build/1) ---

  @doc false
  def new do
    ref = make_ref()

    Process.put(acc_key(ref), %{
      entries: %{},
      nodes: %{},
      node_by_module: %{},
      node_names: %{},
      edges: [],
      sinks: [],
      knobs: %{},
      chains: %{},
      issues: []
    })

    %__MODULE__{ref: ref}
  end

  @doc false
  def take(%__MODULE__{ref: ref}) do
    acc = Process.get(acc_key(ref))
    Process.delete(acc_key(ref))
    acc
  end

  # --- openers ---

  @doc "Bytes-passthrough entry channel."
  def input(%__MODULE__{} = c, name) when is_binary(name),
    do: input_with(c, name, &default_parse/1)

  @doc "Entry channel with a custom parser; a parse failure rides the entry ladder."
  def input_with(%__MODULE__{ref: ref}, name, parser)
      when is_binary(name) and is_function(parser, 1) do
    update(ref, fn acc ->
      if Map.has_key?(acc.entries, name) do
        add_issue(acc, {:DuplicateChannel, name})
      else
        put_in(acc.entries[name], %{parser: parser})
      end
    end)

    %Stream{builder: ref, source: {:entry, name}}
  end

  # --- edge transforms (fused into one closure per consumer edge) ---

  def map(%Stream{} = s, f) when is_function(f, 1), do: push_chain(s, {:map, f})
  def filter(%Stream{} = s, f) when is_function(f, 1), do: push_chain(s, {:filter, f})
  def flat_map(%Stream{} = s, f) when is_function(f, 1), do: push_chain(s, {:flat_map, f})

  def key_by(%Stream{keyed: true} = s, _f) do
    refuse(s, {:DuplicateKeyBy, describe(s.source)})
  end

  def key_by(%Stream{terminal: true} = s, _f) do
    refuse(s, {:TransformAfterTerminal, describe(s.source)})
  end

  def key_by(%Stream{} = s, f) when is_function(f, 1) do
    %{track_chain(s) | chain: s.chain ++ [{:key, f}], keyed: true}
  end

  # --- consumers ---

  @doc "Route into a procedure node. Unnamed mentions of one module unify (fan-in)."
  def into(%Stream{} = s, module), do: do_into(s, module, nil)

  @doc "Route into a named procedure node (identity collisions refuse)."
  def into_named(%Stream{} = s, module, name) when is_binary(name),
    do: do_into(s, module, name)

  @doc "Terminal plugin sink with default serialization (bytes/UTF-8/JSON). Returns the handle."
  def sink(%Stream{} = s, plugin) when is_binary(plugin),
    do: sink_with(s, plugin, &default_serialize/1)

  @doc "Terminal plugin sink with a custom serializer. Returns the handle (hybrid)."
  def sink_with(%Stream{builder: ref} = s, plugin, serializer)
      when is_binary(plugin) and is_function(serializer, 1) do
    consume_chain(ref, s)

    update(ref, fn acc ->
      if Enum.any?(acc.sinks, &(&1.plugin == plugin)) do
        add_issue(acc, {:DuplicatePluginSink, plugin})
      else
        {transform, key_by} = fuse(s.chain)

        %{
          acc
          | sinks: [
              %{
                from: s.source,
                plugin: plugin,
                serializer: serializer,
                transform: transform,
                key_by: key_by,
                keyed: s.keyed_sink
              }
              | acc.sinks
            ]
        }
      end
    end)

    %{s | terminal: true, chain: [], keyed: false, keyed_sink: false, chain_id: nil}
  end

  @doc """
  Declare output identity on the sink this handle reaches — the reference's
  `keyed()`: its frames carry `flz.entity`, `flz.seq` and `flz.key`. Refused on
  an entry handle, and on one that reaches a procedure before any sink.
  """
  def keyed(%Stream{source: {:entry, _}} = s), do: refuse(s, {:KeyedOnEntry, describe(s.source)})
  def keyed(%Stream{} = s), do: %{s | keyed_sink: true}

  @doc "Name the source procedure node."
  def named(%Stream{source: {:entry, _}} = s, _name),
    do: refuse(s, {:NamedOnEntry, describe(s.source)})

  def named(%Stream{source: {:node, node_ref}, builder: ref} = s, name) when is_binary(name) do
    update(ref, fn acc ->
      if Map.get(acc.node_names, name) in [nil, node_ref] do
        acc
        |> put_in([:node_names, name], node_ref)
        |> put_in([:nodes, node_ref, :name], name)
      else
        add_issue(acc, {:NodeIdentityCollision, name})
      end
    end)

    s
  end

  @doc "Type-level terminator; adds no topology."
  def sealed(%Stream{} = s), do: %{s | terminal: true}

  # --- per-edge knobs (ride the handle; applied to the target node at into) ---

  def on_full(%Stream{} = s, policy) when policy in [:drop_nothing, :drop_newest, :drop_oldest],
    do: %{s | on_full: policy}

  def retry_max(%Stream{} = s, 0), do: refuse(s, {:RetryMaxZero, describe(s.source)})
  def retry_max(%Stream{} = s, n) when is_integer(n) and n > 0, do: %{s | retry_max: n}

  def retry_backoff_ms(%Stream{} = s, ms) when is_integer(ms) and ms >= 0,
    do: %{s | retry_backoff_ms: ms}

  # --- default codecs ---

  defp default_parse(bytes) when is_binary(bytes), do: {:ok, bytes}
  defp default_parse(other), do: {:ok, other}

  @doc false
  def default_serialize(v) when is_binary(v), do: v
  def default_serialize(v) when is_bitstring(v), do: v

  def default_serialize(v) do
    case Jason.encode(v) do
      {:ok, json} -> json
      {:error, _} -> :erlang.term_to_binary(v)
    end
  end

  # --- fusion: one closure per edge ---

  @doc false
  def fuse(chain) do
    {steps, key_by} =
      Enum.reduce(chain, {[], nil}, fn
        {:key, f}, {steps, _} -> {steps, f}
        step, {steps, key} -> {[step | steps], key}
      end)

    steps = Enum.reverse(steps)

    transform = fn value ->
      Enum.reduce(steps, [value], fn step, acc ->
        Enum.flat_map(acc, &apply_step(step, &1))
      end)
    end

    {transform, key_by}
  end

  defp apply_step({:map, f}, v), do: [f.(v)]
  defp apply_step({:filter, f}, v), do: if(f.(v), do: [v], else: [])
  defp apply_step({:flat_map, f}, v), do: Enum.to_list(f.(v))

  # --- internals ---

  defp do_into(%Stream{keyed_sink: true} = s, module, name),
    do:
      do_into(
        refuse(%{s | keyed_sink: false}, {:KeyedInterProcedureChannel, describe(s.source)}),
        module,
        name
      )

  defp do_into(%Stream{builder: ref} = s, module, name) do
    consume_chain(ref, s)
    node_ref = resolve_node(ref, module, name)

    update(ref, fn acc ->
      {transform, key_by} = fuse(s.chain)
      edge = %{from: s.source, to: node_ref, transform: transform, key_by: key_by}
      %{acc | edges: [edge | acc.edges]} |> apply_knobs(node_ref, s)
    end)

    %Stream{builder: ref, source: {:node, node_ref}}
  end

  # A consumer edge's mailbox/ladder knobs attach to the target node. Fan-in
  # keeps the first non-nil for each knob.
  defp apply_knobs(acc, node_ref, %Stream{} = s) do
    knobs =
      %{}
      |> maybe_put(:on_full, s.on_full)
      |> maybe_put(:retry_max, s.retry_max)
      |> maybe_put(:retry_backoff_ms, s.retry_backoff_ms)

    existing = Map.get(acc.knobs, node_ref, %{})
    %{acc | knobs: Map.put(acc.knobs, node_ref, Map.merge(knobs, existing))}
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, val), do: Map.put(map, key, val)

  # Unnamed mentions of one module unify to a single node (fan-in). Named nodes
  # are distinct per name; a name reused for a different module collides.
  defp resolve_node(ref, module, nil) do
    node_ref = make_ref()

    update(ref, fn acc ->
      case Map.get(acc.node_by_module, module) do
        nil ->
          acc
          |> put_in([:nodes, node_ref], %{module: module, name: nil})
          |> put_in([:node_by_module, module], node_ref)

        _existing ->
          acc
      end
    end)

    Process.get(acc_key(ref)).node_by_module[module]
  end

  defp resolve_node(ref, module, name) do
    node_ref = make_ref()

    update(ref, fn acc ->
      case Map.get(acc.node_names, name) do
        nil ->
          acc
          |> put_in([:nodes, node_ref], %{module: module, name: name})
          |> put_in([:node_names, name], node_ref)

        _existing ->
          add_issue(acc, {:NodeIdentityCollision, name})
      end
    end)

    Process.get(acc_key(ref)).node_names[name] || node_ref
  end

  defp push_chain(%Stream{keyed: true} = s, _step),
    do: refuse(s, {:TransformAfterKeyBy, describe(s.source)})

  defp push_chain(%Stream{terminal: true} = s, _step),
    do: refuse(s, {:TransformAfterTerminal, describe(s.source)})

  defp push_chain(%Stream{} = s, step), do: %{track_chain(s) | chain: s.chain ++ [step]}

  # Register this handle's chain so an unconsumed one surfaces as DanglingTransformChain.
  defp track_chain(%Stream{chain_id: nil, builder: ref} = s) do
    cid = make_ref()
    update(ref, fn acc -> put_in(acc.chains[cid], describe(s.source)) end)
    %{s | chain_id: cid}
  end

  defp track_chain(%Stream{} = s), do: s

  defp consume_chain(_ref, %Stream{chain_id: nil}), do: :ok

  defp consume_chain(ref, %Stream{chain_id: cid}),
    do: update(ref, fn acc -> %{acc | chains: Map.delete(acc.chains, cid)} end)

  defp refuse(%Stream{builder: ref} = s, issue) do
    update(ref, fn acc -> add_issue(acc, issue) end)
    s
  end

  defp add_issue(acc, issue), do: %{acc | issues: [issue | acc.issues]}

  defp describe({:entry, name}), do: name
  defp describe({:node, ref}), do: inspect(ref)

  defp update(ref, fun), do: Process.put(acc_key(ref), fun.(Process.get(acc_key(ref))))

  defp acc_key(ref), do: {__MODULE__, ref}
end
