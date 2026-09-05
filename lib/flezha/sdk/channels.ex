defmodule Flezha.SDK.Channels do
  @moduledoc """
  The graph-builder token handed to `channels/1`. Openers (`input`/`input_with`),
  edge transforms (`map`/`filter`/`flat_map`/`key_by`), consumers
  (`into`/`into_named`/`sink`/`sink_with`/`sealed`), per-edge knobs
  (`named`/`on_full`/`retry_max`/`retry_backoff_ms`), `keyed` (output
  identity on the plugin wire the handle reaches) and the class-level tier
  declarations (`effectively_once`, the three checkpoint options and the two
  correction options, which take and answer the token itself). Fan-out is
  handle reuse.

  This builder RECORDS; it does not judge. Every fluent call appends one link
  to the chain its handle carries — a transform, a key, a plugin sink, an edge
  into a procedure — and the record is kept in the process dictionary for the
  duration of `channels/1` (the twin of the Rust builder's `pending`).
  `Flezha.SDK.Topology.build/2` then judges the finished record in the three
  stages `flz_core::host::register::check` judges its own: the chain rules,
  the derivation, then the topology and class facts. The Elixir SDK inherits
  nothing from `flz_core`, so this is a re-expression of those rules, measured
  against `test/fixtures/reference_topologies.txt` like every other SDK's.

  The two refusals that stay here are the ones the surface itself makes:
  `keyed` and `named` on an entry handle name a fact an entry channel has no
  end for, and neither reaches the record.
  """

  alias Flezha.SDK.Stream

  @enforce_keys [:ref]
  defstruct [:ref]

  @type t :: %__MODULE__{ref: reference()}

  @typedoc """
  One link of a chain, in the order the author called it. A transform and a
  key carry the closure the derivation fuses; a terminator carries what the
  edge or the plugin wire it closes on needs.
  """
  @type link ::
          {:map | :filter | :flat_map | :key, (term() -> term())}
          | :keyed
          | {:sink, String.t(), (term() -> binary())}
          | {:into, reference()}

  # --- build lifecycle (used by Topology.build/2) ---

  @doc false
  def new do
    ref = make_ref()

    Process.put(acc_key(ref), %{
      entries: %{},
      entry_order: [],
      nodes: %{},
      node_by_module: %{},
      node_names: %{},
      chains: %{},
      chain_seq: 0,
      tier: %{effectively_once: false, checkpoint: nil, correction: nil},
      issues: []
    })

    %__MODULE__{ref: ref}
  end

  @doc false
  def take(%__MODULE__{ref: ref}) do
    acc = Process.get(acc_key(ref))
    Process.delete(acc_key(ref))

    %{
      acc
      | entry_order: Enum.reverse(acc.entry_order),
        issues: Enum.reverse(acc.issues),
        chains: settled_chains(acc.chains)
    }
  end

  # The record the check reads: every chain in declaration order, each one's
  # links in the order they were called.
  defp settled_chains(chains) do
    chains
    |> Map.values()
    |> Enum.sort_by(& &1.seq)
    |> Enum.map(&%{&1 | links: Enum.reverse(&1.links)})
  end

  # --- class-level declarations (the effectively-once tier) ---

  @doc "Declare the effectively-once tier for the whole class."
  def effectively_once(%__MODULE__{ref: ref} = c) do
    update(ref, fn acc -> put_in(acc.tier.effectively_once, true) end)
    c
  end

  @doc "Acknowledge inputs at checkpoint granularity rather than per input."
  def checkpoint_ack(%__MODULE__{} = c), do: checkpoint_policy(c, & &1)

  @doc "Close the staged interval every `n` inputs."
  def checkpoint_every(%__MODULE__{} = c, n) when is_integer(n) and n >= 0,
    do: checkpoint_policy(c, &%{&1 | every: n})

  @doc "Hold the interval's outputs until it closes, and close it after `ms` at the latest."
  def outputs_at_checkpoint(%__MODULE__{} = c, ms) when is_integer(ms) and ms >= 0,
    do: checkpoint_policy(c, &%{&1 | outputs: :at_checkpoint, max_delay_ms: ms})

  # The one Elixir site that implies checkpoint granularity: ANY checkpoint key
  # raises the policy, so the three options compose in any order and none
  # clears another's. This is the re-expression of the core's derivation, where
  # `checkpoint_ack`, `checkpoint_every`, `checkpoint_outputs` and
  # `checkpoint_max_delay_ms` are written raw and the granularity is implied
  # from any one of them.
  defp checkpoint_policy(%__MODULE__{ref: ref} = c, amend) do
    update(ref, fn acc ->
      put_in(
        acc.tier.checkpoint,
        amend.(acc.tier.checkpoint || %{every: nil, outputs: :as_produced, max_delay_ms: nil})
      )
    end)

    c
  end

  @doc "Declare a late-input correction window in milliseconds; requires `effectively_once/1`."
  def correction_window(%__MODULE__{} = c, ms) when is_integer(ms) and ms >= 0,
    do: correction_policy(c, &%{&1 | window_ms: ms})

  @doc "Fate of an input later than the window: `:process_as_arrival` (the default) or `:drop`."
  def beyond_window_policy(%__MODULE__{} = c, policy)
      when policy in [:process_as_arrival, :drop],
      do: correction_policy(c, &%{&1 | beyond_window: policy})

  # Both correction options amend the one policy, so they compose in either
  # order and neither clears the other. A policy declared alone therefore
  # leaves the window at zero, which is refused: a fate for a window nobody
  # declared is an authoring error, not a default.
  defp correction_policy(%__MODULE__{ref: ref} = c, amend) do
    update(ref, fn acc ->
      put_in(
        acc.tier.correction,
        amend.(acc.tier.correction || %{window_ms: 0, beyond_window: :process_as_arrival})
      )
    end)

    c
  end

  # --- openers ---

  @doc "Bytes-passthrough entry channel."
  def input(%__MODULE__{} = c, name) when is_binary(name),
    do: input_with(c, name, &default_parse/1)

  @doc "Entry channel with a custom parser; a parse failure rides the entry ladder."
  def input_with(%__MODULE__{ref: ref}, name, parser)
      when is_binary(name) and is_function(parser, 1) do
    update(ref, fn acc ->
      %{
        acc
        | entries: Map.put_new(acc.entries, name, %{parser: parser}),
          entry_order: [name | acc.entry_order]
      }
    end)

    %Stream{builder: ref, source: {:entry, name}}
  end

  # --- edge transforms (fused into one closure per consumer edge) ---

  def map(%Stream{} = s, f) when is_function(f, 1), do: link(s, {:map, f})
  def filter(%Stream{} = s, f) when is_function(f, 1), do: link(s, {:filter, f})
  def flat_map(%Stream{} = s, f) when is_function(f, 1), do: link(s, {:flat_map, f})
  def key_by(%Stream{} = s, f) when is_function(f, 1), do: link(s, {:key, f})

  # --- consumers ---

  @doc "Route into a procedure node. Unnamed mentions of one module unify (fan-in)."
  def into(%Stream{} = s, module), do: do_into(s, module, nil)

  @doc "Route into a named procedure node (identity collisions refuse)."
  def into_named(%Stream{} = s, module, name) when is_binary(name),
    do: do_into(s, module, name)

  @doc "Terminal plugin sink with default serialization (bytes/UTF-8/JSON). Returns the handle."
  def sink(%Stream{} = s, plugin) when is_binary(plugin),
    do: sink_with(s, plugin, &default_serialize/1)

  @doc """
  Terminal plugin sink with a custom serializer. Returns the handle, so a
  channel carrying a sink AND a consumer (the hybrid shape) is one pipeline.
  """
  def sink_with(%Stream{} = s, plugin, serializer)
      when is_binary(plugin) and is_function(serializer, 1) do
    %{link(s, {:sink, plugin, serializer}) | chain_id: nil}
  end

  @doc """
  Declare output identity on the plugin wire this handle reaches — the
  reference's `keyed()`: its frames carry `flz.entity`, `flz.seq` and
  `flz.key`. Refused on an entry handle, which has no output of its own to
  stamp; a chain declared keyed that reaches no `sink` is refused by the check
  as `KeyedChannelWithoutPluginWire`.
  """
  def keyed(%Stream{source: {:entry, name}} = s), do: refuse(s, {:KeyedOnEntry, name})

  def keyed(%Stream{} = s), do: link(s, :keyed)

  @doc "Name the source procedure node, and with it the channel it produces."
  def named(%Stream{source: {:entry, name}} = s, _name),
    do: refuse(s, {:NamedOnEntry, name})

  def named(%Stream{source: {:node, node_ref}, builder: ref} = s, name) when is_binary(name) do
    update(ref, fn acc -> claim_name(acc, name, node_ref) end)
    s
  end

  @doc "Type-level terminator; adds no topology."
  def sealed(%Stream{} = s), do: s

  # --- per-edge knobs (ride the chain; applied to the edges it closes) ---

  def on_full(%Stream{} = s, policy) when policy in [:drop_nothing, :drop_newest, :drop_oldest],
    do: knob(s, :on_full, policy)

  def retry_max(%Stream{} = s, n) when is_integer(n) and n >= 0, do: knob(s, :retry_max, n)

  def retry_backoff_ms(%Stream{} = s, ms) when is_integer(ms) and ms >= 0,
    do: knob(s, :retry_backoff_ms, ms)

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

  @doc """
  The channel a source is named by on the wire: an entry's own name, a node's
  `named/2` name, else its procedure's. One rule, read by the check's refusals
  and by the compiled routes, so the `named` override is applied once for
  every terminator the channel carries.
  """
  @spec channel_name(map(), {:entry, String.t()} | {:node, reference()}) :: String.t()
  def channel_name(_acc, {:entry, name}), do: name

  def channel_name(acc, {:node, ref}) do
    %{module: module, name: name} = Map.fetch!(acc.nodes, ref)
    name || module.name()
  end

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

  defp do_into(%Stream{builder: ref} = s, module, name) do
    node_ref = resolve_node(ref, module, name)
    _ = link(s, {:into, node_ref})
    %Stream{builder: ref, source: {:node, node_ref}}
  end

  # Unnamed mentions of one module unify to a single node (fan-in). A named
  # node is distinct per name; a name already claimed by another node still
  # creates one, so the record carries both claims and the check names the
  # collision.
  defp resolve_node(ref, module, nil) do
    resolve(ref, fn acc ->
      case Map.get(acc.node_by_module, module) do
        nil ->
          node_ref = make_ref()

          {acc
           |> put_node(node_ref, module, nil)
           |> put_in([:node_by_module, module], node_ref), node_ref}

        existing ->
          {acc, existing}
      end
    end)
  end

  defp resolve_node(ref, module, name) do
    resolve(ref, fn acc ->
      claimed = Map.get(acc.node_names, name)
      node_ref = make_ref()

      cond do
        claimed && Map.fetch!(acc.nodes, claimed).module == module ->
          {acc, claimed}

        claimed ->
          {put_node(acc, node_ref, module, name), node_ref}

        true ->
          {acc |> put_node(node_ref, module, name) |> put_in([:node_names, name], node_ref),
           node_ref}
      end
    end)
  end

  defp put_node(acc, node_ref, module, name),
    do: put_in(acc, [:nodes, node_ref], %{module: module, name: name, seq: map_size(acc.nodes)})

  # `named/2` on a node whose name another node already claims writes the name
  # anyway: two nodes then answer to it and the check names the collision.
  defp claim_name(acc, name, node_ref) do
    acc = put_in(acc, [:nodes, node_ref, :name], name)

    if Map.has_key?(acc.node_names, name),
      do: acc,
      else: put_in(acc, [:node_names, name], node_ref)
  end

  defp link(%Stream{builder: ref} = s, link) do
    s = track_chain(s)
    update(ref, fn acc -> update_in(acc.chains[s.chain_id].links, &[link | &1]) end)
    s
  end

  # A knob rides the chain it was declared on and reaches the edges that chain
  # closes; the first declaration of a knob on one chain wins.
  defp knob(%Stream{builder: ref} = s, key, value) do
    s = track_chain(s)

    update(ref, fn acc ->
      update_in(acc.chains[s.chain_id].knobs, &Map.put_new(&1, key, value))
    end)

    s
  end

  # A handle with no chain of its own opens one; reusing a handle that has one
  # extends it, which is how a fan-out shares its transforms.
  defp track_chain(%Stream{chain_id: nil, builder: ref, source: source} = s) do
    cid = make_ref()

    update(ref, fn acc ->
      %{
        acc
        | chains:
            Map.put(acc.chains, cid, %{
              channel: source,
              seq: acc.chain_seq,
              links: [],
              knobs: %{}
            }),
          chain_seq: acc.chain_seq + 1
      }
    end)

    %{s | chain_id: cid}
  end

  defp track_chain(%Stream{} = s), do: s

  defp refuse(%Stream{builder: ref} = s, issue) do
    update(ref, fn acc -> %{acc | issues: [issue | acc.issues]} end)
    s
  end

  defp update(ref, fun), do: Process.put(acc_key(ref), fun.(Process.get(acc_key(ref))))

  # The one write that also answers: node resolution has to hand the caller the
  # ref it settled on, and that ref is only known inside the update.
  defp resolve(ref, fun) do
    {acc, node_ref} = fun.(Process.get(acc_key(ref)))
    Process.put(acc_key(ref), acc)
    node_ref
  end

  defp acc_key(ref), do: {__MODULE__, ref}
end
