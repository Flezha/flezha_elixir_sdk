defmodule Flezha.SDK.Channels do
  @moduledoc """
  The graph-builder token handed to `channels/1`. Openers (`input`/`input_with`),
  edge transforms (`map`/`filter`/`flat_map`/`key_by`), consumers
  (`into`/`into_named`/`output`/`output_with`/`call`,
  `merge` with `from`, and `route_latest`/`route_latest_named`), per-edge knobs
  (`named`/`on_full`/`retry_max`/`retry_backoff_ms`) and `keyed` (output
  identity on the plugin wire the handle reaches). Fan-out is handle reuse, or
  `branch` over a list of arms. The effectively-once tier is a class setting
  (`Flezha.SDK.Relay`), not a builder call.

  This builder RECORDS; it does not judge. Every fluent call appends one link
  to the chain its handle carries — a transform, a key, a plugin output, an edge
  into a tasklet, a merge source closing on one — and the record is kept in
  the process dictionary for the duration of `channels/1` (the twin of the Rust
  builder's `pending`). `Flezha.SDK.Declaration` hands the finished record to
  `flz_core`, which judges it as it judges every SDK's.

  The refusals that stay here are the ones the surface itself makes: `keyed`,
  `named`, an output and a call on an entry handle name a fact an entry channel
  has no end for, and none reaches the record.
  """

  alias Flezha.SDK.{Merge, Response, RouteLatest, Stream}

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
          | {:call, map(), (term() -> binary())}
          | {:into, reference()}
          | {:merge, reference(), non_neg_integer()}
          | {:branch, non_neg_integer()}

  # --- build lifecycle (used by Flezha.SDK.Declaration) ---

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
      merge_groups: 0,
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

  # --- openers ---

  @doc """
  Entry channel: its key and its tasklet receive the payload decoded by
  `codec:`, which the relay's API also announces. `:json`, the default, hands
  the decoded JSON value (`nil` for an empty payload), `:text` the payload as
  a UTF-8 string, and `:bytes` the payload as it arrives; `:msgpack` hands the
  payload as it arrives too, since this SDK carries no MessagePack decoder. A
  payload its codec cannot read is unreadable. `description:` says what the
  channel is for, and `schema:` is a `:json` channel's JSON Schema, a map with
  a `type`; any other schema refuses the class as `SchemaInvalid`.

      c |> input("place", description: "Places an order", schema: %{"type" => "object"})
  """
  def input(%__MODULE__{} = c, name, opts \\ []) when is_binary(name) and is_list(opts) do
    codec = Keyword.get(opts, :codec, :json)

    unless codec in [:json, :text, :bytes, :msgpack],
      do: raise(ArgumentError, "input codec: is one of :json, :text, :bytes or :msgpack")

    open(c, name, &parse(codec, &1), Atom.to_string(codec), opts)
  end

  @doc """
  Entry channel with a custom parser, `{:ok, value}` or `{:error, reason}`; a
  parse failure rides the entry ladder. `description:` is announced as
  `input/3` announces it; the API names the codec `custom`, so a `schema:`
  refuses the class as `SchemaInvalid`.
  """
  def input_with(%__MODULE__{} = c, name, parser, opts \\ [])
      when is_binary(name) and is_function(parser, 1) and is_list(opts),
      do: open(c, name, parser, "custom", opts)

  # An entry channel and what the relay's API announces for it.
  defp open(%__MODULE__{ref: ref}, name, parser, codec, opts) do
    update(ref, fn acc ->
      entry = %{
        codec: codec,
        schema: opts[:schema],
        description: Keyword.get(opts, :description, ""),
        parser: parser,
        after_nodes: map_size(acc.nodes)
      }

      %{
        acc
        | entries: Map.put_new(acc.entries, name, entry),
          entry_order: [name | acc.entry_order]
      }
    end)

    %Stream{builder: ref, source: {:entry, name}}
  end

  # --- edge transforms (fused into one closure per consumer edge) ---

  def map(%Stream{} = s, f) when is_function(f, 1), do: link(s, {:map, f})
  def filter(%Stream{} = s, f) when is_function(f, 1), do: link(s, {:filter, f})
  def flat_map(%Stream{} = s, f) when is_function(f, 1), do: link(s, {:flat_map, f})

  @doc """
  Key an edge, or key a merged node on the mapped value.

  A merge's key is declared once, on the merge, and runs on every source's
  mapped value. A second declaration is recorded as a second key on every
  source, which the check refuses as `DuplicateKeyBy`.
  """
  @spec key_by(Stream.t() | Merge.t(), (term() -> term())) :: Stream.t() | Merge.t()
  def key_by(handle, f)
  def key_by(%Stream{} = s, f) when is_function(f, 1), do: link(s, {:key, f})
  def key_by(%Merge{} = m, f) when is_function(f, 1), do: %{m | keys: m.keys ++ [f]}

  # --- consumers ---

  @doc """
  Route into a tasklet node, or close a merge on one. Unnamed mentions of one
  module unify (fan-in).
  """
  @spec into(Stream.t() | Merge.t(), module()) :: Stream.t()
  def into(handle, module)
  def into(%Stream{} = s, module), do: do_into(s, module, nil)
  def into(%Merge{} = m, module), do: close_merge(m, module, nil)

  @doc """
  Route into a tasklet node named `name`, or close a merge on one: `name` is
  the node's identity, which its Cells are named by in place of the tasklet's
  `name/0`, so one module can back two nodes. Identity collisions refuse.
  """
  @spec into_named(Stream.t() | Merge.t(), module(), String.t()) :: Stream.t()
  def into_named(handle, module, name)

  def into_named(%Stream{} = s, module, name) when is_binary(name),
    do: do_into(s, module, name)

  def into_named(%Merge{} = m, module, name) when is_binary(name),
    do: close_merge(m, module, name)

  @doc """
  Open a merge: one tasklet fed by several source streams, each under its own
  mapping, closed with `into/2`.

      c
      |> merge()
      |> from(ticks, &{:tick, &1})
      |> from(quotes, &{:quote, &1})
      |> key_by(&symbol_of/1)
      |> into(Detector)

  One import writes the whole pipe: `from/3`, `key_by/2`, `into/2` and
  `into_named/3` all dispatch on the handle they receive, so a merge step needs
  no module prefix of its own.
  """
  @spec merge(t()) :: Merge.t()
  def merge(%__MODULE__{ref: ref}), do: %Merge{builder: ref}

  @doc """
  Add a source stream to a merge and the mapping that lifts its value into the
  merged input. The mapping is recorded on that source's own chain, after any
  transform the handle already carries.
  """
  @spec from(Merge.t(), Stream.t(), (term() -> term())) :: Merge.t()
  def from(%Merge{} = m, %Stream{} = source, mapping) when is_function(mapping, 1),
    do: %{m | sources: m.sources ++ [map(source, mapping)]}

  @doc """
  Hand every key `key_fn` reads off `traffic` each distinct document on
  `documents` once, on that key's own next traffic item, as `{key, document}`.
  Expands to a merge into the packaged `Flezha.SDK.RouteLatest` node, named
  `route_latest`, and answers its output stream.
  """
  @spec route_latest(t(), Stream.t(), Stream.t(), (term() -> String.t())) :: Stream.t()
  def route_latest(%__MODULE__{} = c, documents, traffic, key_fn),
    do: wire_route_latest(c, documents, traffic, key_fn, nil)

  @doc "`route_latest/4` into a router node of the given name."
  @spec route_latest_named(t(), Stream.t(), Stream.t(), (term() -> String.t()), String.t()) ::
          Stream.t()
  def route_latest_named(%__MODULE__{} = c, documents, traffic, key_fn, name)
      when is_binary(name),
      do: wire_route_latest(c, documents, traffic, key_fn, name)

  # The traffic value is replaced by its key on its own edge, so the payload
  # never reaches the router's state.
  defp wire_route_latest(c, %Stream{} = documents, %Stream{} = traffic, key_fn, name)
       when is_function(key_fn, 1) do
    c
    |> merge()
    |> from(documents, &{:document, &1})
    |> from(map(traffic, key_fn), &{:traffic, &1})
    |> close_merge(RouteLatest, name)
  end

  @doc """
  Terminate the channel on the plugin layer with default serialization
  (bytes/UTF-8/JSON), naming it. The string written here IS the channel's name
  everywhere one appears; `output/1` leaves it derived as it would be without an
  output at all. Returns the handle.
  """
  def output(%Stream{} = s, plugin \\ nil) when is_nil(plugin) or is_binary(plugin),
    do: output_with(s, plugin, &__MODULE__.default_serialize/1)

  @doc """
  `output/2` with a custom serializer. Returns the handle, so a channel carrying
  an output AND a consumer (the hybrid shape) is one pipeline.
  """
  def output_with(%Stream{} = s, plugin \\ nil, serializer)
      when (is_nil(plugin) or is_binary(plugin)) and is_function(serializer, 1) do
    s = name_from_sink(s, plugin)
    %{link(s, {:sink, plugin, serializer}) | chain_id: nil}
  end

  @doc """
  End the chain in a call to another relay: every value reaching it, after
  the chain's transforms and key, is one request, written as `output/2` writes
  (`serialize:`). `target` names an instance, or a partition group whose
  member the chain's key chooses; `factory:` instead names a factory whose
  instance the key names, started when absent unless `create: false`.
  `deadline_ms:` is required and positive; `channel:` names the callee's
  entry channel the request enters on, and `answer_channel:` the callee's
  channel whose output answers (its first output for the request when
  unset).

  Answers a handle on the Response channel, the entry channel
  `<channel>.response`, whose values are `%Flezha.SDK.Response{}`: the answer
  read by `decode:` as `input_with/3`'s parser reads (the raw bytes by
  default). A call is not an edge, so the Response channel may feed the
  tasklet that made the call. A channel ends in one output or one call
  (`ChannelEndsTwice`).

      turns |> call("pricing", deadline_ms: 300) |> into(Price)
      turns |> key_by(& &1.session) |> call(factory: "agent", deadline_ms: 60_000, create: false)
  """
  @spec call(Stream.t(), String.t() | nil, keyword()) :: Stream.t()
  def call(stream, target \\ nil, opts)

  def call(%Stream{source: {:entry, name}} = s, _target, _opts),
    do: refuse(s, {:OutputOnEntry, name})

  def call(%Stream{builder: ref} = s, target, opts) when is_list(opts) do
    name = channel_name(Process.get(acc_key(ref)), s.source) <> ".response"
    _ = link(s, {:call, call_end(target, opts, name), opts[:serialize] || (&default_serialize/1)})
    decode = opts[:decode] || (&parse(:bytes, &1))
    input_with(%__MODULE__{ref: ref}, name, &Response.decode(&1, decode))
  end

  # The end as the wire names its callee: `id:<target>` or `factory:<factory>`.
  defp call_end(target, opts, response) do
    {factory, deadline_ms} = {opts[:factory], Keyword.fetch!(opts, :deadline_ms)}

    unless is_binary(target) != is_binary(factory) and is_integer(deadline_ms) and
             deadline_ms >= 0,
           do: raise(ArgumentError, "call takes one target or factory: and deadline_ms: >= 0")

    %{
      target: if(factory, do: "factory:" <> factory, else: "id:" <> target),
      factory?: factory != nil,
      create: Keyword.get(opts, :create, true),
      deadline_ms: deadline_ms,
      channel: opts[:channel],
      answer_channel: opts[:answer_channel],
      response: response
    }
  end

  # An output that names the channel records the name where `named/2` records its
  # own, so one rule answers what a channel is called and the check can see a
  # channel named twice.
  defp name_from_sink(%Stream{} = s, nil), do: s

  defp name_from_sink(%Stream{source: {:node, node_ref}, builder: ref} = s, plugin) do
    update(ref, fn acc ->
      update_in(acc, [:nodes, node_ref, :sink_names], &(&1 ++ [plugin]))
    end)

    s
  end

  defp name_from_sink(%Stream{source: {:entry, name}} = s, plugin),
    do: refuse(s, {:ChannelNamedTwice, name, name, plugin})

  @doc """
  Declare output identity on the plugin wire this handle reaches — the
  reference's `keyed()`: its frames carry `flz.cell`, `flz.seq` and
  `flz.key`. Refused on an entry handle, which has no output of its own to
  stamp; a chain declared keyed that reaches no `output` is refused by the check
  as `KeyedChannelWithoutPluginWire`.
  """
  def keyed(%Stream{source: {:entry, name}} = s), do: refuse(s, {:KeyedOnEntry, name})

  def keyed(%Stream{} = s), do: link(s, :keyed)

  @doc """
  Name the channel the source tasklet node produces on. The tasklet keeps its
  own name, or the identity `into_named/3` gave it.
  """
  def named(%Stream{source: {:entry, name}} = s, _name),
    do: refuse(s, {:NamedOnEntry, name})

  def named(%Stream{source: {:node, node_ref}, builder: ref} = s, name) when is_binary(name) do
    update(ref, fn acc -> put_in(acc, [:nodes, node_ref, :channel], name) end)
    s
  end

  @doc """
  Fan out: call each arm with this same stream, which builds exactly the
  topology the arms' calls off one held handle would, and answer the stream.

      placed |> branch([&into(&1, ConfirmFill), &into(&1, RecordOrder)])

  The arity is recorded on a chain of its own, so the check can refuse a
  branch of fewer than two arms as `BranchTooFewArms`.
  """
  @spec branch(Stream.t(), [(Stream.t() -> term())]) :: Stream.t()
  def branch(%Stream{} = s, arms) when is_list(arms) do
    _ = link(%{s | chain_id: nil}, {:branch, length(arms)})
    Enum.each(arms, & &1.(s))
    s
  end

  # --- per-edge knobs (ride the chain; applied to the edges it closes) ---

  def on_full(%Stream{} = s, policy) when policy in [:drop_nothing, :drop_newest, :drop_oldest],
    do: knob(s, :on_full, policy)

  def retry_max(%Stream{} = s, n) when is_integer(n) and n >= 0, do: knob(s, :retry_max, n)

  def retry_backoff_ms(%Stream{} = s, ms) when is_integer(ms) and ms >= 0,
    do: knob(s, :retry_backoff_ms, ms)

  # --- default codecs ---

  defp parse(:json, ""), do: {:ok, nil}
  defp parse(:json, bytes), do: Jason.decode(bytes)

  defp parse(:text, bytes),
    do: if(String.valid?(bytes), do: {:ok, bytes}, else: {:error, :invalid_utf8})

  defp parse(_bytes_or_msgpack, bytes), do: {:ok, bytes}

  # `output/2`'s serializer, captured by its remote name so the API can tell an
  # output that writes JSON from one with a serializer of its own.
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
  `named/2` name, the name its `output` declared, else its tasklet's.
  """
  @spec channel_name(map(), {:entry, String.t()} | {:node, reference()}) :: String.t()
  def channel_name(_acc, {:entry, name}), do: name

  def channel_name(acc, {:node, ref}), do: node_channel_name(Map.fetch!(acc.nodes, ref))

  @doc """
  The same rule off a node record: `named/2`'s name, the first name its outputs
  wrote, else the tasklet's own name.
  """
  def node_channel_name(%{channel: channel, sink_names: sinks} = node),
    do: channel || List.first(sinks) || node_tasklet_name(node)

  @doc "The name a node's Cells are identified by: `into_named/3`'s, else the tasklet's."
  def node_tasklet_name(%{identity: identity, module: module}), do: identity || module.name()

  # --- internals ---

  defp do_into(%Stream{builder: ref} = s, module, name) do
    node_ref = resolve_node(ref, module, name)
    _ = link(s, {:into, node_ref})
    %Stream{builder: ref, source: {:node, node_ref}}
  end

  # The one body behind `into/2`, `into_named/3` and the router.
  # The node resolves as `into/2`'s does; every source chain takes the merge's
  # keys after its mapping, so a key reads the mapped value, and closes on the
  # node under one group.
  @doc false
  def close_merge(%Merge{builder: ref, sources: sources, keys: keys}, module, name) do
    node_ref = resolve_node(ref, module, name)
    group = resolve(ref, &{%{&1 | merge_groups: &1.merge_groups + 1}, &1.merge_groups})

    for source <- sources do
      keys
      |> Enum.reduce(source, &key_by(&2, &1))
      |> link({:merge, node_ref, group})
    end

    %Stream{builder: ref, source: {:node, node_ref}}
  end

  # Unnamed mentions of one module unify to a single node (fan-in). A named
  # node is distinct per name; a name already claimed by another module's node
  # still creates one, so the record carries both and the core names the
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

  defp put_node(acc, node_ref, module, identity),
    do:
      put_in(acc, [:nodes, node_ref], %{
        module: module,
        identity: identity,
        channel: nil,
        sink_names: [],
        seq: map_size(acc.nodes)
      })

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
  # ref it settled on, and a merge the group it took, and each is only known
  # inside the update.
  defp resolve(ref, fun) do
    {acc, answer} = fun.(Process.get(acc_key(ref)))
    Process.put(acc_key(ref), acc)
    answer
  end

  defp acc_key(ref), do: {__MODULE__, ref}
end
