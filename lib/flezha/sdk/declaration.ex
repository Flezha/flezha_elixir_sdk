defmodule Flezha.SDK.Declaration do
  @moduledoc """
  A relay module as the class declaration every veneer host hands `flz_core`:
  the chains `Flezha.SDK.Channels` recorded, the settings tree as the module
  writes it, and the tasklet facts `Flezha.SDK.Tasklet` answers. The core
  judges it; its verdict, the consumer edges and every channel's final name,
  comes back from `check_class`, and the closures each chain carries are
  attached to those edges here, for `Flezha.SDK.Dispatch` to run.

  What the Elixir surface implies without writing it is declared outright: a
  tasklet exporting `cells_for/1` or `compensate/3` declares that override, and
  the clock is the one `Flezha.SDK.Tasklet.clock/1` answers.
  """

  alias Flezha.SDK.{Channels, Error, Native, Tasklet}

  @sdk_lang 3
  @change_format_id 7
  @sdk_version Mix.Project.config()[:version]

  # The refusals the builder makes itself, before anything reaches the core.
  @surface_issues %{
    ChannelNamedTwice: [:channel, :named, :output],
    DeferredBuilderSurface: [:module, :reason],
    KeyedOnEntry: [:channel],
    NamedOnEntry: [:channel],
    OutputOnEntry: [:channel],
    RetiredCallback: [:tasklet, :callback, :instead],
    TwoClocks: [:tasklet]
  }

  @typedoc """
  A declared class as the dispatcher reads it. `channels` and `nodes` are
  indexed as the declaration indexes them; `out_channel` maps a tasklet to the
  channel it produces on.
  """
  @type t :: %{
          factory_id: String.t(),
          json: binary(),
          steps?: boolean(),
          channels: tuple(),
          nodes: tuple(),
          names: tuple(),
          out_channel: %{non_neg_integer() => non_neg_integer()}
        }

  @doc """
  Declare `module` to `core`, or the refusal: the core's `topology.*` code with
  every issue in `details.issues`, or a refusal the builder made itself.
  """
  @spec declare(Native.core(), module()) :: {:ok, t()} | {:error, Error.t()}
  def declare(core, module) do
    with {:ok, class} <- build(module),
         derived when is_binary(derived) <- Native.answered(Native.check_class(core, class.json)),
         :ok <- Native.answered(Native.declare_class(core, class.json)) do
      {:ok, attach(class, Jason.decode!(derived))}
    end
  end

  @doc """
  The core's verdict on `module` with nothing declared and nothing connected:
  the derived declaration, or the refusal.
  """
  @spec check(module()) :: {:ok, map()} | {:error, Error.t()}
  def check(module) do
    core = Native.context_new(Native.schema_version())

    try do
      with {:ok, class} <- build(module),
           derived when is_binary(derived) <-
             Native.answered(Native.check_class(core, class.json)),
           do: {:ok, Jason.decode!(derived)}
    after
      Native.context_free(core)
    end
  end

  @doc "The class `module` declares, before the core has judged it."
  @spec build(module()) :: {:ok, t()} | {:error, Error.t()}
  def build(module) do
    c = Channels.new()

    case declare_on(module, c) do
      :ok ->
        acc = Channels.take(c)

        case acc.issues ++ tasklet_issues(acc) do
          [] -> {:ok, from_record(module, acc)}
          issues -> {:error, refusal(issues)}
        end

      {:deferred, reason} ->
        _ = Channels.take(c)
        {:error, refusal([{:DeferredBuilderSurface, inspect(module), reason}])}
    end
  end

  # What an Elixir tasklet can write that the declaration cannot carry: the
  # core sees one clock, so these are judged here.
  defp tasklet_issues(acc) do
    for {_ref, %{module: m} = node} <- Enum.sort_by(acc.nodes, fn {_, n} -> n.seq end),
        name = Channels.node_tasklet_name(node),
        issue <- tasklet_issues(m, name),
        uniq: true,
        do: issue
  end

  defp tasklet_issues(module, name) do
    retired =
      for {callback, instead} <- Tasklet.retired_callbacks(module),
          do: {:RetiredCallback, name, callback, instead}

    if Tasklet.exports?(module, :event_time, 1) and Tasklet.logical_clock?(module),
      do: [{:TwoClocks, name} | retired],
      else: retired
  end

  defp declare_on(module, c) do
    _ = module.channels(c)
    :ok
  rescue
    e in UndefinedFunctionError -> {:deferred, Exception.message(e)}
  end

  # The issues as one error, as the core renders a verdict: the first issue's
  # code and details, and every issue in `details.issues`.
  defp refusal(issues) do
    [first | _] =
      listed =
      Enum.map(issues, fn issue ->
        [tag | fields] = Tuple.to_list(issue)
        code = "topology." <> Macro.underscore(Atom.to_string(tag))
        Error.new(code, Enum.zip(Map.fetch!(@surface_issues, tag), fields))
      end)

    %{first | details: Error.plain_details(Map.put(first.details, "issues", listed))}
  end

  defp from_record(module, acc) do
    nodes = Enum.sort_by(acc.nodes, fn {_ref, node} -> node.seq end)
    index_of = nodes |> Enum.with_index() |> Map.new(fn {{ref, _}, i} -> {ref, i} end)
    steps? = steps?(module)

    # A named node keeps its channel with nothing behind it: in steps that
    # channel is the pass's handoff, and either way the core judges its name.
    named = for {ref, %{channel: c}} <- nodes, c != nil, do: ref
    producing = Enum.uniq(for(%{channel: {:node, ref}} <- acc.chains, do: ref) ++ named)

    # Declaration order: an entry where it was opened among the nodes, a node's
    # channel right after the node.
    sources =
      (Enum.map(acc.entry_order, &{acc.entries[&1].after_nodes, {:entry, &1}}) ++
         Enum.map(producing, &{index_of[&1] + 0.5, {:node, &1}}))
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(&elem(&1, 1))

    channels =
      for {source, index} <- Enum.with_index(sources) do
        chains = for chain <- acc.chains, chain.channel == source, do: chain
        channel(acc, source, index, chains, index_of)
      end

    node_list = Enum.map(nodes, fn {_ref, node} -> node end)
    tasklets = Enum.map(node_list, &tasklet/1)

    decl =
      %{
        factory_id: module.factory_id(),
        sdk_lang: @sdk_lang,
        sdk_version: @sdk_version,
        change_format_id: @change_format_id,
        compact_after_bytes: compact_after_bytes(node_list),
        channels: Enum.map(channels, & &1.decl),
        tasklets: tasklets
      }
      |> Map.merge(settings(module))
      |> drop_nil()

    %{
      factory_id: module.factory_id(),
      json: Jason.encode!(decl),
      steps?: steps?,
      channels: Enum.map(channels, &Map.delete(&1, :decl)),
      nodes: node_list |> Enum.map(& &1.module) |> List.to_tuple(),
      names: tasklets |> Enum.map(& &1.name) |> List.to_tuple(),
      out_channel:
        Map.new(producing, fn ref ->
          {index_of[ref], Enum.find_index(sources, &(&1 == {:node, ref}))}
        end)
    }
  end

  defp setting(module, key), do: if(Tasklet.exports?(module, key, 0), do: apply(module, key, []))

  defp steps?(module), do: match?(:steps, run_name(setting(module, :run)))

  defp run_name({name, _members}), do: name
  defp run_name(name), do: name

  # The settings tree as the author wrote it; the core judges it.
  defp settings(module) do
    for key <- [:description, :max_duration_ms, :run, :durable],
        Tasklet.exports?(module, key, 0),
        into: %{},
        do: {key, tree(apply(module, key, []))}
  end

  defp tree(v) when is_boolean(v) or is_number(v) or is_binary(v) or is_nil(v), do: v
  defp tree(v) when is_atom(v), do: Atom.to_string(v)
  defp tree({name, members}) when is_list(members), do: %{tree(name) => tree(members)}
  defp tree([{_, _} | _] = kw), do: Map.new(kw, fn {k, v} -> {tree(k), tree(v)} end)
  defp tree([]), do: %{}
  defp tree(other), do: inspect(other)

  # The smallest threshold across the class's changeful tasklets, declared only
  # when it is not the default.
  defp compact_after_bytes(nodes) do
    threshold =
      nodes
      |> Enum.map(& &1.module)
      |> Enum.filter(&Tasklet.changeful?/1)
      |> Enum.map(&Tasklet.compact_after_bytes/1)
      |> Enum.min(fn -> nil end)

    if threshold != Tasklet.default_compact_after_bytes(), do: threshold
  end

  defp channel(acc, source, index, chains, index_of) do
    {kind, producer, name, entry} =
      case source do
        {:entry, name} ->
          {"entry", nil, name, acc.entries[name]}

        {:node, ref} ->
          node = acc.nodes[ref]
          {"inter_task", index_of[ref], Channels.node_channel_name(node), node}
      end

    read = Enum.map(chains, &read_chain(&1, index_of))
    sink = Enum.find_value(read, & &1.sink)
    call = Enum.find(read, & &1.call)
    knobs = Enum.reduce(chains, %{}, &Map.merge(&1.knobs, &2))
    # `named/2` names the node's channel; as a chain's first link, the core
    # sees it beside any name an output writes.
    named =
      if kind == "inter_task" and entry.channel,
        do: [%{link: "named", name: entry.channel}],
        else: []

    links = if read == [], do: [named], else: Enum.map(read, &(named ++ &1.links))

    decl =
      drop_nil(%{
        name: name,
        kind: kind,
        producer: producer,
        chains: Enum.reject(links, &(&1 == [])),
        keyed: Enum.any?(read, & &1.keyed?),
        codec: codec(kind, entry, sink),
        schema: if(kind == "entry", do: entry.schema),
        description: if(kind == "entry", do: entry.description),
        overflow: knobs[:on_full] && Atom.to_string(knobs[:on_full]),
        retry_max: knobs[:retry_max],
        retry_backoff_ms: knobs[:retry_backoff_ms]
      })

    %{
      decl: decl,
      index: index,
      kind: kind,
      name: name,
      parser: if(kind == "entry", do: entry.parser),
      sink: sink,
      call: call,
      keyed?: decl.keyed,
      chains: read,
      consumers: []
    }
  end

  defp codec("entry", entry, _sink), do: entry.codec
  defp codec(_inter, _node, nil), do: nil

  defp codec(_inter, _node, sink),
    do: if(sink == (&Channels.default_serialize/1), do: "json", else: "custom")

  defp read_chain(%{links: links}, index_of) do
    init = %{
      links: [],
      transforms: [],
      key: nil,
      sink: nil,
      call: nil,
      keyed?: false
    }

    links
    |> Enum.reduce(init, fn link, r -> read_link(link, r, index_of) end)
    |> Map.update!(:links, &Enum.reverse/1)
  end

  defp read_link({t, f}, r, _index_of) when t in [:map, :filter, :flat_map],
    do: %{r | links: [%{link: "transform"} | r.links], transforms: r.transforms ++ [{t, f}]}

  defp read_link({:key, f}, r, _index_of), do: %{r | links: [%{link: "key"} | r.links], key: f}
  defp read_link(:keyed, r, _index_of), do: %{r | keyed?: true}

  defp read_link({:into, ref}, r, index_of),
    do: %{r | links: [%{link: "into", tasklet: index_of[ref]} | r.links]}

  defp read_link({:merge, ref, group}, r, index_of) do
    link = %{link: "merge", tasklet: index_of[ref], group: group}
    %{r | links: [link | r.links]}
  end

  defp read_link({:sink, name, serializer}, r, _index_of),
    do: %{r | links: [drop_nil(%{link: "sink", name: name}) | r.links], sink: serializer}

  defp read_link({:call, end_, serializer}, r, _index_of) do
    {target, factory} =
      case end_.target do
        "factory:" <> factory -> {nil, factory}
        "id:" <> target -> {target, nil}
      end

    link =
      drop_nil(%{
        link: "call",
        target: target,
        factory: factory,
        deadline_ms: end_.deadline_ms,
        create: end_.create,
        channel: end_.channel,
        answer_channel: end_.answer_channel,
        response: end_.response
      })

    %{r | links: [link | r.links], call: serializer}
  end

  defp read_link({:branch, arms}, r, _index_of),
    do: %{r | links: [%{link: "branch", arms: arms} | r.links]}

  defp tasklet(%{module: module} = node) do
    drop_nil(%{
      name: Channels.node_tasklet_name(node),
      cell_keys: Tasklet.cell_keys(module),
      idle_ttl_ms: Tasklet.idle_ttl_ms(module),
      max_cells: Tasklet.max_cells(module),
      cells_for_overridden: Tasklet.exports?(module, :cells_for, 1),
      compensate_overridden: Tasklet.compensates?(module),
      clock: Atom.to_string(Tasklet.clock(module)),
      mailbox_capacity: Tasklet.mailbox_capacity(module),
      change_is_nochange: not Tasklet.changeful?(module),
      schema_version: Tasklet.schema_version(module),
      min_readable_schema_version: Tasklet.min_readable_schema_version(module),
      migrates:
        Tasklet.exports?(module, :migrate_state, 2) or
          Tasklet.exports?(module, :migrate_change, 2)
    })
  end

  defp drop_nil(map), do: Map.reject(map, fn {_k, v} -> is_nil(v) end)

  # The core's derived edges onto the host's channels: each consumer edge with
  # the transforms and key of the chain it came from, and the final names.
  defp attach(class, derived) do
    channels =
      Enum.map(class.channels, fn ch ->
        d = Enum.find(derived["channels"], &(&1["index"] == ch.index))

        consumers =
          for e <- d["consumers"] do
            chain = Enum.at(ch.chains, e["chain"])
            %{tasklet: e["tasklet"], transforms: chain.transforms, key: chain.key}
          end

        %{ch | name: d["name"], consumers: consumers}
      end)

    %{class | channels: List.to_tuple(channels)}
  end
end
