defmodule Flezha.SDK.Scope do
  @moduledoc """
  A stepped scope: a node that embeds a steps class runs one instance of it
  per edge key (one on an unkeyed edge) inside the outer instance, each a
  `Flezha.SDK.Steps.Driver` of the inner class with its own pass sheet, Cells
  and journal. The driver runs it unchanged; what is the scope's own is where
  its frames say they come from (the outer instance, carrying the scope),
  where its input comes from (the outer edge, as the values it carried) and
  where its exit output goes (on along the outer graph, after the node that
  embeds it).

  Inside a durable outer instance every message for a scope goes to the
  controller first (`ScopeInput`), which journals it to the scope's input log
  and binds the scope when it is asleep or absent, as it binds a top-level
  steps instance; inside any other one a scope is started here on its key's
  first message.

  The local id (`id/3`) is the controller's `Flezha.State.Paths.scope_id/3`
  and the core's `scope_id`: the scope's instance id, so its Cells and files
  sit under the outer instance's.
  """

  require Logger

  alias Flezha.SDK.{Codec, Error, Operator, Prefix, Record}
  alias Flezha.SDK.Steps.Spec

  @enforce_keys [:wire, :parent, :inner]
  defstruct [:wire, :parent, :inner, :key, :exit, logged: false]

  @typedoc """
  What a scope's driver carries: the identity its frames name (`{outer id,
  wire scope}`), the outer instance's process, the embedding node's name and
  ref, the edge key, the inner tasklet whose output leaves the scope, and
  whether its exit values go through the outer instance's input log (one
  whose input the controller journals).
  """
  @type t :: %__MODULE__{
          wire: {binary(), struct()},
          parent: pid(),
          inner: {binary(), reference()},
          key: binary() | nil,
          exit: reference() | nil,
          logged: boolean()
        }

  @safe ~r/^[A-Za-z0-9_.-]+$/

  @doc "The local id of the scope `(instance, inner, key)`."
  @spec id(binary(), binary(), binary() | nil) :: binary()
  def id(instance, inner, nil), do: instance <> "/" <> inner
  def id(instance, inner, key), do: id(instance, inner, nil) <> "/" <> encode_key(key)

  @doc """
  What a frame's `flz.scope` adds to the instance it names: `/<inner>` or
  `/<inner>/<key>`, no segment empty; nothing for any other value.
  """
  @spec suffix(map()) :: binary()
  def suffix(%{"flz.scope" => "/" <> rest = suffix}) do
    segments = String.split(rest, "/")
    if length(segments) <= 2 and "" not in segments, do: suffix, else: ""
  end

  def suffix(_metadata), do: ""

  defp encode_key(key) do
    if Regex.match?(@safe, key) and not String.starts_with?(key, "_"),
      do: key,
      else: "_" <> Base.url_encode64(key, padding: false)
  end

  @doc """
  What `into/2` makes of `module`: a tasklet node, a flow class to inline, or
  a steps class to embed as a scope family.
  """
  @spec kind(module()) :: :tasklet | :flow | :steps
  def kind(module) do
    Code.ensure_loaded(module)

    cond do
      not function_exported?(module, :factory_id, 0) -> :tasklet
      match?({:ok, %{run: :steps}}, Operator.settings(module)) -> :steps
      true -> :flow
    end
  end

  @doc """
  The node an edge into the steps class `module` becomes inside a class that
  runs `outer_run`, judged as the core's `embed` judges it: no steps class
  inside a steps class, one entry channel fed (`input`, or the only one), and
  at most one tasklet whose output stays inside the inner graph. Answers the
  node's facts, or the issues it raises.
  """
  @spec judge(binary(), :flow | :steps, module(), binary() | nil) ::
          {:ok, map()} | {:issues, [tuple()]}
  def judge(outer, outer_run, module, input) do
    factory_id = module.factory_id()

    with :ok <- not_nested(outer, outer_run, factory_id),
         {:ok, spec} <- inner_spec(module),
         {:ok, entry} <- entry(factory_id, spec, input),
         {:ok, exit} <- exit_node(factory_id, spec) do
      {:ok, %{factory_id: factory_id, entry: entry, exit: exit, spec: scoped(spec, entry)}}
    end
  end

  defp not_nested(outer, :steps, inner), do: {:issues, [{:NestedSteps, outer, inner}]}
  defp not_nested(_outer, _run, _inner), do: :ok

  defp inner_spec(module) do
    case Spec.build(module) do
      {:ok, spec} ->
        {:ok, spec}

      {:error, %Error{} = error} ->
        {:issues, [{:InvalidSetting, inspect(module), "run", "a refused class", error.message}]}
    end
  end

  defp entry(factory_id, spec, input) do
    names = Enum.uniq(spec.topology.entry_order)

    case {input, names} do
      {nil, [only]} ->
        {:ok, only}

      {name, _} when is_binary(name) and name != "" ->
        if name in names, do: {:ok, name}, else: entry_issue(factory_id, names)

      _ ->
        entry_issue(factory_id, names)
    end
  end

  defp entry_issue(factory_id, names), do: {:issues, [entry_ambiguous(factory_id, names)]}

  @doc "The refusal of an embedding that feeds none of `names`, or no one of them."
  @spec entry_ambiguous(binary(), [binary()]) :: tuple()
  def entry_ambiguous(factory_id, names),
    do: {:EmbeddedEntryAmbiguous, factory_id, names |> Enum.sort() |> Enum.join(", ")}

  # A leaf that writes to a sink keeps its output in the inner graph.
  defp exit_node(factory_id, spec) do
    topo = spec.topology
    exits = Enum.filter(spec.dag.leaves, &(Map.get(topo.routes, {:node, &1}, []) == []))

    case exits do
      [] ->
        {:ok, nil}

      [one] ->
        {:ok, one}

      many ->
        {:issues,
         [
           {:EmbeddedExitAmbiguous, factory_id,
            many |> Enum.map(&Spec.tasklet_name(spec, &1)) |> Enum.sort() |> Enum.join(", ")}
         ]}
    end
  end

  # The scope's entry reads what the outer edge carried, which arrives as the
  # term it was, encoded; a stand-alone instance of the class keeps its parser.
  defp scoped(%Spec{topology: topo} = spec, entry) do
    entries = Map.update!(topo.entries, entry, fn e -> %{e | parser: &Record.decode_body/1} end)
    %{spec | topology: %{topo | entries: entries}}
  end

  # --- the outer instance's side: one family of scopes per embedding node ---

  @doc """
  A value routed into the embedding node `node_ref` at `key`, in the outer
  instance's process state `s`: to the controller first inside a durable
  instance (its prefix holds the input until the controller answered), else
  to the key's scope, started on its first message. A key past the node's
  `max_cells` is dropped and counted.
  """
  @spec deliver(map(), reference(), term(), binary() | nil, map()) :: map()
  def deliver(s, node_ref, value, key, emit) do
    facts = Map.fetch!(s.topology.node_scope, node_ref)
    family = Map.get(s.scopes, node_ref, %{})

    cond do
      not admitted?(family, key, facts.max_cells) ->
        :telemetry.execute([:flezha, :sdk, :dynamic], %{count: 1}, %{
          operator_id: s.operator_id,
          event: :capped
        })

        s

      s.topology.durable? ->
        forward(s, node_ref, facts, value, key, emit.offset)

      true ->
        {pid, s} = local(s, node_ref, facts, key)
        GenServer.cast(pid, {:input, facts.entry, encode(value), %{}})
        s
    end
  end

  defp admitted?(family, key, max_cells),
    do: Map.has_key?(family, key) or max_cells == nil or map_size(family) < max_cells

  defp forward(s, node_ref, facts, value, key, offset) do
    if offset && s.prefix, do: Prefix.hold(s.prefix, offset)
    {msg_id, s} = msg_id(s, node_ref, key, offset)
    wire = Codec.scope(name(s, node_ref), key)
    command = Codec.scope_input_command(s.operator_id, wire, facts.entry, encode(value), msg_id)

    s
    |> Map.update!(
      :scopes,
      &Map.update(&1, node_ref, %{key => :pending}, fn f -> Map.put_new(f, key, :pending) end)
    )
    |> send_input(%{offset: offset, node_ref: node_ref, key: key, command: command, sends: 1})
  end

  defp send_input(s, sent) do
    ref = make_ref()
    GenServer.cast(s.runner, {:barrier, self(), ref, sent.command})
    Map.update!(s, :scope_holds, &Map.put(&1, ref, sent))
  end

  # The value's place among those its outer input sent this key in this life
  # of the instance names it in the scope's log, so a replay of the input
  # appends nothing new; places of inputs the reported prefix passed go.
  defp msg_id(s, _node_ref, _key, nil), do: {"", s}

  defp msg_id(s, node_ref, key, offset) do
    done = (s.prefix && Prefix.reported(s.prefix)) || -1
    sent = for {{_, _, origin}, _} = row <- s.scope_sent, origin > done, into: %{}, do: row
    place = Map.get(sent, {node_ref, key, offset}, 0)
    sent = Map.put(sent, {node_ref, key, offset}, place + 1)
    {"#{s.operator_id}@#{offset}:#{place}", %{s | scope_sent: sent}}
  end

  @doc """
  The controller answered a forwarded message: logged, its input's hold is
  released; dead-lettered (`"refused"`), its key binds no scope; an append
  that failed is sent again up to `sends` times, and then keeps its hold, so
  the instance's next bind forwards the input again.
  """
  @spec answered(map(), reference(), binary(), term(), pos_integer()) :: map()
  def answered(s, ref, result, error, sends) do
    case Map.pop(s.scope_holds, ref) do
      {nil, _holds} ->
        s

      {sent, holds} ->
        s = %{s | scope_holds: holds}

        cond do
          error == nil ->
            if sent.offset && s.prefix, do: Prefix.release(s.prefix, sent.offset)
            if result == "refused", do: not_pending(s, sent.node_ref, sent.key), else: s

          sent.sends < sends ->
            send_input(s, %{sent | sends: sent.sends + 1})

          true ->
            Logger.warning(
              "[Flezha.SDK] #{s.operator_id} a message into a scope was not logged: " <>
                "#{inspect(error)}; its input stays unconfirmed"
            )

            s
        end
    end
  end

  @doc "A key of `node_ref` whose scope is not bound no longer waits for a bind."
  @spec released(map(), reference(), binary() | nil) :: map()
  def released(s, node_ref, key), do: not_pending(s, node_ref, key)

  defp not_pending(s, node_ref, key) do
    case get_in(s.scopes, [node_ref, key]) do
      :pending -> put_child(s, node_ref, key, nil)
      _bound_or_gone -> s
    end
  end

  # A scope inside an instance that keeps nothing: started here, with nothing
  # to restore.
  defp local(s, node_ref, facts, key) do
    case get_in(s.scopes, [node_ref, key]) do
      pid when is_pid(pid) ->
        {pid, s}

      _absent ->
        pid = start(context(s), node_ref, facts, key, [])
        Process.monitor(pid)
        {pid, put_child(s, node_ref, key, pid)}
    end
  end

  @doc """
  Start the scope `key` of `node_ref` as a driver of the inner class, in the
  outer instance `ctx` describes (its id, topology, process, runner, counters,
  secondaries and credit); `bind` carries what a controller's bind does.
  """
  @spec start(map(), reference(), map(), binary() | nil, keyword()) :: pid()
  def start(ctx, node_ref, facts, key, bind) do
    inner = ctx.topology.node_name[node_ref]

    scope = %__MODULE__{
      wire: {ctx.operator_id, Codec.scope(inner, key)},
      parent: ctx.parent,
      inner: {inner, node_ref},
      key: key,
      exit: facts.exit,
      logged: journals_input?(ctx.topology)
    }

    {:ok, pid} =
      Flezha.SDK.Steps.Driver.start_link(
        [
          operator_id: id(ctx.operator_id, inner, key),
          spec: facts.spec,
          range: Flezha.SDK.KeyGroup.decode(0, 0, 0, 0),
          counters: ctx.counters,
          runner: ctx.runner,
          secondaries: ctx.secondaries,
          credit: ctx.credit,
          scope: scope
        ] ++ bind
      )

    pid
  end

  # An outer instance whose input the controller journals (a kept one on the
  # effectively-once tier, or one that sleeps) is owed a scope's exit values
  # through that journal; any other takes them in memory, at most once.
  defp journals_input?(topology),
    do: topology.durable? and (topology.effectively_once? or topology.sleep_after_ms != nil)

  defp context(s),
    do: %{
      operator_id: s.operator_id,
      topology: s.topology,
      parent: self(),
      runner: s.runner,
      counters: s.cells.counters,
      secondaries: s.cells.secondaries,
      credit: s.cells.credit
    }

  @doc "A scope the controller bound joins its family; a stopped one leaves it."
  @spec put_child(map(), reference(), binary() | nil, pid() | nil) :: map()
  def put_child(s, node_ref, key, nil),
    do: %{s | scopes: Map.update(s.scopes, node_ref, %{}, &Map.delete(&1, key))}

  def put_child(s, node_ref, key, pid),
    do: %{s | scopes: Map.update(s.scopes, node_ref, %{key => pid}, &Map.put(&1, key, pid))}

  @doc "The node an embedding node name names in the outer instance's topology."
  @spec node_named(map(), binary()) :: reference() | nil
  def node_named(topology, inner) do
    Enum.find_value(topology.node_scope, fn {ref, _facts} ->
      if topology.node_name[ref] == inner, do: ref
    end)
  end

  defp name(s, node_ref), do: s.topology.node_name[node_ref]

  @doc "The bytes a value on an edge into a scope travels as."
  @spec encode(term()) :: binary()
  def encode(value), do: Record.encode_body(value)

  @doc "The identity a frame names: an instance's id, or a scope's outer id and scope."
  @spec wire(binary(), t() | nil) :: binary() | {binary(), struct()}
  def wire(operator_id, nil), do: operator_id
  def wire(_operator_id, %__MODULE__{wire: wire}), do: wire

  @doc """
  A scope's output on its exit tasklet, on to the node that embeds it in the
  outer instance, where it is routed as that node's emit.
  """
  @spec exit(t() | nil, reference(), [term()]) :: :ok
  def exit(
        %__MODULE__{exit: node_ref, parent: parent, inner: {_name, inner_ref}, key: key},
        node_ref,
        values
      )
      when values != [],
      do: GenServer.cast(parent, {:scope_exit, inner_ref, key, values})

  def exit(_scope, _node_ref, _values), do: :ok

  @doc """
  The exit a logged scope's commit carries: `values` and a completion's
  `result`, named by the embedding node they leave through; `nil` when nothing
  leaves.
  """
  @spec exit_frame(t(), [term()], binary()) :: struct() | nil
  def exit_frame(%__MODULE__{inner: {name, _ref}}, values, result) do
    case Enum.map(values, &encode/1) ++ if(result == <<>>, do: [], else: [result]) do
      [] -> nil
      bytes -> Codec.scope_exit(name, bytes)
    end
  end

  @doc "An exit value a logged scope's commit carried, as the term it left as."
  @spec exited_value(binary()) :: term()
  def exited_value(bytes) do
    case Record.decode_body(bytes) do
      {:ok, value} -> value
      {:error, _raw} -> bytes
    end
  end

  @doc "A completion result as the exit value it carries out, when there is one."
  @spec result(t() | nil, binary()) :: :ok
  def result(%__MODULE__{exit: exit} = scope, result) when result != <<>> do
    value = with {:error, _} <- Record.decode_body(result), do: result
    exit(scope, exit, [unwrap(value)])
  end

  def result(_scope, _result), do: :ok

  defp unwrap({:ok, value}), do: value
  defp unwrap(value), do: value
end
