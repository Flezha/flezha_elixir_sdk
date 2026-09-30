defmodule Flezha.SDK.Bounded.Spec do
  @moduledoc """
  The frozen registration-time result of authoring a bounded class: a
  validated tasklet DAG, plus the class's loop, rollback and durability
  facts.

  A structured class authors in `channels/1` on the same builder an unbounded class
  does, so its topology is compiled by `Flezha.SDK.Topology.build/2`, which
  also refuses the unbounded operator's settings on it, and only the refusals a
  *bounded* graph adds are decided here: an entry channel to arrive on,
  acyclic, reachable, a chained loop whose every chain
  converges on one terminal, and a call end only in a `:per_input` loop, the
  one loop that reads the later message its Response is. Each entry channel roots a chain (the nodes it
  feeds and everything reachable from them), and a message runs only the chain
  of the channel it arrived on.

  Four refusals the reference carries have no spelling in this SDK and are
  deliberately absent rather than written as branches nothing reaches. A
  tasklet's output channel is its node's, one per node by construction, so a
  second producing channel cannot be declared. An inter-tasklet value always
  has a serialization (`Channels`'s default), so a durable handoff can never be
  missing its journal codec. A cycle is refused by the shared topology before
  this module sees the class. And a node exists only where an `into` created
  it, so it always has a producer and following producers back terminates at an
  entry channel — a tasklet unreachable from the entry cannot be declared.

  Every issue is accumulated — a class hears all of what is wrong with it at
  once — and reported as `{:error, {:invalid_bounded, issues}}`.
  """

  alias Flezha.SDK.{CallEnd, Graph, Tasklet, Topology, BoundedOperator}

  @enforce_keys [:factory_id, :module]
  defstruct factory_id: nil,
            module: nil,
            topology: nil,
            dag: nil,
            knobs: %{}

  @type t :: %__MODULE__{}

  @typedoc """
  The tasklet DAG in Kahn order from the entry channels' consumers: a node's
  position in `order` is its `task_idx` on the wire, and reversing the order
  is the compensation order. `chains` holds, per entry channel name, the
  nodes it feeds (`roots`), every node reachable from them (`members`), each
  member's count of producers inside the chain (what a join waits for in a
  pass on that channel), and the chain's `leaves`; `from_node` holds the same
  per tasklet, rooted at it, which a pass its timer firing opened runs.
  """
  @type chain :: %{
          roots: [reference()],
          members: MapSet.t(reference()),
          parents_in_chain: %{reference() => non_neg_integer()},
          leaves: [reference()]
        }
  @type dag :: %{
          order: [reference()],
          pos: %{reference() => non_neg_integer()},
          parents: %{reference() => [reference()]},
          children: %{reference() => [reference()]},
          leaves: [reference()],
          chains: %{String.t() => chain()},
          from_node: %{reference() => chain()}
        }

  @doc """
  Compile `module` into a bounded spec, or refuse it. A class that declares
  no `channels/1` has no body to run and is refused `NoBoundedBody`.
  """
  @spec build(module()) :: {:ok, t()} | {:error, Flezha.SDK.Error.t()}
  def build(module) do
    knobs = BoundedOperator.knobs(module)
    base = %__MODULE__{factory_id: module.factory_id(), module: module, knobs: knobs}

    class_issues =
      []
      |> check_terminable(base.factory_id, knobs)
      |> check_modes(base.factory_id, knobs)

    if Tasklet.exports?(module, :channels, 1),
      do: structured(module, base, class_issues),
      else: refuse([{:NoBoundedBody, mod_name(module)} | class_issues])
  end

  defp structured(module, %__MODULE__{} = spec, issues) do
    case Topology.judge(module, :bounded) do
      {:issues, topology_issues} ->
        refuse(Enum.reverse(topology_issues) ++ issues)

      {:ok, topo} ->
        {dag, shape_issues} = shape(spec.factory_id, topo, spec.knobs)
        issues = shape_issues ++ issues

        if issues == [],
          do: {:ok, %{spec | topology: topo, dag: dag}},
          else: refuse(issues)
    end
  end

  # --- the shape a bounded graph adds on top of an unbounded topology ---

  defp shape(factory_id, topo, knobs) do
    walk = Graph.walk(topo.node_seq, Topology.node_edges(topo.routes))
    dag = dag(topo, walk)

    issues =
      []
      |> check_entry(factory_id, topo)
      |> check_chained_loop(factory_id, dag, knobs)

    {dag, Enum.reverse(CallEnd.outside_per_input_loop(topo.routes, knobs.loop_mode)) ++ issues}
  end

  # A bounded operator's messages arrive on its entry channels; a class with none
  # has nothing its messages could arrive on.
  defp check_entry(is, factory_id, topo) do
    if map_size(topo.entries) == 0, do: [{:NoEntryChannel, factory_id} | is], else: is
  end

  # Iteration N+1 of a handoff-chained loop is seeded from iteration N's
  # terminal batch, which a chain with several leaves does not define; every
  # chain the loop can enter is judged.
  defp check_chained_loop(is, factory_id, dag, %{loop_mode: :chained}) do
    Enum.reduce(dag.chains, is, fn {channel, chain}, is ->
      case chain.leaves do
        [_one] -> is
        many -> [{:BranchingChainedLoop, factory_id, channel, length(many)} | is]
      end
    end)
  end

  defp check_chained_loop(is, _factory_id, _dag, _knobs), do: is

  # --- class facts ---

  # A class whose only exit is an external stop, with no wall-clock backstop,
  # can run for ever and no scenario can prove it finished.
  defp check_terminable(is, factory_id, %{self_terminating?: false, max_duration_ms: nil}),
    do: [{:ExternalStopOnly, factory_id} | is]

  defp check_terminable(is, _factory_id, _knobs), do: is

  # The `use` macro refuses a mode outside its set at compile time; a
  # hand-written `loop_mode/0` or `rollback_mode/0` meets the same set here.
  defp check_modes(is, factory_id, knobs) do
    for {knob, allowed} <- BoundedOperator.modes(),
        (value = Map.fetch!(knobs, knob)) not in allowed,
        reduce: is,
        do: (is -> [{:InvalidMode, factory_id, knob, value} | is])
  end

  @doc "The tasklet a node runs, by the name it declares."
  @spec tasklet_name(t(), reference()) :: String.t()
  def tasklet_name(%__MODULE__{topology: topology}, node_ref),
    do: Map.fetch!(topology.node_modules, node_ref).name()

  # --- projections ---

  defp dag(topo, walk) do
    leaves = Enum.filter(walk.order, &(Map.fetch!(walk.children, &1) == []))

    walk
    |> Map.take([:order, :pos, :parents, :children])
    |> Map.merge(%{
      leaves: leaves,
      chains: entry_chains(topo, walk, leaves),
      from_node: Map.new(walk.order, &{&1, chain(walk, [&1], leaves)})
    })
  end

  # One chain per entry channel, in declaration order: the tasklets the
  # channel feeds.
  defp entry_chains(topo, walk, leaves) do
    for channel <- topo.entry_order, into: %{} do
      roots =
        for(%{to: {:node, ref}} <- Map.get(topo.routes, {:entry, channel}, []), do: ref)
        |> Enum.uniq()
        |> Enum.sort_by(&Map.fetch!(topo.node_seq, &1))

      {channel, chain(walk, roots, leaves)}
    end
  end

  # The chain `roots` start: everything reachable from them, each member's
  # producers inside the chain, and the graph's leaves it reaches.
  defp chain(walk, roots, leaves) do
    members = reachable(walk.children, roots, MapSet.new())

    parents_in_chain =
      Map.new(members, fn ref ->
        {ref, Enum.count(Map.fetch!(walk.parents, ref), &MapSet.member?(members, &1))}
      end)

    leaves = Enum.filter(leaves, &MapSet.member?(members, &1))
    %{roots: roots, members: members, parents_in_chain: parents_in_chain, leaves: leaves}
  end

  defp reachable(_children, [], seen), do: seen

  defp reachable(children, [ref | rest], seen) do
    if MapSet.member?(seen, ref),
      do: reachable(children, rest, seen),
      else: reachable(children, Map.fetch!(children, ref) ++ rest, MapSet.put(seen, ref))
  end

  defp mod_name(module), do: inspect(module)

  defp refuse(issues), do: {:error, Topology.refusal(Enum.reverse(issues))}
end
