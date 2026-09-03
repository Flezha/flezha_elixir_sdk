defmodule Flezha.SDK.Workflow.Spec do
  @moduledoc """
  The frozen registration-time result of authoring a workflow class: a
  run-sugar body or a validated procedure DAG, plus the class's loop, rollback
  and durability facts.

  A structured class authors on the same builder a stream class does, so its
  topology is compiled by `Flezha.SDK.Topology.build/2` and only the refusals a
  *bounded* graph adds are decided here: single-rooted, acyclic, reachable, no
  dynamic entities, and a chained loop that converges on one terminal.

  Four refusals the reference carries have no spelling in this SDK and are
  deliberately absent rather than written as branches nothing reaches. A
  procedure's output channel is its node's, one per node by construction, so a
  second producing channel cannot be declared. An inter-procedure value always
  has a serialization (`Channels`'s default), so a durable handoff can never be
  missing its journal codec. A cycle is refused by the shared topology before
  this module sees the class. And a node exists only where an `into` created
  it, so it always has a producer and following producers back terminates at an
  entry channel — a procedure unreachable from the entry cannot be declared.

  Every issue is accumulated — a class hears all of what is wrong with it at
  once — and reported as `{:error, {:invalid_workflow, issues}}`.
  """

  alias Flezha.SDK.{Graph, Park, Procedure, Topology, WorkflowOperative}

  @enforce_keys [:factory_id, :module]
  defstruct factory_id: nil,
            module: nil,
            topology: nil,
            dag: nil,
            knobs: %{}

  @type t :: %__MODULE__{}

  @typedoc """
  The procedure DAG in Kahn order from the root: a node's position in `order`
  is its `proc_idx` on the wire, and reversing the order is the compensation
  order.
  """
  @type dag :: %{
          order: [reference()],
          pos: %{reference() => non_neg_integer()},
          parents: %{reference() => [reference()]},
          children: %{reference() => [reference()]},
          root: reference() | nil,
          leaves: [reference()]
        }

  @doc """
  Compile `module` into a workflow spec, or refuse it. A class declaring
  `procedures/1` takes the structured path; one declaring only `run/2` takes
  the run-sugar path and carries no topology.
  """
  @spec build(module()) :: {:ok, t()} | {:error, {:invalid_workflow, [term()]}}
  def build(module) do
    knobs = WorkflowOperative.knobs(module)
    base = %__MODULE__{factory_id: module.factory_id(), module: module, knobs: knobs}
    class_issues = class_refusals(module, knobs)

    cond do
      WorkflowOperative.structured?(module) ->
        structured(module, base, class_issues)

      WorkflowOperative.run_sugar?(module) ->
        run_sugar(base, knobs, class_issues)

      true ->
        refuse([{:NoWorkflowBody, mod_name(module)} | class_issues])
    end
  end

  # --- the two bodies ---

  defp run_sugar(%__MODULE__{} = spec, knobs, issues) do
    issues =
      if knobs.loop_mode == :once,
        do: issues,
        else: [{:LoopWithoutChain, spec.factory_id, knobs.loop_mode} | issues]

    if issues == [], do: {:ok, %{spec | dag: empty_dag()}}, else: refuse(issues)
  end

  defp structured(module, %__MODULE__{} = spec, issues) do
    case Topology.build(module, :procedures) do
      {:error, {:invalid_topology, topology_issues}} ->
        refuse(Enum.map(topology_issues, &{:Topology, &1}) ++ issues)

      {:ok, topo} ->
        {dag, shape_issues} = shape(spec.factory_id, topo, spec.knobs)
        issues = shape_issues ++ issues

        if issues == [],
          do: {:ok, %{spec | topology: topo, dag: dag}},
          else: refuse(issues)
    end
  end

  # --- the shape a bounded graph adds on top of a stream topology ---

  defp shape(factory_id, topo, knobs) do
    roots = entry_roots(topo)
    dag = dag(Graph.walk(topo.node_seq, Topology.node_edges(topo.routes)), roots)

    issues =
      []
      |> check_entry(factory_id, topo, roots)
      |> check_merge_from_entry(factory_id, topo)
      |> check_dynamic_entities(factory_id, topo)
      |> check_chained_loop(factory_id, dag, knobs)

    {dag, issues}
  end

  # A structured workflow is seeded through ONE entry channel feeding exactly
  # one procedure, which then fans out: the spawn args carry no channel name,
  # so a second entry — or a second consumer on the one entry — names no
  # sender the driver could route from.
  defp check_entry(is, factory_id, topo, roots) do
    case {map_size(topo.entries), roots} do
      {0, _} -> [{:NoEntryChannel, factory_id} | is]
      {_, [_one]} -> is
      {_, many} -> [{:EntryRoot, factory_id, entry_names(topo), length(many)} | is]
    end
  end

  # An entry channel cannot be one merge source among several: neither the
  # spawn args nor a `:per_input` inbox item carries the channel it arrived
  # on, so a procedure fed by the entry AND by a producer could not tell its
  # two inputs apart. Merge the outputs of two procedures instead.
  defp check_merge_from_entry(is, factory_id, topo) do
    from_entry = consumers_by_source(topo, &match?({:entry, _}, &1))
    from_node = consumers_by_source(topo, &match?({:node, _}, &1))

    from_entry
    |> Enum.filter(&MapSet.member?(from_node, &1))
    |> Enum.reduce(is, fn ref, is ->
      [{:MergeFromEntryChannel, factory_id, entry_names(topo), node_label(topo, ref)} | is]
    end)
  end

  defp check_dynamic_entities(is, factory_id, topo) do
    Enum.reduce(topo.node_mode, is, fn
      {ref, :dynamic}, is -> [{:DynamicEntities, factory_id, node_label(topo, ref)} | is]
      {_ref, _mode}, is -> is
    end)
  end

  # Iteration N+1 of a handoff-chained loop is seeded from iteration N's
  # terminal batch, which a graph with several leaves does not define.
  defp check_chained_loop(is, factory_id, dag, %{loop_mode: :chained}) do
    case dag.leaves do
      [_one] -> is
      many -> [{:BranchingChainedLoop, factory_id, length(many)} | is]
    end
  end

  defp check_chained_loop(is, _factory_id, _dag, _knobs), do: is

  # --- class facts, shared by both bodies ---

  defp class_refusals(module, knobs) do
    factory_id = module.factory_id()

    []
    |> check_terminable(factory_id, knobs)
    |> check_retry_backoff(factory_id, knobs)
  end

  # A class whose only exit is an external stop, with no wall-clock backstop,
  # can run for ever and no scenario can prove it finished.
  defp check_terminable(is, factory_id, %{self_terminating?: false, max_duration_ms: nil}),
    do: [{:ExternalStopOnly, factory_id} | is]

  defp check_terminable(is, _factory_id, _knobs), do: is

  # A ladder with a zero base re-runs the failing entity in a hot loop.
  defp check_retry_backoff(is, factory_id, %{retry_attempts: n, retry_backoff_ms: 0}) when n > 1,
    do: [{:ZeroRetryBackoff, factory_id, n} | is]

  defp check_retry_backoff(is, _factory_id, _knobs), do: is

  @doc """
  The keys a stage's Entities are registered under, in the order they are
  built: one `nil` for a Single node, the declared keyset for a Static one.
  """
  @spec stage_keys(t(), reference()) :: [String.t() | nil]
  def stage_keys(%__MODULE__{topology: topology}, node_ref) do
    case Map.fetch!(topology.node_mode, node_ref) do
      :static -> topology.node_modules |> Map.fetch!(node_ref) |> Procedure.entity_keys()
      _single -> [nil]
    end
  end

  @doc "The wire identity of one Entity of a stage."
  @spec entity_identity(t(), String.t(), reference(), String.t() | nil) :: binary()
  def entity_identity(%__MODULE__{topology: topology}, operative_id, node_ref, key) do
    name = Map.fetch!(topology.node_modules, node_ref).name()
    Park.identity(operative_id, name, key || "")
  end

  @doc "The wire identities of a stage's Entities, in the same order as `stage_keys/2`."
  @spec entity_identities(t(), String.t(), reference()) :: [binary()]
  def entity_identities(%__MODULE__{} = spec, operative_id, node_ref) do
    spec
    |> stage_keys(node_ref)
    |> Enum.map(&entity_identity(spec, operative_id, node_ref, &1))
  end

  @doc "The procedure a node runs, by the name it declares."
  @spec procedure_name(t(), reference()) :: String.t()
  def procedure_name(%__MODULE__{topology: topology}, node_ref),
    do: Map.fetch!(topology.node_modules, node_ref).name()

  # --- projections ---

  defp dag(walk, roots) do
    leaves = Enum.filter(walk.order, &(Map.fetch!(walk.children, &1) == []))

    walk
    |> Map.take([:order, :pos, :parents, :children])
    |> Map.merge(%{root: List.first(roots), leaves: leaves})
  end

  defp empty_dag,
    do: %{order: [], pos: %{}, parents: %{}, children: %{}, root: nil, leaves: []}

  # The procedures an entry channel feeds, in declaration order.
  defp entry_roots(topo) do
    topo
    |> consumers_by_source(&match?({:entry, _}, &1))
    |> Enum.sort_by(&Map.fetch!(topo.node_seq, &1))
  end

  defp consumers_by_source(topo, source?) do
    for {source, routes} <- topo.routes,
        source?.(source),
        %{to: {:node, ref}} <- routes,
        into: MapSet.new(),
        do: ref
  end

  defp entry_names(topo), do: topo.entries |> Map.keys() |> Enum.sort() |> Enum.join(", ")

  # How a refusal names a node: the name the author gave it, else the
  # procedure's own. `procedure_name/2` is the other rule — what `name/0`
  # answers, which is what a running failure is reported against.
  defp node_label(topo, ref) do
    Map.get(topo.node_name, ref) || Map.fetch!(topo.node_modules, ref).name()
  end

  defp mod_name(module), do: inspect(module)

  defp refuse(issues), do: {:error, {:invalid_workflow, Enum.reverse(issues)}}
end
