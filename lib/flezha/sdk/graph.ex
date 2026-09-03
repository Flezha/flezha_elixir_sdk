defmodule Flezha.SDK.Graph do
  @moduledoc """
  One Kahn walk over a class's procedure nodes, read by both declaration
  surfaces. A stream topology asks it only whether a cycle exists; a workflow
  asks it for the order its stages run in, the parents each stage waits on and
  the leaves a chained loop re-seeds from. The rule is one rule, so the walk is
  one walk.

  The walk starts at every node nothing feeds. A node the walk leaves out sits
  on, or strictly downstream of, a cycle and is named by `on_cycle`; there is
  no separate unreachable case, because a node exists only where an `into`
  created it, so it always has a producer, and following producers back
  terminates at an entry channel.

  The queue is FIFO and every seed and child set is ordered by the node's
  declaration `seq`, so `order` is a function of the declaration alone. A
  workflow's `proc_idx` is a wire field: two runs of one class must number its
  stages identically, and node refs are `make_ref/0` values whose map order is
  not declaration order.
  """

  @type node_ref :: reference()

  @type t :: %{
          order: [node_ref()],
          pos: %{node_ref() => non_neg_integer()},
          parents: %{node_ref() => [node_ref()]},
          children: %{node_ref() => [node_ref()]},
          on_cycle: node_ref() | nil
        }

  @doc """
  Walk the nodes of `seq` (`ref => declaration index`) over `edges`
  (`{producer, consumer}` pairs).
  """
  @spec walk(%{node_ref() => non_neg_integer()}, [{node_ref(), node_ref()}]) :: t()
  def walk(seq, edges) do
    refs = order_by_seq(seq, Map.keys(seq))
    parents = adjacency(edges, refs, fn {producer, consumer} -> {consumer, producer} end)
    children = adjacency(edges, refs, fn {producer, consumer} -> {producer, consumer} end)

    queue = Enum.filter(refs, &(Map.fetch!(parents, &1) == []))
    indegree = Map.new(refs, fn ref -> {ref, length(Map.fetch!(parents, ref))} end)
    order = drain(queue, MapSet.new(queue), indegree, children, seq, [])
    pos = order |> Enum.with_index() |> Map.new()

    %{
      order: order,
      pos: pos,
      parents: parents,
      children: children,
      on_cycle: Enum.find(refs, &(not Map.has_key?(pos, &1)))
    }
  end

  # Every node keys the map, so a node with no edge reads as `[]` rather than
  # as missing and no caller needs a default.
  defp adjacency(edges, refs, pair) do
    empty = Map.new(refs, &{&1, []})

    edges
    |> Enum.map(pair)
    |> Enum.reduce(empty, fn {key, value}, m -> Map.update!(m, key, &[value | &1]) end)
    |> Map.new(fn {k, vs} -> {k, Enum.reverse(vs)} end)
  end

  defp drain([], _queued, _indegree, _children, _seq, acc), do: Enum.reverse(acc)

  defp drain([ref | rest], queued, indegree, children, seq, acc) do
    {ready, indegree} =
      children
      |> Map.fetch!(ref)
      |> Enum.reduce({[], indegree}, fn child, {ready, indegree} ->
        left = Map.fetch!(indegree, child) - 1
        indegree = Map.put(indegree, child, left)

        if left == 0 and not MapSet.member?(queued, child),
          do: {[child | ready], indegree},
          else: {ready, indegree}
      end)

    ready = order_by_seq(seq, ready)

    drain(
      rest ++ ready,
      MapSet.union(queued, MapSet.new(ready)),
      indegree,
      children,
      seq,
      [ref | acc]
    )
  end

  defp order_by_seq(seq, refs), do: Enum.sort_by(refs, &Map.fetch!(seq, &1))
end
