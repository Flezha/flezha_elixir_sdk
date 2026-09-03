defmodule Flezha.SDK.Workflow.Stage do
  @moduledoc """
  One stage of a workflow chain: its Entities, how a batch is split across
  them, and how the driver learns the stage has finished.

  Every Entity here is an ordinary `Flezha.SDK.Entity` — the same process a
  stream operative runs, with the same state, records, ladder and timers. Two
  options are all that differ: the ladder comes from the class's
  `retry_attempts`/`retry_backoff_ms` rather than from an edge's knobs, and an
  exhausted ladder reports the failure instead of parking to the DLQ, because
  a workflow answers a failed procedure by unwinding rather than by shedding
  the input.

  A stage is finished when every one of its Entities has drained past a
  barrier queued behind its batch. An invocation that emits nothing reports
  nothing, so counting outputs could never tell the driver a stage was done.
  """

  alias Flezha.SDK.{Entity, Procedure}
  alias Flezha.SDK.Workflow.Spec

  @doc """
  Build every stage's Entities, in DAG order. They exist before anything runs
  so a `RestoreInitState` frame arriving right after the `AddOperative` ack
  always finds its Entity registered.
  """
  @spec build_all(Spec.t(), keyword()) :: %{reference() => [pid()]}
  def build_all(%Spec{} = spec, opts) do
    Map.new(spec.dag.order, fn node_ref ->
      pids =
        spec
        |> Spec.stage_keys(node_ref)
        |> Enum.map(&start_entity(spec, node_ref, &1, opts))

      {node_ref, pids}
    end)
  end

  @doc """
  Hand `batch` to a stage's Entities and queue a barrier behind each. Answers
  `{ref, node_ref}` pairs the driver waits on: the stage is finished when the
  last of them has replied.
  """
  @spec dispatch(Spec.t(), reference(), [pid()], [term()], pid()) :: [{reference(), reference()}]
  def dispatch(%Spec{} = spec, node_ref, pids, batch, reply_to) do
    partitions = partition(spec, node_ref, batch)

    pids
    |> Enum.with_index()
    |> Enum.map(fn {pid, index} ->
      Enum.each(Map.get(partitions, index, []), &Entity.enqueue_block(pid, &1))
      ref = make_ref()
      Entity.barrier(pid, ref, reply_to)
      {ref, node_ref}
    end)
  end

  @doc """
  Split `batch` across a stage's Entities by slot index: everything to slot 0
  in Single mode, by `entities_for/1` in Static mode. A value naming a key the
  stage does not declare reaches nothing, as it does on the stream path.
  """
  @spec partition(Spec.t(), reference(), [term()]) :: %{non_neg_integer() => [term()]}
  def partition(%Spec{} = spec, node_ref, batch) do
    keys = Spec.stage_keys(spec, node_ref)

    Enum.reduce(batch, %{}, fn value, acc ->
      spec
      |> slots_for(node_ref, keys, value)
      |> Enum.reduce(acc, fn slot, acc -> Map.update(acc, slot, [value], &(&1 ++ [value])) end)
    end)
  end

  @doc "The slot `key` is registered in, or 0 when the stage has one Entity."
  @spec index_of(Spec.t(), reference(), String.t() | nil) :: non_neg_integer()
  def index_of(%Spec{} = spec, node_ref, key),
    do: spec |> Spec.stage_keys(node_ref) |> Enum.find_index(&(&1 == key)) |> Kernel.||(0)

  defp slots_for(%Spec{topology: topology}, node_ref, keys, value) do
    case Map.fetch!(topology.node_mode, node_ref) do
      :static ->
        topology.node_modules
        |> Map.fetch!(node_ref)
        |> Procedure.entities_for(value)
        |> Enum.map(&Enum.find_index(keys, fn k -> k == &1 end))
        |> Enum.reject(&is_nil/1)

      _single ->
        [0]
    end
  end

  defp start_entity(%Spec{topology: topology, knobs: knobs}, node_ref, key, opts) do
    module = Map.fetch!(topology.node_modules, node_ref)
    operative_id = Keyword.fetch!(opts, :operative_id)

    {:ok, pid} =
      Entity.start(
        operative: Keyword.fetch!(opts, :operative),
        operative_id: operative_id,
        node_ref: node_ref,
        node_name: topology.node_name[node_ref],
        input_channel: topology.node_channel[node_ref],
        module: module,
        entity_key: key,
        capacity: Procedure.mailbox_capacity(module),
        policy: :drop_nothing,
        retry_max: max(knobs.retry_attempts, 1),
        retry_backoff_ms: knobs.retry_backoff_ms,
        park_on_exhaustion: false,
        counters: Keyword.fetch!(opts, :counters),
        secondaries: Keyword.get(opts, :secondaries, []),
        slots: Keyword.get(opts, :slots),
        credit: Keyword.get(opts, :credit),
        persistence_enabled: knobs.persistence_enabled,
        ctx_opts: [operative_id: operative_id]
      )

    pid
  end
end
