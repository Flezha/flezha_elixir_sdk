defmodule Flezha.SDK.Operatives do
  @moduledoc """
  The operatives a runner hosts: each started under the class its
  `AddOperative` names, stopped on a `RemOperative`, and recorded in the
  runner's state and its registry. This is the half of what
  `Flezha.SDK.RunnerServer` answers that writes its state; `Flezha.SDK.Commands`
  is the half that only reads it.

  The state travels as a map, as it does through `Commands`, so neither module
  names the server's struct and the server can call both.
  """

  require Logger

  alias Flezha.SDK.{Codec, Commands, Declarations, EntryInput, KeyGroup, Operative}
  alias Flezha.SDK.Workflow.{Driver, Spec}

  @doc """
  Start the operative `add` names under its registered class. Answers
  `{:ok, s, declarations}`, or `{:error, reason, s}` for a factory this runner
  does not carry or a kind the class was not registered under.
  """
  @spec add(map(), map()) :: {:ok, map(), map()} | {:error, String.t(), map()}
  def add(s, add) do
    kind = if add.kind in [nil, ""], do: "stream", else: add.kind

    range =
      KeyGroup.decode(
        add.partition_index || 0,
        add.partition_count || 0,
        add.key_group_from || 0,
        add.key_group_to || 0
      )

    topo = Map.get(s.topologies, add.factory_id)

    cond do
      is_nil(topo) ->
        {:error, "unknown factory #{add.factory_id}", s}

      # The kind the controller placed under and the class registered under
      # this id must agree: a mismatch would drive a stream topology through
      # the workflow driver, or the reverse, and neither has the other's shape.
      kind == "workflow" ->
        start_workflow(s, add, topo)

      match?(%Spec{}, topo) ->
        {:error, "factory #{add.factory_id} is a workflow class; kind was #{inspect(kind)}", s}

      KeyGroup.partitioned?(range) and topo.keyless? ->
        {:error, "PartitionsWithoutKeyedProcedure: a partition_count >= 2 class needs a key_by",
         s}

      true ->
        start_stream(s, add, topo, range)
    end
  end

  @doc "Stop `oid` and forget it; an id this runner does not host changes nothing."
  @spec remove(map(), binary()) :: map()
  def remove(s, oid) do
    case Map.pop(s.operatives, oid) do
      {nil, _} ->
        s

      {{pid, _fid}, operatives} ->
        :ets.delete(s.registry, oid)
        if Process.alive?(pid), do: Operative.stop(pid)
        %{s | operatives: operatives}
    end
  end

  defp start_stream(s, add, topo, range) do
    {:ok, op_pid} =
      Operative.start_link(
        operative_id: add.operative_id,
        topology: topo,
        cells: EntryInput.cells_for(topo.entries),
        range: range,
        counters: s.counters,
        runner: self(),
        secondaries: s.secondaries,
        credit: s.credit
      )

    :ets.insert(s.registry, {add.operative_id, op_pid})
    seed_init_values(op_pid, add)

    {:ok, %{s | operatives: Map.put(s.operatives, add.operative_id, {op_pid, add.factory_id})},
     Declarations.for(topo)}
  end

  defp start_workflow(s, add, %Spec{} = spec) do
    {:ok, pid} =
      Driver.start_link(
        operative_id: add.operative_id,
        spec: spec,
        args: add.init_values || "",
        journal: Codec.journal_facts(add.proc_journal),
        owed_inbox: add.inbox_tail_count || 0,
        resume_entity_count: add.resume_entity_count || 0,
        counters: s.counters,
        runner: self(),
        secondaries: s.secondaries,
        credit: s.credit
      )

    :ets.insert(s.registry, {add.operative_id, pid})

    {:ok, %{s | operatives: Map.put(s.operatives, add.operative_id, {pid, add.factory_id})},
     Declarations.for(spec)}
  end

  defp start_workflow(s, add, _topology),
    do: {:error, "factory #{add.factory_id} is a stream class; kind was \"workflow\"", s}

  defp seed_init_values(op_pid, add) do
    case Commands.apply_json(op_pid, add.operative_id, add.init_values) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("[Flezha.SDK] #{add.operative_id} init_values: #{reason}")
    end
  end
end
