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

  alias Flezha.SDK.{Codec, Commands, Declarations, KeyGroup, Operative}
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

    with :ok <- check_kind(topo, kind, add.factory_id),
         :ok <- check_partitionable(topo, range, add.factory_id) do
      case topo do
        %Spec{} -> start_workflow(s, add, topo, range)
        _stream -> start_stream(s, add, topo, range)
      end
    else
      {:error, reason} -> {:error, reason, s}
    end
  end

  # The kind the controller placed under and the class registered under this
  # id must agree: a mismatch would drive a stream topology through the
  # workflow driver, or the reverse, and neither has the other's shape.
  defp check_kind(nil, _kind, factory_id), do: {:error, "unknown factory #{factory_id}"}
  defp check_kind(%Spec{}, "workflow", _factory_id), do: :ok

  defp check_kind(%Spec{}, kind, factory_id),
    do: {:error, "factory #{factory_id} is a workflow class; kind was #{inspect(kind)}"}

  defp check_kind(_stream, "workflow", factory_id),
    do: {:error, "factory #{factory_id} is a stream class; kind was \"workflow\""}

  defp check_kind(_stream, _kind, _factory_id), do: :ok

  # A workflow that reads one message would reach one member and leave the
  # rest waiting, whatever its keying, so its loop mode is refused first.
  defp check_partitionable(topo, range, factory_id) do
    cond do
      not KeyGroup.partitioned?(range) ->
        :ok

      match?(%Spec{knobs: %{loop_mode: mode}} when mode != :per_input, topo) ->
        {:error, KeyGroup.one_message_refusal(factory_id, topo.knobs.loop_mode, range.count)}

      stream_topology(topo).keyless? ->
        {:error, "PartitionsWithoutKeyedTasklet: a partition_count >= 2 class needs a key_by"}

      true ->
        :ok
    end
  end

  defp stream_topology(%Spec{topology: topology}), do: topology
  defp stream_topology(topology), do: topology

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
        range: range,
        counters: s.counters,
        runner: self(),
        secondaries: s.secondaries,
        credit: s.credit,
        owed_inbox: add.inbox_tail_count || 0
      )

    registered(s, add, op_pid, topo)
  end

  defp start_workflow(s, add, %Spec{} = spec, range) do
    {:ok, pid} =
      Driver.start_link(
        operative_id: add.operative_id,
        spec: spec,
        range: range,
        journal: Codec.journal_facts(add.task_journal),
        owed_inbox: add.inbox_tail_count || 0,
        resume_cell_count: add.resume_cell_count || 0,
        counters: s.counters,
        runner: self(),
        secondaries: s.secondaries,
        credit: s.credit
      )

    registered(s, add, pid, spec)
  end

  # A started instance of either kind is registered, takes its `init_values`
  # as its tasklets' initial state, and answers its class's declarations.
  defp registered(s, add, pid, declared) do
    :ets.insert(s.registry, {add.operative_id, pid})
    seed_init_values(pid, add)

    {:ok, %{s | operatives: Map.put(s.operatives, add.operative_id, {pid, add.factory_id})},
     Declarations.for(declared)}
  end

  defp seed_init_values(op_pid, add) do
    case Commands.apply_json(op_pid, add.operative_id, add.init_values) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("[Flezha.SDK] #{add.operative_id} init_values: #{reason}")
    end
  end
end
