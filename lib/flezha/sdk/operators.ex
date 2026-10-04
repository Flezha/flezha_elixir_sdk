defmodule Flezha.SDK.Operators do
  @moduledoc """
  The operators a runner hosts: each started under the class its
  `AddOperator` names, stopped on a `RemOperator`, and recorded in the
  runner's state and its registry. This is the half of what
  `Flezha.SDK.RunnerServer` answers that writes its state; `Flezha.SDK.Commands`
  is the half that only reads it.

  The state travels as a map, as it does through `Commands`, so neither module
  names the server's struct and the server can call both.
  """

  require Logger

  alias Flezha.SDK.{Codec, Commands, Declarations, Error, KeyGroup, Operator}
  alias Flezha.SDK.Steps.{Driver, Spec}

  @doc """
  Start the operator `add` names under its registered class, which decides
  how it runs. Answers `{:ok, s, declarations}`, or `{:error, reason, s}` for
  a factory this runner does not carry.
  """
  @spec add(map(), map()) :: {:ok, map(), map()} | {:error, Error.t(), map()}
  def add(s, add) do
    s = answered(s, add.operator_id)

    range =
      KeyGroup.decode(
        add.partition_index || 0,
        add.partition_count || 0,
        add.key_group_from || 0,
        add.key_group_to || 0
      )

    topo = Map.get(s.topologies, add.factory_id)

    with :ok <- check_registered(topo, add.factory_id),
         :ok <- check_partitionable(topo, range, add.factory_id) do
      case topo do
        %Spec{} -> start_steps(s, add, topo, range)
        _flow -> start_flow(s, add, topo, range)
      end
    else
      {:error, reason} -> {:error, reason, s}
    end
  end

  defp check_registered(nil, factory_id),
    do: {:error, Error.new("runtime.class_not_registered", factory: factory_id)}

  defp check_registered(_class, _factory_id), do: :ok

  # A steps operator that reads one message would reach one member and leave the
  # rest waiting, whatever its keying, so its loop mode is refused first.
  defp check_partitionable(topo, range, factory_id) do
    cond do
      not KeyGroup.partitioned?(range) ->
        :ok

      match?(%Spec{settings: %{each: mode}} when mode != :per_input, topo) ->
        {:error,
         Error.new("topology.partitions_without_per_input_loop",
           factory: factory_id,
           each: topo.settings.each,
           partitions: range.count
         )}

      flow_topology(topo).keyless? ->
        {:error, Error.new("topology.partitions_without_keyed_tasklet", factory: factory_id)}

      true ->
        :ok
    end
  end

  @doc "The flow topology a class runs on: a steps operator's own, or the class itself."
  def flow_topology(%Spec{topology: topology}), do: topology
  def flow_topology(topology), do: topology

  @doc "Stop `oid` and forget it; an id this runner does not host changes nothing."
  @spec remove(map(), binary()) :: map()
  def remove(s, oid) do
    case s |> answered(oid) |> drop(oid) do
      {{pid, _fid}, s} ->
        if Process.alive?(pid), do: Operator.Server.stop(pid)
        s

      {nil, s} ->
        s
    end
  end

  @doc """
  The process registered for an instance exited: forget the instance, so no
  input is routed to the dead process and the next `AddOperator` for its id
  starts a fresh one. An abnormal exit, while the runner is neither
  disconnecting nor stopped, is kept in `exited` until the controller answers
  it and answers `{:report, oid, s}`; anything else, including the exit of a
  process no id names any more, answers `{:ok, s}`.
  """
  @spec exited(map(), pid(), term()) :: {:report, binary(), map()} | {:ok, map()}
  def exited(s, pid, reason) do
    case Enum.find(s.operators, fn {_oid, {registered, _fid}} -> registered == pid end) do
      nil ->
        {:ok, s}

      {oid, _} ->
        {_entry, s} = drop(s, oid)

        if Operator.Server.clean_exit?(reason) or s.disconnecting? or s.stopped?,
          do: {:ok, s},
          else: {:report, oid, %{s | exited: Map.put(s.exited, oid, reason)}}
    end
  end

  # Out of the operator map and the registry, so nothing is routed to it.
  defp drop(s, oid) do
    {entry, operators} = Map.pop(s.operators, oid)
    :ets.delete(s.registry, oid)
    {entry, %{s | operators: operators}}
  end

  # An `AddOperator` or a `RemOperator` for the id is the controller's answer
  # to its exit report.
  defp answered(s, oid), do: %{s | exited: Map.delete(s.exited, oid)}

  defp start_flow(s, add, topo, range) do
    {:ok, op_pid} =
      Operator.Server.start_link(
        operator_id: add.operator_id,
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

  defp start_steps(s, add, %Spec{} = spec, range) do
    {:ok, pid} =
      Driver.start_link(
        operator_id: add.operator_id,
        spec: spec,
        range: range,
        journal: Codec.journal_facts(add.task_journal),
        owed_inbox: add.inbox_tail_count || 0,
        resume_cell_count: add.resume_cell_count || 0,
        dead_letter_turns: add.dead_letter_turns || false,
        counters: s.counters,
        runner: self(),
        secondaries: s.secondaries,
        credit: s.credit
      )

    registered(s, add, pid, spec)
  end

  # A started instance of either run is registered, takes its `init_values`
  # as its tasklets' initial state, and answers its class's declarations.
  defp registered(s, add, pid, declared) do
    :ets.insert(s.registry, {add.operator_id, pid})
    seed_init_values(pid, add)

    {:ok, %{s | operators: Map.put(s.operators, add.operator_id, {pid, add.factory_id})},
     Declarations.for(declared)}
  end

  defp seed_init_values(op_pid, add) do
    case Commands.apply_json(op_pid, add.operator_id, add.init_values) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("[Flezha.SDK] #{add.operator_id} init_values: #{reason.message}")
    end
  end
end
