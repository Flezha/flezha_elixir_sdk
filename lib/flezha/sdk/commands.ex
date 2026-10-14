defmodule Flezha.SDK.Commands do
  @moduledoc """
  The persistence-family commands the RunnerServer answers, and the shutdown
  notifications it ships — its zero-mutation half: every function reads the
  server's state (the relays, the control handle, the credit window) and
  answers through the reply seam it is handed; none writes the state. A
  command that needs a Cell's answer runs off the server's loop, so a fold
  or a serialisation never stalls the control channel; the credit commands
  answer on it and never block.

  The live read is the Decision-10 shape: `%{tasklet => state}` for a
  Single node, `%{tasklet => %{key => state}}` for a Static or Dynamic one,
  narrowed by the selector, with the reply frame carrying `flz.change_seq`
  when exactly one Cell was read. Refusals carry the tokens the controller
  matches as prefixes: `cell_key_without_tasklet`, `tasklet_not_found`,
  `cell_not_found`, `cell_busy`.
  """

  require Logger

  alias Flezha.SDK.{Call, Codec, Credit, Cell, Error, Hold, Knob, Relay}
  alias Flezha.SDK.Wire
  # A serialise's wait for a mid-dispatch Cell, and the serialise bound of a
  # graceful shutdown: inside a container's grace period.
  @change_seq_key Wire.change_seq_key()
  @whole %{tasklet: nil, cell_key: nil}
  # One refusal, and each surface adds the prefix its siblings carry.
  @typedoc "How a command answers: status, errors, raw_data, the reply frame's metadata."
  @type respond :: (String.t(), [Error.t()], binary(), map() -> any())

  @doc "Answer `tag`'s `cmd` for the runner in `s` through `respond`."
  @spec dispatch(atom(), map(), respond(), map()) :: :ok
  def dispatch(:restore_init_state, cmd, respond, s) do
    off_loop(respond, fn ->
      case cell(s, cmd.cell_id, materialise: true) do
        {:ok, pid} ->
          answer = restore(pid, cmd)
          land(s, cmd, respond, answer)

        {:error, :bad_cell_id} ->
          error = Error.new("state.bad_cell_path", path: Base.encode16(cmd.cell_id, case: :lower))
          respond.("error", [error], response(cmd, 0), %{})

        # A router that did not answer is not a Cell that is not there: the
        # ack below would tell the Controller its state was covered.
        {:error, :relay_busy} ->
          respond.("error", [relay_busy(cmd.cell_id)], response(cmd, 0), %{})

        # No Cell to rebuild — a tasklet the class dropped, a key its cap
        # refuses: the controller's terminal covers what it shipped, and the
        # ack is what lets the bind proceed.
        {:error, _absent} ->
          land(s, cmd, respond, {"success", [], response(cmd, cmd.terminal_seq)})
      end
    end)
  end

  def dispatch(:compact_cell, cmd, respond, s) do
    off_loop(respond, fn ->
      with {:ok, pid} <- cell(s, cmd.cell_id, []),
           {:ok, seq, size} <- Cell.snapshot(pid) do
        respond.(
          "success",
          [],
          Codec.encode_compact_response(cmd.cell_id, seq, size),
          %{}
        )
      else
        {:error, reason} ->
          respond.(
            "error",
            [snapshot_refusal(reason)],
            Codec.encode_compact_response(cmd.cell_id, 0, 0),
            %{}
          )
      end
    end)
  end

  def dispatch(:cell_state, cmd, respond, s) do
    answer_off_loop(respond, fn ->
      with {:ok, json, seq} <- state_json(s, cmd.relay_id, selector(cmd)),
           do: {:ok, json, seq_metadata(seq)}
    end)
  end

  def dispatch(:tasklet_activity, cmd, respond, s),
    do: answer_off_loop(respond, fn -> activity(s, cmd.relay_id, cmd.cells_of) end)

  def dispatch(:migration_serialization, cmd, respond, s) do
    answer_off_loop(respond, fn ->
      with {:ok, json, _seq} <- state_json(s, cmd.relay_id, @whole), do: {:ok, json, %{}}
    end)
  end

  def dispatch(:cell_apply_state, cmd, respond, s),
    do: apply_off_loop(s, cmd.relay_id, cmd.state_binary, respond)

  def dispatch(:migration_deserialization, cmd, respond, s),
    do: apply_off_loop(s, cmd.relay_id, cmd.serialized_state, respond)

  def dispatch(:credit_window_init, cmd, respond, s) do
    Credit.arm(s.credit, cmd.stream_id, cmd.window_bytes)
    respond.("success", [], Codec.encode_credit_ack(cmd.stream_id), %{})
  end

  def dispatch(:credit_grant, cmd, respond, s) do
    Credit.grant(s.credit, cmd.stream_id, cmd.bytes)
    respond.("success", [], <<>>, %{})
  end

  # A pre-credit controller's back-pressure flag; this SDK's send path is
  # bounded by its mailboxes and the window.
  def dispatch(:cell_mailbox_status, cmd, respond, _s) do
    Logger.debug(
      "[Flezha.SDK] legacy cell mailbox status from a pre-credit controller: #{cmd.status}"
    )

    respond.("success", [], <<>>, %{})
  end

  @doc """
  The Decision-10 JSON of `relay_id`'s Cells narrowed by `selector`
  (`%{tasklet: name | nil, cell_key: key | nil}`), and the seq when
  exactly one Cell was read. A walk omits a Cell that does not answer; a
  read that names one Cell answers its failure.
  """
  @spec state_json(map(), String.t(), map()) ::
          {:ok, binary(), non_neg_integer() | nil} | {:error, Error.t()}
  def state_json(s, relay_id, selector) do
    with {:ok, op} <- relay(s, relay_id),
         :ok <- check_selector(selector),
         {:ok, all} <- nodes(op, relay_id),
         {:ok, nodes} <- select_nodes(all, relay_id, selector.tasklet),
         {:ok, top, seqs} <- collect(op, nodes, selector.cell_key),
         {:ok, json} <- to_json(top) do
      {:ok, json, single(seqs)}
    end
  end

  @doc """
  Apply a Decision-10 JSON object — `AddRelay.init_values`, a migration's
  state — to `op`'s Cells through each Tasklet's `state_from_json/1`: a
  Single node takes its value, a Static node its declared keys' values; a key
  that names no Tasklet or no declared Cell is logged and ignored.
  """
  @spec apply_json(pid(), String.t(), binary() | nil) :: :ok | {:error, Error.t()}
  def apply_json(_op, _relay_id, json) when json in [nil, ""], do: :ok

  def apply_json(op, relay_id, json) do
    case Jason.decode(json) do
      {:ok, %{} = top} ->
        with {:ok, nodes} <- nodes(op, relay_id), do: apply_top(nodes, relay_id, top)

      {:ok, other} ->
        {:error, Error.new("state.init_values_not_object", kind: json_kind(other))}

      {:error, e} ->
        {:error, Error.new("state.init_values_invalid_json", reason: Exception.message(e))}
    end
  end

  @doc """
  Ship one `RunnerShutdownNotification` per live relay and wait at most
  `ack_ms` for the acks — in the caller's process, the RunnerServer's, whose
  mailbox the control frames reach. Relays serialise in parallel under the
  serialise wait; a timed-out ack is logged and the exit proceeds.
  """
  @spec shutdown(map(), non_neg_integer()) :: :ok
  def shutdown(%{control_handle: nil}, _ack_ms), do: :ok
  def shutdown(%{relays: relays}, _ack_ms) when map_size(relays) == 0, do: :ok

  def shutdown(%{control: control, control_handle: handle, relays: relays} = s, ack_ms) do
    {pending, _hold} =
      relays
      |> Map.keys()
      |> Enum.map(fn oid -> Task.async(fn -> {oid, state_json(s, oid, @whole)} end) end)
      |> Task.yield_many(Knob.get(:sdk_serialize_yield_ms))
      |> Enum.map_reduce(Hold.bind(control, [handle]), fn {task, result}, hold ->
        ship(hold, result || Task.shutdown(task, :brutal_kill))
      end)

    await_acks(
      pending |> List.flatten() |> Map.new(),
      System.monotonic_time(:millisecond) + ack_ms
    )
  end

  # --- restore, compaction ---

  defp restore(pid, cmd) do
    case Cell.restore(
           pid,
           cmd.snapshot_header,
           cmd.snapshot_bytes,
           cmd.change_log_bytes,
           cmd.terminal_seq
         ) do
      {:ok, terminal, migrated} ->
        {"success", [], Codec.encode_restore_response(cmd.cell_id, terminal, migrated)}

      {:error, reason} ->
        {"error", [Error.new("state.restore_failed", reason: describe(reason))], response(cmd, 0)}
    end
  end

  # The frame has landed, whether or not a Cell took it: the relay counts
  # it toward a steps relay's restore gate, so a frame no Cell takes never holds
  # the gate, and takes the saga markers the controller persisted for the
  # Cell — `intent` that its tasklet completed forward and may need undo,
  # `done` that its compensation already ran. A call, not a cast: the driver
  # decides whether to resume an unwind when the last frame lands, and a
  # marker racing that decision would let a crash mid-rollback resume forward
  # instead. A relay that will not take it fails the restore, so the
  # Controller retries rather than resuming from a record that never landed.
  defp land(s, cmd, respond, {status, errors, raw}) do
    intent = Map.get(cmd, :has_compensation_intent, false)
    done = Map.get(cmd, :has_compensation_done, false)

    landed =
      with {:ok, {relay_id, _tasklet, _key}} <- Wire.decode_cell_id(cmd.cell_id),
           {:ok, op} <- relay(s, relay_id) do
        Call.control(op, {:restore_landed, cmd.cell_id, intent, done}, {:error, relay_id})
      else
        # No relay to count it: nothing waits on this frame.
        _unaddressable -> :ok
      end

    case landed do
      :ok ->
        respond.(status, errors, raw, %{})

      {:error, relay_id} ->
        respond.(
          "error",
          [Error.new("runtime.relay_busy", relay: relay_id)],
          response(cmd, 0),
          %{}
        )
    end
  end

  defp response(cmd, terminal),
    do: Codec.encode_restore_response(cmd.cell_id, terminal, false)

  defp cell(s, cell_id, opts) do
    with {:ok, {relay_id, tasklet, key}} <- Wire.decode_cell_id(cell_id),
         {:ok, op} <- relay(s, relay_id) do
      Relay.Server.lookup(op, tasklet, blank_to_nil(key), opts)
    end
  end

  # --- the live read ---

  # Live Cells and their queued items per tasklet. This runtime keeps no
  # per-Cell totals, so every total is absent, and a Cell read (`cells_of`) is
  # answered without Cells once its tasklet is found; a tasklet with a Cell
  # that does not answer by the deadline reports its queue as absent too. The
  # router and every Cell answer inside one deadline. The instance is the relay
  # process, so its hash tells a new one apart.
  defp activity(s, relay_id, cells_of) do
    now = System.monotonic_time(:millisecond)
    deadline = now + Knob.get(:sdk_activity_read_ms)

    with {:ok, op} <- relay(s, relay_id),
         {:ok, nodes} <- nodes(op, relay_id, deadline - now),
         {:ok, _named} <- select_nodes(nodes, relay_id, cells_of) do
      # The wire's clock is unsigned; the VM's monotonic clock may be negative.
      read_at_ms =
        now - System.convert_time_unit(:erlang.system_info(:start_time), :native, :millisecond)

      sizes =
        nodes
        |> Enum.flat_map(fn {_name, _mode, cells} -> Enum.map(cells, &elem(&1, 1)) end)
        |> Cell.queued(deadline)

      {rows, []} =
        Enum.map_reduce(nodes, sizes, fn {name, _mode, cells}, sizes ->
          {mine, rest} = Enum.split(sizes, length(cells))
          {activity_row(name, Enum.reject(mine, &(&1 == :gone))), rest}
        end)

      instance_seq = :erlang.phash2(op, 4_294_967_296)
      {:ok, Codec.encode_tasklet_activity(rows, read_at_ms, instance_seq), %{}}
    end
  end

  defp activity_row(name, live) do
    if Enum.member?(live, :timeout),
      do: {name, length(live), nil, nil},
      else: {name, length(live), Enum.sum(live), Enum.max(live, fn -> 0 end)}
  end

  defp relay(%{relays: relays}, relay_id) do
    case Map.get(relays, relay_id) do
      {pid, _factory_id} -> {:ok, pid}
      nil -> {:error, Error.new("runtime.relay_not_found", relay: relay_id)}
    end
  end

  # The router's nodes, or its refusal. `[]` would be a lie: a walk that read
  # it would report a live relay as empty.
  defp nodes(op, relay_id, ms \\ Knob.get(:sdk_control_call_ms)) do
    case Relay.Server.cells(op, ms) do
      {:error, :relay_busy} ->
        {:error, Error.new("runtime.relay_busy", relay: relay_id)}

      nodes ->
        {:ok, nodes}
    end
  end

  defp relay_busy(cell_id) do
    relay_id =
      case Wire.decode_cell_id(cell_id) do
        {:ok, {relay_id, _tasklet, _key}} -> relay_id
        _unreadable -> ""
      end

    Error.new("runtime.relay_busy", relay: relay_id)
  end

  defp selector(cmd),
    do: %{tasklet: blank_to_nil(cmd.tasklet), cell_key: blank_to_nil(cmd.cell_key)}

  defp check_selector(%{tasklet: nil, cell_key: key}) when key != nil,
    do: {:error, Error.new("state.cell_key_without_tasklet")}

  defp check_selector(_selector), do: :ok

  defp select_nodes(nodes, _relay_id, nil), do: {:ok, nodes}

  defp select_nodes(nodes, relay_id, tasklet) do
    case Enum.filter(nodes, fn {name, _mode, _cells} -> name == tasklet end) do
      [] ->
        {:error, Error.new("state.tasklet_not_found", relay: relay_id, tasklet: tasklet)}

      some ->
        {:ok, some}
    end
  end

  defp collect(op, nodes, key) do
    Enum.reduce_while(nodes, {:ok, %{}, []}, fn {name, mode, cells}, {:ok, top, seqs} ->
      case read_node(op, name, mode, cells, key) do
        {:ok, :absent, _} -> {:cont, {:ok, top, seqs}}
        {:ok, value, node_seqs} -> {:cont, {:ok, Map.put(top, name, value), node_seqs ++ seqs}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  # A Single node's one Cell, or a keyed node's live Cells by key; a key
  # named against a Single node, or absent from a keyed one, is a refusal.
  defp read_node(op, name, :single, _cells, key) when key != nil,
    do: {:error, missing(op, name, key)}

  defp read_node(_op, _name, :single, [], nil), do: {:ok, :absent, []}

  defp read_node(_op, name, :single, [{nil, pid} | _], nil) do
    case read(pid, name, "") do
      {:ok, state, seq} -> {:ok, state, [seq]}
      :omit -> {:ok, :absent, []}
    end
  end

  defp read_node(_op, name, _keyed, cells, nil) do
    Enum.reduce(cells, {:ok, %{}, []}, fn {key, pid}, {:ok, inner, seqs} ->
      case read(pid, name, key) do
        {:ok, state, seq} -> {:ok, Map.put(inner, key, state), [seq | seqs]}
        :omit -> {:ok, inner, seqs}
      end
    end)
  end

  defp read_node(op, name, _keyed, cells, key) do
    case List.keyfind(cells, key, 0) do
      nil ->
        {:error, missing(op, name, key)}

      {^key, pid} ->
        case Cell.read(pid, Knob.get(:sdk_serialize_wait_ms)) do
          {:ok, state, seq} ->
            {:ok, %{key => state}, [seq]}

          {:error, :busy} ->
            seconds = div(Knob.get(:sdk_serialize_wait_ms), 1000)
            {:error, Error.new("state.cell_busy", cell: "#{name}/#{key}", seconds: seconds)}

          {:error, :gone} ->
            {:error, missing(op, name, key)}
        end
    end
  end

  # In a walk a failing Cell is logged and omitted.
  defp read(pid, name, key) do
    case Cell.read(pid, Knob.get(:sdk_serialize_wait_ms)) do
      {:ok, state, seq} ->
        {:ok, state, seq}

      {:error, reason} ->
        Logger.warning(
          "[Flezha.SDK] serialize: tasklet `#{name}` key `#{key}` did not answer (#{reason})"
        )

        :omit
    end
  end

  # Evicted and never-existed serve the same absence; the diagnosis differs,
  # and the Relay's trace is what tells them apart.
  defp missing(op, name, key) do
    code =
      case Relay.Server.lookup(op, name, key) do
        {:error, :cell_evicted} -> "state.cell_evicted"
        _ -> "state.cell_not_found"
      end

    Error.new(code, tasklet: name, cell_key: key)
  end

  defp to_json(top) do
    case Jason.encode(top) do
      {:ok, json} ->
        {:ok, json}

      {:error, e} ->
        {:error, Error.new("state.serialize_failed", tasklet: "", reason: Exception.message(e))}
    end
  end

  defp single([seq]), do: seq
  defp single(_seqs), do: nil

  defp seq_metadata(nil), do: %{}
  defp seq_metadata(seq), do: %{@change_seq_key => Integer.to_string(seq)}

  # --- the JSON apply ---

  defp apply_off_loop(s, relay_id, json, respond) do
    answer_off_loop(respond, fn ->
      with {:ok, op} <- relay(s, relay_id),
           :ok <- apply_json(op, relay_id, json),
           do: {:ok, <<>>, %{}}
    end)
  end

  defp apply_top(nodes, relay_id, top) do
    top
    |> Enum.flat_map(fn {name, value} ->
      apply_node(List.keyfind(nodes, name, 0), relay_id, name, value)
    end)
    |> case do
      [] -> :ok
      errors -> {:error, Error.new("state.apply_failed", reason: Enum.join(errors, "; "))}
    end
  end

  defp apply_node(nil, relay_id, name, _value) do
    Logger.info("[Flezha.SDK] init_values for #{relay_id} names no Tasklet `#{name}`; ignored")

    []
  end

  defp apply_node({_name, :single, [{nil, pid} | _]}, _relay_id, name, value),
    do: apply_one(pid, name, "", value)

  defp apply_node({_name, :static, cells}, relay_id, name, %{} = by_key) do
    Enum.flat_map(by_key, fn {key, value} ->
      case List.keyfind(cells, key, 0) do
        {^key, pid} ->
          apply_one(pid, name, key, value)

        nil ->
          Logger.info(
            "[Flezha.SDK] init_values for #{relay_id}: `#{name}` declares no Cell `#{key}`; ignored"
          )

          []
      end
    end)
  end

  defp apply_node({_name, mode, _cells}, relay_id, name, _value) do
    Logger.info(
      "[Flezha.SDK] init_values for #{relay_id}: `#{name}` (#{mode}) takes no seeded state; ignored"
    )

    []
  end

  defp apply_one(pid, name, key, value) do
    case Cell.apply_state(pid, value) do
      :ok -> []
      {:error, reason} -> ["#{name}/#{key}: #{reason}"]
    end
  end

  # --- shutdown ---

  # The relay's final state is what makes a reconnect-resume loss-free, so a
  # refused write is held for the next bound stream, never printed and dropped.
  # Only what reached the wire is waited on: an ack cannot answer a held frame.
  defp ship(hold, {:ok, {relay_id, {:ok, json, _seq}}}) do
    ref = "shutdown-#{relay_id}-#{System.unique_integer([:positive])}"
    command = Codec.shutdown_notification_command(relay_id, json)
    frame = Codec.encode_request(ref, command)

    case Hold.write({"control_command", elem(command, 0)}, hold, {frame, %{}}) do
      {:ok, hold} -> {[{ref, relay_id}], hold}
      {{:held, _reason}, hold} -> {[], hold}
    end
  end

  defp ship(hold, {:ok, {relay_id, {:error, reason}}}) do
    Logger.warning("[Flezha.SDK] shutdown could not serialize #{relay_id}: #{reason.message}")
    {[], hold}
  end

  defp ship(hold, other) do
    Logger.warning("[Flezha.SDK] shutdown serialize did not answer: #{inspect(other)}")
    {[], hold}
  end

  defp await_acks(pending, _deadline) when map_size(pending) == 0, do: :ok

  defp await_acks(pending, deadline) do
    receive do
      {:flz_data, _id, "control_command", bin, _meta} ->
        case Codec.decode_command(bin) do
          {:ok, %{reference_id: ref, command: {:runner_shutdown_ack, _ack}}} ->
            await_acks(Map.delete(pending, ref), deadline)

          _other ->
            await_acks(pending, deadline)
        end
    after
      max(deadline - System.monotonic_time(:millisecond), 0) ->
        Enum.each(pending, fn {_ref, oid} ->
          Logger.warning("[Flezha.SDK] RunnerShutdownAck timeout for #{oid}")
        end)
    end
  end

  # --- helpers ---

  # A command that needs a Cell's answer never runs on the RunnerServer's
  # loop; a failure on the way is the command's error, never a silent timeout.
  defp off_loop(respond, fun) do
    Task.start(fn ->
      try do
        fun.()
      rescue
        e -> respond.("error", [crashed(Exception.message(e))], <<>>, %{})
      catch
        :exit, reason -> respond.("error", [crashed("exit: #{inspect(reason)}")], <<>>, %{})
      end
    end)

    :ok
  end

  # `fun` answers `{:ok, raw_data, metadata}` or the refusal.
  defp answer_off_loop(respond, fun) do
    off_loop(respond, fn ->
      case fun.() do
        {:ok, raw, metadata} -> respond.("success", [], raw, metadata)
        {:error, reason} -> respond.("error", [reason], <<>>, %{})
      end
    end)
  end

  defp blank_to_nil(""), do: nil
  defp blank_to_nil(value), do: value

  defp crashed(reason), do: Error.new("runtime.command_crashed", reason: reason)

  defp snapshot_refusal(%Error{} = error), do: error
  defp snapshot_refusal(reason), do: Error.new("state.snapshot_failed", reason: describe(reason))

  # The JSON type word the reference core names.
  defp json_kind(value) when is_list(value), do: "array"
  defp json_kind(value) when is_binary(value), do: "string"
  defp json_kind(value) when is_number(value), do: "number"
  defp json_kind(value) when is_boolean(value), do: "bool"
  defp json_kind(nil), do: "null"

  defp describe(reason) when is_binary(reason), do: reason
  defp describe(reason), do: to_string(reason)
end
