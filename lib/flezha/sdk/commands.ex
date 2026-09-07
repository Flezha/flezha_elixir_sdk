defmodule Flezha.SDK.Commands do
  @moduledoc """
  The persistence-family commands the RunnerServer answers, and the shutdown
  notifications it ships — its zero-mutation half: every function reads the
  server's state (the operatives, the control handle, the credit window) and
  answers through the reply seam it is handed; none writes the state. A
  command that needs an Entity's answer runs off the server's loop, so a fold
  or a serialisation never stalls the control channel; the credit commands
  answer on it and never block.

  The live read is the Decision-10 shape: `%{procedure => state}` for a
  Single node, `%{procedure => %{key => state}}` for a Static or Dynamic one,
  narrowed by the selector, with the reply frame carrying `flz.change_seq`
  when exactly one Entity was read. Refusals carry the tokens the controller
  matches as prefixes: `entity_key_without_procedure`, `procedure_not_found`,
  `entity_not_found`, `entity_busy`.
  """

  require Logger

  alias Flezha.SDK.{Call, Codec, Credit, Entity, Hold, Knob, Operative}
  alias Flezha.State.Wire

  # A serialise's wait for a mid-dispatch Entity, and the serialise bound of a
  # graceful shutdown: inside a container's grace period.
  @change_seq_key Wire.change_seq_key()
  @whole %{procedure: nil, entity_key: nil}
  # One refusal, and each surface adds the prefix its siblings carry.
  @router_silent "entity_busy: the operative did not answer"
  @marker_silent "entity_busy: the operative did not take the compensation markers"

  @typedoc "How a command answers: status, errors, raw_data, the reply frame's metadata."
  @type respond :: (String.t(), [String.t()], binary(), map() -> any())

  @doc "Answer `tag`'s `cmd` for the runner in `s` through `respond`."
  @spec dispatch(atom(), map(), respond(), map()) :: :ok
  def dispatch(:restore_init_state, cmd, respond, s) do
    off_loop(respond, fn ->
      case entity(s, cmd.entity_id, materialise: true) do
        {:ok, pid} ->
          case note_compensation(s, cmd) do
            :ok -> restore(pid, cmd, respond)
            {:error, reason} ->
              respond.("error", ["restore_init_state: " <> reason], response(cmd, 0), %{})
          end

        {:error, :bad_entity_id} ->
          respond.("error", ["restore_init_state: malformed entity_id"], response(cmd, 0), %{})

        # A router that did not answer is not an Entity that is not there: the
        # ack below would tell the Controller its state was covered.
        {:error, :operative_busy} ->
          respond.("error", ["restore_init_state: " <> @router_silent], response(cmd, 0), %{})

        # No live Entity to rebuild: the controller's terminal covers what it
        # shipped, and the ack is what lets the bind proceed.
        {:error, _absent} ->
          respond.("success", [], response(cmd, cmd.terminal_seq), %{})
      end
    end)
  end

  def dispatch(:compact_entity, cmd, respond, s) do
    off_loop(respond, fn ->
      with {:ok, pid} <- entity(s, cmd.entity_id, []),
           {:ok, seq, size} <- Entity.snapshot(pid) do
        respond.(
          "success",
          [],
          Codec.encode_compact_response(cmd.entity_id, seq, size),
          %{}
        )
      else
        {:error, reason} ->
          respond.(
            "error",
            ["compact_entity: #{describe(reason)}"],
            Codec.encode_compact_response(cmd.entity_id, 0, 0),
            %{}
          )
      end
    end)
  end

  def dispatch(:entity_state, cmd, respond, s) do
    off_loop(respond, fn ->
      case state_json(s, cmd.operative_id, selector(cmd)) do
        {:ok, json, seq} -> respond.("success", [], json, seq_metadata(seq))
        {:error, reason} -> respond.("error", [reason], <<>>, %{})
      end
    end)
  end

  def dispatch(:migration_serialization, cmd, respond, s) do
    off_loop(respond, fn ->
      case state_json(s, cmd.operative_id, @whole) do
        {:ok, json, _seq} -> respond.("success", [], json, %{})
        {:error, reason} -> respond.("error", [reason], <<>>, %{})
      end
    end)
  end

  def dispatch(:entity_apply_state, cmd, respond, s),
    do: apply_off_loop(s, cmd.operative_id, cmd.state_binary, respond)

  def dispatch(:migration_deserialization, cmd, respond, s),
    do: apply_off_loop(s, cmd.operative_id, cmd.serialized_state, respond)

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
  def dispatch(:entity_mailbox_status, cmd, respond, _s) do
    Logger.debug(
      "[Flezha.SDK] legacy entity mailbox status from a pre-credit controller: #{cmd.status}"
    )

    respond.("success", [], <<>>, %{})
  end

  @doc """
  The Decision-10 JSON of `operative_id`'s Entities narrowed by `selector`
  (`%{procedure: name | nil, entity_key: key | nil}`), and the seq when
  exactly one Entity was read. A walk omits an Entity that does not answer; a
  read that names one Entity answers its failure.
  """
  @spec state_json(map(), String.t(), map()) ::
          {:ok, binary(), non_neg_integer() | nil} | {:error, String.t()}
  def state_json(s, operative_id, selector) do
    with {:ok, op} <- operative(s, operative_id),
         :ok <- check_selector(selector),
         {:ok, all} <- nodes(op),
         {:ok, nodes} <- select_nodes(all, operative_id, selector.procedure),
         {:ok, top, seqs} <- collect(op, nodes, selector.entity_key),
         {:ok, json} <- to_json(top) do
      {:ok, json, single(seqs)}
    end
  end

  @doc """
  Apply a Decision-10 JSON object — `AddOperative.init_values`, a migration's
  state — to `op`'s Entities through each Procedure's `state_from_json/1`: a
  Single node takes its value, a Static node its declared keys' values; a key
  that names no Procedure or no declared Entity is logged and ignored.
  """
  @spec apply_json(pid(), String.t(), binary() | nil) :: :ok | {:error, String.t()}
  def apply_json(_op, _operative_id, json) when json in [nil, ""], do: :ok

  def apply_json(op, operative_id, json) do
    case Jason.decode(json) do
      {:ok, %{} = top} -> with {:ok, nodes} <- nodes(op), do: apply_top(nodes, operative_id, top)
      {:ok, _other} -> {:error, "init_values: not a JSON object"}
      {:error, e} -> {:error, "init_values: #{Exception.message(e)}"}
    end
  end

  @doc """
  Ship one `RunnerShutdownNotification` per live operative and wait at most
  `ack_ms` for the acks — in the caller's process, the RunnerServer's, whose
  mailbox the control frames reach. Operatives serialise in parallel under the
  serialise wait; a timed-out ack is logged and the exit proceeds.
  """
  @spec shutdown(map(), non_neg_integer()) :: :ok
  def shutdown(%{control_handle: nil}, _ack_ms), do: :ok
  def shutdown(%{operatives: operatives}, _ack_ms) when map_size(operatives) == 0, do: :ok

  def shutdown(%{control: control, control_handle: handle, operatives: operatives} = s, ack_ms) do
    {pending, _hold} =
      operatives
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

  defp restore(pid, cmd, respond) do
    case Entity.restore(
           pid,
           cmd.snapshot_header,
           cmd.snapshot_bytes,
           cmd.change_log_bytes,
           cmd.terminal_seq
         ) do
      {:ok, terminal, migrated} ->
        respond.(
          "success",
          [],
          Codec.encode_restore_response(cmd.entity_id, terminal, migrated),
          %{}
        )

      {:error, reason} ->
        respond.("error", ["restore_init_state: #{reason}"], response(cmd, 0), %{})
    end
  end

  # The saga markers the controller persisted for this Entity ride the restore
  # frame: `intent` that its procedure completed forward and may need undo,
  # `done` that its compensation already ran. They are instance-scoped, so
  # they go to the operative rather than to the Entity — a workflow driver
  # reads them to finish an unwind the crash interrupted, and a stream
  # operative ignores the cast.
  defp note_compensation(s, cmd) do
    intent = Map.get(cmd, :has_compensation_intent, false)
    done = Map.get(cmd, :has_compensation_done, false)
    if intent or done, do: ship_markers(s, cmd, intent, done), else: :ok
  end

  defp ship_markers(s, cmd, intent, done) do
    with {:ok, {operative_id, _procedure, _key}} <- Wire.decode_entity_id(cmd.entity_id),
         {:ok, op} <- operative(s, operative_id) do
      # A call, not a cast: the driver decides whether to resume an unwind when
      # the last restore lands, and a marker racing that decision would let a
      # crash mid-rollback resume forward instead. Only a workflow class ever
      # gets here — both flags are always false for a stream operative. A driver
      # that will not take the markers fails the restore, so the Controller
      # retries rather than resuming an unwind from a record that never landed.
      Call.control(op, {:compensation_markers, cmd.entity_id, intent, done},
        {:error, @marker_silent})
    else
      _unaddressable -> :ok
    end
  end

  defp response(cmd, terminal),
    do: Codec.encode_restore_response(cmd.entity_id, terminal, false)

  defp entity(s, entity_id, opts) do
    with {:ok, {operative_id, procedure, key}} <- Wire.decode_entity_id(entity_id),
         {:ok, op} <- operative(s, operative_id) do
      Operative.lookup(op, procedure, blank_to_nil(key), opts)
    end
  end

  # --- the live read ---

  defp operative(%{operatives: operatives}, operative_id) do
    case Map.get(operatives, operative_id) do
      {pid, _factory_id} -> {:ok, pid}
      nil -> {:error, "StreamOperative instance with id #{operative_id} not found"}
    end
  end

  # The router's nodes, or its refusal in the words the Controller matches as a
  # prefix. `[]` would be a lie: a walk that read it would report a live
  # operative as empty.
  defp nodes(op) do
    case Operative.entities(op) do
      {:error, :operative_busy} -> {:error, @router_silent}
      nodes -> {:ok, nodes}
    end
  end

  defp selector(cmd),
    do: %{procedure: blank_to_nil(cmd.procedure), entity_key: blank_to_nil(cmd.entity_key)}

  defp check_selector(%{procedure: nil, entity_key: key}) when key != nil,
    do:
      {:error,
       "entity_key_without_procedure: an Entity key is unique only within its Procedure — " <>
         "name the Procedure as well"}

  defp check_selector(_selector), do: :ok

  defp select_nodes(nodes, _operative_id, nil), do: {:ok, nodes}

  defp select_nodes(nodes, operative_id, procedure) do
    case Enum.filter(nodes, fn {name, _mode, _entities} -> name == procedure end) do
      [] ->
        {:error,
         "procedure_not_found: operative `#{operative_id}` has no Procedure named `#{procedure}`"}

      some ->
        {:ok, some}
    end
  end

  defp collect(op, nodes, key) do
    Enum.reduce_while(nodes, {:ok, %{}, []}, fn {name, mode, entities}, {:ok, top, seqs} ->
      case read_node(op, name, mode, entities, key) do
        {:ok, :absent, _} -> {:cont, {:ok, top, seqs}}
        {:ok, value, node_seqs} -> {:cont, {:ok, Map.put(top, name, value), node_seqs ++ seqs}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  # A Single node's one Entity, or a keyed node's live Entities by key; a key
  # named against a Single node, or absent from a keyed one, is a refusal.
  defp read_node(op, name, :single, _entities, key) when key != nil,
    do: {:error, missing(op, name, key)}

  defp read_node(_op, _name, :single, [], nil), do: {:ok, :absent, []}

  defp read_node(_op, name, :single, [{nil, pid} | _], nil) do
    case read(pid, name, "") do
      {:ok, state, seq} -> {:ok, state, [seq]}
      :omit -> {:ok, :absent, []}
    end
  end

  defp read_node(_op, name, _keyed, entities, nil) do
    Enum.reduce(entities, {:ok, %{}, []}, fn {key, pid}, {:ok, inner, seqs} ->
      case read(pid, name, key) do
        {:ok, state, seq} -> {:ok, Map.put(inner, key, state), [seq | seqs]}
        :omit -> {:ok, inner, seqs}
      end
    end)
  end

  defp read_node(op, name, _keyed, entities, key) do
    case List.keyfind(entities, key, 0) do
      nil ->
        {:error, missing(op, name, key)}

      {^key, pid} ->
        case Entity.read(pid, Knob.get(:sdk_serialize_wait_ms)) do
          {:ok, state, seq} ->
            {:ok, %{key => state}, [seq]}

          {:error, :busy} ->
            {:error, "entity_busy: Procedure `#{name}` Entity `#{key}` is mid-dispatch"}

          {:error, :gone} ->
            {:error, missing(op, name, key)}
        end
    end
  end

  # In a walk a failing Entity is logged and omitted.
  defp read(pid, name, key) do
    case Entity.read(pid, Knob.get(:sdk_serialize_wait_ms)) do
      {:ok, state, seq} ->
        {:ok, state, seq}

      {:error, reason} ->
        Logger.warning(
          "[Flezha.SDK] serialize: procedure `#{name}` key `#{key}` did not answer (#{reason})"
        )

        :omit
    end
  end

  # Evicted and never-existed serve the same absence; the diagnosis differs,
  # and the Operative's trace is what tells them apart.
  defp missing(op, name, key) do
    case Operative.lookup(op, name, key) do
      {:error, :entity_evicted} ->
        "entity_evicted: Entity `#{key}` of procedure `#{name}` was idle-evicted; eviction is " <>
          "destructive, so there is no durable value to serve"

      _ ->
        "entity_not_found: Procedure `#{name}` has no live Entity `#{key}`"
    end
  end

  defp to_json(top) do
    case Jason.encode(top) do
      {:ok, json} -> {:ok, json}
      {:error, e} -> {:error, "state is not JSON: #{Exception.message(e)}"}
    end
  end

  defp single([seq]), do: seq
  defp single(_seqs), do: nil

  defp seq_metadata(nil), do: %{}
  defp seq_metadata(seq), do: %{@change_seq_key => Integer.to_string(seq)}

  # --- the JSON apply ---

  defp apply_off_loop(s, operative_id, json, respond) do
    off_loop(respond, fn ->
      with {:ok, op} <- operative(s, operative_id),
           :ok <- apply_json(op, operative_id, json) do
        respond.("success", [], <<>>, %{})
      else
        {:error, reason} -> respond.("error", [reason], <<>>, %{})
      end
    end)
  end

  defp apply_top(nodes, operative_id, top) do
    top
    |> Enum.flat_map(fn {name, value} ->
      apply_node(List.keyfind(nodes, name, 0), operative_id, name, value)
    end)
    |> case do
      [] -> :ok
      errors -> {:error, Enum.join(errors, " | ")}
    end
  end

  defp apply_node(nil, operative_id, name, _value) do
    Logger.info(
      "[Flezha.SDK] init_values for #{operative_id} names no Procedure `#{name}`; ignored"
    )

    []
  end

  defp apply_node({_name, :single, [{nil, pid} | _]}, _operative_id, name, value),
    do: apply_one(pid, name, "", value)

  defp apply_node({_name, :static, entities}, operative_id, name, %{} = by_key) do
    Enum.flat_map(by_key, fn {key, value} ->
      case List.keyfind(entities, key, 0) do
        {^key, pid} ->
          apply_one(pid, name, key, value)

        nil ->
          Logger.info(
            "[Flezha.SDK] init_values for #{operative_id}: `#{name}` declares no Entity `#{key}`; ignored"
          )

          []
      end
    end)
  end

  defp apply_node({_name, mode, _entities}, operative_id, name, _value) do
    Logger.info(
      "[Flezha.SDK] init_values for #{operative_id}: `#{name}` (#{mode}) takes no seeded state; ignored"
    )

    []
  end

  defp apply_one(pid, name, key, value) do
    case Entity.apply_state(pid, value) do
      :ok -> []
      {:error, reason} -> ["#{name}/#{key}: #{reason}"]
    end
  end

  # --- shutdown ---

  # The operative's final state is what makes a reconnect-resume loss-free, so a
  # refused write is held for the next bound stream, never printed and dropped.
  # Only what reached the wire is waited on: an ack cannot answer a held frame.
  defp ship(hold, {:ok, {operative_id, {:ok, json, _seq}}}) do
    ref = "shutdown-#{operative_id}-#{System.unique_integer([:positive])}"
    command = Codec.shutdown_notification_command(operative_id, json)
    frame = Codec.encode_request(ref, command)

    case Hold.write({"control_command", elem(command, 0)}, hold, {frame, %{}}) do
      {:ok, hold} -> {[{ref, operative_id}], hold}
      {{:held, _reason}, hold} -> {[], hold}
    end
  end

  defp ship(hold, {:ok, {operative_id, {:error, reason}}}) do
    Logger.warning("[Flezha.SDK] shutdown could not serialize #{operative_id}: #{reason}")
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

  # A command that needs an Entity's answer never runs on the RunnerServer's
  # loop; a failure on the way is the command's error, never a silent timeout.
  defp off_loop(respond, fun) do
    Task.start(fn ->
      try do
        fun.()
      rescue
        e -> respond.("error", [Exception.message(e)], <<>>, %{})
      catch
        :exit, reason -> respond.("error", ["exit: #{inspect(reason)}"], <<>>, %{})
      end
    end)

    :ok
  end

  defp blank_to_nil(""), do: nil
  defp blank_to_nil(value), do: value

  defp describe(reason) when is_binary(reason), do: reason
  defp describe(reason), do: to_string(reason)
end
