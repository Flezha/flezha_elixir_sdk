defmodule Flezha.SDK.SafeCall do
  @moduledoc """
  One bounded `GenServer.call` wrapper for calls into a process that may
  be wedged, mid-restart, or already gone.

  `bounded/3` returns `{:ok, reply}` on success and `{:error, reason}`
  on timeout / `:noproc` / any other exit, after logging once and
  emitting `[:flezha, :convergence, :safe_call]` telemetry. Callers that
  only care about side effects ignore the result; callers that need the
  reply pattern-match it.

  `bounded/4` is the same call with the refusal declared as DATA instead of
  written as a `catch` clause at the site. It answers the reply VERBATIM on
  success and the caller's declared value on a refusal, which is exactly the
  shape a hand-written `try/catch` around a `GenServer.call` has — so a site
  folds into it without its own callers changing. A reason the declaration
  does not name is re-raised, so a site that caught two of three exits keeps
  propagating the third.

      SafeCall.bounded(pid, :stats, 1_000, on_error: {:error, :not_running})
      SafeCall.bounded(unit, req, ms, on_error: %{timeout: {:error, :timeout}})
      SafeCall.bounded(pid, req, ms, on_error: %{other: &{:error, {:exit, &1}}})

  A map whose keys are all reasons — `:timeout`, `:noproc`, `:other` — is the
  reason table, and each of its values is either the result or a 1-arity
  function of the RAW exit reason, for the sites whose answer quotes it. Any
  other term, a map included, is the single answer for every refusal.
  """

  require Logger

  @type target :: pid() | atom() | {atom(), node()}

  @spec bounded(target(), term(), timeout()) :: {:ok, term()} | {:error, term()}
  def bounded(server, request, timeout) when is_integer(timeout) or timeout == :infinity do
    case attempt(server, request, timeout, &{:error, classify(&1)}, :warning) do
      {:ok, reply} -> {:ok, reply}
      {:refused, result} -> result
    end
  end

  @doc """
  A bounded call whose refusal is declared. See the moduledoc; `opts` takes
  `:on_error` (required) and `:log`, the level a refusal is logged at
  (default `:warning`; `false` silences the line for a site where a refusal
  is routine, the telemetry event still fires).
  """
  @spec bounded(target(), term(), timeout(), keyword()) :: term()
  def bounded(server, request, timeout, opts)
      when (is_integer(timeout) or timeout == :infinity) and is_list(opts) do
    declared = Keyword.fetch!(opts, :on_error)

    case attempt(server, request, timeout, declared, Keyword.get(opts, :log, :warning)) do
      {:ok, reply} -> reply
      {:refused, result} -> result
    end
  end

  # The one call, the one catch, and the one place a refusal is observed. Both
  # entry points differ in nothing but what they do with the reply.
  defp attempt(server, request, timeout, declared, level) do
    {:ok, GenServer.call(server, request, timeout)}
  catch
    :exit, reason ->
      classified = classify(reason)

      case declare(declared, classified, reason) do
        # A reason the site did not name keeps propagating: a site that caught
        # two of three exits must still die on the third.
        :undeclared ->
          exit(reason)

        {:ok, result} ->
          # On a timeout the target is alive but wedged — introspect it so the
          # log names WHERE it is stuck instead of forcing operators to infer
          # the wedge from the caller side. A `:noproc` target is already gone.
          probe = if classified == :timeout, do: probe_target(server), else: %{}
          report(server, classified, probe)
          log(level, server, request, reason, probe)
          {:refused, result}
      end
  end

  @reasons [:timeout, :noproc, :other]

  # A map whose keys are reasons declares one answer each and re-raises the
  # rest; every other term — a map that is plainly a value included — is the
  # answer for every reason. Keying on the reason names rather than on "is a
  # map" is what lets a site answer WITH a map.
  defp declare(table, classified, reason)
       when is_map(table) and map_size(table) > 0 do
    if Enum.all?(Map.keys(table), &(&1 in @reasons)),
      do: by_reason(table, classified, reason),
      else: {:ok, apply_declared(table, reason)}
  end

  defp declare(value, _classified, reason), do: {:ok, apply_declared(value, reason)}

  # The reason's own row, else `:other`, else nothing was declared for it.
  defp by_reason(table, classified, reason) do
    with :error <- Map.fetch(table, classified),
         :error <- Map.fetch(table, :other) do
      :undeclared
    else
      {:ok, value} -> {:ok, apply_declared(value, reason)}
    end
  end

  defp apply_declared(value, reason) when is_function(value, 1), do: value.(reason)
  defp apply_declared(value, _reason), do: value

  defp log(false, _server, _request, _reason, _probe), do: :ok

  defp log(level, server, request, reason, probe) do
    Logger.log(
      level,
      "[convergence] bounded call failed server=#{inspect(server)} " <>
        "request=#{inspect(elem_tag(request))} reason=#{inspect(reason)}" <>
        format_probe(probe)
    )
  end

  # The event name is a published surface and is deliberately not derived from
  # this module's name.
  defp report(server, classified, probe) do
    :telemetry.execute(
      [:flezha, :convergence, :safe_call],
      %{count: 1},
      Map.merge(%{result: classified, server: server}, probe)
    )
  end

  defp classify({:timeout, _}), do: :timeout
  defp classify({:noproc, _}), do: :noproc
  defp classify(:noproc), do: :noproc
  defp classify(reason), do: reason

  # Resolve the target (pid | name | {name, node}) to a pid and sample its
  # mailbox depth + current function. `current_function` is the high-signal
  # field: it shows `:gen.do_call/4` when the target is itself blocked on a
  # downstream call. `Process.info/2` only accepts a LOCAL pid — it raises
  # `ArgumentError` on a remote pid (and yields `nil` only for a dead local
  # one), so we introspect locally and report a remote pid as such.
  defp probe_target(server) do
    case GenServer.whereis(server) do
      pid when is_pid(pid) and node(pid) == node() ->
        case Process.info(pid, [:message_queue_len, :current_function]) do
          [{:message_queue_len, qlen}, {:current_function, cf}] ->
            %{target_pid: pid, target_queue_len: qlen, target_current_function: cf}

          nil ->
            %{target_pid: pid, target_status: :unavailable}
        end

      pid when is_pid(pid) ->
        %{target_pid: pid, target_status: :remote, target_node: node(pid)}

      _ ->
        %{target_status: :no_task}
    end
  end

  defp format_probe(probe) when map_size(probe) == 0, do: ""

  defp format_probe(%{target_queue_len: qlen, target_current_function: cf}),
    do: " target_queue_len=#{qlen} target_current_function=#{inspect(cf)}"

  defp format_probe(probe), do: " target=#{inspect(probe)}"

  # Log only the message tag, never the full payload (frames can be large).
  defp elem_tag(request) when is_tuple(request) and tuple_size(request) > 0, do: elem(request, 0)
  defp elem_tag(request), do: request
end
