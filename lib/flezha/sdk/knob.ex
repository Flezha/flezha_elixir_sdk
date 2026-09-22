defmodule Flezha.SDK.Knob do
  @moduledoc """
  Every knob this SDK reads and every wait it takes before a retry, declared
  once.

  A row is a name, the variable that sets it, the type its text parses as and
  the value it takes when nothing sets it. A runner reads a name from its
  variable first, then from this application's env
  (`config :flezha_sdk, mailbox_capacity: 2048`), then the row's default; a row
  with no variable is set through the application env alone. A value that does
  not parse — a pool size of `"0"`, a flag spelled `"maybe"` — reads as the
  default, which is what every other SDK's reader answers too. The contract
  set's `knob_parse_cases.txt` is that rule written down, and this package's
  suite feeds it to `parse/2`.

  A ladder is how long a retry waits: `base · 2^(n−1)`, the exponent capped where
  the Rust runtime caps it, the value capped always, and, for a jittered ladder,
  spread uniformly over `[d/2, 3d/2)` before the cap. Every wait and every reset
  is announced on `[:flezha, :backoff, :wait]` and `[:flezha, :backoff, :reset]`.
  The arithmetic and the announcements are the controller's too: its ladder table
  waits through `delay/3` with rows of its own.

  The names, variables, types, defaults and ladders are the ones Flezha's
  configuration table declares for the Elixir SDK, and the core's suite holds
  these to it, as it holds every other runtime's reader.
  """

  @typedoc "A knob's name."
  @type name :: atom()
  @typedoc "How a knob's text is read."
  @type type :: :ms | :int | :positive_int | :bool | :string | :path
  @typedoc "A declared retry ladder's name."
  @type ladder :: :sdk_reconnect | :sdk_retry | :park_resend | :cell_wake_retry

  # {name, variable, type, default}
  @rows [
    {:broken_pipe_reconnect_threshold, "FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD", :positive_int,
     10},
    {:broken_pipe_window_ms, "FLEZHA_BROKEN_PIPE_WINDOW_MS", :ms, 5_000},
    {:ca_cert_path, "FLEZHA_CA_CERT_PATH", :path, "app_data/certs/ca.pem"},
    {:control_keepalive_ms, nil, :ms, 15_000},
    {:data_heartbeat_ms, "FLEZHA_DATA_HEARTBEAT_MS", :ms, 10_000},
    {:local_stream_call_ms, nil, :ms, 5_000},
    {:mailbox_capacity, "FLEZHA_MAILBOX_CAPACITY", :positive_int, 1_024},
    {:namespace_secret, "FLEZHA_NAMESPACE_SECRET", :string, nil},
    {:quic_addresses, "FLEZHA_QUIC_ADDRESSES", :string, "127.0.0.1:4433"},
    # The cadence a keyed host sweeps idle cells at. Unset, the caller's own
    # default applies: a quarter of the cell TTL.
    {:reap_interval_ms, "FLEZHA_REAP_INTERVAL_MS", :ms, nil},
    {:reconnect_request_retry_ms, "FLEZHA_RECONNECT_REQUEST_RETRY_MS", :ms, 30_000},
    {:reconnect_ring_capacity, "FLEZHA_RECONNECT_RING_CAPACITY", :positive_int, 1_024},
    {:runner_id_path, "FLEZHA_RUNNER_ID_PATH", :path, "/var/lib/flezha-runner/id"},
    # A call into one of the runner's own control processes: the RunnerServer, an
    # Operative, a local run. None of them runs author code on the answering
    # clause, so a wait here is a wedged process rather than a busy one.
    {:sdk_control_call_ms, nil, :ms, 5_000},
    # A call into a Cell that answers from its queue rather than its state.
    {:sdk_cell_call_ms, nil, :ms, 5_000},
    # The three calls whose duration grows with the state. Each waits inside the
    # controller request that asked for it, one or five seconds short of it: a
    # refused call is not a cancelled one, and an inner bound equal to the outer
    # is no bound at all.
    {:sdk_cell_apply_ms, nil, :ms, 4_000},
    {:sdk_cell_restore_ms, nil, :ms, 10_000},
    {:sdk_cell_snapshot_ms, nil, :ms, 25_000},
    {:sdk_idle_probe_ms, nil, :ms, 50},
    {:sdk_max_reap_interval_ms, nil, :ms, 30_000},
    # How long a rehydrated per-input workflow loop waits for the inbox tail its
    # bind still owes before it may park: a park there would drop the tail.
    {:sdk_owed_inbox_wait_ms, nil, :ms, 60_000},
    {:sdk_serialize_wait_ms, nil, :ms, 5_000},
    # The task yield outlasts the read it awaits, so a wedged Cell answers the
    # read's own bound rather than the yield's.
    {:sdk_serialize_yield_ms, nil, :ms, 6_000},
    {:sdk_shutdown_ack_ms, nil, :ms, 2_000},
    # How long a tasklet's spawn request waits for the controller's answer.
    {:sdk_spawn_reply_ms, nil, :ms, 30_000},
    # Unset, the socket directory is the first rung of the ladder every runtime
    # walks, which `flz_transport` resolves.
    {:socket_files_path, "FLEZHA_SOCKET_FILES_PATH", :path, nil},
    {:workflow_park_idle_ms, "FLEZHA_WORKFLOW_PARK_IDLE_MS", :ms, 5_000}
  ]

  @by_name Map.new(@rows, fn {name, env, type, default} ->
             {name, %{name: name, env: env, type: type, default: default}}
           end)

  @ladders %{
    # Reset only on a completed handshake, so a connect that keeps failing climbs.
    sdk_reconnect: %{base: 100, cap: 30_000, exponent_cap: 8, jitter: true},
    sdk_retry: %{base: 50, cap: 5_000, exponent_cap: 16, jitter: true},
    park_resend: %{base: 2_000, cap: 2_000, exponent_cap: nil, jitter: false},
    cell_wake_retry: %{base: 5, cap: 5, exponent_cap: nil, jitter: false}
  }

  @wait_event [:flezha, :backoff, :wait]
  @reset_event [:flezha, :backoff, :reset]

  @doc "Every row, keyed by name: `%{name, env, type, default}`."
  @spec rows() :: %{name() => map()}
  def rows, do: @by_name

  @doc "Every ladder, keyed by name: `%{base, cap, exponent_cap, jitter}`."
  @spec ladders() :: %{ladder() => map()}
  def ladders, do: @ladders

  @doc "What this runner reads for `name`: its variable, its application env, else the default."
  @spec get(name()) :: term()
  def get(name), do: get(name, Map.fetch!(@by_name, name).default)

  @doc """
  What this runner reads for `name`, with the caller's own fallback for a
  quantity the row cannot state — a sweep cadence derived from a cell's TTL.
  """
  @spec get(name(), term()) :: term()
  def get(name, fallback) do
    row = Map.fetch!(@by_name, name)

    with var when is_binary(var) <- row.env,
         raw when is_binary(raw) <- System.get_env(var),
         {:ok, value} <- parse(row.type, raw) do
      value
    else
      _ -> Application.get_env(:flezha_sdk, name, fallback)
    end
  end

  @doc """
  The value `raw` means for `type`, or `:error` when it is not one. Text is
  trimmed first; `:positive_int` refuses anything below one, `:ms` anything
  below zero, and `:bool` reads `1/true/yes/on` and `0/false/no/off` whatever
  their case.
  """
  @spec parse(type(), String.t()) :: {:ok, term()} | :error
  def parse(type, raw) when is_binary(raw), do: parse_trimmed(type, String.trim(raw))

  defp parse_trimmed(:bool, raw) do
    case String.downcase(raw) do
      truthy when truthy in ["1", "true", "yes", "on"] -> {:ok, true}
      falsy when falsy in ["0", "false", "no", "off"] -> {:ok, false}
      _ -> :error
    end
  end

  defp parse_trimmed(:ms, raw), do: whole(raw, 0)
  defp parse_trimmed(:positive_int, raw), do: whole(raw, 1)
  defp parse_trimmed(:int, raw), do: whole(raw, nil)
  defp parse_trimmed(text, raw) when text in [:string, :path], do: {:ok, raw}

  defp whole(raw, floor) do
    case Integer.parse(raw) do
      {n, ""} when floor == nil -> {:ok, n}
      {n, ""} when n >= floor -> {:ok, n}
      _ -> :error
    end
  end

  @doc "Sends of one workflow commit barrier, the first included, before the run fails."
  # A re-send is the connection's business, not a class's, so nothing sets it;
  # in lockstep with the core's `BARRIER_RESEND_ATTEMPTS`.
  @spec barrier_resend_attempts() :: pos_integer()
  def barrier_resend_attempts, do: 3

  @doc "The row `ladder` declares, for a site that takes one of its bounds as a default."
  @spec ladder(ladder()) :: map()
  def ladder(ladder), do: Map.fetch!(@ladders, ladder)

  @doc """
  The wait before `attempt` (1-based) of a ladder: one this SDK declares, by name, or a row
  whose bounds are numbers, which is how the controller's own table waits by this same
  arithmetic. A row is `rungs` indexed by the attempt and held at the last, or a geometric
  `base` growing by `factor` (2 unless the row says) with the exponent held at
  `exponent_cap` when it names one, and capped at `cap` always. `opts` may carry `:base`,
  the caller's own first rung, and `:rand`, the determinism seam a test injects in place
  of `:rand.uniform/1`.
  """
  @spec delay(ladder() | map(), pos_integer(), keyword()) :: non_neg_integer()
  def delay(ladder, attempt, opts \\ [])
  def delay(ladder, attempt, opts) when is_atom(ladder), do: delay(ladder(ladder), attempt, opts)

  def delay(%{} = row, attempt, opts) do
    plain = rung(row, attempt, opts)

    if row[:jitter], do: min(jitter(plain, opts[:rand]), cap(row)), else: plain
  end

  defp rung(%{rungs: rungs}, attempt, _opts) when is_list(rungs),
    do: Enum.at(rungs, attempt - 1) || List.last(rungs)

  defp rung(row, attempt, opts) do
    exponent = if row[:exponent_cap], do: min(attempt - 1, row.exponent_cap), else: attempt - 1
    factor = row[:factor] || 2

    min(round((opts[:base] || row.base) * Float.pow(factor / 1, exponent / 1)), cap(row))
  end

  defp cap(row), do: row[:cap] || List.last(row.rungs)

  @doc "Uniform in `[value/2, value · 3/2)`, the one jitter rule every ladder spreads by."
  @spec jitter(non_neg_integer(), (pos_integer() -> pos_integer()) | nil) :: non_neg_integer()
  def jitter(value, rand \\ nil) do
    rand = rand || (&:rand.uniform/1)
    div(value, 2) + rand.(max(value, 1))
  end

  @doc "The wait before `attempt` against `target`, announced — for a site that arms a timer."
  @spec arm(ladder(), pos_integer(), term(), keyword()) :: non_neg_integer()
  def arm(ladder, attempt, target, opts \\ []),
    do: announce(ladder, attempt, target, delay(ladder, attempt, opts))

  @doc """
  Announce a wait of `delay` before `attempt` against `target` on the ladder named `ladder`,
  and answer the delay. Every ladder in this VM announces here, the controller's included.
  """
  @spec announce(atom(), pos_integer(), term(), non_neg_integer()) :: non_neg_integer()
  def announce(ladder, attempt, target, delay) do
    :telemetry.execute(@wait_event, %{delay_ms: delay, attempt: attempt}, %{
      ladder: ladder,
      target: target
    })

    delay
  end

  @doc "`arm/4` and the sleep, for a site that retries in place."
  @spec wait(ladder(), pos_integer(), term(), keyword()) :: :ok
  def wait(ladder, attempt, target, opts \\ []),
    do: ladder |> arm(attempt, target, opts) |> Process.sleep()

  @doc "Announce that `target` succeeded on `ladder`; answers the attempt count a reset leaves."
  @spec reset(atom(), term()) :: 0
  def reset(ladder, target) do
    :telemetry.execute(@reset_event, %{attempt: 0}, %{ladder: ladder, target: target})
    0
  end
end
