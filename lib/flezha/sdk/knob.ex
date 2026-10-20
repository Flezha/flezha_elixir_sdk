defmodule Flezha.SDK.Knob do
  @moduledoc """
  Every knob this SDK reads, declared once.

  A row is a name, the variable that sets it, the type its text parses as and
  the value it takes when nothing sets it. A runner reads a name from its
  variable first, then from this application's env
  (`config :flezha_sdk, socket_files_path: "/run/flezha"`), then the row's
  default; a row with no variable is set through the application env alone. A
  value that does not parse — a pool size of `"0"`, a flag spelled `"maybe"` —
  reads as the default, which is what every other SDK's reader answers too.
  The contract set's `knob_parse_cases.txt` is that rule written down, and this
  package's suite feeds it to `parse/2`.

  The connection rows are read here and handed to `flz_core` when a runner
  starts, because the BEAM keeps its own copy of the environment. The names,
  variables, types and defaults are the ones Flezha's configuration table
  declares for the Elixir SDK, and the core's suite holds these to it, as it
  holds every other runtime's reader.
  """

  @typedoc "A knob's name."
  @type name :: atom()
  @typedoc "How a knob's text is read."
  @type type :: :ms | :int | :positive_int | :bool | :string | :path | {:enum, [atom()]}

  # {name, variable, type, default}
  @rows [
    {:ca_cert_path, "FLEZHA_CA_CERT_PATH", :path, "app_data/certs/ca.pem"},
    # The lowest level of a core notice this runner logs.
    {:log_level, "FLEZHA_LOG_LEVEL", {:enum, [:debug, :info, :warn, :error]}, :warn},
    {:mailbox_capacity, "FLEZHA_MAILBOX_CAPACITY", :positive_int, 1_024},
    {:namespace_secret, "FLEZHA_NAMESPACE_SECRET", :string, nil},
    {:quic_addresses, "FLEZHA_QUIC_ADDRESSES", :string, "127.0.0.1:4433"},
    {:quic_client_cert, "FLEZHA_QUIC_CLIENT_CERT", :path, "app_data/certs/client.pem"},
    {:quic_client_key, "FLEZHA_QUIC_CLIENT_KEY", :path, "app_data/certs/client.key"},
    # How long a start waits for the controller's answer; `0` waits for ever.
    {:runner_start_wait_ms, "FLEZHA_RUNNER_START_WAIT_MS", :ms, 10_000},
    # A call into one of the runner's own processes: the runner or a local run.
    # None of them runs author code on the answering clause, so a wait here is
    # a wedged process rather than a busy one.
    {:sdk_control_call_ms, nil, :ms, 5_000},
    # How long `Flezha.SDK.Spawn.await/2` waits for the controller's answer.
    {:sdk_spawn_reply_ms, nil, :ms, 30_000},
    # Unset, the socket directory is the first rung of the ladder every runtime
    # walks, which `flz_transport` resolves.
    {:socket_files_path, "FLEZHA_SOCKET_FILES_PATH", :path, nil}
  ]

  @by_name Map.new(@rows, fn {name, env, type, default} ->
             {name, %{name: name, env: env, type: type, default: default}}
           end)

  @doc "Every row, keyed by name: `%{name, env, type, default}`."
  @spec rows() :: %{name() => map()}
  def rows, do: @by_name

  @doc "What this runner reads for `name`: its variable, its application env, else the default."
  @spec get(name()) :: term()
  def get(name) do
    row = Map.fetch!(@by_name, name)

    with var when is_binary(var) <- row.env,
         raw when is_binary(raw) <- System.get_env(var),
         {:ok, value} <- parse(row.type, raw) do
      value
    else
      _ -> configured(row)
    end
  end

  # The application env is held to the row's type as the variable is: a value
  # that is not one reads as the default.
  defp configured(row) do
    value = Application.get_env(:flezha_sdk, row.name, row.default)

    case if(is_binary(value), do: parse(row.type, value), else: {:ok, value}) do
      {:ok, parsed} -> if fits?(row.type, parsed), do: parsed, else: row.default
      :error -> row.default
    end
  end

  defp fits?(_type, nil), do: true
  defp fits?(:positive_int, v), do: is_integer(v) and v >= 1
  defp fits?(:ms, v), do: is_integer(v) and v >= 0
  defp fits?(:int, v), do: is_integer(v)
  defp fits?(:bool, v), do: is_boolean(v)
  defp fits?(text, v) when text in [:string, :path], do: is_binary(v)
  defp fits?({:enum, values}, v), do: v in values

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

  defp parse_trimmed({:enum, values}, raw) do
    case Enum.find(values, &(Atom.to_string(&1) == String.downcase(raw))) do
      nil -> :error
      value -> {:ok, value}
    end
  end

  defp whole(raw, floor) do
    case Integer.parse(raw) do
      {n, ""} when floor == nil -> {:ok, n}
      {n, ""} when n >= floor -> {:ok, n}
      _ -> :error
    end
  end
end
