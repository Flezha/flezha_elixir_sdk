defmodule Flezha.SDK.Transport do
  @moduledoc """
  The client-transport seam: connect + `ConnectSocket` establishment, differing
  by mode, with everything above it shared. `:native` opens `Local.Stream` pairs
  and reaches the in-node controller through `Flezha.Transport.Server`; `:socket`
  dials the binary protocol over UDS/QUIC via the `flz_nif` client entry points.
  The connection parameters come from `flz_transport` through
  `Native.resolve_connect/1` — the same ladder and the same candidate list a
  Rust, Java or Node runner resolves, asked for rather than re-derived here.
  """

  alias Flezha.SDK.{Hold, Knob}
  alias Flezha.Transport.Local
  alias Flezha.Transport.Native
  alias Flezha.Transport.Server, as: TransportServer
  alias Flezha.Transport.Stream, as: TStream

  @doc """
  Connect the control channel. `owner` receives inbound control frames. Returns
  `{:ok, control_handle, conn}` — `conn` is nil for native/UDS and the QUIC
  connection resource for QUIC (needed to open data streams).
  """
  @spec connect(:native | :uds | :quic, map(), pid()) ::
          {:ok, term(), term()} | {:error, term()}
  def connect(:native, payload, owner) do
    {sdk, controller} = Local.Stream.open()
    :ok = Flezha.Transport.Stream.controlling_process(sdk, owner)
    :ok = Flezha.Transport.Stream.set_active(sdk)

    case TransportServer.connect(payload, controller) do
      {:ok, _runner_id} -> {:ok, sdk, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  def connect(:uds, payload, owner) do
    with {:ok, path} <- control_socket_path(),
         {:ok, handle} <- Native.client_connect_uds(path, owner) do
      send_handshake(handle, payload)
      {:ok, handle, nil}
    end
  end

  def connect(:quic, payload, owner) do
    # Every entry is dialled in turn; the NIF re-resolves the one it is given on
    # this attempt, so a controller that moved behind its name is followed.
    with {:ok, entries} <- quic_addresses(),
         {:ok, conn} <- dial_quic(entries, ca_cert_path()),
         {:ok, handle} <- Native.client_open_stream(conn, 0, 0, owner) do
      send_handshake(handle, payload)
      {:ok, handle, conn}
    end
  end

  @doc """
  Why a connect failed, in one line — the shared probe's words for `:uds`, the
  candidate list for `:quic`. Emitted once, before the SDK gives up or starts
  retrying, so an Elixir runner reports what a Rust one reports.
  """
  @spec diagnosis(:native | :uds | :quic) :: String.t()
  def diagnosis(:native), do: "in-node transport; no socket is involved"
  def diagnosis(mode), do: Native.connect_diagnosis(mode)

  @doc """
  Establish `count` primary+secondary data stream pairs. Returns
  `{:ok, primaries, secondaries}` — lists of handles — or the first refusal,
  with every stream opened before it closed. For native the SDK also binds the
  controller side directly through `bind_data_stream/3`.
  """
  @spec connect_sockets(:native | :uds | :quic, term(), non_neg_integer(), pid(), term()) ::
          {:ok, [term()], [term()]} | {:error, term()}
  def connect_sockets(:native, runner_id, count, _owner, _conn) do
    # `bind_data_stream/3` closes a stream it has no channel for.
    dial_pairs(count, fn type ->
      {sdk, controller} = Local.Stream.open()

      with {:ok, _type, _channel} <-
             TransportServer.bind_data_stream(runner_id, type_byte(type), controller),
           do: {:ok, sdk}
    end)
  end

  def connect_sockets(:uds, runner_id, count, owner, _conn) do
    with {:ok, base} <- socket_base() do
      dial_pairs(count, fn type ->
        with {:ok, path} <- Native.stream_socket_path(base, runner_id, type),
             do: Native.client_connect_uds(path, owner)
      end)
    end
  end

  def connect_sockets(:quic, _runner_id, count, owner, conn) do
    # A single cell holding the next payload index. One cell has no slot
    # layout to declare, so it is not a `Flezha.Process.Counters` table.
    idx = :counters.new(1, [])

    dial_pairs(count, fn type ->
      i = :counters.get(idx, 1)
      :counters.add(idx, 1, 1)
      Native.client_open_stream(conn, type_byte(type), i, owner)
    end)
  end

  # --- helpers ---

  # A refusal is the answer, and nothing dialled before it is left open.
  defp dial_pairs(count, dial) do
    types = for _pair <- 1..count//1, type <- ["primary", "secondary"], do: type

    Enum.reduce_while(types, %{"primary" => [], "secondary" => []}, fn type, opened ->
      case dial.(type) do
        {:ok, handle} ->
          {:cont, Map.update!(opened, type, &[handle | &1])}

        {:error, _} = refused ->
          opened |> Map.values() |> Enum.concat() |> Enum.each(&TStream.close/1)
          {:halt, refused}
      end
    end)
    |> case do
      %{"primary" => prims, "secondary" => secs} -> {:ok, Enum.reverse(prims), Enum.reverse(secs)}
      refused -> refused
    end
  end

  defp type_byte("primary"), do: 1
  defp type_byte("secondary"), do: 2

  defp send_handshake(handle, payload) do
    json = Jason.encode!(payload)

    {verdict, _hold} =
      Hold.write("connect_control_channel", Hold.new(streams: [handle]), {json, %{}})

    verdict
  end

  # Each entry in turn; the accumulator carries the last failure so a list that
  # resolves nowhere reports the address it last dialled.
  defp dial_quic(entries, ca_cert) do
    Enum.reduce_while(entries, {:error, "no QUIC endpoint configured"}, fn entry, _last ->
      case Native.client_connect_quic(entry, ca_cert) do
        {:ok, conn} -> {:halt, {:ok, conn}}
        {:error, reason} -> {:cont, {:error, "#{entry}: #{reason}"}}
      end
    end)
  end

  # The ladder and the candidate list, resolved by `flz_transport` — never
  # re-derived here, so the SDK and a Rust runner in the same environment
  # cannot disagree about where the controller is.
  defp resolve_connect, do: Native.resolve_connect("runner")

  defp socket_base do
    with {:ok, dir, _quic} <- resolve_connect(), do: {:ok, dir}
  end

  defp quic_addresses do
    with {:ok, _dir, entries} <- resolve_connect(), do: {:ok, entries}
  end

  # The name is `flz_transport`'s, reached through the NIF: this SDK is its own
  # core and inherits nothing from `flz_core`, so it asks for the composition
  # rather than repeating the controller's spelling of it.
  defp control_socket_path do
    with {:ok, base} <- socket_base(), do: Native.control_socket_path_in(base)
  end

  defp ca_cert_path, do: Knob.get(:ca_cert_path)
end
