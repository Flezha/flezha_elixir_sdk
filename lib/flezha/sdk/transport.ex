defmodule Flezha.SDK.Transport do
  @moduledoc """
  The client-transport seam: connect + `ConnectSocket` establishment, differing
  by mode, with everything above it shared. `:native` opens `Local.Stream` pairs
  and reaches the in-node controller through `Flezha.Transport.Server`; `:socket`
  dials the binary protocol over UDS/QUIC via the `flz_nif` client entry points.
  The connection parameters (socket base, QUIC address, CA cert) come from the
  same env the Rust client reads.
  """

  alias Flezha.Transport.Local
  alias Flezha.Transport.Native

  @default_socket_base "app_data/sockets"
  @default_quic_address "127.0.0.1:4433"
  @default_ca_cert "app_data/certs/ca.pem"

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

    case Flezha.Transport.Server.connect(payload, controller) do
      {:ok, _runner_id} -> {:ok, sdk, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  def connect(:uds, payload, owner) do
    with {:ok, handle} <- Native.client_connect_uds(control_socket_path(), owner) do
      send_handshake(handle, payload)
      {:ok, handle, nil}
    end
  end

  def connect(:quic, payload, owner) do
    with {:ok, conn} <- Native.client_connect_quic(quic_address(), ca_cert_path()),
         {:ok, handle} <- Native.client_open_stream(conn, 0, 0, owner) do
      send_handshake(handle, payload)
      {:ok, handle, conn}
    end
  end

  @doc """
  Establish `count` primary+secondary data stream pairs. Returns
  `{:ok, primaries, secondaries}` — lists of handles. For native the SDK also
  binds the controller side directly through `bind_data_stream/3`.
  """
  @spec connect_sockets(:native | :uds | :quic, term(), non_neg_integer(), pid(), term()) ::
          {:ok, [term()], [term()]} | {:error, term()}
  def connect_sockets(:native, runner_id, count, _owner, _conn) do
    result =
      Enum.reduce(0..(count - 1), {[], []}, fn _key, {prims, secs} ->
        {sdk_p, ctrl_p} = Local.Stream.open()
        {:ok, :primary, _} = Flezha.Transport.Server.bind_data_stream(runner_id, 1, ctrl_p)

        {sdk_s, ctrl_s} = Local.Stream.open()
        {:ok, :secondary, _} = Flezha.Transport.Server.bind_data_stream(runner_id, 2, ctrl_s)

        {[sdk_p | prims], [sdk_s | secs]}
      end)

    {prims, secs} = result
    {:ok, Enum.reverse(prims), Enum.reverse(secs)}
  end

  def connect_sockets(:uds, runner_id, count, owner, _conn) do
    dial_pairs(count, fn type ->
      Native.client_connect_uds(stream_socket_path(runner_id, type), owner)
    end)
  end

  def connect_sockets(:quic, _runner_id, count, owner, conn) do
    idx = :counters.new(1, [])

    dial_pairs(count, fn type ->
      i = :counters.get(idx, 1)
      :counters.add(idx, 1, 1)
      type_byte = if type == "primary", do: 1, else: 2
      Native.client_open_stream(conn, type_byte, i, owner)
    end)
  end

  # --- helpers ---

  defp dial_pairs(count, dial) do
    Enum.reduce_while(0..(count - 1), {:ok, [], []}, fn _key, {:ok, prims, secs} ->
      with {:ok, p} <- dial.("primary"),
           {:ok, s} <- dial.("secondary") do
        {:cont, {:ok, [p | prims], [s | secs]}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, prims, secs} -> {:ok, Enum.reverse(prims), Enum.reverse(secs)}
      other -> other
    end
  end

  defp send_handshake(handle, payload) do
    json = Jason.encode!(payload)
    Flezha.Transport.Stream.send_frame(handle, "connect_control_channel", json, %{})
  end

  defp socket_base do
    (System.get_env("FLEZHA_SOCKET_FILES_PATH") || @default_socket_base)
    |> String.replace_trailing("/", "")
  end

  defp control_socket_path, do: socket_base() <> "/runner_control_channel.sock"

  defp stream_socket_path(runner_id, type),
    do: socket_base() <> "/#{type}_stream_#{runner_id}.sock"

  defp quic_address, do: System.get_env("FLEZHA_QUIC_ADDRESS") || @default_quic_address
  defp ca_cert_path, do: System.get_env("FLEZHA_CA_CERT_PATH") || @default_ca_cert
end
