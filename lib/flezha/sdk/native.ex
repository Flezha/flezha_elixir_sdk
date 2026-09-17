defmodule Flezha.SDK.Native do
  @moduledoc """
  The socket client: this package's own NIF over `flz_transport`'s UDS and QUIC
  client, the one `Flezha.SDK.Transport` dials `:uds` and `:quic` through.

  The library is precompiled for the supported triples, downloaded when the
  package compiles and checked against the checksum file the package carries.
  A host that file names no artefact for, or a build with `FLEZHA_SDK_BUILD_NIF=1`,
  compiles it from source instead, which needs a Rust toolchain.

  A node that never dials a socket, a controller running this SDK in its own VM,
  sets `config :flezha_sdk, socket_nif: false`: nothing is downloaded or compiled,
  and every function here raises `:nif_not_loaded`.

  ## Messages sent to a stream's owner

    * `{:flz_data, stream_id, payload_id, message, metadata}` — one frame
    * `{:flz_closed, stream_id, reason}` — the stream ended
  """

  alias Flezha.SDK.Knob

  version = Mix.Project.config()[:version]

  if Application.compile_env(:flezha_sdk, :socket_nif, true) do
    checksums = Path.expand("../../../checksum-#{__MODULE__}.exs", __DIR__)
    @external_resource checksums

    # A checksum file with no row for this host's triple has nothing to download.
    precompiled? =
      with {:ok, "nif-" <> nif_target} <- RustlerPrecompiled.target(),
           [_nif_version, triple] <- String.split(nif_target, "-", parts: 2),
           true <- File.exists?(checksums),
           {%{} = rows, _binding} <- Code.eval_file(checksums) do
        Enum.any?(Map.keys(rows), &String.contains?(&1, "-#{triple}."))
      else
        _ -> false
      end

    use RustlerPrecompiled,
      otp_app: :flezha_sdk,
      crate: "flz_sdk_nif",
      base_url: "https://github.com/flezha/flezha/releases/download/v#{version}",
      version: version,
      nif_versions: ["2.16"],
      targets: ~w(
        x86_64-unknown-linux-gnu
        aarch64-unknown-linux-gnu
        x86_64-apple-darwin
        aarch64-apple-darwin
        x86_64-pc-windows-gnu
      ),
      force_build: System.get_env("FLEZHA_SDK_BUILD_NIF") in ["1", "true"] or not precompiled?
  end

  # --- connecting ---

  @doc """
  Where this runner's controller is: `{:ok, socket_dir, quic_addresses}`.
  `socket_dir` and `quic_addresses` pass this node's own reading of the two
  knobs, `nil` when unset: the BEAM keeps its own environment table, so a value
  set with `System.put_env/2` never reaches `std::env::var` in the NIF.
  """
  def resolve_connect(_socket_dir, _quic_addresses), do: :erlang.nif_error(:not_loaded)

  @doc "`resolve_connect/2` with this node's environment view supplied."
  @spec resolve_connect() :: {:ok, String.t(), [String.t()]} | {:error, String.t()}
  def resolve_connect, do: resolve_connect(socket_dir_env(), quic_env())

  @doc """
  Why this node cannot reach a controller over `transport` (`"uds"` or `"quic"`),
  as one line: for UDS the resolved directory, the candidates walked, which of
  the four situations it is and the fix; for QUIC the candidate list. The same
  text a Rust runner logs and `flz runner check` prints.
  """
  def connect_diagnosis(_transport, _socket_dir, _quic_addresses),
    do: :erlang.nif_error(:not_loaded)

  @doc "`connect_diagnosis/3` with this node's environment view supplied."
  @spec connect_diagnosis(:uds | :quic) :: String.t()
  def connect_diagnosis(transport),
    do: connect_diagnosis(to_string(transport), socket_dir_env(), quic_env())

  defp socket_dir_env, do: Knob.get(:socket_files_path)
  defp quic_env, do: Knob.get(:quic_addresses)

  @doc "A data-socket path in `socket_dir`, composed by `flz_transport`."
  @spec stream_socket_path(String.t(), String.t(), String.t()) ::
          {:ok, String.t()} | {:error, String.t()}
  def stream_socket_path(_socket_dir, _runner_id, _stream_type),
    do: :erlang.nif_error(:not_loaded)

  @doc "The control-socket path in `socket_dir`, composed by `flz_transport`."
  @spec control_socket_path_in(String.t()) :: {:ok, String.t()} | {:error, String.t()}
  def control_socket_path_in(_socket_dir), do: :erlang.nif_error(:not_loaded)

  @doc """
  Connect a UDS client stream to `socket_path`, retrying the connect on a
  deadline (exponential from 10 ms, cap 500 ms, ~3 s total). Frames arrive at
  `owner_pid`. Returns `{:ok, stream_handle} | {:error, reason}`.
  """
  def client_connect_uds(_socket_path, _owner_pid), do: :erlang.nif_error(:not_loaded)

  @doc """
  Open a QUIC client connection to `address` — one `resolve_connect/0` entry,
  re-resolved on this attempt — verifying the server against `ca_cert_path`
  (and presenting the client identity when the cert/key variables point at
  readable files). Returns `{:ok, conn} | {:error, reason}`.
  """
  def client_connect_quic(_address, _ca_cert_path), do: :erlang.nif_error(:not_loaded)

  @doc """
  Open a bidirectional QUIC stream on `conn`. `stream_type` 1/2 writes the 2-byte
  `[type, index]` preamble the server keys the stream on; 0 means the control
  stream and writes no preamble. Frames arrive at `owner_pid`.
  """
  def client_open_stream(_conn, _stream_type, _index, _owner_pid),
    do: :erlang.nif_error(:not_loaded)

  # --- per stream ---

  @doc "Queue one frame; `{:error, :driver_gone}` when the stream's writer has ended."
  def send_frame(_stream, _payload_id, _message, _metadata), do: :erlang.nif_error(:not_loaded)

  @doc "Close the stream."
  def close(_stream), do: :erlang.nif_error(:not_loaded)

  @doc "Deliver the stream's later frames to `pid`."
  def controlling_process(_stream, _pid), do: :erlang.nif_error(:not_loaded)

  @doc "The id this stream's `{:flz_data, ...}` and `{:flz_closed, ...}` carry."
  def stream_id(_stream), do: :erlang.nif_error(:not_loaded)
end
