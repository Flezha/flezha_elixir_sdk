defmodule Flezha.SDK.Native do
  @moduledoc """
  The native library: `flz_core`, the runtime every Flezha SDK runs on, bound
  through `flz_core_ffi`'s veneer. Activations reach a worker as
  `{:flz_activation, thread, activation}` from a thread the library owns;
  nothing that waits for one runs on a BEAM scheduler.

  The library is precompiled for the supported triples, downloaded when the
  package compiles and checked against the checksum file the package carries.
  A host that file names no artefact for, or a build with `FLEZHA_SDK_BUILD_NIF=1`,
  compiles it from source instead, which needs a Rust toolchain.

  A node that never runs a runner, a controller with no runner in its own VM,
  sets `config :flezha_sdk, socket_nif: false`: nothing is downloaded or
  compiled, and every function here raises `:nif_not_loaded`.

  A refusal is `{:error, document}`, the core's JSON error document.
  """

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
      crate: "flz_core_beam",
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

  @typedoc "A core context."
  @type core :: reference()
  @typedoc "A thread the library owns: a poller or the starvation watch."
  @type thread :: reference()
  @type refused :: {:error, String.t()}

  @doc "A call's answer, with a refusal read into a `Flezha.SDK.Error`."
  @spec answered(term()) :: term()
  def answered({:error, document}) when is_binary(document),
    do: {:error, Flezha.SDK.Error.from_document(document)}

  def answered(answer), do: answer

  # --- the context ---

  @doc "The activation schema version the library speaks."
  def schema_version, do: :erlang.nif_error(:not_loaded)
  @doc "A core context for `schema_version`."
  def context_new(_schema_version), do: :erlang.nif_error(:not_loaded)
  @doc "Free a context once nothing polls it."
  def context_free(_core), do: :erlang.nif_error(:not_loaded)
  @doc "The core's verdict on a class declaration: the derived declaration as JSON."
  def check_class(_core, _json), do: :erlang.nif_error(:not_loaded)
  @doc "Declare a class to the core."
  def declare_class(_core, _json), do: :erlang.nif_error(:not_loaded)
  @doc "Connect as `namespace`, with the connection settings as a JSON object."
  def start(_core, _namespace, _settings), do: :erlang.nif_error(:not_loaded)
  @doc "Stop the runner and release every poller."
  def shutdown(_core), do: :erlang.nif_error(:not_loaded)
  @doc "The relay instances bound on this runner."
  def relays(_core), do: :erlang.nif_error(:not_loaded)

  # --- dispatch ---

  @doc "A thread polling for `worker`, which receives `{:flz_activation, activation}`."
  def poller(_core, _worker), do: :erlang.nif_error(:not_loaded)
  @doc "A thread sending `owner` `:flz_starved` whenever the core observes starvation."
  def starvation_watch(_core, _owner), do: :erlang.nif_error(:not_loaded)
  @doc "Stop a thread once it is back from its wait; its worker's activation stays the worker's."
  def stop_thread(_thread), do: :erlang.nif_error(:not_loaded)
  @doc "Stop a thread whose worker is gone, failing the activation that worker never answered."
  def release_thread(_core, _thread), do: :erlang.nif_error(:not_loaded)
  @doc "Answer the activation `thread` handed out, then let it poll again."
  def complete(_core, _thread, _result), do: :erlang.nif_error(:not_loaded)

  # --- calls from a live activation ---

  def spawn_request(_core, _token, _factory, _id, _payload, _if_running, _replay, _offset),
    do: :erlang.nif_error(:not_loaded)

  def spawn_result(_core, _handle, _timeout_ms), do: :erlang.nif_error(:not_loaded)
  def part(_core, _token, _item, _payload), do: :erlang.nif_error(:not_loaded)
  @doc "The pending timers of the Cell `token` runs, as `{kind, at}` pairs."
  def pending_timers(_core, _token), do: :erlang.nif_error(:not_loaded)
  def effect_begin(_core, _token, _item, _name), do: :erlang.nif_error(:not_loaded)
  def effect_end(_core, _token, _item, _payload), do: :erlang.nif_error(:not_loaded)
  def effect_release(_core, _token, _item), do: :erlang.nif_error(:not_loaded)
  def admin_call(_core, _verb, _args), do: :erlang.nif_error(:not_loaded)

  def send_message(_core, _relay, _channel, _message, _msg_id, _key, _local),
    do: :erlang.nif_error(:not_loaded)

  # --- the local run ---

  def local_start(_core, _factory_id, _relay_id), do: :erlang.nif_error(:not_loaded)
  def local_on_sink(_core, _channel), do: :erlang.nif_error(:not_loaded)
  def local_feed(_core, _channel, _payload), do: :erlang.nif_error(:not_loaded)
  def local_next_output(_core, _timeout_ms), do: :erlang.nif_error(:not_loaded)
  def local_run_until_idle(_core, _timeout_ms), do: :erlang.nif_error(:not_loaded)
  def local_stop(_core), do: :erlang.nif_error(:not_loaded)
end
