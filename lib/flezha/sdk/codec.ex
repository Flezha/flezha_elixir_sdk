defmodule Flezha.SDK.Codec do
  @moduledoc """
  The single protobuf seam for the Elixir SDK's control channel. Encodes and
  decodes `RunnerCommands.CommandRequest`/`CommandResponse` via the already
  compiled `Flezha.Runner.CommandsProto` Protox modules. No other SDK or
  native-transport module may reference Protox or `RunnerCommands` — the
  packaging-boundary gate (`scripts/check-sdk-boundary.sh`) enforces it, so the
  later hex extraction swaps only this module's internals.
  """

  # The Elixir SDK's allocated language byte (`flz_core::header`'s table of
  # record: 1 Rust, 2 Java, 3 Elixir). Spelled ONCE: it rides both halves of
  # the factory-advertisement seam, and a factory whose two halves disagreed
  # would be placeable on one path and not the other. The SDK persists no
  # Entity state in v1 and so stamps no record headers, but it advertises
  # factory ids the Rust and Java test apps also advertise — the controller's
  # placement guard is what keeps those ids from crossing runtimes.
  @sdk_lang 3

  @doc "The SDK's allocated language byte, carried on every factory it advertises."
  @spec sdk_lang() :: pos_integer()
  def sdk_lang, do: @sdk_lang

  @doc "Decode an inbound control message into a `CommandRequest`."
  @spec decode_command(binary()) :: {:ok, struct()} | {:error, term()}
  def decode_command(bin) when is_binary(bin) do
    RunnerCommands.CommandRequest.decode(bin)
  end

  @doc "Encode a `CommandResponse` reply (protobuf bytes for the control frame)."
  @spec encode_response(String.t(), String.t(), [String.t()], binary()) :: binary()
  def encode_response(reference_id, status, errors \\ [], raw_data \\ <<>>) do
    response = %RunnerCommands.CommandResponse{
      reference_id: reference_id,
      status: status,
      errors: errors,
      raw_data: raw_data
    }

    {:ok, iodata, _size} = Protox.encode(response)
    IO.iodata_to_binary(iodata)
  end

  @doc "Encode an SDK-initiated `CommandRequest` (e.g. `UpdateFactories`)."
  @spec encode_request(String.t(), {atom(), struct()}) :: binary()
  def encode_request(reference_id, command) do
    request = %RunnerCommands.CommandRequest{reference_id: reference_id, command: command}
    {:ok, iodata, _size} = Protox.encode(request)
    IO.iodata_to_binary(iodata)
  end

  @doc "Encode an `AddOperativeResponse` metadata map into its raw_data bytes."
  @spec encode_add_operative_response(map()) :: binary()
  def encode_add_operative_response(metadata) when is_map(metadata) do
    {:ok, iodata, _size} = Protox.encode(%RunnerCommands.AddOperativeResponse{metadata: metadata})
    IO.iodata_to_binary(iodata)
  end

  @doc "Encode a `PingResponse` carrying the nonce."
  @spec encode_ping_response(String.t()) :: binary()
  def encode_ping_response(nonce) do
    {:ok, iodata, _size} = Protox.encode(%RunnerCommands.PingResponse{nonce: nonce})
    IO.iodata_to_binary(iodata)
  end

  @doc "Build the `UpdateFactories` command tuple from a list of `{id, description}`."
  @spec update_factories_command([{String.t(), String.t() | nil}]) :: {atom(), struct()}
  def update_factories_command(factories) do
    decls =
      Enum.map(factories, fn {id, desc} ->
        %RunnerCommands.FactoryDecl{id: id, description: desc || "", sdk_lang: @sdk_lang}
      end)

    {:update_factories, %RunnerCommands.UpdateFactories{factories: decls}}
  end
end
