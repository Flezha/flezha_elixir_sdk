defmodule Flezha.SDK.Connect do
  @moduledoc """
  How a runner inside a controller's own VM reaches that controller: the
  in-node (`:native`) transport.

  This package names no controller module. A controller implements this
  behaviour and registers it at boot with `register/1`; the SDK's `:native`
  connect calls whatever is registered. A runner in a node of its own has no
  controller beside it, registers nothing, and is refused by name if it asks for
  `:native`.
  """

  alias Flezha.SDK.LocalStream

  @doc """
  Connect a runner's control stream. `payload` is the handshake as a term (no
  JSON, no frame) and `controller_side` the controller's end of the stream the
  SDK opened. Answers the runner id, or the refusal.
  """
  @callback connect(payload :: map(), controller_side :: LocalStream.t()) ::
              {:ok, term()} | {:error, term()}

  @doc """
  Bind one data stream to a channel of `runner_id`'s pool. `stream_type` is the
  wire constant, 1 primary and 2 secondary; a stream the pool has no channel for
  is closed and refused.
  """
  @callback bind_data_stream(
              runner_id :: term(),
              stream_type :: 1 | 2,
              controller_side :: LocalStream.t()
            ) :: {:ok, :primary | :secondary, term()} | {:error, term()}

  @doc "Register `module` as this VM's in-node controller."
  @spec register(module()) :: :ok
  def register(module) when is_atom(module),
    do: Application.put_env(:flezha_sdk, __MODULE__, module)

  @doc "The registered implementation, or the refusal naming what is missing."
  @spec implementation() :: {:ok, module()} | {:error, String.t()}
  def implementation do
    case Application.get_env(:flezha_sdk, __MODULE__) do
      nil ->
        {:error,
         "NativeTransportUnavailable: the :native transport reaches a controller in this " <>
           "VM through #{inspect(__MODULE__)}, and none is registered here; a runner in a " <>
           "node of its own connects with :uds or :quic"}

      module ->
        {:ok, module}
    end
  end
end
