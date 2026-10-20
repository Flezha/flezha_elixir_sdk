defmodule Flezha.SDK.Response do
  @moduledoc """
  One outcome of a `Flezha.SDK.ChannelBuilder.call/3`, as its Response channel
  delivers it: the `request` the call end wrote (its bytes), the `result`
  (`{:ok, answer}` with the answer read by the call's `decode:`, or
  `{:error, %Flezha.SDK.Error{}}`, a missed deadline being `call.timeout`),
  the callee's instance id as `target`, and `created`, true when this call
  started the callee.
  """

  alias Flezha.SDK.{Error, Native}

  @enforce_keys [:request, :result, :target, :created]
  defstruct [:request, :result, :target, :created]

  @type t :: %__MODULE__{
          request: binary(),
          result: {:ok, term()} | {:error, Error.t()},
          target: String.t(),
          created: boolean()
        }

  @doc """
  The Response a controller's envelope carries, read by the core's decoder.
  `decode` reads the answer's bytes as an `input_with/3` parser does. A
  payload that is no envelope is `{:error, %Flezha.SDK.Error{}}` with the code
  `call.malformed_response`.
  """
  @spec decode(binary(), (binary() -> {:ok, term()} | {:error, term()})) ::
          {:ok, t()} | {:error, term()}
  def decode(payload, decode) when is_binary(payload) and is_function(decode, 1) do
    with {target, created, request, result} <- Native.answered(Native.response_decode(payload)),
         {:ok, result} <- result(result, decode) do
      {:ok, %__MODULE__{request: request, result: result, target: target, created: created}}
    end
  end

  defp result({:ok, value}, decode) do
    with {:ok, answer} <- decode.(value), do: {:ok, {:ok, answer}}
  end

  defp result({:error, document}, _decode), do: {:ok, {:error, Error.from_document(document)}}
end
