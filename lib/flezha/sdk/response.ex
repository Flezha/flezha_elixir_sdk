defmodule Flezha.SDK.Response do
  @moduledoc """
  One outcome of a `Flezha.SDK.Channels.call/3`, as its Response channel
  delivers it: the `request` the call end wrote (its bytes), the `result`
  (`{:ok, answer}` with the answer read by the call's `decode:`, or
  `{:error, %Flezha.SDK.Error{}}`, a missed deadline being `call.timeout`),
  the callee's instance id as `target`, and `created`, true when this call
  started the callee.
  """

  alias Flezha.SDK.Error

  @enforce_keys [:request, :result, :target, :created]
  defstruct [:request, :result, :target, :created]

  @type t :: %__MODULE__{
          request: binary(),
          result: {:ok, term()} | {:error, Error.t()},
          target: String.t(),
          created: boolean()
        }

  @doc """
  The Response a controller's envelope carries: a JSON object whose `request`
  and `value` are padded base64 and whose `error` is an error object. `decode`
  reads the answer's bytes as an `input_with/3` parser does.
  """
  @spec decode(binary(), (binary() -> {:ok, term()} | {:error, term()})) ::
          {:ok, t()} | {:error, term()}
  def decode(payload, decode) when is_binary(payload) and is_function(decode, 1) do
    with {:ok, %{"target" => target, "created" => created, "request" => request} = envelope}
         when is_binary(target) and is_boolean(created) and is_binary(request) <-
           Jason.decode(payload),
         {:ok, request} <- Base.decode64(request),
         {:ok, result} <- result(envelope, decode) do
      {:ok, %__MODULE__{request: request, result: result, target: target, created: created}}
    else
      {:error, reason} -> {:error, reason}
      _malformed -> {:error, :malformed_response_envelope}
    end
  end

  defp result(%{"value" => value}, decode) when is_binary(value) do
    with {:ok, bytes} <- Base.decode64(value),
         {:ok, answer} <- decode.(bytes),
         do: {:ok, {:ok, answer}}
  end

  defp result(%{"error" => error}, _decode) do
    case Error.from_object(error) do
      nil -> :error
      error -> {:ok, {:error, error}}
    end
  end

  defp result(_envelope, _decode), do: :error
end
