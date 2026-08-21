defmodule Flezha.SDK.Park do
  @moduledoc """
  Shared construction of the tier-off DLQ `ParkBody` map (the input to
  `Flezha.State.Wire.encode_park_body/1`). Every park path — an exhausted Entity,
  an entry-parse failure, a partition mis-route — routes its body through here so
  the shape stays in one place. Payload bytes are recoverable for binary inputs
  only (Rust parity); other terms park without bytes. This module also owns the
  postcard-encoding seam (`Flezha.State.Wire`) for the `park_payload` frame.
  """

  alias Flezha.State.Wire

  @spec body(binary(), binary(), binary(), non_neg_integer(), term()) :: map()
  def body(park_id, channel, error, attempts, payload) do
    %{
      park_id: park_id,
      origin_offset: nil,
      channel: channel,
      error: error,
      attempts: attempts,
      payload: recoverable_payload(payload)
    }
  end

  @spec recoverable_payload(term()) :: binary() | nil
  def recoverable_payload(value) when is_binary(value), do: value
  def recoverable_payload(_value), do: nil

  @doc "Assemble the `park_payload` frame: a zero 16-byte header + the postcard body."
  @spec frame(map()) :: binary()
  def frame(body), do: <<0::128>> <> Wire.encode_park_body(body)
end
