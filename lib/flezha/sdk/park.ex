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

  @doc """
  The on-wire identity of the Entity a park belongs to — the triple the
  controller keys its DLQ row by (`operative_class`, `procedure_id`,
  `entity_key`). A pre-dispatch park (entry parse, partition mis-route) has no
  Entity yet and parks under an empty key.
  """
  @spec identity(binary(), binary(), binary() | nil) :: binary()
  def identity(operative_id, procedure_id, entity_key) do
    Wire.encode_entity_id(operative_id, procedure_id, entity_key || "")
  end

  @spec recoverable_payload(term()) :: binary() | nil
  def recoverable_payload(value) when is_binary(value), do: value
  def recoverable_payload(_value), do: nil

  @doc """
  The `park_payload` frame: the Entity identity, a seq and a zero header over
  the postcard body. A park carries no record header, so the 16 bytes are
  zero — the same shape the runner core sends.
  """
  @spec frame(binary(), non_neg_integer(), map()) :: binary()
  def frame(identity, seq, body) do
    Wire.encode_frame(identity, seq, <<0::128>>, Wire.encode_park_body(body))
  end
end
