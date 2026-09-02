defmodule Flezha.SDK.Park do
  @moduledoc """
  Shared construction of the tier-off DLQ `ParkBody` map (the input to
  `Flezha.State.Wire.encode_park_body/1`). Every park path — an exhausted Entity,
  an entry-parse failure, a partition mis-route — routes its body through here so
  the shape stays in one place. Payload bytes are recoverable for binary inputs
  only (Rust parity); other terms park without bytes. This module also owns the
  postcard-encoding seam (`Flezha.State.Wire`) for the `park_payload` frame, and
  `ship/4`, the one hop a park takes towards the runner that owns its ack.

  `ship/4`'s `{:park, …}` is answered by `Flezha.SDK.Operative.handle_cast/2`
  when the destination is an operative, and by
  `Flezha.SDK.RunnerServer.handle_cast/2` when it is the runner.
  """

  alias Flezha.State.Wire

  @spec body(binary(), binary(), binary(), non_neg_integer(), term()) :: map()
  def body(park_id, channel, error, attempts, payload),
    do:
      Map.merge(
        %{park_id: park_id, origin_offset: nil},
        dlq_entry(channel, error, attempts, payload)
      )

  @doc """
  The `DlqEntryBody` an effect record carries: the same four fields the park
  frame carries, without the ack correlation the tier has no use for — a tier
  park rides its own record and is durable when the record is.
  """
  @spec dlq_entry(binary(), binary(), non_neg_integer(), term()) :: map()
  def dlq_entry(channel, error, attempts, payload),
    do: %{
      channel: channel,
      error: error,
      attempts: attempts,
      payload: recoverable_payload(payload)
    }

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
  defp recoverable_payload(value) when is_binary(value), do: value
  defp recoverable_payload(_value), do: nil

  @doc """
  The `park_payload` frame: the Entity identity, a seq and a zero header over
  the postcard body. A park carries no record header, so the 16 bytes are
  zero — the same shape the runner core sends.
  """
  @spec frame(binary(), non_neg_integer(), map()) :: binary()
  def frame(identity, seq, body) do
    Wire.encode_frame(identity, seq, <<0::128>>, Wire.encode_park_body(body))
  end

  @doc """
  Hand a park to `to` — an Entity's operative, or an operative's runner. One hop
  per level; the runner owns the ack loop.
  """
  @spec ship(GenServer.server(), pid(), binary(), map()) :: :ok
  def ship(to, entity_pid, identity, body),
    do: GenServer.cast(to, {:park, entity_pid, identity, body})
end
