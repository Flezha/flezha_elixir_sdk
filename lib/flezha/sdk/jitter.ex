defmodule Flezha.SDK.Jitter do
  @moduledoc """
  The heartbeat timer's interval jitter. A retry ladder's jitter is the backoff
  table's own rule, applied where `Flezha.SDK.Knob` waits a ladder; this is a
  cadence, not a retry, and spreads over a different band.
  """

  @doc "Uniform in `[value/2, value]` — the heartbeat interval jitter."
  @spec jitter_within(non_neg_integer()) :: non_neg_integer()
  def jitter_within(value), do: div(value, 2) + :rand.uniform(max(div(value, 2), 1))
end
