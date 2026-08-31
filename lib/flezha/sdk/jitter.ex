defmodule Flezha.SDK.Jitter do
  @moduledoc """
  Shared backoff jitter for the SDK's retry ladder, reconnect loop, and heartbeat
  timer, so the jitter shape lives in one place.
  """

  @doc "Uniform in `[value/2, value * 3/2)` — the retry-ladder / reconnect ±50% jitter."
  @spec jitter(non_neg_integer()) :: non_neg_integer()
  def jitter(value), do: div(value, 2) + :rand.uniform(max(value, 1))

  @doc "Uniform in `[value/2, value]` — the heartbeat interval jitter."
  @spec jitter_within(non_neg_integer()) :: non_neg_integer()
  def jitter_within(value), do: div(value, 2) + :rand.uniform(max(div(value, 2), 1))
end
