defmodule Flezha.SDK.IdleObserver do
  @moduledoc """
  What an application does about an entry channel that has gone quiet.

  Registered per channel with a threshold through `Flezha.SDK.on_entry_idle/4`;
  the SDK reports **episodes**, not ticks: `idle/1` fires once when the silence
  first exceeds the threshold and not again while it continues, and
  `resumed/1` fires once when a frame finally arrives, re-arming the next
  episode. Silence before the very first frame is never an episode — an
  operative waiting for its subscriptions to take is not an incident.

  Both run in the SDK's idle process. A raise, throw or exit is caught and
  logged; delivery and later episodes are unaffected. A fun of arity one is an
  observer whose `resumed/1` does nothing.
  """

  @doc "The channel has been silent for `gap_ms`, which first exceeded the threshold."
  @callback idle(gap_ms :: non_neg_integer()) :: any()

  @doc """
  A frame arrived, ending an episode that had lasted `gap_ms` — measured to the
  newest frame the tick saw, so a burst that resumes and continues within one
  tick reports the silence plus that burst, bounded by the tick.
  """
  @callback resumed(gap_ms :: non_neg_integer()) :: any()

  @optional_callbacks resumed: 1
end
