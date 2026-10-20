defmodule Flezha.SDK.TimerFiring do
  @moduledoc """
  The firing `c:Flezha.SDK.Tasklet.on_timer/3` receives: the timer's `kind` and
  the `scheduled_at` timestamp its registration answered, the pair
  `Flezha.SDK.Ctx.cancel_timer/3` takes.
  """

  @enforce_keys [:kind, :scheduled_at]
  defstruct [:kind, :scheduled_at]

  @type kind :: :processing_time | :event_time
  @type t :: %__MODULE__{kind: kind(), scheduled_at: non_neg_integer()}
end
