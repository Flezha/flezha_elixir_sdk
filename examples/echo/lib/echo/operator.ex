defmodule Echo.Operator do
  @moduledoc """
  The deployable unit. An operator declares the graph a message travels: the
  entry channel it arrives on, the steps it passes through, and the channel its
  result leaves on.

  The graph is checked as a whole when the runner declares it, so one that
  cannot work is refused at startup.
  """

  # the name `flz operator start` takes
  use Flezha.SDK.Operator, factory_id: "echo"

  @impl true
  def channels(c) do
    c
    # messages arrive on the entry channel "in"
    |> input("in")
    # each one runs through the echo tasklet
    |> into(Echo.Tasklet)
    # the result leaves on "out", for a plugin to pick up
    |> sink("out")
  end
end
