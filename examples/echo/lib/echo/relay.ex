defmodule Echo.Relay do
  @moduledoc """
  The deployable unit. A relay declares the graph a message travels: the
  entry channel it arrives on, the steps it passes through, and the channel its
  result leaves on.

  The graph is checked as a whole when the runner declares it, so one that
  cannot work is refused at startup.
  """

  use Flezha.SDK.Relay, factory_id: "echo"

  @impl true
  def channels(c) do
    c
    |> input("in", codec: :text)
    |> into(Echo.Tasklet)
    |> output("out")
  end
end
