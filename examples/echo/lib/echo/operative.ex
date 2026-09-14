defmodule Echo.Operative do
  @moduledoc """
  The deployable unit. An operative declares the graph a message travels: the
  entry channel it arrives on, the steps it passes through, and the channel its
  result leaves on.

  The graph is checked as a whole when the runner declares it, so one that
  cannot work is refused at startup.
  """
  @behaviour Flezha.SDK.StreamOperative

  # The operative's stable identity, and the name `flz operative start` takes.
  @impl true
  def factory_id, do: "echo"

  @impl true
  def channels(c) do
    import Flezha.SDK.Channels

    c |> input("in") |> into(Echo.Procedure) |> sink("out")
  end
end
