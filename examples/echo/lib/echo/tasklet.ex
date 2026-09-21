defmodule Echo.Tasklet do
  @moduledoc """
  A processing stage of the operative's graph, run once per message. This one
  prefixes the input and emits the result.

  With no `state` block, nothing is kept between messages and nothing is
  journaled. Where the result goes is not decided here: the operative decides
  that, so the graph can be rewired without touching this module.
  """

  # `name` is the tasklet's stable identity. State is filed under it, so
  # renaming it later declares a different tasklet rather than renaming this
  # one.
  use Flezha.SDK.Tasklet, name: "echo"

  @impl true
  def run(input, state, ctx) do
    emit(ctx, "echo: " <> input)
    {:ok, state}
  end
end
