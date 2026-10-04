defmodule Echo.Tasklet do
  @moduledoc """
  A processing stage of the operator's graph, run once per message. This one
  prefixes the input and emits the result.

  With no `state` block, nothing is kept between messages and nothing is
  journaled. Where the result goes is not decided here: the operator decides
  that, so the graph can be rewired without touching this module.
  """

  # state is saved under this name, so choose it once
  use Flezha.SDK.Tasklet, name: "echo"

  @impl true
  def run(input, state, ctx) do
    # where it goes is the operator's business
    emit(ctx, "echo: " <> input)
    {:ok, state}
  end
end
