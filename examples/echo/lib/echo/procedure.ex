defmodule Echo.Procedure do
  @moduledoc """
  A processing stage of the operative's graph, run once per message. This one
  prefixes the input and emits the result.

  With no `state` block, nothing is kept between messages and nothing is
  journaled. Where the result goes is not decided here: the operative decides
  that, so the graph can be rewired without touching this module.
  """

  # `name` is the procedure's stable identity. State is filed under it, so
  # renaming it later declares a different procedure rather than renaming this
  # one.
  use Flezha.SDK.Procedure, name: "echo"

  @impl true
  def run(input, state, ctx) do
    emit(ctx, "echo: " <> input)
    {:ok, state}
  end
end
