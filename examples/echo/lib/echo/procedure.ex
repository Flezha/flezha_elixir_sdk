defmodule Echo.Procedure do
  @moduledoc """
  A processing stage of the operative's graph, run once per message. This one
  prefixes the input and emits the result.

  With no `apply_change/2`, nothing is journaled and a restart begins with no
  history. Where the result goes is not decided here: the operative decides
  that, so the graph can be rewired without touching this module.
  """
  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  # The procedure's stable identity. State is filed under it, so renaming it
  # later declares a different procedure rather than renaming this one.
  @impl true
  def name, do: "echo"

  # The state for an entity seen for the first time. Nothing is kept here.
  @impl true
  def init_state(_entity_key), do: nil

  @impl true
  def run(input, state, ctx) do
    Ctx.emit(ctx, "echo: " <> input)
    {:ok, state}
  end
end
