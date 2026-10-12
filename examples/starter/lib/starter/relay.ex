defmodule Starter.Relay do
  @moduledoc """
  A visit arrives on `hit`, goes to the cell of its page, and the count leaves
  on the output `counts`, which also answers a caller. The description is shown
  by `flz relay info` and to AI agents.
  """

  use Flezha.SDK.Relay, factory_id: "starter"

  @hit %{type: "object", properties: %{page: %{type: "string"}}, required: ["page"]}

  @impl true
  def channels(c) do
    c
    |> input("hit", description: "Counts visits per page", schema: @hit)
    # the body arrives as JSON text
    |> map(&Jason.decode!/1)
    # one cell, and one count, per page
    |> key_by(& &1["page"])
    |> into(Starter.Count)
    |> output("counts")
  end
end
