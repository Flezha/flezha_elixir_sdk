defmodule Agentdesk.AgentOperator do
  @moduledoc """
  One conversation: a bounded operator that runs one turn per message it is
  sent, for as long as the session lasts. Every session is an instance of its
  own, with its own state.
  """
  use Flezha.SDK.BoundedOperator,
    factory_id: "agent",
    # one turn per message
    loop_mode: :per_input,
    # a failed turn fails alone
    rollback_mode: :keep_progress

  alias Agentdesk.{Deliver, Reply}

  @impl true
  def channels(c) do
    c
    |> input("turn")
    |> into(Reply)
    # to whoever listens
    |> sink("answer")
    |> into(Deliver)
    # to whoever called
    |> response()

    # from the gateway
    c
    |> input_with("message", &Jason.decode/1)
    |> map(& &1["text"])
    |> into(Reply)
  end
end
