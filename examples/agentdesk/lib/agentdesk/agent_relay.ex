defmodule Agentdesk.AgentRelay do
  @moduledoc """
  One conversation: a steps relay that runs one turn per message it is
  sent, for as long as the session lasts. Every session is an instance of its
  own, with its own state.
  """
  use Flezha.SDK.Relay,
    factory_id: "agent",
    # one turn per message; a failed turn fails alone
    run: {:steps, each: :per_input, on_failure: :keep_progress},
    # an idle session sleeps after 10 s
    durable: {:local, sleep_after: 10_000}

  alias Agentdesk.Reply

  @impl true
  def channels(c) do
    c
    # entry 1: `flz relay call`
    |> input("turn", codec: :text)
    |> into(Reply)
    # to whoever called, and whoever listens
    |> output("answer")

    # entry 2: the gateway, into the same Reply
    c
    |> input("message")
    |> map(& &1["text"])
    |> into(Reply)
  end
end
