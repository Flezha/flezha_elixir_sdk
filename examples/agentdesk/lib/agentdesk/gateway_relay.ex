defmodule Agentdesk.GatewayRelay do
  @moduledoc """
  The HTTP side of the desk: a flow relay that carries the HTTP
  plugin, answers every request, and calls the session a message is for.
  """
  use Flezha.SDK.Relay,
    factory_id: "gateway",
    # each request and answer on disk before it runs
    run: {:flow, exactly: :effectively_once}

  alias Agentdesk.{Admit, Budget, Request, Watch}

  @deadline_ms 60_000

  @impl true
  def channels(c) do
    # every request arrives on "http"; kept as a handle to split it
    http = input_with(c, "http", &Request.decode/1)

    # route 1: GET .../events gets the session's stream
    http
    |> filter(&(not Request.sends?(&1)))
    |> into(Watch)
    |> keyed()
    # a stream, or 404
    |> output()

    # route 2: POST .../messages is checked against the user's budget
    verdicts =
      http
      |> filter(&Request.sends?/1)
      |> map(&Request.message/1)
      # one cell per user
      |> key_by(& &1.user)
      |> into(Budget)

    # a verdict: the message plus its user's tokens today; used twice
    # use 1: answer the POST
    verdicts
    |> into(Admit)
    |> keyed()
    # 202, or 429 over the budget
    |> output()

    # use 2: pass an admitted message on to the user's session
    verdicts
    |> filter(&Budget.admitted?/1)
    |> map(& &1.message)
    # the user's session
    |> key_by(& &1.user)
    # call the "agent" relay; its answer comes back here
    |> call(
      factory: "agent",
      channel: "message",
      deadline_ms: @deadline_ms,
      decode: &Jason.decode/1
    )
    # the session's answer, as tokens used
    |> map(&Budget.used/1)
    |> key_by(& &1.user)
    # back into the same budget
    |> into(Budget)
  end
end
