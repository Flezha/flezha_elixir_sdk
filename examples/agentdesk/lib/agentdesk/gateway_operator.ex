defmodule Agentdesk.GatewayOperator do
  @moduledoc """
  The HTTP side of the desk: an unbounded operator that carries the HTTP
  plugin, answers every request, and calls the session a message is for.
  """
  use Flezha.SDK.UnboundedOperator, factory_id: "gateway"

  alias Agentdesk.{Admit, Budget, Request, Watch}

  @deadline_ms 60_000

  @impl true
  def channels(c) do
    # each request and answer on disk before it runs
    effectively_once(c)
    http = input_with(c, "http", &Request.decode/1)

    http
    |> filter(&(not Request.sends?(&1)))
    |> into(Watch)
    # a stream, or 404
    |> response()

    verdicts =
      http
      |> filter(&Request.sends?/1)
      |> map(&Request.message/1)
      # one cell per user
      |> key_by(& &1.user)
      |> into(Budget)

    verdicts
    |> into(Admit)
    # 202, or 429 over the budget
    |> response()

    verdicts
    |> filter(&Budget.admitted?/1)
    |> map(& &1.message)
    # the user's session
    |> key_by(& &1.user)
    |> call(
      factory: "agent",
      channel: "message",
      deadline_ms: @deadline_ms,
      decode: &Jason.decode/1
    )
    |> map(&Budget.used/1)
    |> key_by(& &1.user)
    # the tokens the answer used
    |> into(Budget)
  end
end
