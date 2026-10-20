defmodule Agentdesk.Admit do
  @moduledoc """
  Answers a message 202 when it goes on to the session, and 429 when its user is
  over the budget.
  """
  use Flezha.SDK.Tasklet, name: "admit"

  alias Agentdesk.Budget

  @impl true
  def run(verdict, state, ctx) do
    user = verdict.message.user

    emit(
      ctx,
      if Budget.admitted?(verdict) do
        %{status: 202, body: "sent to #{user}"}
      else
        %{
          status: 429,
          body: "#{user} has used #{verdict.spent} of today's #{Budget.daily_tokens()} tokens"
        }
      end,
      key: user
    )

    {:ok, state}
  end
end
