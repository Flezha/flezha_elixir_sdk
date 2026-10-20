defmodule Agentdesk.Watch do
  @moduledoc """
  Answers a request for a session's events with its stream, and any other
  request with 404.
  """
  use Flezha.SDK.Tasklet, name: "watch"

  alias Agentdesk.Request

  @impl true
  def run(request, state, ctx) do
    # keyed by the user: an output of an effectively-once relay needs a key
    emit(
      ctx,
      if Request.listens?(request) do
        %{stream: Request.user(request)}
      else
        %{status: 404, body: "no such route"}
      end,
      key: Request.user(request)
    )

    {:ok, state}
  end
end
