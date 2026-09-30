defmodule Agentdesk.Request do
  @moduledoc """
  One HTTP request, as the plugin hands it to the gateway. The gateway knows two
  routes: `POST /sessions/<user>/messages`, whose body is the message, and
  `GET /sessions/<user>/events`, the session's answers as they are written.
  """

  defstruct [:method, :path, :body]

  @route ~r"^/sessions/([a-z][a-z0-9_-]{0,63})/(messages|events)$"

  # The envelope carries more than the gateway reads.
  def decode(json) do
    case Jason.decode(json) do
      {:ok, %{"method" => method, "path" => path} = envelope} ->
        {:ok, %__MODULE__{method: method, path: path, body: envelope["body"]}}

      _other ->
        {:error, "not a request envelope"}
    end
  end

  def sends?(request), do: is?(request, "POST", "messages")

  def listens?(request), do: is?(request, "GET", "events")

  def user(request), do: route(request, 1)

  # The session is named after the user.
  def message(request), do: %{user: user(request), text: request.body || ""}

  defp is?(request, verb, what), do: request.method == verb and route(request, 2) == what

  # "" when the path is not a route.
  defp route(request, part) do
    case Regex.run(@route, request.path) do
      nil -> ""
      groups -> Enum.at(groups, part)
    end
  end
end
