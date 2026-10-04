defmodule Agentdesk.Budget do
  @moduledoc """
  Keeps the tokens a user's answers used today, and admits a message while they
  are under the day's budget. One cell per user.

  What reaches a user's cell is a message to admit, or the tokens an answer
  used; what it hands on is a verdict: the message, and the tokens the user had
  spent when it arrived.
  """
  use Flezha.SDK.Tasklet, name: "budget"

  alias Flezha.SDK.Response

  @daily_tokens 40

  def daily_tokens, do: @daily_tokens

  def admitted?(verdict), do: verdict.spent < @daily_tokens

  # The tokens an answer used; a failed turn used none.
  def used(%Response{request: request, result: result}) do
    %{user: Jason.decode!(request)["user"], tokens: tokens(result)}
  end

  defp tokens({:ok, answer}), do: answer["tokens"]
  defp tokens({:error, _error}), do: 0

  # What each user's cell remembers.
  state do
    field(:day, :string, default: "")
    field(:tokens, :integer, default: 0)
  end

  defchange counted(spent, day, tokens) do
    %{spent | day: day, tokens: on(spent, day) + tokens}
  end

  # The tokens an answer used: count them.
  @impl true
  def run(%{tokens: tokens}, spent, ctx), do: {:ok, counted(spent, ctx, today(ctx), tokens)}

  # A message to admit: hand on its verdict.
  def run(message, spent, ctx) do
    emit(ctx, %{message: message, spent: on(spent, today(ctx))})
    {:ok, spent}
  end

  defp today(ctx), do: ctx |> now() |> DateTime.from_unix!(:millisecond) |> Date.to_iso8601()

  defp on(%{day: day, tokens: tokens}, day), do: tokens
  defp on(_spent, _today), do: 0
end
