defmodule Agentdesk.Reply do
  @moduledoc """
  Answers a message with the demo model, sending the answer as it grows, and
  keeps the conversation so the next answer knows what came before.
  """
  use Flezha.SDK.Tasklet, name: "reply"

  alias Agentdesk.DemoModel

  # What the session remembers.
  state do
    field(:messages, :list, default: [])
  end

  defchange said(conversation, message) do
    %{conversation | messages: conversation.messages ++ [message]}
  end

  defchange unsaid(conversation) do
    %{conversation | messages: Enum.drop(conversation.messages, -1)}
  end

  @impl true
  def run(message, conversation, ctx) do
    # Each piece streams out as it is made; then the whole answer.
    with {:ok, answer} <- DemoModel.answer(conversation.messages, message, &part(ctx, &1)) do
      emit(ctx, answer)
      {:ok, said(conversation, ctx, message)}
    end
  end

  # The undo, when a turn is taken back: the conversation forgets its message.
  @impl true
  def compensate(_answers, conversation, ctx) do
    IO.puts(~s([undo] forgot "#{List.last(conversation.messages)}"))
    {:ok, unsaid(conversation, ctx)}
  end
end
