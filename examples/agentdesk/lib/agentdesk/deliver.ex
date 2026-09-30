defmodule Agentdesk.Deliver do
  @moduledoc """
  Hands the finished answer to the caller: a channel has one end, and `answer`
  is a sink.
  """
  use Flezha.SDK.Tasklet, name: "deliver"

  @impl true
  def run(answer, state, ctx) do
    emit(ctx, answer)
    {:ok, state}
  end

  # The undo, when a turn is taken back: an answer already given has nothing to take back.
  @impl true
  def compensate(_answers, state, _ctx), do: {:ok, state}
end
