defmodule Agentdesk.DemoModel do
  @moduledoc """
  The demo model: a canned answer, one token at a time, and the number of
  tokens it used. It stands in for a real model so the example runs anywhere; a
  real model's client would be called here instead, handing each token it
  streams to `part` the same way.
  """

  defmodule Answer do
    @moduledoc "An answer, or a part of one: its text and the tokens used."
    # Written as JSON with its text first.
    @derive {Jason.Encoder, only: [:text, :tokens]}
    defstruct [:text, :tokens]
  end

  @max_message 200
  @per_token_ms 50

  def answer(earlier, message, part) do
    characters = String.length(message)

    if characters > @max_message do
      {:error,
       {:non_retryable,
        "the demo model reads at most #{@max_message} characters, and this message has #{characters}"}}
    else
      text = ~s(Message #{length(earlier) + 1}: you said "#{message}". I am a demo model.)
      tokens = Regex.split(~r/(?<= )/, text)

      for {token, i} <- Enum.with_index(tokens, 1) do
        part.(%Answer{text: token, tokens: i})
        Process.sleep(@per_token_ms)
      end

      {:ok, %Answer{text: text, tokens: length(tokens)}}
    end
  end
end
