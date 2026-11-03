defmodule Tradedesk.MixProject do
  use Mix.Project

  # Standalone on purpose: it depends on the SDK and on Jason alone, the way
  # your own project would. It is a runner node of its own, connecting to a
  # controller over a socket.
  def project do
    [
      app: :tradedesk,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [mod: {Tradedesk.Application, []}, extra_applications: [:logger]]
  end

  defp deps do
    [{:flezha_sdk, "~> 0.1.0"}, {:jason, "~> 1.4"}]
  end
end
