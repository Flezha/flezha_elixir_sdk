defmodule Tradedesk.MixProject do
  use Mix.Project

  # Standalone on purpose: it depends on the SDK and on Jason alone, the way
  # your own project would. Until the SDK is released, its dependency is a path
  # into this checkout. It is a runner node of its own, connecting to a
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
    # TODO: change later to {:flezha_sdk, "~> 0.1.0"} when flezha_sdk is released
    [{:flezha_sdk, path: "../.."}, {:jason, "~> 1.4"}]
  end
end
