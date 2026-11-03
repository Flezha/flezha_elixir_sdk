defmodule Agentdesk.MixProject do
  use Mix.Project

  # Standalone on purpose: it depends on the SDK and on Jason alone, the way
  # your own project would. The runner's image copies the release
  # `mix release` builds.
  def project do
    [
      app: :agentdesk,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      # Building it again replaces the last one without asking.
      releases: [agentdesk: [overwrite: true]]
    ]
  end

  def application do
    [mod: {Agentdesk.Application, []}, extra_applications: [:logger]]
  end

  defp deps do
    [{:flezha_sdk, "~> 0.1.0"}, {:jason, "~> 1.4"}]
  end
end
