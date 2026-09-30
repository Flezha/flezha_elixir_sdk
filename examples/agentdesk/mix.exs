defmodule Agentdesk.MixProject do
  use Mix.Project

  # Standalone on purpose: it depends on the SDK and on Jason alone, the way
  # your own project would. Until the SDK is released, its dependency is a path
  # into this checkout. The runner's image copies the release `mix release`
  # builds.
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
    # TODO: change later to {:flezha_sdk, "~> 0.1.0"} when flezha_sdk is released
    [{:flezha_sdk, path: "../.."}, {:jason, "~> 1.4"}]
  end
end
