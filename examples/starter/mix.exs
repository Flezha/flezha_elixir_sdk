defmodule Starter.MixProject do
  use Mix.Project

  # A runner node of its own, connecting to a controller over a socket, with
  # the SDK named by its released version, as your own project names it.
  def project do
    [
      app: :starter,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: [{:flezha_sdk, "~> 0.1.0"}],
      # The test runs the relay in this process, so the runner is not started.
      aliases: [test: "test --no-start"]
    ]
  end

  def application do
    [mod: {Starter.Application, []}, extra_applications: [:logger]]
  end
end
