defmodule ElixirRunner.MixProject do
  use Mix.Project

  # A standalone Elixir SDK runner node: depends on the package in the directory
  # above, sharing its resolved dependency set and lockfile. This is the Elixir
  # runtime in the e2e matrix beside every other SDK's test app.
  # `mix run --no-halt -- <ns>`.
  def project do
    [
      app: :elixir_runner,
      version: "0.1.0",
      elixir: "~> 1.17",
      deps_path: Path.expand("../deps", __DIR__),
      lockfile: Path.expand("../mix.lock", __DIR__),
      build_path: Path.expand("_build", __DIR__),
      deps: deps()
    ]
  end

  def application do
    [mod: {ElixirRunner.Application, []}, extra_applications: [:logger]]
  end

  defp deps do
    [{:flezha_sdk, path: Path.expand("..", __DIR__)}]
  end
end
