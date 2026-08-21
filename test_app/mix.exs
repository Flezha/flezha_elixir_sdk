defmodule ElixirRunner.MixProject do
  use Mix.Project

  # A standalone Elixir SDK runner node: depends on flezha as a library (path
  # dep, sharing the repo's resolved dep set and lockfile) and boots it
  # runner-only (`controller_enabled: false`). This is the third runtime in the
  # e2e matrix beside rust_runner and java_runner. `mix run --no-halt -- <ns>`.
  def project do
    [
      app: :elixir_runner,
      version: "0.1.0",
      elixir: "~> 1.17",
      deps_path: Path.expand("../../../deps", __DIR__),
      lockfile: Path.expand("../../../mix.lock", __DIR__),
      build_path: Path.expand("_build", __DIR__),
      deps: deps()
    ]
  end

  def application do
    [mod: {ElixirRunner.Application, []}, extra_applications: [:logger]]
  end

  defp deps do
    [{:flezha, path: Path.expand("../../..", __DIR__)}]
  end
end
