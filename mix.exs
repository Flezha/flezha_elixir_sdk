defmodule Flezha.SDK.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/flezha/flezha_elixir_sdk"

  def project do
    [
      app: :flezha_sdk,
      version: @version,
      elixir: "~> 1.17",
      description: "The Elixir SDK for Flezha: flow and steps relays run by a Flezha controller",
      source_url: @source_url,
      package: package(),
      elixirc_paths: elixirc_paths(Mix.env()),
      # The starter example's test runs with the suite, against this checkout.
      test_paths: ["test", "examples/starter/test"],
      deps: deps(),
      docs: [main: "Flezha.SDK", source_ref: "v#{@version}"]
    ]
  end

  def application, do: [extra_applications: [:logger]]

  defp elixirc_paths(:test), do: ["lib", "test/support", "examples/starter/lib"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.0"},
      {:rustler_precompiled, "~> 0.8"},
      # A source build of the native library: a host the checksum file names no
      # precompiled artefact for, or `FLEZHA_SDK_BUILD_NIF=1`. Not optional, so
      # a project depending on this package can take that path too.
      {:rustler, "~> 0.37", runtime: false},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end

  # The error catalogue is read at compile time, so the package carries that
  # file of the contract set.
  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url},
      files: ~w(
        lib
        native/flz_core_beam/src
        native/flz_core_beam/Cargo.toml
        contract/crates/flz_transport/errors.json
        checksum-Elixir.Flezha.SDK.Native.exs
        mix.exs
        README.md
        LICENSE
      )
    ]
  end
end
