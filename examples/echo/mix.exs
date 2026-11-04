defmodule Echo.MixProject do
  use Mix.Project

  # Standalone on purpose: one dependency on a released version, so this
  # builds the way your own project would. It is a runner node of its own,
  # connecting to a controller over a socket, rather than code running inside
  # the controller's node.
  def project do
    [
      app: :echo,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [mod: {Echo.Application, []}, extra_applications: [:logger]]
  end

  defp deps do
    [{:flezha_sdk, "~> 0.1.1"}]
  end
end
