defmodule Nats.MixProject do
  use Mix.Project

  def project do
    [
      app: :nats,
      version: "0.1.0",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {Nats.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:connection, "~> 1.0"},
      {:jason, "~> 1.0"},
      {:gen_stage, "~> 1.0"},
      {:telemetry, "~> 0.4 or ~> 1.0"},
      {:nimble_options, "~> 0.5"},
      {:ex_doc, "~> 0.0", only: :dev},
    ]
  end
end
