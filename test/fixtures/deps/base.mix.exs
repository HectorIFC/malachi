# A trimmed copy of the project's mix.exs, fixed in time, so the tests that compare two mix.exs files do
# not depend on the constraints the project happens to pin today.
defmodule Fixture.MixProject do
  use Mix.Project

  def project do
    [app: :fixture, version: "0.0.1", elixir: "~> 1.19", deps: deps()]
  end

  defp deps do
    [
      {:joken, "~> 2.6.2"},
      {:ra, "~> 3.1"},
      {:dialyxir, "~> 1.4.7", only: [:dev, :test], runtime: false}
    ]
  end
end
