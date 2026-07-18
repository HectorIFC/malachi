defmodule Malachi.MixProject do
  use Mix.Project

  @version "0.5.1"
  @source_url "https://github.com/HectorIFC/malachi"

  def project do
    [
      app: :malachi,
      version: @version,
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      releases: releases(),
      description: description(),
      package: package(),
      source_url: @source_url,
      test_coverage: [tool: ExCoveralls, threshold: 85],
      elixirc_paths: elixirc_paths(Mix.env()),
      # `:mix` is a build-time app, so it is not in the default PLT; the `Mix.Tasks.*` admin task references
      # Mix.Task/Mix.shell/Mix.raise, which dialyzer would otherwise flag as unknown functions.
      dialyzer: [plt_add_apps: [:mix]]
    ]
  end

  # Specify which paths to compile per environment
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def cli do
    [
      preferred_envs: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.json": :test,
        "deps.audit": :test,
        sobelow: :test
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger, :runtime_tools, :crypto, :ssl],
      mod: {Malachi.Application, []}
    ]
  end

  defp deps do
    [
      # Runtime dependencies - PINNED to patch-level
      {:jason, "~> 1.4.4"},
      {:argon2_elixir, "~> 4.1.3"},
      # JWT/JWS validation for the OIDC auth provider (P4). Built on erlang-jose; handles the algorithm
      # pitfalls (alg:none, RS256/HS256 confusion) that hand-rolled JWT verification gets wrong.
      {:joken, "~> 2.6.2"},
      {:inet_cidr, "~> 1.0.9"},
      # Observability: emit telemetry events on the hot paths (produce/consume/auth/replication).
      {:telemetry, "~> 1.3"},
      # OpenTelemetry: trace client operations (produce/fetch). Exporter is off by default (traces_exporter
      # :none) — set it to :otlp with an endpoint to ship spans to a collector.
      {:opentelemetry_api, "~> 1.4"},
      {:opentelemetry, "~> 1.5"},
      # Raft (RabbitMQ's) — replicates the Metadata state machine (DS-RSM vnodes)
      {:ra, "~> 2.16"},
      # Automatic node discovery + connection (Erlang distribution) for a multi-node deploy; opt-in via
      # MALACHIMQ_CLUSTER_STRATEGY (gossip/kubernetes/epmd). Absent => single-node, no distribution.
      {:libcluster, "~> 3.5"},

      # Development and test dependencies - PINNED to patch-level
      # 1.7.19 fixes the Credo.Code.Token sigil-token crash under Elixir 1.20 (1.7.15 crashed on ~r//).
      {:credo, "~> 1.7.19", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4.7", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.39.3", only: :dev, runtime: false},
      {:excoveralls, "~> 0.18.5", only: :test},
      {:mix_audit, "~> 2.1.5", only: [:dev, :test], runtime: false},
      {:sobelow, "~> 0.13.0", only: [:dev, :test], runtime: false},
      {:benchee, "~> 1.5.0", only: :dev, runtime: false},
      {:stream_data, "~> 1.1", only: [:dev, :test], runtime: false}
    ]
  end

  defp releases do
    [
      malachi: [
        include_executables_for: [:unix],
        applications: [runtime_tools: :permanent],
        steps: [:assemble, :tar]
      ]
    ]
  end

  defp description do
    "A High-performance message system"
  end

  defp package do
    [
      name: "malachi",
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    ]
  end
end
