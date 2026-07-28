defmodule Malachi.MixProject do
  use Mix.Project

  @version "0.6.0"
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
      docs: docs(),
      test_coverage: [tool: ExCoveralls, threshold: 85],
      elixirc_paths: elixirc_paths(Mix.env()),
      # `:mix` is a build-time app, so it is not in the default PLT; the `Mix.Tasks.*` admin task references
      # Mix.Task/Mix.shell/Mix.raise, which dialyzer would otherwise flag as unknown functions.
      dialyzer: [plt_add_apps: [:mix]]
    ]
  end

  # The published documentation site (ExDoc): API reference plus the repository's guides as extra pages.
  # `mix docs` writes to `doc/` (gitignored); the Pages workflow deploys that. Note `docs/` is *source*
  # (the guides), so the two never collide.
  defp docs do
    [
      main: "introduction",
      logo: "priv/static/logo.svg",
      favicon: "priv/static/logo.svg",
      source_ref: "v#{@version}",
      # HTML only: the site is what gets published, and skipping the epub halves the build (CI runs this).
      formatters: ["html"],
      # Render ```mermaid fenced blocks as diagrams. GitHub renders them natively; ExDoc needs this hook.
      before_closing_body_tag: &before_closing_body_tag/1,
      extras: extras(),
      groups_for_extras: groups_for_extras(),
      groups_for_modules: groups_for_modules()
    ]
  end

  # Loads Mermaid and turns the ```mermaid code blocks (which ExDoc emits as `pre code.mermaid`) into
  # diagrams, picking a light or dark theme from the ExDoc theme in effect at load.
  defp before_closing_body_tag(:html) do
    # Force Mermaid's light theme and place each diagram on a white card, so it reads on the ExDoc site in
    # both light and dark mode. This style only affects the ExDoc site; on GitHub the same fenced block
    # renders with GitHub's own theme-aware Mermaid.
    """
    <script src="https://cdn.jsdelivr.net/npm/mermaid@11.4.1/dist/mermaid.min.js" integrity="sha384-rbtjAdnIQE/aQJGEgXrVUlMibdfTSa4PQju4HDhN3sR2PmaKFzhEafuePsl9H/9I" crossorigin="anonymous"></script>
    <style>.mermaid { background: #ffffff; border-radius: 8px; padding: 16px; margin: 1.5em 0; overflow-x: auto; }</style>
    <script>
      document.addEventListener("DOMContentLoaded", function () {
        mermaid.initialize({ startOnLoad: false, theme: "default", securityLevel: "strict" });
        var blocks = document.querySelectorAll("pre > code.mermaid, pre > code.language-mermaid");
        blocks.forEach(function (code, i) {
          var container = document.createElement("div");
          container.className = "mermaid";
          code.parentElement.replaceWith(container);
          mermaid.render("mermaid-diagram-" + i, code.textContent).then(function (res) {
            container.innerHTML = res.svg;
          }).catch(function () {
            container.textContent = code.textContent;
          });
        });
      });
    </script>
    """
  end

  defp before_closing_body_tag(_), do: ""

  defp extras do
    [
      "docs/guides/introduction.md": [title: "Introduction"],
      "docs/guides/getting-started.md": [title: "Getting started"],
      "docs/guides/log-model.md": [title: "The log model"],
      "docs/guides/produce-and-consume.md": [title: "Produce and consume"],
      "docs/guides/streaming-with-backpressure.md": [title: "Streaming with backpressure"],
      "docs/guides/authentication.md": [title: "Authentication"],
      "docs/guides/per-topic-acls.md": [title: "Per-topic ACLs"],
      "docs/guides/clustering-and-resharding.md": [title: "Clustering and re-sharding"],
      "docs/guides/operations.md": [title: "Operations"],
      "README.md": [title: "Overview"],
      "CHANGELOG.md": [title: "Changelog"],
      "docs/ARCHITECTURE.md": [title: "Architecture"],
      "docs/AUTH_USER_MANAGEMENT.md": [title: "Auth and user management (ADR)"],
      "SECURITY.md": [title: "Security policy"],
      "docs/SECURITY_DEVELOPMENT.md": [title: "Secure development"],
      "docs/RATE_LIMITING.md": [title: "Rate limiting"],
      "docs/DOCKER_README.md": [title: "Running with Docker"],
      "docs/DOCKER_TESTING.md": [title: "Testing with Docker"],
      "docs/MULTI_ARCH_BUILD.md": [title: "Multi-arch builds"],
      "docs/HOOKS.md": [title: "Git hooks"]
    ]
  end

  defp groups_for_extras do
    [
      Guides: [
        "docs/guides/introduction.md",
        "docs/guides/getting-started.md",
        "docs/guides/log-model.md",
        "docs/guides/produce-and-consume.md",
        "docs/guides/streaming-with-backpressure.md",
        "docs/guides/authentication.md",
        "docs/guides/per-topic-acls.md",
        "docs/guides/clustering-and-resharding.md",
        "docs/guides/operations.md",
        "README.md"
      ],
      Architecture: ["docs/ARCHITECTURE.md", "docs/AUTH_USER_MANAGEMENT.md"],
      Security: ["SECURITY.md", "docs/SECURITY_DEVELOPMENT.md"],
      Operations: [
        "docs/RATE_LIMITING.md",
        "docs/DOCKER_README.md",
        "docs/DOCKER_TESTING.md",
        "docs/MULTI_ARCH_BUILD.md"
      ],
      Development: ["docs/HOOKS.md", "CHANGELOG.md"]
    ]
  end

  # ~90 modules, so group them by concern rather than listing one flat sidebar.
  defp groups_for_modules do
    [
      "Log and storage": [
        Malachi.Log,
        Malachi.LogApi,
        Malachi.Broker,
        Malachi.BrokerServer,
        Malachi.Keyspace,
        Malachi.Metadata,
        ~r/^Malachi\.Log\./,
        ~r/^Malachi\.Storage\./
      ],
      "Cluster and Raft": [~r/^Malachi\.Cluster\./],
      "Consumer groups": [~r/^Malachi\.Consumer\./],
      "Auth and security": [Malachi.Auth, Malachi.AuditLog, Malachi.TLSValidator, ~r/^Malachi\.Auth\./],
      "Wire protocol and networking": [
        Malachi.Wire,
        Malachi.TCPProtocol,
        Malachi.TCPAcceptor,
        Malachi.TCPAcceptorPool,
        Malachi.SocketHelper,
        Malachi.ConnectionRegistry,
        Malachi.ConnectionLimiter,
        Malachi.RateLimiter
      ],
      Observability: [
        Malachi.Metrics,
        Malachi.Telemetry,
        Malachi.Dashboard,
        ~r/^Malachi\.(Metrics|Telemetry|Dashboard)\./
      ],
      Operations: [
        Malachi.Application,
        Malachi.Config,
        Malachi.Shutdown,
        Malachi.MemoryMonitor,
        Malachi.AtomMonitor,
        Malachi.I18n,
        ~r/^Malachi\.CLI\./
      ],
      "Mix tasks": [~r/^Mix\.Tasks\./]
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
      # :none). Set it to :otlp with an endpoint to ship spans to a collector.
      {:opentelemetry_api, "~> 1.4"},
      {:opentelemetry, "~> 1.5"},
      # Raft (RabbitMQ's): replicates the Metadata state machine (DS-RSM vnodes)
      {:ra, "~> 2.16"},
      # Automatic node discovery + connection (Erlang distribution) for a multi-node deploy; opt-in via
      # MALACHI_CLUSTER_STRATEGY (gossip/kubernetes/epmd). Absent => single-node, no distribution.
      {:libcluster, "~> 3.5"},

      # Development and test dependencies - PINNED to patch-level
      # 1.7.19 fixes the Credo.Code.Token sigil-token crash under Elixir 1.20 (1.7.15 crashed on ~r//).
      {:credo, "~> 1.7.19", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4.7", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.3", only: :dev, runtime: false},
      {:excoveralls, "~> 0.18.5", only: :test},
      {:mix_audit, "~> 2.1.5", only: [:dev, :test], runtime: false},
      {:sobelow, "~> 0.13.0", only: [:dev, :test], runtime: false},
      {:benchee, "~> 1.5.0", only: :dev, runtime: false},
      {:benchee_html, "~> 1.0", only: :dev, runtime: false},
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
