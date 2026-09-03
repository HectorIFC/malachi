defmodule Malachi.MixProject do
  use Mix.Project

  @version "0.8.16"
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
      aliases: aliases(),
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

  defp aliases do
    [
      # `mix docs` renders the recorded results into docs/generated first, then runs the strict build (a
      # broken ref fails the build), then stages the standalone benchmark dashboard at doc/benchmarks, so
      # /benchmarks/ ships with the site and the sidebar link resolves both locally and on GitHub Pages.
      # The generation step has to come FIRST: ExDoc requires every extra to exist on disk before it
      # starts, and those pages are build output rather than tracked files. rm_rf before cp_r keeps the
      # staging idempotent across re-runs (`cp -r` into an existing dir would nest a second copy). Baking
      # `--warnings-as-errors` into the alias makes a plain `mix docs` strict everywhere (local and CI), so
      # neither has to pass the flag and local output matches CI. Overriding `docs` here does not recurse:
      # Mix runs the underlying docs task (forwarding any CLI args to it) and then the copy function (the
      # documented idiom, e.g. `test: ["test", &fun/1]`).
      docs: ["malachi.docs.results", "docs --warnings-as-errors", &copy_benchmarks/1]
    ]
  end

  # Stages the standalone dashboard, then the results it renders beside it. The page fetches them by
  # relative path, so everything it needs lives under `/benchmarks/` and it stays a directory anyone
  # can serve on its own. The JSON is the same set the generated pages read, which is the point: the
  # dashboard used to render a hand-captured sample of its own and drifted three versions behind the
  # project without anyone noticing.
  defp copy_benchmarks(_args) do
    File.rm_rf!("doc/benchmarks")
    File.cp_r!("benchmark/dashboard", "doc/benchmarks")

    for source <- Path.wildcard("benchmark/published/*.json") do
      File.cp!(source, Path.join("doc/benchmarks", Path.basename(source)))
    end
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
      // Monotonic counter for render ids: mermaid derives the SVG's internal element ids from the id passed
      // to render(), so it must be globally unique across every call (a timestamp could repeat within the
      // same millisecond across rapid swup passes); a plain incrementing counter never repeats.
      var mermaidRenderSeq = 0;
      function renderMermaidDiagrams() {
        if (typeof mermaid === "undefined") return;
        mermaid.initialize({ startOnLoad: false, theme: "default", securityLevel: "strict" });
        var blocks = document.querySelectorAll("pre > code.mermaid, pre > code.language-mermaid");
        blocks.forEach(function (code) {
          var container = document.createElement("div");
          container.className = "mermaid";
          code.parentElement.replaceWith(container);
          mermaid.render("mermaid-diagram-" + (mermaidRenderSeq++), code.textContent).then(function (res) {
            container.innerHTML = res.svg;
          }).catch(function () {
            container.textContent = code.textContent;
          });
        });
      }
      // First (full) load renders on DOMContentLoaded. ExDoc navigates with swup (SPA-style), which swaps
      // the page in without a reload, so DOMContentLoaded does not fire on internal navigation (Next Page,
      // sidebar links); swup dispatches a `swup:page:view` DOM event there, so render on that too. The
      // second pass is a no-op once the code blocks have already been turned into `div.mermaid`.
      document.addEventListener("DOMContentLoaded", renderMermaidDiagrams);
      document.addEventListener("swup:page:view", renderMermaidDiagrams);
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
      "docs/ARCHITECTURE.md": [title: "Architecture"],
      "docs/AUTH_USER_MANAGEMENT.md": [title: "Auth and user management (ADR)"],
      "SECURITY.md": [title: "Security policy"],
      "docs/SECURITY_DEVELOPMENT.md": [title: "Secure development"],
      "docs/RATE_LIMITING.md": [title: "Rate limiting"],
      "docs/DOCKER_README.md": [title: "Running with Docker"],
      "docs/DOCKER_TESTING.md": [title: "Testing with Docker"],
      "docs/MULTI_ARCH_BUILD.md": [title: "Multi-arch builds"],
      "docs/HOOKS.md": [title: "Git hooks"],
      "docs/guides/running-the-node-loadtest.md": [title: "Running the Node.js load test"],
      "docs/guides/running-the-elixir-loadtest.md": [title: "Running the Elixir load test"],
      "docs/guides/running-chaos-drills.md": [title: "Running the chaos drills"],
      # Build output, not tracked files: `mix malachi.docs.results` renders these from the JSON under
      # benchmark/published before ExDoc runs (see the `docs` alias). They are listed here like any other
      # extra, which is why that ordering is not optional.
      "docs/generated/loadtest-node-results.md": [title: "Node.js load test results"],
      "docs/generated/loadtest-elixir-results.md": [title: "Elixir load test results"],
      "docs/generated/chaos-results.md": [title: "Chaos certification results"],
      # External link (ExDoc :url extra -> URLNode): the benchmark dashboard is a standalone static page
      # staged at /benchmarks/, not an ExDoc-generated page. The trailing slash and no `.html` matter: ExDoc
      # navigates with swup, which only intercepts relative links ending in `.html`, so `benchmarks/` is a
      # full-page navigation (an in-site `.html` would be hijacked and break, since the page has no swup root).
      "All results, one page": [url: "benchmarks/"],
      # External link to the published container image. An absolute URL renders as a plain sidebar link
      # (no swup interception, since it leaves the site), so an operator reading the docs can reach the
      # image without hunting for it in the README.
      "Docker Hub": [url: "https://hub.docker.com/r/hectorcardoso/malachi"]
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
      # ExDoc groups extras one level deep (`groups_for_extras` is a flat keyword list), so a section with
      # subsections inside it is not expressible: each subsection is its own group instead. Each pairs the
      # page that explains how to run something with the page that shows what the last run measured.
      #
      # The pattern for the dashboard is the URL itself, not a file path: for a URLNode,
      # Config.match_extra compares the group pattern against the node's url (`path == string`).
      # Its own group, and no longer inside the Node.js one. It sat there while it rendered a single
      # Node sample; it now renders all three recorded results, so filing it under one of them would
      # describe it wrongly and hide the other two behind a heading that does not mention them.
      "Benchmark dashboard": ["benchmarks/"],
      "Benchmarks: Node.js": [
        "docs/guides/running-the-node-loadtest.md",
        "docs/generated/loadtest-node-results.md"
      ],
      "Benchmarks: Elixir": [
        "docs/guides/running-the-elixir-loadtest.md",
        "docs/generated/loadtest-elixir-results.md"
      ],
      "Chaos Engineering": [
        "docs/guides/running-chaos-drills.md",
        "docs/generated/chaos-results.md"
      ],
      Development: ["docs/HOOKS.md"]
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
      # JWT/JWS validation for the OIDC auth provider. Built on erlang-jose; handles the algorithm
      # pitfalls (alg:none, RS256/HS256 confusion) that hand-rolled JWT verification gets wrong.
      {:joken, "~> 2.6.2"},
      {:inet_cidr, "~> 1.0.9"},
      # Observability: emit telemetry events on the hot paths (produce/consume/auth/replication).
      {:telemetry, "~> 1.3"},
      # OpenTelemetry: trace client operations (produce/fetch). Still OFF by default: the sampler drops
      # every span, so `Tracer.with_span` on the hot path stays a no-op until MALACHI_TRACING_ENABLED
      # turns it on (see config/runtime.exs). The exporter ships as a dependency rather than as an
      # instruction to add one, because a collector nobody can reach without editing mix.exs and
      # recompiling is a collector nobody reaches: the shipped compose files point Jaeger at it.
      # The exporter is listed BEFORE the SDK on purpose, here and in the release's `applications`.
      # Nothing in the dependency graph orders these two relative to each other, so the start order
      # falls out of this list, and the SDK reads its exporter configuration while it initializes: an
      # SDK that starts first can come up with no exporter registered and drop the spans from that
      # window. It is a boot race, so it does not reproduce on demand, which is the argument for
      # pinning the order rather than for waiting to see whether it bites.
      {:opentelemetry_api, "~> 1.4"},
      {:opentelemetry_exporter, "~> 1.8"},
      {:opentelemetry, "~> 1.5"},
      # Raft (RabbitMQ's): replicates the Metadata state machine (DS-RSM vnodes)
      {:ra, "~> 3.1"},
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
      {:sobelow, "~> 0.15.0", only: [:dev, :test], runtime: false},
      {:benchee, "~> 1.5.0", only: :dev, runtime: false},
      {:benchee_html, "~> 1.0", only: :dev, runtime: false},
      {:stream_data, "~> 1.1", only: [:dev, :test], runtime: false}
    ]
  end

  defp releases do
    [
      malachi: [
        include_executables_for: [:unix],
        # The OpenTelemetry pair is listed explicitly, and exporter first, for the reason spelled out
        # beside them in deps/0: a release boots applications in this order, and an SDK that starts
        # before its exporter can drop the spans from that window.
        applications: [
          opentelemetry_exporter: :permanent,
          opentelemetry: :permanent,
          runtime_tools: :permanent
        ],
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
