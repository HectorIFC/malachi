defmodule Mix.Tasks.Malachi.Loadtest do
  @shortdoc "Multi-core Elixir load generator for a running Malachi server"

  @moduledoc """
  #{@shortdoc}.

  One BEAM process per connection, so N connections use every scheduler (unlike a single-threaded
  client). Connects to an already-running server over the binary wire protocol and drives load.

      mix malachi.loadtest --scenario produce --connections 512 --batch 10 --duration 10

  ## Options

    * `--scenario` produce | fetch | mixed | stream | user | acl (default produce)
    * `--connections` concurrent connections (128)
    * `--duration` measured seconds (10), `--warmup` seconds excluded (2)
    * `--batch` records per produce (10), `--record-size` bytes (256), `--keys` cardinality (1000)
    * `--pipeline` in-flight requests per connection, produce only (1 = closed-loop)
    * `--max` records per fetch/push (100), `--window` stream credit (100)
    * `--prepopulate` records to seed before fetch/stream/mixed (10000 for those)
    * `--topic` (auto), `--topics` distinct topics to fan out over (1); N > 1 spreads load across
      data-plane shards
    * `--host` (127.0.0.1); accepts a comma-separated list for a cluster (connections round-robin
      across the hosts), `--port` (4040)
    * `--connect-strategy` bounded | stagger | all-at-once (default bounded): how the connections are
      opened, since each one pays a server-side credential verification and opening hundreds at once is
      an auth storm. bounded keeps at most `--connect-concurrency` (32) connects in flight; stagger
      delays connection i by `i * --connect-stagger-ms` (100); all-at-once opens everything together.
      Each pacing knob is only accepted with the strategy that reads it.
    * auth: `--user` (admin) `--pass` (admin123), or `--token`, or `--tls`/`--cacert`/`--cert`/`--key`
    * `--tls` verifies the server's certificate and hostname against `--cacert`, or against the system
      trust store when none is given. `--insecure` skips that verification: development only, since it
      makes the connection encrypted but unauthenticated and therefore open to interception.
    * `--json` emit the report as JSON
  """

  use Mix.Task

  @scenarios ~w(produce fetch mixed stream user acl)

  # CLI spelling => internal atom; the map is the validation (anything else is a clean Mix error).
  @connect_strategies %{"bounded" => :bounded, "stagger" => :stagger, "all-at-once" => :all_at_once}

  @switches [
    scenario: :string,
    connect_strategy: :string,
    connect_concurrency: :integer,
    connect_stagger_ms: :integer,
    connections: :integer,
    duration: :integer,
    warmup: :integer,
    batch: :integer,
    record_size: :integer,
    keys: :integer,
    pipeline: :integer,
    max: :integer,
    window: :integer,
    prepopulate: :integer,
    topic: :string,
    topics: :integer,
    host: :string,
    port: :integer,
    user: :string,
    pass: :string,
    token: :string,
    tls: :boolean,
    insecure: :boolean,
    cacert: :string,
    cert: :string,
    key: :string,
    json: :boolean
  ]

  @impl Mix.Task
  def run(argv) do
    {opts, _rest} = OptionParser.parse!(argv, strict: @switches)

    opts =
      opts
      |> Keyword.update(:scenario, :produce, &scenario!/1)
      |> Keyword.replace_lazy(:connect_strategy, &connect_strategy!/1)

    Malachi.Loadtest.run(opts)
  rescue
    # Both are conditions with a one-line explanation, not bugs: a SetupError is operational (the server
    # would not take the connections) and an ArgumentError is input validation (bad counts, a pacing
    # knob passed with the wrong connect strategy). Exit non-zero with that line alone, parity with the
    # Node generator's clean errors, instead of a crash dump.
    e in Malachi.Loadtest.SetupError -> Mix.raise(Exception.message(e))
    e in ArgumentError -> Mix.raise(Exception.message(e))
  end

  defp scenario!(s) when s in @scenarios, do: String.to_atom(s)
  defp scenario!(s), do: Mix.raise("unknown --scenario #{inspect(s)} (expected one of: #{Enum.join(@scenarios, ", ")})")

  defp connect_strategy!(s) do
    case @connect_strategies do
      %{^s => strategy} ->
        strategy

      _ ->
        Mix.raise(
          "unknown --connect-strategy #{inspect(s)} (expected one of: #{Enum.join(Map.keys(@connect_strategies), ", ")})"
        )
    end
  end
end
