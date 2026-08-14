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
    * `--topic` (auto), `--host` (127.0.0.1), `--port` (4040)
    * auth: `--user` (admin) `--pass` (admin123), or `--token`, or `--tls`/`--cacert`/`--cert`/`--key`
    * `--json` emit the report as JSON
  """

  use Mix.Task

  @scenarios ~w(produce fetch mixed stream user acl)

  @switches [
    scenario: :string,
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
    host: :string,
    port: :integer,
    user: :string,
    pass: :string,
    token: :string,
    tls: :boolean,
    cacert: :string,
    cert: :string,
    key: :string,
    json: :boolean
  ]

  @impl Mix.Task
  def run(argv) do
    {opts, _rest} = OptionParser.parse!(argv, strict: @switches)
    Malachi.Loadtest.run(Keyword.update(opts, :scenario, :produce, &scenario!/1))
  end

  defp scenario!(s) when s in @scenarios, do: String.to_atom(s)
  defp scenario!(s), do: Mix.raise("unknown --scenario #{inspect(s)} (expected one of: #{Enum.join(@scenarios, ", ")})")
end
