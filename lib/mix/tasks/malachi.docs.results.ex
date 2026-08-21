defmodule Mix.Tasks.Malachi.Docs.Results do
  @shortdoc "Renders the recorded benchmark and chaos results into documentation pages"

  @moduledoc """
  #{@shortdoc}.

  Reads the JSON that the load generators and the chaos harnesses record under
  `benchmark/published/` and writes one Markdown page per result into `docs/generated/`, which the
  ExDoc build then publishes. The pages are generated rather than maintained: the numbers on the site
  are whatever the last recorded run measured, so they cannot quietly drift from it. Before this, the
  site rendered a single hand-captured sample and had gone eight months and three minor versions
  stale without anyone noticing, which is the failure mode generation removes.

  The `docs` mix alias runs this first, ahead of the strict ExDoc build, because ExDoc requires every
  extra to exist on disk before it starts.

      mix malachi.docs.results

  A source file that is absent still produces its page, saying so and pointing at the guide that
  explains how to record one. A fresh clone, a shallow CI checkout, and a branch before its first run
  all reach this code with no results present, and `mix docs --warnings-as-errors` must survive all
  three. A file that exists but does not parse is a different matter and fails the build: something
  wrote garbage where a result belongs, and publishing around that would hide it.
  """

  use Mix.Task

  @published_dir "benchmark/published"
  @output_dir "docs/generated"

  # One entry per published page. `how_to` names the guide that explains how to produce the result, so
  # a reader who wants a fresher number is one link from the command rather than hunting for it.
  @pages [
    %{
      kind: :loadtest,
      source: "loadtest-node.json",
      output: "loadtest-node-results.md",
      title: "Node.js load test results",
      generator: "`scripts/loadtest.js`, the Node reference client",
      how_to: {"../guides/running-the-node-loadtest.md", "Running the Node.js load test"}
    },
    %{
      kind: :loadtest,
      source: "loadtest-elixir.json",
      output: "loadtest-elixir-results.md",
      title: "Elixir load test results",
      generator: "`mix malachi.loadtest`, the multi-core BEAM generator",
      how_to: {"../guides/running-the-elixir-loadtest.md", "Running the Elixir load test"}
    },
    %{
      kind: :chaos,
      source: "chaos-node.json",
      output: "chaos-results.md",
      title: "Chaos certification results",
      generator: "`scripts/docker-chaos-test.sh`, the node-fault certification drill",
      how_to: {"../guides/running-chaos-drills.md", "Running the chaos drills"}
    }
  ]

  @impl Mix.Task
  def run(_argv) do
    File.mkdir_p!(@output_dir)

    for page <- @pages do
      page
      |> read_result()
      |> render(page)
      |> write(page)
    end

    :ok
  end

  defp write(body, page) do
    path = Path.join(@output_dir, page.output)
    File.write!(path, body)
    Mix.shell().info("wrote #{path}")
  end

  defp read_result(page) do
    path = Path.join(@published_dir, page.source)

    case File.read(path) do
      {:ok, body} -> decode(path, body)
      {:error, :enoent} -> :missing
      {:error, reason} -> Mix.raise("cannot read #{path}: #{inspect(reason)}")
    end
  end

  defp decode(path, body) do
    case Jason.decode(body) do
      {:ok, result} -> {:ok, result}
      {:error, error} -> Mix.raise("#{path} is not valid JSON: #{Exception.message(error)}")
    end
  end

  defp render(:missing, page), do: render_missing(page)
  defp render({:ok, result}, %{kind: :loadtest} = page), do: render_loadtest(page, result)
  defp render({:ok, result}, %{kind: :chaos} = page), do: render_chaos(page, result)

  defp render_missing(page) do
    {how_to_path, how_to_title} = page.how_to

    page_body([
      "# #{page.title}",
      "No run has been recorded yet.",
      "This page renders `#{@published_dir}/#{page.source}`, written from #{page.generator}. " <>
        "A checkout that has never had one recorded shows this instead of numbers from somewhere else.",
      "See [#{how_to_title}](#{how_to_path}) to record one."
    ])
  end

  # --- load test pages ---

  defp render_loadtest(page, result) do
    {how_to_path, how_to_title} = page.how_to

    page_body([
      "# #{page.title}",
      loadtest_headline(result),
      "Measured with #{page.generator}. Every number here comes from the recorded run described " <>
        "under *Reproduce*; none of it is maintained by hand. See " <>
        "[#{how_to_title}](#{how_to_path}) for the other ways to drive it.",
      "## Throughput",
      table(throughput_rows(result)),
      "## Latency",
      table(latency_rows(result)),
      backpressure_section(result),
      "## Reproduce",
      table(meta_rows(result["meta"]))
    ])
  end

  defp loadtest_headline(result) do
    "**#{number(result["records_per_s"])} records per second** over #{result["duration_s"]}s with " <>
      "#{result["errors"]} errors, scenario `#{result["scenario"]}` on #{result["connections"]} connections."
  end

  defp throughput_rows(result) do
    [
      {"Records per second", number(result["records_per_s"])},
      {"Operations per second", number(result["ops_per_s"])},
      {"Data rate", suffix(result["mb_per_s"], " MB/s")},
      {"Records", number(result["records"])},
      # The Node client calls it `operations` and the BEAM one `ops`; same measure, two spellings.
      {"Operations", number(result["ops"] || result["operations"])},
      {"Duration", suffix(result["duration_s"], "s")},
      {"Errors", result["errors"]}
    ]
  end

  # The two generators do not record the same percentiles: the Node client keeps a full histogram
  # (minimum, mean, the whole curve, maximum) while the BEAM one keeps the four that describe a tail.
  # Rendering only what a run actually recorded beats a fixed grid with holes: a missing row reads as
  # missing, an empty one would read as measured and zero.
  @latency_labels [
    {"min", "Minimum"},
    {"mean", "Mean"},
    {"stddev", "Standard deviation"},
    {"p50", "P50"},
    {"p90", "P90"},
    {"p95", "P95"},
    {"p99", "P99"},
    {"p99_9", "P99.9"},
    {"p99_99", "P99.99"},
    {"max", "Maximum"}
  ]

  defp latency_rows(result) do
    latency = result["latency_ms"] || %{}
    for {key, label} <- @latency_labels, do: {label, suffix(latency[key], " ms")}
  end

  # Only the BEAM generator counts these. An empty section would read as "no backpressure occurred"
  # rather than "this tool does not measure it", so it is left out entirely instead.
  defp backpressure_section(result) do
    rows = [
      {"Dropped connections", result["dropped"]},
      {"Server-shed produces", result["overloaded"]},
      {"Reconnects", result["reconnects"]}
    ]

    if Enum.all?(rows, fn {_label, value} -> value == nil end) do
      ""
    else
      "## Backpressure\n\n" <> table(rows)
    end
  end

  # --- chaos page ---

  defp render_chaos(page, result) do
    {how_to_path, how_to_title} = page.how_to

    page_body([
      "# #{page.title}",
      chaos_headline(result),
      "Measured with #{page.generator}. See [#{how_to_title}](#{how_to_path}) for the other drills " <>
        "and what each one certifies.",
      "## Faults injected",
      bullets(result["events"]),
      "## Invariants",
      table(chaos_invariant_rows(result)),
      failures_section(result),
      "## Reproduce",
      table(meta_rows(result["meta"]))
    ])
  end

  defp chaos_headline(%{"verdict" => "passed"} = result) do
    faults = length(result["events"] || [])

    "**#{result["certification"]} passed** at replication factor #{result["replication_factor"]}: " <>
      "every invariant held through #{faults} injected #{plural(faults, "fault")}."
  end

  defp chaos_headline(result) do
    "**#{result["certification"]} FAILED** at replication factor #{result["replication_factor"]}. " <>
      "What broke is listed under *Failures*."
  end

  defp chaos_invariant_rows(result) do
    invariants = result["invariants"] || %{}

    [
      {"Acknowledged writes still readable", number(invariants["acked_writes"])},
      {"Post-chaos produce", suffix(number(invariants["post_chaos_records_per_s"]), " records/s")}
    ]
  end

  defp failures_section(%{"failures" => [_ | _] = failures}), do: "## Failures\n\n" <> bullets(failures)
  defp failures_section(_result), do: ""

  # --- shared rendering ---

  # Blocks joined by a blank line, empties dropped. Assembled from a list rather than from one
  # heredoc because a heredoc gets the spacing wrong exactly when an optional section is absent,
  # which is the case nobody looks at.
  defp page_body(sections) do
    sections
    |> Enum.reject(&(&1 in [nil, ""]))
    |> Enum.join("\n\n")
    |> Kernel.<>("\n")
  end

  defp meta_rows(nil), do: [{"Recorded", "without metadata"}]

  defp meta_rows(meta) do
    hardware = meta["hardware"] || %{}

    [
      {"Command", code(meta["command"])},
      {"Run at", meta["timestamp"]},
      {"Version", meta["malachi_version"]},
      {"Commit", commit(meta)},
      {"CPU", hardware["cpu"]},
      {"Cores", hardware["cores"]},
      {"Schedulers", hardware["schedulers"]},
      {"Memory", gigabytes(hardware["memory_bytes"])},
      {"OS", hardware["os"]}
    ]
  end

  defp commit(meta) do
    case {meta["git_ref"], meta["git_ref_date"]} do
      {ref, _date} when ref in [nil, ""] -> nil
      {ref, date} when date in [nil, ""] -> code(ref)
      {ref, date} -> "#{code(ref)} (#{date})"
    end
  end

  # A row whose value is nil is dropped rather than rendered blank, so a table never claims to have
  # measured something the run did not record.
  defp table(rows) do
    body = for {label, value} <- rows, value != nil, do: "| #{label} | #{value} |"
    Enum.join(["| Measure | Value |", "| --- | --- |" | body], "\n")
  end

  defp bullets(items) when items in [nil, []], do: "None recorded."
  defp bullets(items), do: Enum.map_join(items, "\n", fn item -> "- #{item}" end)

  # Grouped digits: a throughput headline is read at a glance, and 357650 is harder to place there
  # than 357,650.
  defp number(value) when is_integer(value) do
    value
    |> Integer.to_string()
    |> String.graphemes()
    |> Enum.reverse()
    |> Enum.chunk_every(3)
    |> Enum.map_join(",", &Enum.join/1)
    |> String.reverse()
  end

  defp number(value), do: value

  defp suffix(nil, _unit), do: nil
  defp suffix(value, unit), do: "#{value}#{unit}"

  defp code(nil), do: nil
  defp code(value), do: "`#{value}`"

  defp gigabytes(nil), do: nil
  defp gigabytes(bytes), do: "#{Float.round(bytes / 1_073_741_824, 1)} GB"

  defp plural(1, word), do: word
  defp plural(_count, word), do: word <> "s"
end
