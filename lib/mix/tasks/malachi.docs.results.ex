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

  alias Malachi.Loadtest.Ceiling

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

  # Both directories are overridable, and only so the tests can point at a scratch pair. The task is
  # run without arguments everywhere else (the `docs` alias, CI, by hand), which is why the defaults
  # are the real paths rather than something a caller has to supply.
  @switches [published_dir: :string, output_dir: :string]

  @impl Mix.Task
  def run(argv) do
    {opts, _rest} = OptionParser.parse!(argv, strict: @switches)
    published_dir = Keyword.get(opts, :published_dir, @published_dir)
    output_dir = Keyword.get(opts, :output_dir, @output_dir)

    File.mkdir_p!(output_dir)

    for page <- @pages do
      page
      |> read_result(published_dir)
      |> render(page)
      |> write(page, output_dir)
    end

    :ok
  end

  defp write(body, page, output_dir) do
    path = Path.join(output_dir, page.output)
    File.write!(path, body)
    Mix.shell().info("wrote #{path}")
  end

  # `:missing` carries the path it looked at rather than letting the page rebuild one from the module
  # attribute: with `--published-dir` those two disagree, and the page would tell a reader to go look
  # at a file the task never opened.
  defp read_result(page, published_dir) do
    path = Path.join(published_dir, page.source)

    case File.read(path) do
      {:ok, body} -> decode(path, body)
      {:error, :enoent} -> {:missing, path}
      {:error, reason} -> Mix.raise("cannot read #{path}: #{inspect(reason)}")
    end
  end

  defp decode(path, body) do
    case Jason.decode(body) do
      {:ok, result} -> {:ok, result}
      {:error, error} -> Mix.raise("#{path} is not valid JSON: #{Exception.message(error)}")
    end
  end

  defp render({:missing, path}, page), do: render_missing(page, path)
  defp render({:ok, result}, %{kind: :loadtest} = page), do: render_loadtest(page, result)
  defp render({:ok, result}, %{kind: :chaos} = page), do: render_chaos(page, result)

  defp render_missing(page, path) do
    {how_to_path, how_to_title} = page.how_to

    page_body([
      "# #{page.title}",
      "No run has been recorded yet.",
      "This page renders `#{path}`, written from #{page.generator}. " <>
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
      curve_section(result),
      "## Latency",
      table(latency_rows(result)),
      backpressure_section(result),
      "## Reproduce",
      table(meta_rows(result["meta"]) ++ sweep_rows(result["sweep"]))
    ])
  end

  # The regime sits INSIDE the sentence, not beside it: the headline is what gets quoted, and a
  # throughput quoted without its batch size gets applied to flush sizes it never described (#145).
  defp loadtest_headline(%{"records_per_s" => rate} = result) when is_number(rate) do
    "**#{number(rate)} records per second** at saturation#{regime_clause(result)} over " <>
      "#{result["duration_s"]}s with #{result["errors"]} errors, scenario `#{result["scenario"]}`, " <>
      "peaking at #{result["connections"]} connections#{cpu_phrase(result)}." <>
      lower_bound_note(result)
  end

  # A sweep whose headline batch size had no clean rung still writes its result, which is how a local
  # run reaches this page; CI never publishes one. Saying so beats a headline with a blank number.
  defp loadtest_headline(result) do
    regime =
      case result["regime_label"] do
        label when is_binary(label) -> ", #{label}"
        _unrecorded -> ""
      end

    "**No clean peak was measured at the headline batch size**#{regime}. " <>
      "The batch-size table below shows what each batch size recorded."
  end

  # Absent on results recorded before the sweep carried its regime, and then left out rather than parsed
  # from meta.command, whose syntax differs between the two generators.
  defp regime_clause(%{"regime_label" => label}) when is_binary(label), do: ", #{label},"
  defp regime_clause(_result), do: ""

  # A peak is a lower bound on the ceiling when the sweep says why: it sat at the top of its connection
  # ladder, or its generator core was saturated. Results from before the reasons were recorded carry only
  # the ladder-limit flag, which still earns the note.
  defp lower_bound_note(%{"lower_bound_reasons" => [_ | _] = reasons}),
    do:
      " This is a lower bound: " <>
        Enum.map_join(reasons, " and ", &reason_text/1) <> ", so the true ceiling may be higher."

  defp lower_bound_note(%{"lower_bound_reasons" => _none}), do: ""

  defp lower_bound_note(%{"peak_at_ladder_limit" => true}),
    do: lower_bound_note(%{"lower_bound_reasons" => ["ladder_limit"]})

  defp lower_bound_note(_result), do: ""

  defp reason_text("ladder_limit"), do: "the connection sweep peaked at its top rung"
  defp reason_text("generator_saturated"), do: "the single generator core was saturated"
  defp reason_text(other), do: to_string(other)

  # --- the curve over batch sizes ---

  @curve_headers ["Batch", "Values per request", "Peak records/s", "At connections", "Status", "Lower bound because"]

  defp curve_section(%{"curve" => [_ | _] = curve} = result) do
    "## Throughput by batch size\n\n" <>
      noise_paragraph(result["sweep"]) <>
      table(Enum.map(curve, &curve_row/1), @curve_headers)
  end

  defp curve_section(_result), do: ""

  # Every cell is rendered in words rather than left nil: a batch size with no peak is a finding, and a
  # dropped row would hide it.
  defp curve_row(item) do
    peak = item["peak"] || %{}

    {
      number(item["batch"]),
      bytes(item["bytes_per_request"]),
      number(peak["records_per_s"]) || "none",
      number(peak["connections"]) || "none",
      status_text(item),
      reasons_cell(item)
    }
  end

  defp bytes(value) when is_integer(value) and value >= 0, do: Ceiling.format_bytes(value)
  defp bytes(_value), do: "not recorded"

  defp status_text(%{"status" => "peak"}), do: "peak"

  defp status_text(%{"status" => "no_clean_rung", "rungs" => rungs}) do
    errorful =
      for %{"status" => "errorful"} = rung <- rungs || [] do
        "#{rung["connections"]} connections, #{rung["errors"] || "unrecorded"} errors"
      end

    "no clean rung (#{Enum.join(errorful, "; ")})"
  end

  defp status_text(%{"status" => "no_completed_rung"}), do: "no rung completed"
  defp status_text(item), do: to_string(item["status"] || "not recorded")

  defp reasons_cell(%{"peak" => nil}), do: "n/a"
  defp reasons_cell(%{"lower_bound_reasons" => [_ | _] = reasons}), do: Enum.map_join(reasons, "; ", &reason_text/1)
  defp reasons_cell(_item), do: "no"

  # How far to trust a difference between rows, stated next to the rows.
  defp noise_paragraph(%{} = sweep) do
    repetitions = sweep["repetitions"]

    "Each batch size ran over its own connection ladder, #{repetitions} #{plural(repetitions, "repetition")} " <>
      "per point, with the points interleaved across batch sizes. #{aa_sentence(sweep)} Compare batch sizes " <>
      "within this run rather than across runs: on a shared CI runner the published ceiling has moved by more " <>
      "than 30% between runs of unchanged code.\n\n"
  end

  defp noise_paragraph(_sweep), do: ""

  defp aa_sentence(%{"aa_control" => %{"delta_pct" => delta} = aa}) when is_number(delta) do
    "Repeating the headline peak moved it #{delta}% (#{number(aa["first_records_per_s"])} then " <>
      "#{number(aa["repeat_records_per_s"])} records per second), so a difference between batch sizes smaller " <>
      "than that is noise."
  end

  defp aa_sentence(%{"aa_control_reason" => reason}) when is_binary(reason),
    do: "No A-A repeat of the headline peak was recorded (#{reason}), so this run carries no noise estimate."

  defp aa_sentence(_sweep),
    do: "No A-A repeat of the headline peak was recorded, so this run carries no noise estimate."

  defp sweep_rows(%{} = sweep) do
    [
      {"Batch sizes", join(sweep["batch_ladder"])},
      {"Connection ladders", conns_ladders(sweep)},
      {"Repetitions per point", sweep["repetitions"]},
      {"Record size", bytes(sweep["record_size"])},
      {"Group commit", on_off(sweep["group_commit"])},
      {"Segment preallocation", bytes(sweep["segment_prealloc_bytes"])}
    ]
  end

  defp sweep_rows(_sweep), do: []

  defp conns_ladders(%{"batch_ladder" => [_ | _] = batches, "conns_ladders" => %{} = ladders}) do
    Enum.map_join(batches, "; ", fn batch -> "batch #{batch}: #{join(ladders[to_string(batch)])}" end)
  end

  defp conns_ladders(_sweep), do: nil

  defp join(values) when is_list(values), do: Enum.join(values, " ")
  defp join(_values), do: nil

  defp on_off(true), do: "on"
  defp on_off(false), do: "off"
  defp on_off(_value), do: nil

  # One side's CPU attribution ("2.47 of 3"), or nil when the run did not sample that side, so it is
  # dropped rather than rendered as a measured zero. `side` is "server" or "generator".
  defp cpu_cell(result, side) do
    cores = result["#{side}_cpu_cores"]
    budget = result["#{side}_cpu_budget"]
    if is_number(cores) and is_number(budget), do: "#{cores} of #{budget}"
  end

  # Which side saturated: the generator is pinned to one core and the server to the rest, so a server
  # near its budget found its ceiling, while a generator near its budget capped first and the number is
  # a lower bound. Only the sides a run actually sampled are rendered.
  defp cpu_phrase(result) do
    sides =
      for side <- ["server", "generator"], cell = cpu_cell(result, side), cell != nil do
        "#{side} at #{cell} cores"
      end

    if sides == [], do: "", else: " (" <> Enum.join(sides, ", ") <> ")"
  end

  defp throughput_rows(result) do
    [
      {"Records per second", number(result["records_per_s"])},
      {"Operations per second", number(result["ops_per_s"])},
      {"Data rate", suffix(result["mb_per_s"], " MB/s")},
      {"Records", number(result["records"])},
      # The Node client calls it `operations` and the BEAM one `ops`; same measure, two spellings.
      {"Operations", number(result["ops"] || result["operations"])},
      # The connection count at the sweep's peak: the load that drove this ceiling number.
      {"Peak connections", number(result["connections"])},
      # Absent on runs that did not sample them (skipped by table/1), so they never read as measured zeros.
      {"Server CPU (cores)", cpu_cell(result, "server")},
      {"Generator CPU (cores)", cpu_cell(result, "generator")},
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
      {"Quota-refused produces", result["rate_limited"]},
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

  # A row with a nil cell is dropped rather than rendered blank, so a table never claims to have
  # measured something the run did not record.
  # A header with no rows under it renders as an empty table, which reads as a measurement that came
  # back with nothing rather than a section that had nothing to render. `bullets/1` already answers
  # that case in words; this matches it.
  # Rows are tuples with one element per header; the two-column measure/value table is the default.
  defp table(rows, headers \\ ["Measure", "Value"]) do
    body =
      for row <- rows, cells = Tuple.to_list(row), nil not in cells do
        "| " <> Enum.map_join(cells, " | ", &cell/1) <> " |"
      end

    case body do
      [] ->
        "None recorded."

      body ->
        header = "| " <> Enum.join(headers, " | ") <> " |"
        separator = "|" <> String.duplicate(" --- |", length(headers))
        Enum.join([header, separator | body], "\n")
    end
  end

  # A pipe inside a cell ends the cell. The recorded command and the CPU model both land in one, and
  # both come from outside this module, so a topic named `a|b` or a CPU string with a pipe in it would
  # not merely look wrong: it would shift every later column and change what the numbers appear to
  # measure. Escaped even inside the backticks of `code/1`, which GitHub's table parser splits on
  # regardless.
  defp cell(value), do: value |> to_string() |> String.replace("|", "\\|")

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
