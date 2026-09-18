defmodule Malachi.Loadtest.Ceiling do
  @moduledoc """
  Plans the ceiling sweep and turns its recorded runs into the published result.

  `scripts/loadtest-ceiling.sh` measures one generator against a freshly booted server at every point of
  two ladders: a batch size (how many records each produce carries) and, per batch size, a connection
  count. Everything that decides what gets PUBLISHED lives here rather than in the script, so it is
  tested: which inputs are valid, the order the points run in, which point is a batch size's peak, what
  a batch size with no usable point reports, and the shape of the JSON every reader consumes.

  ## Why a curve

  The per-flush cost of the commit path is a curve over the flush size, and it changes sign: segment
  preallocation improves the p50 by 69.7% at 2.5KB per flush and worsens it by 16.2% at 1MB, crossing
  near 170KB (`Malachi.Storage.Preallocation`). A ceiling measured at one batch size describes one slice
  of that surface, so the published result carries the ceiling for every batch size in the ladder, and
  every headline names the regime it describes.

  ## Rules

    * A rung (one batch size at one connection count) is `clean` when every repetition completed with
      zero errors, `errorful` when every repetition completed and at least one recorded errors (or did
      not record the count), and `failed` when some repetition produced no result.
    * A rung's `records_per_s` is the median across its completed repetitions; with an even count it is
      the lower of the two middle values, so it is always a value some run actually measured, and that
      run supplies the rung's other fields.
    * A batch size's peak is its clean rung with the most records per second; a tie goes to the fewer
      connections. Its status is `peak`, or `no_clean_rung` when rungs completed but none was clean, or
      `no_completed_rung` when nothing completed.
    * A peak is a lower bound on the ceiling, with the reasons listed, when it sits at the top of its
      connection ladder (`ladder_limit`) or its generator used at least 90% of its CPU budget
      (`generator_saturated`).
    * A rung's server flush latency (`flush_latency_seconds`, the window the harness scraped, see
      `Malachi.Loadtest.FlushWindow`) comes from that same representative run, like its request latency,
      and so does `flush_latency_error` when that run's scrape gave no window.
    * A run that reports a batch size, record size or connection count other than the point that
      launched it is an error, not a data point: the harness and the generator disagree about what was
      measured.
  """

  # Keep in step with the moduledoc above.
  @generator_saturation_threshold 0.9

  @statuses_with_results ~w(clean errorful)

  @typedoc "A validated sweep: the ladders and the regime every point runs in."
  @type sweep :: %{
          batch_ladder: [pos_integer()],
          headline_batch: pos_integer(),
          conns_ladders: %{pos_integer() => [pos_integer()]},
          repetitions: pos_integer(),
          record_size: pos_integer(),
          group_commit: boolean(),
          segment_prealloc_bytes: non_neg_integer()
        }

  @typedoc "One planned repetition: batch size, connection count, repetition number."
  @type point :: {pos_integer(), pos_integer(), pos_integer()}

  @typedoc "A planned repetition with the generator's decoded JSON, or nil when it produced none."
  @type run :: %{batch: pos_integer(), connections: pos_integer(), rep: pos_integer(), result: map() | nil}

  @typedoc "How a summary ended: with a headline peak, or without one (the result is still written)."
  @type outcome :: :ok | :no_headline_peak

  # --- planning ---

  @doc """
  Builds a sweep from the harness's environment, given as strings exactly as the environment holds them.

  `params` has `:batch_ladder` (\"10 100\"), `:conns_ladders` (a list of \"<batch>=<ladder>\" entries),
  `:headline_batch`, `:repetitions`, `:record_size`, `:group_commit` (\"true\" or \"false\") and
  `:segment_prealloc_bytes`. Every problem is returned as a message naming the variable to fix.
  """
  @spec plan(map()) :: {:ok, sweep()} | {:error, String.t()}
  def plan(params) do
    with {:ok, batch_ladder} <- parse_ladder(params[:batch_ladder], "BATCH_LADDER"),
         {:ok, conns_ladders} <- parse_conns_ladders(params[:conns_ladders] || []),
         {:ok, headline_batch} <- parse_integer(params[:headline_batch], "HEADLINE_BATCH"),
         {:ok, repetitions} <- parse_integer(params[:repetitions], "REPS"),
         {:ok, record_size} <- parse_integer(params[:record_size], "RSIZE"),
         {:ok, group_commit} <- parse_boolean(params[:group_commit], "MALACHI_GROUP_COMMIT"),
         {:ok, prealloc} <- parse_integer(params[:segment_prealloc_bytes], "MALACHI_SEGMENT_PREALLOC_BYTES") do
      validate(%{
        batch_ladder: batch_ladder,
        headline_batch: headline_batch,
        conns_ladders: conns_ladders,
        repetitions: repetitions,
        record_size: record_size,
        group_commit: group_commit,
        segment_prealloc_bytes: prealloc
      })
    end
  end

  @doc """
  The order the points run in. Interleaved rather than one batch size after another: the connection
  position is the outer loop, and the batch sizes rotate by one at each position, so a slow stretch on a
  shared runner lands on every batch size instead of reading as the effect of whichever ran during it.
  Repetitions of a rung run back to back.
  """
  @spec run_order(sweep()) :: [point()]
  def run_order(sweep) do
    width = length(sweep.batch_ladder)
    depth = sweep.conns_ladders |> Map.values() |> Enum.map(&length/1) |> Enum.max()

    for position <- 0..(depth - 1),
        batch <- rotate(sweep.batch_ladder, rem(position, width)),
        connections = Enum.at(sweep.conns_ladders[batch], position),
        connections != nil,
        rep <- 1..sweep.repetitions,
        do: {batch, connections, rep}
  end

  defp rotate(list, by), do: Enum.drop(list, by) ++ Enum.take(list, by)

  @doc "The file a sweep repetition's generator JSON is written to, relative to the run directory."
  @spec run_file(pos_integer(), pos_integer(), pos_integer()) :: String.t()
  def run_file(batch, connections, rep), do: "run-b#{batch}-c#{connections}-r#{rep}.json"

  @doc "The file the A-A repeat of the headline peak is written to, relative to the run directory."
  @spec aa_file(pos_integer(), pos_integer()) :: String.t()
  def aa_file(batch, connections), do: "aa-b#{batch}-c#{connections}.json"

  # --- sweep files ---

  @doc "The JSON form of a sweep, as written to `sweep.json`."
  @spec encode_sweep(sweep()) :: map()
  def encode_sweep(sweep) do
    %{
      "batch_ladder" => sweep.batch_ladder,
      "headline_batch" => sweep.headline_batch,
      "conns_ladders" => Map.new(sweep.conns_ladders, fn {batch, ladder} -> {Integer.to_string(batch), ladder} end),
      "repetitions" => sweep.repetitions,
      "record_size" => sweep.record_size,
      "group_commit" => sweep.group_commit,
      "segment_prealloc_bytes" => sweep.segment_prealloc_bytes
    }
  end

  @doc "Reads a sweep back from its JSON form, validating it as `plan/1` does."
  @spec decode_sweep(term()) :: {:ok, sweep()} | {:error, String.t()}
  def decode_sweep(%{} = json) do
    validate(%{
      batch_ladder: json["batch_ladder"],
      headline_batch: json["headline_batch"],
      conns_ladders: decode_conns_ladders(json["conns_ladders"]),
      repetitions: json["repetitions"],
      record_size: json["record_size"],
      group_commit: json["group_commit"],
      segment_prealloc_bytes: json["segment_prealloc_bytes"]
    })
  end

  def decode_sweep(_other), do: {:error, "a sweep must be a JSON object"}

  defp decode_conns_ladders(%{} = ladders) do
    Map.new(ladders, fn {key, ladder} ->
      case Integer.parse(to_string(key)) do
        {batch, ""} -> {batch, ladder}
        _not_a_batch -> {key, ladder}
      end
    end)
  end

  defp decode_conns_ladders(other), do: other

  # --- validation (shared by plan/1 and decode_sweep/1) ---

  defp validate(sweep) do
    checks = [
      fn -> check_ladder(sweep.batch_ladder, "BATCH_LADDER") end,
      fn -> check_conns_ladders(sweep.batch_ladder, sweep.conns_ladders) end,
      fn -> check_headline(sweep.headline_batch, sweep.batch_ladder) end,
      fn -> check_positive(sweep.repetitions, "REPS") end,
      fn -> check_positive(sweep.record_size, "RSIZE") end,
      fn -> check_boolean(sweep.group_commit, "MALACHI_GROUP_COMMIT") end,
      fn -> check_non_negative(sweep.segment_prealloc_bytes, "MALACHI_SEGMENT_PREALLOC_BYTES") end
    ]

    Enum.find_value(checks, {:ok, sweep}, fn check ->
      case check.() do
        :ok -> nil
        {:error, _message} = error -> error
      end
    end)
  end

  defp check_ladder(ladder, name) when is_list(ladder) and ladder != [] do
    cond do
      not Enum.all?(ladder, &(is_integer(&1) and &1 > 0)) ->
        {:error, "#{name} must hold positive integers only, got #{show(ladder)}"}

      Enum.uniq(Enum.sort(ladder)) != ladder ->
        {:error, "#{name} must be strictly ascending without repeats, got #{show(ladder)}"}

      true ->
        :ok
    end
  end

  defp check_ladder(_ladder, name), do: {:error, "#{name} is empty"}

  defp check_conns_ladders(batches, %{} = ladders) do
    given = Map.keys(ladders)

    case {given -- batches, batches -- given} do
      {[_ | _] = outside, _missing} ->
        {:error, "connection ladders were given for batch sizes outside BATCH_LADDER: #{show(outside)}"}

      {[], [_ | _] = missing} ->
        {:error, "no connection ladder for batch size #{show(missing)}"}

      {[], []} ->
        Enum.find_value(batches, :ok, fn batch ->
          case check_ladder(ladders[batch], "CONNS_LADDER_#{batch}") do
            :ok -> nil
            error -> error
          end
        end)
    end
  end

  defp check_conns_ladders(_batches, _ladders),
    do: {:error, "conns_ladders must map every batch size to a connection ladder"}

  defp check_headline(headline, batches) do
    if headline in batches,
      do: :ok,
      else: {:error, "HEADLINE_BATCH #{show_term(headline)} is not in BATCH_LADDER (#{show(batches)})"}
  end

  defp check_positive(value, _name) when is_integer(value) and value > 0, do: :ok
  defp check_positive(value, name), do: {:error, "#{name} must be a positive integer, got #{show_term(value)}"}

  defp check_non_negative(value, _name) when is_integer(value) and value >= 0, do: :ok
  defp check_non_negative(value, name), do: {:error, "#{name} must be a non-negative integer, got #{show_term(value)}"}

  defp check_boolean(value, _name) when is_boolean(value), do: :ok
  defp check_boolean(value, name), do: {:error, "#{name} must be true or false, got #{show_term(value)}"}

  # --- parsing the environment's strings ---

  defp parse_ladder(value, name) when is_binary(value) do
    value
    |> String.split(~r/\s+/, trim: true)
    |> Enum.reduce_while({:ok, []}, fn token, {:ok, acc} ->
      case Integer.parse(token) do
        {integer, ""} -> {:cont, {:ok, [integer | acc]}}
        _not_an_integer -> {:halt, {:error, "#{name} has #{inspect(token)}, which is not an integer"}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      error -> error
    end
  end

  defp parse_ladder(nil, name), do: {:error, "#{name} is not set"}

  defp parse_conns_ladders(entries) do
    Enum.reduce_while(entries, {:ok, %{}}, fn entry, {:ok, acc} ->
      case parse_conns_entry(entry, acc) do
        {:ok, batch, ladder} -> {:cont, {:ok, Map.put(acc, batch, ladder)}}
        error -> {:halt, error}
      end
    end)
  end

  defp parse_conns_entry(entry, seen) do
    with [batch_text, ladder_text] <- String.split(entry, "=", parts: 2),
         {:ok, batch} <- parse_integer(batch_text, "a connection ladder's batch size"),
         false <- Map.has_key?(seen, batch),
         {:ok, ladder} <- parse_ladder(ladder_text, "CONNS_LADDER_#{batch}") do
      {:ok, batch, ladder}
    else
      [_no_separator] -> {:error, "connection ladder #{inspect(entry)} is not <batch>=<ladder>"}
      true -> {:error, "more than one connection ladder for batch size #{hd(String.split(entry, "="))}"}
      {:error, _message} = error -> error
    end
  end

  defp parse_integer(value, name) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {integer, ""} -> {:ok, integer}
      _not_an_integer -> {:error, "#{name} must be an integer, got #{inspect(value)}"}
    end
  end

  defp parse_integer(nil, name), do: {:error, "#{name} is not set"}

  # An integer from a string, then held to the same check `validate/1` applies to a sweep.
  defp parse_checked(value, name, check) do
    with {:ok, integer} <- parse_integer(value, name),
         :ok <- check.(integer, name) do
      {:ok, integer}
    end
  end

  defp parse_boolean("true", _name), do: {:ok, true}
  defp parse_boolean("false", _name), do: {:ok, false}
  defp parse_boolean(nil, name), do: {:error, "#{name} is not set"}
  defp parse_boolean(value, name), do: {:error, "#{name} must be true or false, got #{inspect(value)}"}

  defp show(list) when is_list(list), do: Enum.map_join(list, " ", &show_term/1)

  defp show_term(value) when is_integer(value), do: Integer.to_string(value)
  defp show_term(value), do: inspect(value)

  # --- summarizing ---

  @doc """
  The connection count of the headline batch size's peak, `:none` when that batch size has no clean
  rung, or an error when a run does not describe the point that launched it.
  """
  @spec headline_peak(sweep(), [run()]) :: {:ok, pos_integer()} | :none | {:error, String.t()}
  def headline_peak(sweep, runs) do
    with :ok <- check_runs(sweep, runs) do
      case batch_summary(sweep, sweep.headline_batch, runs) do
        {_item, nil} -> :none
        {_item, peak} -> {:ok, peak.connections}
      end
    end
  end

  @doc """
  The published result: the headline peak's run at the top level, as every reader of the flat format
  expects, with the regime, the sweep and the whole curve beside it.

  `aa_result` is the generator JSON of the A-A repeat of the headline peak, or nil when it was not run
  or produced nothing. Returns `{:no_headline_peak, result}` when the headline batch size has no clean
  rung: the result still carries the curve and says why, but has no flat peak fields.
  """
  @spec summarize(sweep(), [run()], map() | nil) :: {outcome(), map()} | {:error, String.t()}
  def summarize(sweep, runs, aa_result) do
    with :ok <- check_runs(sweep, runs) do
      summaries = Map.new(sweep.batch_ladder, &{&1, batch_summary(sweep, &1, runs)})
      {headline_item, headline_peak} = summaries[sweep.headline_batch]

      with :ok <- check_aa(sweep, headline_peak, aa_result) do
        curve = Enum.map(sweep.batch_ladder, fn batch -> summaries[batch] |> elem(0) end)
        {aa_control, aa_reason} = aa_control(sweep.headline_batch, headline_peak, aa_result)

        shared =
          sweep
          |> regime(sweep.headline_batch)
          |> Map.merge(%{
            "headline_status" => headline_item["status"],
            "sweep" =>
              sweep
              |> encode_sweep()
              |> Map.merge(%{
                "order" => "interleaved",
                "cpu_sampled" => cpu_sampled?(runs),
                "generator_saturation_threshold" => @generator_saturation_threshold,
                "aa_control" => aa_control,
                "aa_control_reason" => aa_reason
              }),
            "curve" => curve
          })

        case headline_peak do
          nil ->
            {:no_headline_peak, shared}

          peak ->
            flat =
              peak.representative
              |> Map.merge(%{
                "peak_at_ladder_limit" => headline_item["peak_at_ladder_limit"],
                "lower_bound_reasons" => headline_item["lower_bound_reasons"]
              })

            {:ok, Map.merge(flat, shared)}
        end
      end
    end
  end

  @doc "One line per batch size, for the harness log: the peak and why it is a lower bound, or why there is none."
  @spec summary_lines(map()) :: [String.t()]
  def summary_lines(%{"curve" => curve}), do: Enum.map(curve, &summary_line/1)

  defp summary_line(%{"status" => "peak", "peak" => peak} = item) do
    reasons =
      case item["lower_bound_reasons"] do
        [] -> ""
        reasons -> ", lower bound (#{Enum.join(reasons, ", ")})"
      end

    "#{item["regime_label"]}: #{peak["records_per_s"]} rec/s at #{peak["connections"]} connections#{reasons}"
  end

  defp summary_line(%{"status" => "no_clean_rung", "rungs" => rungs} = item) do
    errorful =
      for %{"status" => "errorful"} = rung <- rungs do
        "#{rung["connections"]} conns (#{show_errors(rung["errors"])} errors)"
      end

    "#{item["regime_label"]}: no clean rung; #{Enum.join(errorful, ", ")}"
  end

  defp summary_line(item), do: "#{item["regime_label"]}: no rung completed"

  defp show_errors(errors) when is_integer(errors), do: Integer.to_string(errors)
  defp show_errors(_unrecorded), do: "unrecorded"

  @doc """
  The regime a batch size describes, as a sentence fragment every surface prints verbatim:
  `batch 10 x 256B (2.5KB of values per request, group commit off, segment preallocation 64MB)`.
  Formatted once here so the generated pages, the dashboard, the workflow and the benchmark scripts
  cannot disagree about rounding or wording. Both settings in the parentheses move the per-flush cost:
  group commit decides how many produces one sync carries, and preallocation changes sign across the
  flush sizes the ladder spans (`Malachi.Storage.Preallocation`), so a label naming one and not the
  other would still let two numbers from different regimes read alike.
  """
  @spec regime_label(pos_integer(), pos_integer(), boolean(), non_neg_integer()) :: String.t()
  def regime_label(batch, record_size, group_commit, segment_prealloc_bytes)
      when is_integer(batch) and batch > 0 and is_integer(record_size) and record_size > 0 and
             is_boolean(group_commit) and is_integer(segment_prealloc_bytes) and segment_prealloc_bytes >= 0 do
    commit = if group_commit, do: "on", else: "off"

    "batch #{batch} x #{format_bytes(record_size)} (#{format_bytes(batch * record_size)} of values per request, " <>
      "group commit #{commit}, segment preallocation #{preallocation(segment_prealloc_bytes)})"
  end

  defp preallocation(0), do: "off"
  defp preallocation(bytes), do: format_bytes(bytes)

  @doc """
  `regime_label/4` for a harness that holds its regime as strings, as `benchmark/docker-cluster.sh` does.

  `params` has `:batch` and `:record_size` (positive integers), `:group_commit` (\"true\" or \"false\")
  and `:segment_prealloc_bytes` (a non-negative integer, 0 for off), as the command line gives them.
  Every problem is returned as a message naming the flag to fix.
  """
  @spec label(map()) :: {:ok, String.t()} | {:error, String.t()}
  def label(params) do
    with {:ok, batch} <- parse_checked(params[:batch], "--batch", &check_positive/2),
         {:ok, record_size} <- parse_checked(params[:record_size], "--record-size", &check_positive/2),
         {:ok, group_commit} <- parse_boolean(params[:group_commit], "--group-commit"),
         {:ok, prealloc} <-
           parse_checked(params[:segment_prealloc_bytes], "--segment-prealloc-bytes", &check_non_negative/2) do
      {:ok, regime_label(batch, record_size, group_commit, prealloc)}
    end
  end

  @doc """
  A byte count in binary units with at most one decimal, trailing `.0` dropped: 2560 is `2.5KB`, 25600
  is `25KB`, 1048576 is `1MB`. The units #83 measured the preallocation curve in.
  """
  @spec format_bytes(non_neg_integer()) :: String.t()
  def format_bytes(bytes) when bytes < 1024, do: "#{bytes}B"
  def format_bytes(bytes) when bytes < 1024 * 1024, do: scaled(bytes / 1024, "KB")
  def format_bytes(bytes), do: scaled(bytes / (1024 * 1024), "MB")

  defp scaled(value, unit) do
    rounded = Float.round(value, 1)
    text = if rounded == trunc(rounded), do: Integer.to_string(trunc(rounded)), else: Float.to_string(rounded)
    text <> unit
  end

  defp regime(sweep, batch) do
    %{
      "batch" => batch,
      "record_size" => sweep.record_size,
      "bytes_per_request" => batch * sweep.record_size,
      "group_commit" => sweep.group_commit,
      "segment_prealloc_bytes" => sweep.segment_prealloc_bytes,
      "regime_label" => regime_label(batch, sweep.record_size, sweep.group_commit, sweep.segment_prealloc_bytes)
    }
  end

  defp check_runs(sweep, runs) do
    Enum.find_value(runs, :ok, fn
      %{result: nil} ->
        nil

      %{batch: batch, connections: connections, rep: rep, result: result} ->
        describes_point(
          result,
          batch,
          sweep.record_size,
          connections,
          "batch #{batch}, #{connections} connections, repetition #{rep}"
        )
    end)
  end

  defp check_aa(_sweep, _peak, nil), do: :ok
  defp check_aa(_sweep, nil, _aa_result), do: :ok

  defp check_aa(sweep, peak, aa_result) do
    describes_point(aa_result, sweep.headline_batch, sweep.record_size, peak.connections, "the A-A repeat") || :ok
  end

  # nil when the run describes the point, so it slots into Enum.find_value/3.
  defp describes_point(result, batch, record_size, connections, label) do
    reported = {result["batch"], result["record_size"], result["connections"]}

    if reported == {batch, record_size, connections} do
      nil
    else
      {:error,
       "the run for #{label} reports batch #{show_term(elem(reported, 0))}, record_size " <>
         "#{show_term(elem(reported, 1))}, connections #{show_term(elem(reported, 2))}; it does not describe " <>
         "the point that launched it (batch #{batch}, record_size #{record_size}, connections #{connections})"}
    end
  end

  # {curve item, peak rung or nil} for one batch size.
  defp batch_summary(sweep, batch, runs) do
    ladder = sweep.conns_ladders[batch]
    rungs = Enum.map(ladder, &rung(&1, results_for(runs, batch, &1), sweep.repetitions))
    clean = Enum.filter(rungs, &(&1.status == "clean"))
    peak = if clean != [], do: Enum.max_by(clean, &{&1.records_per_s, -&1.connections})

    item =
      sweep
      |> regime(batch)
      |> Map.merge(%{
        "status" => batch_status(peak, rungs),
        "peak" => peak && peak_json(peak),
        "peak_at_ladder_limit" => peak && peak.connections == List.last(ladder),
        "lower_bound_reasons" => lower_bound_reasons(peak, ladder),
        "rungs" => Enum.map(rungs, &rung_json/1)
      })

    {item, peak}
  end

  # {rep, result} pairs, so ties between repetitions break on the repetition number and the summary
  # does not depend on the order the runs were listed in.
  defp results_for(runs, batch, connections) do
    for %{batch: ^batch, connections: ^connections, rep: rep, result: result} <- runs, do: {rep, result}
  end

  defp batch_status(nil, rungs) do
    if Enum.any?(rungs, &(&1.status in @statuses_with_results)), do: "no_clean_rung", else: "no_completed_rung"
  end

  defp batch_status(_peak, _rungs), do: "peak"

  defp lower_bound_reasons(nil, _ladder), do: []

  defp lower_bound_reasons(peak, ladder) do
    Enum.reject(
      [
        if(peak.connections == List.last(ladder), do: "ladder_limit"),
        if(peak.generator_saturated, do: "generator_saturated")
      ],
      &is_nil/1
    )
  end

  defp rung(connections, results, expected) do
    completed =
      results
      |> Enum.filter(fn {_rep, result} -> completed?(result) end)
      |> Enum.sort_by(fn {rep, result} -> {result["records_per_s"], rep} end)
      |> Enum.map(fn {_rep, result} -> result end)

    {representative, low, high} =
      case completed do
        [] -> {nil, nil, nil}
        _ -> {Enum.at(completed, div(length(completed) - 1, 2)), hd(completed), List.last(completed)}
      end

    %{
      connections: connections,
      status: rung_status(completed, expected),
      completed: length(completed),
      records_per_s: representative && representative["records_per_s"],
      records_per_s_min: low && low["records_per_s"],
      records_per_s_max: high && high["records_per_s"],
      representative: representative,
      generator_saturated: representative != nil and generator_saturated?(representative)
    }
  end

  defp rung_status(completed, expected) when length(completed) < expected, do: "failed"

  defp rung_status(completed, _expected) do
    if Enum.all?(completed, &(&1["errors"] === 0)), do: "clean", else: "errorful"
  end

  defp completed?(%{"records_per_s" => records_per_s}) when is_number(records_per_s), do: true
  defp completed?(_result), do: false

  defp generator_saturated?(%{"generator_cpu_cores" => cores, "generator_cpu_budget" => budget})
       when is_number(cores) and is_number(budget) and budget > 0,
       do: cores / budget >= @generator_saturation_threshold

  defp generator_saturated?(_result), do: false

  defp rung_json(rung) do
    representative = rung.representative || %{}

    %{
      "connections" => rung.connections,
      "status" => rung.status,
      "repetitions_completed" => rung.completed,
      "records_per_s" => rung.records_per_s,
      "records_per_s_min" => rung.records_per_s_min,
      "records_per_s_max" => rung.records_per_s_max,
      "errors" => representative["errors"],
      "latency_ms" => representative["latency_ms"],
      "flush_latency_seconds" => representative["flush_latency_seconds"],
      "flush_latency_error" => representative["flush_latency_error"],
      "server_cpu_cores" => representative["server_cpu_cores"],
      "generator_cpu_cores" => representative["generator_cpu_cores"],
      "generator_saturated" => rung.generator_saturated
    }
  end

  defp peak_json(peak) do
    peak
    |> rung_json()
    |> Map.drop(["status", "generator_saturated"])
  end

  defp aa_control(_batch, nil, _aa_result), do: {nil, "no clean peak at the headline batch size"}

  defp aa_control(batch, peak, aa_result) do
    if completed?(aa_result) do
      first = peak.records_per_s
      repeat = aa_result["records_per_s"]

      {%{
         "batch" => batch,
         "connections" => peak.connections,
         "first_records_per_s" => first,
         "repeat_records_per_s" => repeat,
         "repeat_errors" => aa_result["errors"],
         "delta_pct" => if(first > 0, do: Float.round((repeat - first) / first * 100, 1))
       }, nil}
    else
      {nil, "the A-A repeat produced no result"}
    end
  end

  defp cpu_sampled?(runs) do
    completed = for %{result: result} <- runs, completed?(result), do: result

    completed != [] and
      Enum.all?(completed, &(is_number(&1["server_cpu_cores"]) and is_number(&1["generator_cpu_cores"])))
  end
end
