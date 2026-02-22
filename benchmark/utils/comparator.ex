defmodule BenchmarkComparator do
  @moduledoc """
  Compares benchmark results against baseline to detect performance regressions.
  """

  @threshold 5.0  # 5% degradation threshold

  @doc """
  Compares current results against baseline.
  Returns {:ok, comparison} or {:regression, comparison} if degradation > threshold.
  """
  def compare(baseline_file, current_results) do
    case File.read(baseline_file) do
      {:ok, content} ->
        baseline = Jason.decode!(content)
        comparison = perform_comparison(baseline, current_results)

        if has_regressions?(comparison) do
          {:regression, comparison}
        else
          {:ok, comparison}
        end

      {:error, _} ->
        {:error, "Baseline file not found: #{baseline_file}"}
    end
  end

  @doc """
  Performs detailed comparison between baseline and current results.
  """
  def perform_comparison(baseline, current) do
    baseline_results = get_results(baseline)
    current_results = get_results(current)

    comparisons =
      baseline_results
      |> Enum.map(fn {test_name, baseline_metrics} ->
        current_metrics = Map.get(current_results, test_name, %{})
        {test_name, compare_metrics(baseline_metrics, current_metrics)}
      end)
      |> Enum.into(%{})

    %{
      baseline_version: Map.get(baseline, "version", "unknown"),
      current_version: Map.get(current, "version", "unknown"),
      baseline_timestamp: Map.get(baseline, "timestamp", ""),
      current_timestamp: Map.get(current, "timestamp", ""),
      threshold: @threshold,
      comparisons: comparisons,
      has_regressions: has_regressions_in_comparisons?(comparisons)
    }
  end

  defp get_results(data) do
    Map.get(data, "results", %{})
  end

  defp compare_metrics(baseline, current) do
    all_keys =
      (Map.keys(baseline) ++ Map.keys(current))
      |> Enum.uniq()
      |> Enum.filter(&is_numeric_key?(&1, baseline, current))

    all_keys
    |> Enum.map(fn key ->
      baseline_value = get_numeric_value(baseline, key)
      current_value = get_numeric_value(current, key)

      {key, calculate_change(key, baseline_value, current_value)}
    end)
    |> Enum.into(%{})
  end

  defp is_numeric_key?(key, baseline, current) do
    is_number(get_in(baseline, [key])) or is_number(get_in(current, [key]))
  end

  defp get_numeric_value(map, key) do
    case get_in(map, [key]) do
      value when is_number(value) -> value
      _ -> nil
    end
  end

  defp calculate_change(key, baseline_value, current_value) do
    cond do
      is_nil(baseline_value) or is_nil(current_value) ->
        %{
          baseline: baseline_value,
          current: current_value,
          change_percent: nil,
          is_regression: false,
          status: "missing_data"
        }

      baseline_value == 0 ->
        %{
          baseline: baseline_value,
          current: current_value,
          change_percent: nil,
          is_regression: false,
          status: "baseline_zero"
        }

      true ->
        change_percent = ((current_value - baseline_value) / baseline_value) * 100
        is_regression = is_regression?(key, change_percent)

        %{
          baseline: baseline_value,
          current: current_value,
          change_percent: Float.round(change_percent, 2),
          is_regression: is_regression,
          status: get_status(is_regression, change_percent)
        }
    end
  end

  # Metrics where higher is better (throughput, msgs/sec)
  @higher_is_better ["throughput", "msgs_per_sec", "mb_per_sec", "ops_per_sec", "connections_per_sec"]

  # Metrics where lower is better (latency, memory)
  @lower_is_better ["latency", "us", "memory", "mb", "ms"]

  defp is_regression?(key, change_percent) do
    key_str = to_string(key)

    cond do
      # Higher is better: regression if decrease > threshold
      Enum.any?(@higher_is_better, &String.contains?(key_str, &1)) ->
        change_percent < -@threshold

      # Lower is better: regression if increase > threshold
      Enum.any?(@lower_is_better, &String.contains?(key_str, &1)) ->
        change_percent > @threshold

      # Unknown metric type: conservative - flag increases as potential regression
      true ->
        change_percent > @threshold
    end
  end

  defp get_status(true, _), do: "regression"

  defp get_status(false, change_percent) do
    cond do
      change_percent > 5 -> "improved"
      change_percent < -5 -> "degraded"
      true -> "stable"
    end
  end

  defp has_regressions_in_comparisons?(comparisons) do
    comparisons
    |> Enum.any?(fn {_test_name, metrics} ->
      Enum.any?(metrics, fn {_key, change} ->
        Map.get(change, :is_regression, false)
      end)
    end)
  end

  defp has_regressions?(comparison) do
    Map.get(comparison, :has_regressions, false)
  end

  @doc """
  Formats comparison results as a colored console report.
  """
  def format_report(comparison) do
    header = """
    #{IO.ANSI.cyan()}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #{IO.ANSI.bright()}#{IO.ANSI.white()}Performance Comparison Report#{IO.ANSI.reset()}
    #{IO.ANSI.cyan()}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━#{IO.ANSI.reset()}
    Baseline: #{comparison.baseline_version} (#{comparison.baseline_timestamp})
    Current:  #{comparison.current_version} (#{comparison.current_timestamp})
    Threshold: ±#{comparison.threshold}%
    """

    test_sections =
      comparison.comparisons
      |> Enum.map(fn {test_name, metrics} ->
        format_test_comparison(test_name, metrics)
      end)
      |> Enum.join("\n")

    summary = format_summary(comparison)

    header <> "\n" <> test_sections <> "\n" <> summary
  end

  defp format_test_comparison(test_name, metrics) do
    title = """
    #{IO.ANSI.yellow()}#{String.replace(to_string(test_name), "_", " ") |> String.capitalize()}:#{IO.ANSI.reset()}
    """

    metric_lines =
      metrics
      |> Enum.filter(fn {_key, change} -> Map.get(change, :change_percent) != nil end)
      |> Enum.map(fn {key, change} -> format_metric_change(key, change) end)
      |> Enum.join("\n")

    if metric_lines == "" do
      ""
    else
      title <> metric_lines <> "\n"
    end
  end

  defp format_metric_change(key, change) do
    key_str = to_string(key) |> String.replace("_", " ") |> String.pad_trailing(30)
    baseline = format_metric_value(change.baseline)
    current = format_metric_value(change.current)
    percent = change.change_percent

    {color, symbol} =
      case change.status do
        "regression" -> {IO.ANSI.red(), "↓ REGRESSION"}
        "improved" -> {IO.ANSI.green(), "↑ Improved"}
        "degraded" -> {IO.ANSI.yellow(), "↓ Degraded"}
        "stable" -> {IO.ANSI.white(), "→ Stable"}
        _ -> {IO.ANSI.white(), "?"}
      end

    change_str = format_change_percent(percent)

    "  #{key_str} #{baseline} → #{current}  #{color}#{change_str} #{symbol}#{IO.ANSI.reset()}"
  end

  defp format_metric_value(value) when is_float(value), do: "#{Float.round(value, 2)}" |> String.pad_leading(12)
  defp format_metric_value(value) when is_integer(value), do: "#{value}" |> String.pad_leading(12)
  defp format_metric_value(value), do: "#{value}" |> String.pad_leading(12)

  defp format_change_percent(percent) when percent > 0, do: "+#{Float.round(percent, 2)}%"
  defp format_change_percent(percent), do: "#{Float.round(percent, 2)}%"

  defp format_summary(comparison) do
    regression_count = count_regressions(comparison.comparisons)
    improvement_count = count_improvements(comparison.comparisons)
    stable_count = count_stable(comparison.comparisons)

    status_color = if comparison.has_regressions, do: IO.ANSI.red(), else: IO.ANSI.green()
    status_text = if comparison.has_regressions, do: "FAILED", else: "PASSED"

    """
    #{IO.ANSI.cyan()}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━#{IO.ANSI.reset()}
    #{IO.ANSI.yellow()}Summary:#{IO.ANSI.reset()}
      Regressions:  #{IO.ANSI.red()}#{regression_count}#{IO.ANSI.reset()}
      Improvements: #{IO.ANSI.green()}#{improvement_count}#{IO.ANSI.reset()}
      Stable:       #{stable_count}

    #{status_color}Overall Status: #{status_text}#{IO.ANSI.reset()}
    #{IO.ANSI.cyan()}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━#{IO.ANSI.reset()}
    """
  end

  defp count_regressions(comparisons) do
    count_by_status(comparisons, "regression")
  end

  defp count_improvements(comparisons) do
    count_by_status(comparisons, "improved")
  end

  defp count_stable(comparisons) do
    count_by_status(comparisons, "stable")
  end

  defp count_by_status(comparisons, status) do
    comparisons
    |> Enum.reduce(0, fn {_test_name, metrics}, acc ->
      count =
        metrics
        |> Enum.count(fn {_key, change} ->
          Map.get(change, :status) == status
        end)

      acc + count
    end)
  end

  @doc """
  Generates a markdown table for GitHub PR comments.
  """
  def format_github_comment(comparison) do
    header = """
    ## 📊 Performance Comparison Report

    **Baseline:** #{comparison.baseline_version} (#{comparison.baseline_timestamp})
    **Current:** #{comparison.current_version} (#{comparison.current_timestamp})
    **Threshold:** ±#{comparison.threshold}%

    """

    status_badge =
      if comparison.has_regressions do
        "### ❌ Performance Regressions Detected\n\n"
      else
        "### ✅ No Performance Regressions\n\n"
      end

    tables =
      comparison.comparisons
      |> Enum.map(fn {test_name, metrics} ->
        format_github_table(test_name, metrics)
      end)
      |> Enum.filter(&(&1 != ""))
      |> Enum.join("\n")

    summary = format_github_summary(comparison)

    header <> status_badge <> tables <> summary
  end

  defp format_github_table(test_name, metrics) do
    filtered_metrics =
      metrics
      |> Enum.filter(fn {_key, change} -> Map.get(change, :change_percent) != nil end)

    if Enum.empty?(filtered_metrics) do
      ""
    else
      title = "#### #{String.replace(to_string(test_name), "_", " ") |> String.capitalize()}\n\n"

      table_header = """
      | Metric | Baseline | Current | Change | Status |
      |--------|----------|---------|--------|--------|
      """

      rows =
        filtered_metrics
        |> Enum.map(fn {key, change} ->
          format_github_row(key, change)
        end)
        |> Enum.join("\n")

      title <> table_header <> rows <> "\n\n"
    end
  end

  defp format_github_row(key, change) do
    key_str = to_string(key) |> String.replace("_", " ")
    baseline = format_metric_value(change.baseline) |> String.trim()
    current = format_metric_value(change.current) |> String.trim()
    percent = format_change_percent(change.change_percent)

    status_emoji =
      case change.status do
        "regression" -> "🔴 REGRESSION"
        "improved" -> "🟢 Improved"
        "degraded" -> "🟡 Degraded"
        "stable" -> "⚪ Stable"
        _ -> "⚫ Unknown"
      end

    "| #{key_str} | #{baseline} | #{current} | #{percent} | #{status_emoji} |"
  end

  defp format_github_summary(comparison) do
    regression_count = count_regressions(comparison.comparisons)
    improvement_count = count_improvements(comparison.comparisons)
    stable_count = count_stable(comparison.comparisons)

    """
    ---

    **Summary:**
    - 🔴 Regressions: #{regression_count}
    - 🟢 Improvements: #{improvement_count}
    - ⚪ Stable: #{stable_count}
    """
  end
end
