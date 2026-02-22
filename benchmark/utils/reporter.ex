defmodule BenchmarkReporter do
  @moduledoc """
  Formats and outputs benchmark results in JSON, CSV, or console format.
  """

  @doc """
  Saves benchmark results to file in the specified format.
  Format is determined by config or defaults to JSON.
  """
  def save_results(results, filename, format \\ nil) do
    output_format = format || get_output_format()

    content =
      case output_format do
        "json" -> format_json(results)
        "csv" -> format_csv(results)
        "console" -> format_console(results)
        _ -> format_json(results)
      end

    File.write!(filename, content)
    IO.puts("Results saved to: #{filename}")
  end

  @doc """
  Outputs results to console in the specified format.
  """
  def display_results(results, format \\ nil) do
    output_format = format || get_output_format()

    case output_format do
      "json" -> IO.puts(format_json(results))
      "csv" -> IO.puts(format_csv(results))
      "console" -> IO.puts(format_console(results))
      _ -> IO.puts(format_console(results))
    end
  end

  @doc """
  Formats results as pretty-printed JSON.
  """
  def format_json(results) do
    Jason.encode!(results, pretty: true)
  end

  @doc """
  Formats results as CSV.
  Flattens nested structures with dot notation.
  """
  def format_csv(results) do
    flat = flatten_map(results)
    headers = Map.keys(flat) |> Enum.sort()
    values = Enum.map(headers, fn key -> Map.get(flat, key, "") end)

    header_line = Enum.join(headers, ",")
    value_line = Enum.map(values, &format_csv_value/1) |> Enum.join(",")

    "#{header_line}\n#{value_line}"
  end

  defp format_csv_value(value) when is_binary(value), do: "\"#{value}\""
  defp format_csv_value(value) when is_number(value), do: "#{value}"
  defp format_csv_value(value), do: "\"#{inspect(value)}\""

  @doc """
  Formats results as human-readable console output.
  """
  def format_console(results) do
    benchmark_name = Map.get(results, "benchmark", "unknown")
    timestamp = Map.get(results, "timestamp", "")
    version = Map.get(results, "version", "unknown")

    header = """
    #{IO.ANSI.cyan()}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #{IO.ANSI.bright()}#{IO.ANSI.white()}Benchmark: #{benchmark_name}#{IO.ANSI.reset()}
    #{IO.ANSI.cyan()}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━#{IO.ANSI.reset()}
    Version:   #{version}
    Timestamp: #{timestamp}
    """

    system_info = format_system_info(Map.get(results, "system_info", %{}))
    test_results = format_test_results(Map.get(results, "results", %{}))

    header <> "\n" <> system_info <> "\n" <> test_results
  end

  defp format_system_info(system_info) when is_map(system_info) and map_size(system_info) > 0 do
    """
    #{IO.ANSI.yellow()}System Information:#{IO.ANSI.reset()}
      Schedulers:      #{Map.get(system_info, "schedulers_online", "N/A")}
      Process Count:   #{Map.get(system_info, "process_count", "N/A")}
      Process Limit:   #{Map.get(system_info, "process_limit", "N/A")}
      OS:              #{Map.get(system_info, "os", "N/A")}
      OTP Release:     #{Map.get(system_info, "otp_release", "N/A")}
    """
  end

  defp format_system_info(_), do: ""

  defp format_test_results(results) when is_map(results) do
    results
    |> Enum.map(fn {test_name, metrics} ->
      format_test_section(test_name, metrics)
    end)
    |> Enum.join("\n")
  end

  defp format_test_section(test_name, metrics) when is_map(metrics) do
    title = "#{IO.ANSI.green()}#{String.replace(test_name, "_", " ") |> String.capitalize()}:#{IO.ANSI.reset()}"

    formatted_metrics =
      metrics
      |> Enum.map(fn {key, value} -> format_metric(key, value) end)
      |> Enum.join("\n")

    "#{title}\n#{formatted_metrics}\n"
  end

  defp format_test_section(test_name, _), do: "#{test_name}: (no data)\n"

  defp format_metric(key, value) when is_number(value) do
    label = key |> to_string() |> String.replace("_", " ") |> String.pad_trailing(25)
    "  #{label} #{format_value(key, value)}"
  end

  defp format_metric(key, value) when is_map(value) do
    label = key |> to_string() |> String.replace("_", " ")
    nested = Enum.map(value, fn {k, v} -> "    #{k}: #{v}" end) |> Enum.join("\n")
    "  #{label}:\n#{nested}"
  end

  defp format_metric(key, value) do
    label = key |> to_string() |> String.replace("_", " ") |> String.pad_trailing(25)
    "  #{label} #{value}"
  end

  defp format_value(key, value) when is_float(value) do
    key_str = to_string(key)

    cond do
      String.contains?(key_str, "_ms") ->
        "#{Float.round(value, 2)} ms"

      String.contains?(key_str, ["latency", "us"]) ->
        "#{Float.round(value, 2)} μs"

      String.contains?(key_str, ["mb", "memory"]) ->
        "#{Float.round(value, 2)} MB"

      String.contains?(key_str, ["percent", "cpu"]) ->
        "#{Float.round(value, 2)}%"

      String.contains?(key_str, "per_sec") ->
        "#{BenchmarkHelpers.format_number(round(value))}/sec"

      true ->
        "#{Float.round(value, 2)}"
    end
  end

  defp format_value(_key, value) when is_integer(value) do
    BenchmarkHelpers.format_number(value)
  end

  defp format_value(_key, value), do: "#{value}"

  @doc """
  Flattens a nested map into a single-level map with dot-notation keys.
  """
  def flatten_map(map, prefix \\ "") do
    map
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      new_key = if prefix == "", do: "#{key}", else: "#{prefix}.#{key}"

      case value do
        %{} = nested_map ->
          Map.merge(acc, flatten_map(nested_map, new_key))

        _ ->
          Map.put(acc, new_key, value)
      end
    end)
  end

  defp get_output_format do
    Application.get_env(:malachimq, :benchmark_output_format, "json")
  end
end
