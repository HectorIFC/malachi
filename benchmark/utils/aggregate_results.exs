#!/usr/bin/env elixir

# Aggregates multiple benchmark runs into a single reference baseline.
# Calculates mean and median for each metric across runs.

defmodule BenchmarkAggregator do
  @doc """
  Aggregates multiple benchmark result files into a single reference baseline.
  """
  def aggregate(result_files) do
    if length(result_files) == 0 do
      IO.puts(:stderr, "Error: No result files provided")
      System.halt(1)
    end

    results =
      result_files
      |> Enum.map(&load_result/1)
      |> Enum.filter(&(&1 != nil))

    if length(results) == 0 do
      IO.puts(:stderr, "Error: No valid result files found")
      System.halt(1)
    end

    aggregated = aggregate_results(results)

    # Output to stdout
    IO.puts(Jason.encode!(aggregated, pretty: true))
  end

  defp load_result(file) do
    case File.read(file) do
      {:ok, content} ->
        case Jason.decode(content) do
          {:ok, data} -> data
          {:error, _} ->
            IO.puts(:stderr, "Warning: Failed to decode #{file}")
            nil
        end

      {:error, _} ->
        IO.puts(:stderr, "Warning: Failed to read #{file}")
        nil
    end
  end

  defp aggregate_results(results) do
    # Use first result as template
    template = List.first(results)

    %{
      "benchmark" => "baseline_reference",
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "version" => Map.get(template, "version", "unknown"),
      "runs_aggregated" => length(results),
      "system_info" => Map.get(template, "system_info", %{}),
      "results" => aggregate_test_results(results)
    }
  end

  defp aggregate_test_results(results) do
    # Get all test names from all results
    all_test_names =
      results
      |> Enum.flat_map(fn r -> Map.get(r, "results", %{}) |> Map.keys() end)
      |> Enum.uniq()

    all_test_names
    |> Enum.map(fn test_name ->
      test_results =
        results
        |> Enum.map(fn r -> get_in(r, ["results", test_name]) end)
        |> Enum.filter(&(&1 != nil))

      {test_name, aggregate_metrics(test_results)}
    end)
    |> Enum.into(%{})
  end

  defp aggregate_metrics(test_results) do
    if length(test_results) == 0 do
      %{}
    else
      # Get all metric keys
      all_keys =
        test_results
        |> Enum.flat_map(&Map.keys/1)
        |> Enum.uniq()

      all_keys
      |> Enum.map(fn key ->
        values =
          test_results
          |> Enum.map(&Map.get(&1, key))
          |> Enum.filter(&is_number/1)

        aggregated_value = aggregate_values(values)
        {key, aggregated_value}
      end)
      |> Enum.filter(fn {_key, value} -> value != nil end)
      |> Enum.into(%{})
    end
  end

  defp aggregate_values([]), do: nil

  defp aggregate_values(values) do
    # Use median for more robust aggregation (less affected by outliers)
    sorted = Enum.sort(values)
    count = length(sorted)
    mid = div(count, 2)

    if rem(count, 2) == 0 do
      (Enum.at(sorted, mid - 1) + Enum.at(sorted, mid)) / 2
    else
      Enum.at(sorted, mid)
    end
  end
end

# Main execution
case System.argv() do
  [] ->
    IO.puts(:stderr, "Usage: aggregate_results.exs <result_file1.json> <result_file2.json> ...")
    System.halt(1)

  files ->
    BenchmarkAggregator.aggregate(files)
end
