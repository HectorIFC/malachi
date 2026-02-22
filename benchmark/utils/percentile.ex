defmodule Percentile do
  @moduledoc """
  Percentile calculation utilities for benchmark latency measurements.
  """

  @doc """
  Calculates percentiles (p50, p95, p99) from a list of samples.
  Returns a map with p50, p95, p99, min, max, avg, and count.
  """
  def calculate(samples) when is_list(samples) and length(samples) > 0 do
    sorted = Enum.sort(samples)
    count = length(sorted)

    %{
      p50: percentile(sorted, count, 50),
      p95: percentile(sorted, count, 95),
      p99: percentile(sorted, count, 99),
      min: List.first(sorted),
      max: List.last(sorted),
      avg: avg(samples),
      count: count
    }
  end

  def calculate([]) do
    %{
      p50: 0,
      p95: 0,
      p99: 0,
      min: 0,
      max: 0,
      avg: 0,
      count: 0
    }
  end

  @doc """
  Calculates a specific percentile from sorted samples.
  """
  def percentile(sorted_samples, count, percentile) do
    index = calculate_percentile_index(count, percentile)
    Enum.at(sorted_samples, index, 0)
  end

  defp calculate_percentile_index(count, percentile) do
    # Use nearest rank method
    rank = ceil(percentile / 100 * count)
    max(0, rank - 1)
  end

  @doc """
  Calculates average from samples.
  """
  def avg(samples) when length(samples) > 0 do
    Enum.sum(samples) / length(samples)
  end

  def avg([]), do: 0

  @doc """
  Calculates median from samples.
  """
  def median(samples) when is_list(samples) and length(samples) > 0 do
    sorted = Enum.sort(samples)
    count = length(sorted)
    mid = div(count, 2)

    if rem(count, 2) == 0 do
      (Enum.at(sorted, mid - 1) + Enum.at(sorted, mid)) / 2
    else
      Enum.at(sorted, mid)
    end
  end

  def median([]), do: 0

  @doc """
  Calculates standard deviation from samples.
  """
  def stddev(samples) when length(samples) > 1 do
    mean = avg(samples)
    variance = Enum.reduce(samples, 0, fn x, acc -> acc + :math.pow(x - mean, 2) end) / length(samples)
    :math.sqrt(variance)
  end

  def stddev(_), do: 0

  @doc """
  Collects samples from an ETS bag table and calculates percentiles.
  """
  def from_ets_bag(table, key) do
    samples =
      :ets.lookup(table, key)
      |> Enum.map(fn {^key, value} -> value end)

    calculate(samples)
  end

  @doc """
  Creates an ETS bag table for collecting latency samples.
  """
  def create_sample_table(name \\ :benchmark_samples) do
    :ets.new(name, [:bag, :public, :named_table])
  end

  @doc """
  Clears all samples from a table.
  """
  def clear_samples(table) do
    :ets.delete_all_objects(table)
  end

  @doc """
  Formats percentile results for display.
  """
  def format_results(results, unit \\ "μs") do
    """
    Latency Statistics (#{unit}):
      Count:   #{BenchmarkHelpers.format_number(results.count)}
      Min:     #{Float.round(results.min * 1.0, 2)}
      P50:     #{Float.round(results.p50 * 1.0, 2)}
      P95:     #{Float.round(results.p95 * 1.0, 2)}
      P99:     #{Float.round(results.p99 * 1.0, 2)}
      Max:     #{Float.round(results.max * 1.0, 2)}
      Avg:     #{Float.round(results.avg * 1.0, 2)}
    """
  end
end
