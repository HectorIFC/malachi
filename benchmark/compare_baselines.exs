#!/usr/bin/env elixir

# Comparison script for baseline benchmarks
# Compares current results against baseline_reference.json

Code.require_file("benchmark/utils/comparator.ex")

defmodule CompareBaselines do
  def run(baseline_file, current_file) do
    IO.puts("#{IO.ANSI.cyan()}Comparing benchmarks...#{IO.ANSI.reset()}\n")
    IO.puts("Baseline: #{baseline_file}")
    IO.puts("Current:  #{current_file}\n")

    # Load current results
    case File.read(current_file) do
      {:ok, content} ->
        case Jason.decode(content) do
          {:ok, current_results} ->
            # Compare
            case BenchmarkComparator.compare(baseline_file, current_results) do
              {:ok, comparison} ->
                # No regressions
                IO.puts(BenchmarkComparator.format_report(comparison))
                System.halt(0)

              {:regression, comparison} ->
                # Regressions detected
                IO.puts(BenchmarkComparator.format_report(comparison))
                IO.puts("\n#{IO.ANSI.red()}❌ Performance regressions detected!#{IO.ANSI.reset()}\n")
                System.halt(1)

              {:error, message} ->
                IO.puts(:stderr, "Error: #{message}")
                System.halt(1)
            end

          {:error, reason} ->
            IO.puts(:stderr, "Error: Failed to decode current results: #{inspect(reason)}")
            System.halt(1)
        end

      {:error, reason} ->
        IO.puts(:stderr, "Error: Failed to read current results file: #{inspect(reason)}")
        System.halt(1)
    end
  end

  def print_usage do
    IO.puts("""
    Usage: compare_baselines.exs <baseline_file> <current_file>

    Compares current benchmark results against a baseline.
    Exits with code 1 if regressions are detected (>5% degradation).

    Example:
      ./benchmark/compare_baselines.exs benchmark/results/baseline_reference.json benchmark/results/throughput_20260203_120000.json
    """)
  end
end

# Main execution
case System.argv() do
  [baseline_file, current_file] ->
    CompareBaselines.run(baseline_file, current_file)

  _ ->
    CompareBaselines.print_usage()
    System.halt(1)
end
