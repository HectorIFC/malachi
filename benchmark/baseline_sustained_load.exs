#!/usr/bin/env elixir

# Sustained Load Baseline Benchmark
# Runs for 24 hours with mixed load to monitor memory growth and performance degradation

Mix.install([{:jason, "~> 1.4"}])

Code.require_file("utils/benchmark_helpers.ex", "benchmark")
Code.require_file("utils/percentile.ex", "benchmark")
Code.require_file("utils/reporter.ex", "benchmark")

defmodule SustainedLoadBenchmark do
  @duration_hours 24
  @sample_interval_sec 300  # 5 minutes
  @queue_count 10
  @producers_per_queue 5
  @consumers_per_queue 5
  @message_size 1024

  def run do
    IO.puts("\n#{IO.ANSI.cyan()}Starting Sustained Load Baseline Benchmark (#{@duration_hours} hours)#{IO.ANSI.reset()}\n")
    IO.puts("#{IO.ANSI.yellow()}WARNING: This benchmark will run for #{@duration_hours} hours#{IO.ANSI.reset()}")
    IO.puts("Press Ctrl+C twice to abort\n")

    Process.sleep(3000)

    # Start the application
    Application.ensure_all_started(:malachimq)
    Process.sleep(1000)

    # Initialize result structure
    start_time = DateTime.utc_now()

    results = %{
      "benchmark" => "baseline_sustained_load",
      "timestamp" => DateTime.to_iso8601(start_time),
      "version" => BenchmarkHelpers.get_version(),
      "system_info" => BenchmarkHelpers.get_system_info(),
      "config" => %{
        "duration_hours" => @duration_hours,
        "sample_interval_sec" => @sample_interval_sec,
        "queue_count" => @queue_count,
        "producers_per_queue" => @producers_per_queue,
        "consumers_per_queue" => @consumers_per_queue,
        "message_size_bytes" => @message_size
      },
      "samples" => []
    }

    # Start workload
    IO.puts("Starting sustained workload...")
    queues = start_workload()

    IO.puts("Workload started. Beginning monitoring...\n")

    # Run monitoring loop
    final_results = monitoring_loop(results, queues, start_time)

    # Cleanup
    IO.puts("\n#{IO.ANSI.yellow()}Stopping workload...#{IO.ANSI.reset()}")
    stop_workload(queues)

    # Save results
    filename = BenchmarkHelpers.timestamped_filename("sustained_load")
    BenchmarkReporter.save_results(final_results, filename)

    IO.puts("\n#{IO.ANSI.green()}✓ Sustained load benchmark complete#{IO.ANSI.reset()}\n")

    # Display summary
    display_summary(final_results)
  end

  defp start_workload do
    payload = :crypto.strong_rand_bytes(@message_size)

    queues =
      Enum.map(1..@queue_count, fn i ->
        queue_name = "sustained_load_#{i}"

        # Start consumers
        consumers =
          Enum.map(1..@consumers_per_queue, fn _ ->
            {:ok, pid} =
              BenchmarkHelpers.start_benchmark_consumer(queue_name, fn _ -> :ok end)

            pid
          end)

        # Start producer tasks (background)
        producers =
          Enum.map(1..@producers_per_queue, fn _ ->
            Task.async(fn ->
              producer_loop(queue_name, payload)
            end)
          end)

        %{
          name: queue_name,
          consumers: consumers,
          producers: producers
        }
      end)

    Process.sleep(1000)
    queues
  end

  defp producer_loop(queue_name, payload) do
    # Send messages continuously with small delay
    MalachiMQ.Queue.enqueue(queue_name, payload)
    Process.sleep(10)  # ~100 msgs/sec per producer
    producer_loop(queue_name, payload)
  end

  defp monitoring_loop(results, queues, start_time) do
    duration_sec = @duration_hours * 3600
    end_time = DateTime.add(start_time, duration_sec, :second)

    do_monitoring_loop(results, queues, start_time, end_time, 0)
  end

  defp do_monitoring_loop(results, queues, start_time, end_time, sample_count) do
    now = DateTime.utc_now()

    if DateTime.compare(now, end_time) == :lt do
      # Collect sample
      sample = collect_sample(queues, start_time, now, sample_count)

      # Display progress
      elapsed_hours = DateTime.diff(now, start_time) / 3600
      remaining_hours = @duration_hours - elapsed_hours

      IO.puts("#{IO.ANSI.cyan()}[Sample #{sample_count + 1}]#{IO.ANSI.reset()} " <>
                "Elapsed: #{Float.round(elapsed_hours, 2)}h | " <>
                "Remaining: #{Float.round(remaining_hours, 2)}h")

      IO.puts("  Memory: #{Float.round(sample.memory_total_mb, 2)} MB | " <>
                "Processes: #{sample.process_count} | " <>
                "Throughput: #{BenchmarkHelpers.format_number(sample.total_throughput_msgs_per_sec)} msgs/sec")

      # Add sample to results
      updated_results = %{results | "samples" => results["samples"] ++ [sample]}

      # Wait for next sample interval
      Process.sleep(@sample_interval_sec * 1000)

      # Continue
      do_monitoring_loop(updated_results, queues, start_time, end_time, sample_count + 1)
    else
      # Done - add final analysis
      add_final_analysis(results)
    end
  end

  defp collect_sample(queues, start_time, now, sample_number) do
    elapsed_sec = DateTime.diff(now, start_time)

    # Get system metrics
    system_metrics = MalachiMQ.Metrics.get_system_metrics()
    memory_usage = BenchmarkHelpers.get_memory_usage()

    # Get queue metrics
    queue_metrics =
      Enum.map(queues, fn queue ->
        metrics = MalachiMQ.Metrics.get_metrics(queue.name)

        %{
          "queue" => queue.name,
          "enqueued" => metrics.enqueued,
          "processed" => metrics.processed,
          "acked" => metrics.acked,
          "buffered" => metrics.buffered
        }
      end)

    # Calculate total throughput (msgs processed since last sample)
    total_processed = Enum.sum(Enum.map(queue_metrics, fn m -> m["processed"] end))
    throughput_msgs_per_sec = if sample_number > 0, do: total_processed / @sample_interval_sec, else: 0

    %{
      "sample_number" => sample_number,
      "timestamp" => DateTime.to_iso8601(now),
      "elapsed_seconds" => elapsed_sec,
      "elapsed_hours" => Float.round(elapsed_sec / 3600, 2),
      "memory_total_mb" => Float.round(memory_usage.total_mb, 2),
      "memory_processes_mb" => Float.round(memory_usage.processes_mb, 2),
      "memory_ets_mb" => Float.round(memory_usage.ets_mb, 2),
      "process_count" => system_metrics.process_count,
      "run_queue" => system_metrics.run_queue,
      "total_throughput_msgs_per_sec" => trunc(throughput_msgs_per_sec),
      "queues" => queue_metrics
    }
  end

  defp add_final_analysis(results) do
    samples = results["samples"]

    if length(samples) < 2 do
      Map.put(results, "analysis", %{"error" => "Insufficient samples for analysis"})
    else
      first_sample = List.first(samples)
      last_sample = List.last(samples)

      # Memory growth analysis
      memory_growth_mb = last_sample["memory_total_mb"] - first_sample["memory_total_mb"]
      memory_growth_percent =
        (memory_growth_mb / first_sample["memory_total_mb"]) * 100

      # Throughput degradation analysis
      throughputs = Enum.map(samples, fn s -> s["total_throughput_msgs_per_sec"] end)
      avg_throughput = Enum.sum(throughputs) / length(throughputs)
      min_throughput = Enum.min(throughputs)
      max_throughput = Enum.max(throughputs)

      # Process count growth
      process_growth = last_sample["process_count"] - first_sample["process_count"]

      analysis = %{
        "total_duration_hours" => @duration_hours,
        "total_samples" => length(samples),
        "memory_analysis" => %{
          "initial_mb" => first_sample["memory_total_mb"],
          "final_mb" => last_sample["memory_total_mb"],
          "growth_mb" => Float.round(memory_growth_mb, 2),
          "growth_percent" => Float.round(memory_growth_percent, 2)
        },
        "throughput_analysis" => %{
          "avg_msgs_per_sec" => trunc(avg_throughput),
          "min_msgs_per_sec" => min_throughput,
          "max_msgs_per_sec" => max_throughput,
          "variance_percent" =>
            Float.round(((max_throughput - min_throughput) / avg_throughput) * 100, 2)
        },
        "process_analysis" => %{
          "initial_count" => first_sample["process_count"],
          "final_count" => last_sample["process_count"],
          "growth" => process_growth
        }
      }

      Map.put(results, "analysis", analysis)
    end
  end

  defp stop_workload(queues) do
    Enum.each(queues, fn queue ->
      # Cleanup queue
      BenchmarkHelpers.cleanup_queue(queue.name)
    end)
  end

  defp display_summary(results) do
    if Map.has_key?(results, "analysis") do
      analysis = results["analysis"]

      IO.puts("\n#{IO.ANSI.cyan()}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
      IO.puts("#{IO.ANSI.bright()}#{IO.ANSI.white()}Sustained Load Analysis#{IO.ANSI.reset()}")
      IO.puts("#{IO.ANSI.cyan()}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━#{IO.ANSI.reset()}")

      mem = analysis["memory_analysis"]

      IO.puts("\n#{IO.ANSI.yellow()}Memory:#{IO.ANSI.reset()}")
      IO.puts("  Initial:  #{mem["initial_mb"]} MB")
      IO.puts("  Final:    #{mem["final_mb"]} MB")
      IO.puts("  Growth:   #{mem["growth_mb"]} MB (#{mem["growth_percent"]}%)")

      throughput = analysis["throughput_analysis"]
      IO.puts("\n#{IO.ANSI.yellow()}Throughput:#{IO.ANSI.reset()}")
      IO.puts("  Average:  #{BenchmarkHelpers.format_number(throughput["avg_msgs_per_sec"])} msgs/sec")
      IO.puts("  Min:      #{BenchmarkHelpers.format_number(throughput["min_msgs_per_sec"])} msgs/sec")
      IO.puts("  Max:      #{BenchmarkHelpers.format_number(throughput["max_msgs_per_sec"])} msgs/sec")
      IO.puts("  Variance: #{throughput["variance_percent"]}%")

      proc = analysis["process_analysis"]
      IO.puts("\n#{IO.ANSI.yellow()}Processes:#{IO.ANSI.reset()}")
      IO.puts("  Initial:  #{BenchmarkHelpers.format_number(proc["initial_count"])}")
      IO.puts("  Final:    #{BenchmarkHelpers.format_number(proc["final_count"])}")
      IO.puts("  Growth:   #{proc["growth"]}")

      IO.puts("\n#{IO.ANSI.cyan()}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━#{IO.ANSI.reset()}\n")
    end
  end
end

# Run the benchmark
SustainedLoadBenchmark.run()
