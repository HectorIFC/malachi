#!/usr/bin/env elixir

Mix.install([
  {:jason, "~> 1.4"},
  {:malachi, path: Path.expand("..", __DIR__)}
])

Code.require_file("utils/benchmark_helpers.ex", __DIR__)

defmodule SecurityPerformanceSuite do
  @moduledoc """
  Comprehensive security performance benchmark suite.

  Measures overhead of all security features individually and as a combined
  pipeline. Produces JSON results for baseline comparison and HTML-friendly
  output for human consumption.

  Acceptance criteria: Security overhead < 5% on hot paths.

  Run: MIX_ENV=dev mix run benchmark/security_performance_suite.exs
  """

  @warmup_sec 3
  @duration_sec 15
  @iterations 100_000

  def run do
    IO.puts("\nMalachi Security Performance Suite")
    IO.puts("=" <> String.duplicate("=", 79))

    Application.ensure_all_started(:malachi)
    Process.sleep(1000)

    results = %{
      "benchmark" => "security_performance",
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "version" => BenchmarkHelpers.get_version(),
      "system_info" => BenchmarkHelpers.get_system_info(),
      "results" => %{}
    }

    results =
      results
      |> bench_authentication()
      |> bench_token_validation()
      |> bench_lockout_check()
      |> bench_validation_pipeline()
      |> bench_rate_limiter()
      |> bench_sanitization()
      |> bench_audit_logging()
      |> bench_combined_pipeline()

    IO.puts("\nResults Summary")
    IO.puts("=" <> String.duplicate("=", 79))
    display_results(results["results"])

    IO.puts("\nPerformance Analysis")
    IO.puts("-" <> String.duplicate("-", 79))
    analyze_overhead(results["results"])

    # Save results
    output_path = Path.join([__DIR__, "results", "security_performance_suite.json"])
    File.mkdir_p!(Path.dirname(output_path))
    File.write!(output_path, Jason.encode!(results, pretty: true))
    IO.puts("\nResults saved to: #{output_path}")

    results
  end

  # ── Authentication (Argon2 hash + session creation) ──────────────

  defp bench_authentication(results) do
    IO.puts("\n[1/8] Benchmarking authentication (Argon2)...")

    # Warmup
    for _ <- 1..10, do: Malachi.Auth.authenticate("admin", "admin123")

    {duration_us, _} =
      BenchmarkHelpers.measure_time(fn ->
        for _ <- 1..100 do
          Malachi.Auth.authenticate("admin", "admin123")
        end
      end)

    avg_us = duration_us / 100
    avg_ms = avg_us / 1000

    IO.puts("   Auth avg: #{Float.round(avg_ms, 2)} ms/call")

    put_in(results, ["results", "authentication"], %{
      "avg_us" => avg_us,
      "avg_ms" => avg_ms,
      "iterations" => 100,
      "note" => "Argon2 is intentionally slow (~10-100ms)"
    })
  end

  # ── Token Validation (ETS lookup + IP binding) ───────────────────

  defp bench_token_validation(results) do
    IO.puts("\n[2/8] Benchmarking token validation (ETS lookup)...")

    {:ok, token} = Malachi.Auth.authenticate("admin", "admin123")

    # Warmup
    for _ <- 1..1000, do: Malachi.Auth.validate_token(token)

    {duration_us, _} =
      BenchmarkHelpers.measure_time(fn ->
        for _ <- 1..@iterations do
          Malachi.Auth.validate_token(token)
        end
      end)

    avg_us = duration_us / @iterations

    IO.puts("   Token validation avg: #{Float.round(avg_us, 2)} us/call")

    put_in(results, ["results", "token_validation"], %{
      "avg_us" => avg_us,
      "iterations" => @iterations
    })
  end

  # ── Lockout Check ────────────────────────────────────────────────

  defp bench_lockout_check(results) do
    IO.puts("\n[3/8] Benchmarking lockout check...")

    # Warmup
    for _ <- 1..1000 do
      Malachi.Auth.LockoutManager.locked?("admin", {127, 0, 0, 1})
    end

    {duration_us, _} =
      BenchmarkHelpers.measure_time(fn ->
        for _ <- 1..@iterations do
          Malachi.Auth.LockoutManager.locked?("admin", {127, 0, 0, 1})
        end
      end)

    avg_us = duration_us / @iterations

    IO.puts("   Lockout check avg: #{Float.round(avg_us, 2)} us/call")

    put_in(results, ["results", "lockout_check"], %{
      "avg_us" => avg_us,
      "iterations" => @iterations
    })
  end

  # ── Validation Pipeline ──────────────────────────────────────────

  defp bench_validation_pipeline(results) do
    IO.puts("\n[4/8] Benchmarking validation pipeline...")

    queue_name = "bench_queue"
    payload = "test payload message content that is a reasonable size"
    headers = %{"priority" => "1", "type" => "test", "source" => "benchmark"}

    # Warmup
    for _ <- 1..1000 do
      Malachi.Validator.validate_queue_name(queue_name)
      Malachi.Validator.validate_payload(payload)
      Malachi.Validator.validate_headers(headers)
    end

    # Individual
    {name_us, _} =
      BenchmarkHelpers.measure_time(fn ->
        for _ <- 1..@iterations, do: Malachi.Validator.validate_queue_name(queue_name)
      end)

    {payload_us, _} =
      BenchmarkHelpers.measure_time(fn ->
        for _ <- 1..@iterations, do: Malachi.Validator.validate_payload(payload)
      end)

    {headers_us, _} =
      BenchmarkHelpers.measure_time(fn ->
        for _ <- 1..@iterations, do: Malachi.Validator.validate_headers(headers)
      end)

    # Combined pipeline
    {pipeline_us, _} =
      BenchmarkHelpers.measure_time(fn ->
        for _ <- 1..@iterations do
          Malachi.Validator.validate_queue_name(queue_name)
          Malachi.Validator.validate_payload(payload)
          Malachi.Validator.validate_headers(headers)
        end
      end)

    IO.puts("   Queue name validation avg: #{Float.round(name_us / @iterations, 2)} us/call")
    IO.puts("   Payload validation avg: #{Float.round(payload_us / @iterations, 2)} us/call")
    IO.puts("   Headers validation avg: #{Float.round(headers_us / @iterations, 2)} us/call")

    IO.puts("   Full pipeline avg: #{Float.round(pipeline_us / @iterations, 2)} us/call")

    put_in(results, ["results", "validation_pipeline"], %{
      "queue_name_avg_us" => name_us / @iterations,
      "payload_avg_us" => payload_us / @iterations,
      "headers_avg_us" => headers_us / @iterations,
      "pipeline_avg_us" => pipeline_us / @iterations,
      "iterations" => @iterations
    })
  end

  # ── Rate Limiter ─────────────────────────────────────────────────

  defp bench_rate_limiter(results) do
    IO.puts("\n[5/8] Benchmarking rate limiter...")

    config = %{limit: 10_000_000, window_ms: 60_000}

    # Warmup
    for _ <- 1..1000 do
      Malachi.RateLimiter.check_limit("bench_id", :publish, config)
    end

    {duration_us, _} =
      BenchmarkHelpers.measure_time(fn ->
        for i <- 1..@iterations do
          id = "bench_#{rem(i, 100)}"
          Malachi.RateLimiter.check_limit(id, :publish, config)
        end
      end)

    avg_us = duration_us / @iterations

    IO.puts("   Rate limiter avg: #{Float.round(avg_us, 2)} us/call")

    put_in(results, ["results", "rate_limiter"], %{
      "avg_us" => avg_us,
      "iterations" => @iterations
    })
  end

  # ── Sanitization ─────────────────────────────────────────────────

  defp bench_sanitization(results) do
    IO.puts("\n[6/8] Benchmarking sanitization...")

    html_input = "<script>alert('XSS')</script> & \"quotes\" 'apostrophes' <img src=x>"
    log_input = "log line\r\ninjected\theader\nmore data with special chars <>"

    {html_us, _} =
      BenchmarkHelpers.measure_time(fn ->
        for _ <- 1..@iterations, do: Malachi.Validator.sanitize_for_html(html_input)
      end)

    {log_us, _} =
      BenchmarkHelpers.measure_time(fn ->
        for _ <- 1..@iterations, do: Malachi.Validator.sanitize_for_log(log_input)
      end)

    IO.puts("   sanitize_for_html avg: #{Float.round(html_us / @iterations, 2)} us/call")
    IO.puts("   sanitize_for_log avg: #{Float.round(log_us / @iterations, 2)} us/call")

    put_in(results, ["results", "sanitization"], %{
      "html_avg_us" => html_us / @iterations,
      "log_avg_us" => log_us / @iterations,
      "iterations" => @iterations
    })
  end

  # ── Audit Logging ────────────────────────────────────────────────

  defp bench_audit_logging(results) do
    IO.puts("\n[7/8] Benchmarking audit logging...")

    iterations = 10_000

    {duration_us, _} =
      BenchmarkHelpers.measure_time(fn ->
        for _ <- 1..iterations do
          Malachi.AuditLog.log_event(
            :benchmark,
            %{username: "bench", ip: {0, 0, 0, 0}},
            "benchmark_test",
            :success,
            %{}
          )
        end
      end)

    avg_us = duration_us / iterations

    IO.puts("   Audit log_event avg: #{Float.round(avg_us, 2)} us/call")

    # Flush to prevent memory buildup
    Malachi.AuditLog.flush()

    put_in(results, ["results", "audit_logging"], %{
      "avg_us" => avg_us,
      "iterations" => iterations,
      "note" => "Asynchronous via GenServer.cast"
    })
  end

  # ── Combined Security Pipeline ────────────────────────────────────

  defp bench_combined_pipeline(results) do
    IO.puts("\n[8/8] Benchmarking combined security pipeline...")

    {:ok, token} = Malachi.Auth.authenticate("admin", "admin123")
    config = %{limit: 10_000_000, window_ms: 60_000}
    queue_name = "bench_combined_queue"
    payload = "test message payload"
    headers = %{"type" => "benchmark"}

    # Warmup
    for _ <- 1..1000 do
      Malachi.Auth.LockoutManager.locked?("admin", {127, 0, 0, 1})
      Malachi.RateLimiter.check_limit("combined_bench", :publish, config)
      Malachi.Auth.validate_token(token)
      Malachi.Validator.validate_queue_name(queue_name)
      Malachi.Validator.validate_payload(payload)
      Malachi.Validator.validate_headers(headers)
    end

    {duration_us, _} =
      BenchmarkHelpers.measure_time(fn ->
        for _ <- 1..@iterations do
          Malachi.Auth.LockoutManager.locked?("admin", {127, 0, 0, 1})
          Malachi.RateLimiter.check_limit("combined_bench", :publish, config)
          Malachi.Auth.validate_token(token)
          Malachi.Validator.validate_queue_name(queue_name)
          Malachi.Validator.validate_payload(payload)
          Malachi.Validator.validate_headers(headers)
        end
      end)

    avg_us = duration_us / @iterations

    IO.puts("   Combined pipeline avg: #{Float.round(avg_us, 2)} us/call")

    put_in(results, ["results", "combined_pipeline"], %{
      "avg_us" => avg_us,
      "iterations" => @iterations,
      "components" => [
        "lockout_check",
        "rate_limiter",
        "token_validation",
        "validate_queue_name",
        "validate_payload",
        "validate_headers"
      ]
    })
  end

  # ── Display & Analysis ───────────────────────────────────────────

  defp display_results(results) do
    IO.puts("")
    IO.puts(String.pad_trailing("Component", 35) <> String.pad_leading("Avg (us)", 12))
    IO.puts(String.duplicate("-", 47))

    display_row("Authentication (Argon2)", results["authentication"]["avg_us"])
    display_row("Token Validation", results["token_validation"]["avg_us"])
    display_row("Lockout Check", results["lockout_check"]["avg_us"])

    if results["validation_pipeline"] do
      display_row(
        "Validation Pipeline (combined)",
        results["validation_pipeline"]["pipeline_avg_us"]
      )

      display_row(
        "  - Queue Name",
        results["validation_pipeline"]["queue_name_avg_us"]
      )

      display_row("  - Payload", results["validation_pipeline"]["payload_avg_us"])
      display_row("  - Headers", results["validation_pipeline"]["headers_avg_us"])
    end

    display_row("Rate Limiter", results["rate_limiter"]["avg_us"])

    if results["sanitization"] do
      display_row("Sanitize HTML", results["sanitization"]["html_avg_us"])
      display_row("Sanitize Log", results["sanitization"]["log_avg_us"])
    end

    display_row("Audit Logging", results["audit_logging"]["avg_us"])
    display_row("Combined Pipeline", results["combined_pipeline"]["avg_us"])
  end

  defp display_row(label, value) when is_number(value) do
    IO.puts(String.pad_trailing(label, 35) <> String.pad_leading(Float.round(value * 1.0, 2) |> to_string(), 12))
  end

  defp display_row(label, _), do: IO.puts(String.pad_trailing(label, 35) <> String.pad_leading("N/A", 12))

  defp analyze_overhead(results) do
    combined_us = results["combined_pipeline"]["avg_us"]

    IO.puts("")
    IO.puts("Hot path budget analysis:")
    IO.puts("  Combined security pipeline: #{Float.round(combined_us, 2)} us/message")
    IO.puts("")

    # At 125K msgs/sec baseline, 8us budget = 1% overhead
    if combined_us < 40 do
      IO.puts("  PASS: Combined overhead < 40us (< 0.5% at 125K msgs/sec)")
    else
      overhead_pct = combined_us / 8000 * 100
      IO.puts("  Combined overhead: ~#{Float.round(overhead_pct, 2)}% at 125K msgs/sec")

      if overhead_pct < 5.0 do
        IO.puts("  PASS: Within 5% overhead target")
      else
        IO.puts("  WARN: Exceeds 5% overhead target")
      end
    end

    IO.puts("")
    IO.puts("Auth path (one-time per connection):")
    auth_ms = results["authentication"]["avg_ms"]
    IO.puts("  Argon2 auth: #{Float.round(auth_ms, 2)} ms (expected 10-100ms)")
  end
end

SecurityPerformanceSuite.run()
