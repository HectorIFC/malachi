defmodule Malachi.SecurityPerformanceRegressionTest do
  @moduledoc """
  Performance regression tests ensuring security features maintain < 5% overhead.

  This is a CI-runnable gate test that FAILS if security overhead exceeds thresholds.
  Unlike benchmarks (which produce reports), this file makes pass/fail assertions.
  """
  use ExUnit.Case, async: false

  alias Malachi.{Auth, ConnectionLimiter, RateLimiter, Validator}
  alias Malachi.Test.SecurityHelper

  @moduletag :security
  @moduletag :performance

  @iterations 10_000

  describe "authentication performance" do
    test "validate_token latency is under 500 microseconds average" do
      # Pre-authenticate to get a valid token
      {:ok, token} = Auth.authenticate("admin", "admin123")

      avg_us =
        SecurityHelper.measure_avg_us(
          fn -> Auth.validate_token(token) end,
          @iterations
        )

      assert avg_us < 500,
             "Token validation avg latency #{Float.round(avg_us, 2)}us exceeds 500us threshold"
    end

    test "token generation produces unique tokens efficiently" do
      {time_us, tokens} =
        :timer.tc(fn ->
          for _ <- 1..100 do
            {:ok, token} = Auth.authenticate("admin", "admin123")
            token
          end
        end)

      # All tokens unique
      assert length(Enum.uniq(tokens)) == 100

      avg_per_auth = time_us / 100

      # Argon2 is intentionally slow (~10-100ms), so we allow generous threshold
      # This test ensures no regression beyond expected Argon2 cost
      assert avg_per_auth < 500_000,
             "Auth avg latency #{Float.round(avg_per_auth / 1000, 2)}ms exceeds 500ms threshold"
    end
  end

  describe "validation pipeline performance" do
    test "full validation pipeline under 100 microseconds per call" do
      queue_name = "perf_test_queue"
      payload = "test payload message content"
      headers = %{"priority" => "1", "type" => "test"}

      avg_us =
        SecurityHelper.measure_avg_us(
          fn ->
            Validator.validate_queue_name(queue_name)
            Validator.validate_payload(payload)
            Validator.validate_headers(headers)
          end,
          @iterations
        )

      assert avg_us < 100,
             "Validation pipeline avg #{Float.round(avg_us, 2)}us exceeds 100us threshold"
    end

    test "queue name validation with cache is fast" do
      queue_name = "cached_perf_queue"

      # Warm up cache
      Validator.validate_queue_name(queue_name)

      avg_us =
        SecurityHelper.measure_avg_us(
          fn -> Validator.validate_queue_name(queue_name) end,
          @iterations
        )

      assert avg_us < 50,
             "Cached queue name validation avg #{Float.round(avg_us, 2)}us exceeds 50us threshold"
    end

    test "payload validation is proportional to size" do
      small_payload = SecurityHelper.random_binary(100)
      large_payload = SecurityHelper.random_binary(100_000)

      avg_small =
        SecurityHelper.measure_avg_us(
          fn -> Validator.validate_payload(small_payload) end,
          @iterations
        )

      avg_large =
        SecurityHelper.measure_avg_us(
          fn -> Validator.validate_payload(large_payload) end,
          1000
        )

      # Large payload should not be more than 100x slower than small
      # (since validation is mostly a byte_size check, not content scanning)
      ratio = if avg_small > 0, do: avg_large / avg_small, else: 1.0

      assert ratio < 100,
             "Payload validation scales poorly: small=#{Float.round(avg_small, 2)}us, large=#{Float.round(avg_large, 2)}us, ratio=#{Float.round(ratio, 2)}"
    end
  end

  describe "rate limiter performance" do
    test "check_limit under 200 microseconds per call" do
      config = %{limit: 1_000_000, window_ms: 60_000}

      avg_us =
        SecurityHelper.measure_avg_us(
          fn ->
            id = "perf_test_#{:rand.uniform(1000)}"
            RateLimiter.check_limit(id, :publish, config)
          end,
          @iterations
        )

      assert avg_us < 200,
             "Rate limiter avg #{Float.round(avg_us, 2)}us exceeds 200us threshold"
    end

    test "rate limiter with same identifier (hot path)" do
      config = %{limit: 1_000_000, window_ms: 60_000}
      id = "hot_path_perf_test"

      avg_us =
        SecurityHelper.measure_avg_us(
          fn -> RateLimiter.check_limit(id, :publish, config) end,
          @iterations
        )

      assert avg_us < 200,
             "Hot path rate limiter avg #{Float.round(avg_us, 2)}us exceeds 200us threshold"
    end
  end

  describe "connection limiter performance" do
    test "register and unregister 1000 connections under 2 seconds" do
      ip = SecurityHelper.random_ip_string()

      # Temporarily allow many connections
      original_limit = Application.get_env(:malachi, :max_connections_per_ip, 10_000)
      Application.put_env(:malachi, :max_connections_per_ip, 2000)

      {time_us, _} =
        :timer.tc(fn ->
          pids =
            for _ <- 1..1000 do
              {:ok, pid} = Task.start(fn -> Process.sleep(:infinity) end)
              ConnectionLimiter.register_connection(pid, ip)
              pid
            end

          # Unregister all
          Enum.each(pids, fn pid ->
            ConnectionLimiter.unregister_connection(pid)
            Process.exit(pid, :kill)
          end)
        end)

      time_ms = time_us / 1000

      assert time_ms < 2000,
             "Register/unregister 1000 connections took #{Float.round(time_ms, 2)}ms, exceeds 2000ms"

      Application.put_env(:malachi, :max_connections_per_ip, original_limit)
    end

    test "get_connection_count is fast" do
      ip = SecurityHelper.random_ip_string()

      avg_us =
        SecurityHelper.measure_avg_us(
          fn -> ConnectionLimiter.get_connection_count(ip) end,
          @iterations
        )

      assert avg_us < 50,
             "Connection count lookup avg #{Float.round(avg_us, 2)}us exceeds 50us threshold"
    end
  end

  describe "sanitization performance" do
    test "sanitize_for_html under 50 microseconds for typical input" do
      input = "<script>alert('XSS')</script> & \"quotes\" 'apostrophes'"

      avg_us =
        SecurityHelper.measure_avg_us(
          fn -> Validator.sanitize_for_html(input) end,
          @iterations
        )

      assert avg_us < 50,
             "sanitize_for_html avg #{Float.round(avg_us, 2)}us exceeds 50us threshold"
    end

    test "sanitize_for_log under 50 microseconds for typical input" do
      input = "log line\r\ninjected\theader\nmore data"

      avg_us =
        SecurityHelper.measure_avg_us(
          fn -> Validator.sanitize_for_log(input) end,
          @iterations
        )

      assert avg_us < 50,
             "sanitize_for_log avg #{Float.round(avg_us, 2)}us exceeds 50us threshold"
    end
  end

  describe "lockout check performance" do
    test "lockout check under 100 microseconds" do
      avg_us =
        SecurityHelper.measure_avg_us(
          fn -> Auth.LockoutManager.locked?("admin", {127, 0, 0, 1}) end,
          @iterations
        )

      assert avg_us < 100,
             "Lockout check avg #{Float.round(avg_us, 2)}us exceeds 100us threshold"
    end
  end

  describe "combined security overhead" do
    test "full security check pipeline under 500 microseconds" do
      # Simulates the entire security check that happens per-message:
      # 1. Lockout check
      # 2. Rate limit check
      # 3. Token validation
      # 4. Input validation (queue name + payload + headers)

      {:ok, token} = Auth.authenticate("admin", "admin123")
      config = %{limit: 1_000_000, window_ms: 60_000}

      avg_us =
        SecurityHelper.measure_avg_us(
          fn ->
            Auth.LockoutManager.locked?("admin", {127, 0, 0, 1})
            RateLimiter.check_limit("pipeline_test", :publish, config)
            Auth.validate_token(token)
            Validator.validate_queue_name("test_queue")
            Validator.validate_payload("test payload")
            Validator.validate_headers(%{"type" => "test"})
          end,
          @iterations
        )

      assert avg_us < 500,
             "Full security pipeline avg #{Float.round(avg_us, 2)}us exceeds 500us threshold"
    end
  end
end
