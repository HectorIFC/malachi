defmodule Malachi.RateLimitingIntegrationTest do
  use ExUnit.Case, async: false

  # SKIP (B3a): exercises the JSON queue/log protocol via socket; to be rewritten against the
  # binary Malachi.Wire protocol in B1b. The underlying infra (Auth/RateLimiter/Validator) stays
  # covered by its own unit tests.
  @moduletag :skip

  @tcp_port Application.compile_env(:malachi, :tcp_port, 4040)

  setup do
    # Save original config values
    original_config = %{
      rate_limit_enabled: Application.get_env(:malachi, :rate_limit_enabled),
      connection_limit_enabled: Application.get_env(:malachi, :connection_limit_enabled),
      dashboard_auth_enabled: Application.get_env(:malachi, :dashboard_auth_enabled),
      auth_rate_limit: Application.get_env(:malachi, :auth_rate_limit),
      auth_rate_window_ms: Application.get_env(:malachi, :auth_rate_window_ms),
      publish_rate_limit: Application.get_env(:malachi, :publish_rate_limit),
      publish_rate_window_ms: Application.get_env(:malachi, :publish_rate_window_ms),
      subscribe_rate_limit: Application.get_env(:malachi, :subscribe_rate_limit),
      subscribe_rate_window_ms: Application.get_env(:malachi, :subscribe_rate_window_ms),
      max_connections_per_ip: Application.get_env(:malachi, :max_connections_per_ip),
      max_total_connections: Application.get_env(:malachi, :max_total_connections)
    }

    # Enable rate limiting and connection limiting with low limits for testing
    Application.put_env(:malachi, :rate_limit_enabled, true)
    Application.put_env(:malachi, :connection_limit_enabled, true)
    Application.put_env(:malachi, :dashboard_auth_enabled, false)
    Application.put_env(:malachi, :auth_rate_limit, 5)
    Application.put_env(:malachi, :auth_rate_window_ms, 60_000)
    Application.put_env(:malachi, :publish_rate_limit, 10)
    Application.put_env(:malachi, :publish_rate_window_ms, 1_000)
    Application.put_env(:malachi, :subscribe_rate_limit, 5)
    Application.put_env(:malachi, :subscribe_rate_window_ms, 60_000)
    Application.put_env(:malachi, :max_connections_per_ip, 20)
    Application.put_env(:malachi, :max_total_connections, 10_000)

    # Reset any existing rate limit buckets to avoid cross-test contamination
    Malachi.RateLimiter.reset_bucket("127.0.0.1", :auth)
    Malachi.RateLimiter.reset_bucket("producer", :publish)
    Malachi.RateLimiter.reset_bucket("consumer", :subscribe)

    on_exit(fn ->
      # Reset rate limit buckets
      Malachi.RateLimiter.reset_bucket("127.0.0.1", :auth)
      Malachi.RateLimiter.reset_bucket("producer", :publish)
      Malachi.RateLimiter.reset_bucket("consumer", :subscribe)

      # Restore original config values
      for {key, value} <- original_config do
        if value == nil do
          Application.delete_env(:malachi, key)
        else
          Application.put_env(:malachi, key, value)
        end
      end
    end)

    :ok
  end

  # Helper: connect and authenticate, returns socket
  defp connect_and_auth(username, password) do
    {:ok, socket} =
      :gen_tcp.connect({127, 0, 0, 1}, @tcp_port, [:binary, active: false, packet: :line], 5000)

    auth_msg =
      Jason.encode!(%{"action" => "auth", "username" => username, "password" => password}) <> "\n"

    :gen_tcp.send(socket, auth_msg)
    {:ok, response} = :gen_tcp.recv(socket, 0, 5000)
    decoded = Jason.decode!(String.trim(response))

    case decoded["s"] do
      "ok" -> {:ok, socket}
      "err" -> {:error, decoded["reason"], socket}
    end
  end

  # Helper: attempt auth on a fresh connection, returns {response_map, socket}
  defp attempt_auth(username, password) do
    {:ok, socket} =
      :gen_tcp.connect({127, 0, 0, 1}, @tcp_port, [:binary, active: false, packet: :line], 5000)

    auth_msg =
      Jason.encode!(%{"action" => "auth", "username" => username, "password" => password}) <> "\n"

    :gen_tcp.send(socket, auth_msg)

    case :gen_tcp.recv(socket, 0, 5000) do
      {:ok, response} ->
        decoded = Jason.decode!(String.trim(response))
        {decoded, socket}

      {:error, :closed} ->
        # Connection was rejected (e.g., connection limit)
        {%{"s" => "err", "reason" => "connection_closed"}, socket}
    end
  end

  describe "authentication rate limiting" do
    test "blocks excessive auth attempts from same IP" do
      # Auth rate limit is set to 5 in setup
      # Each failed auth attempt uses a separate connection since the protocol
      # closes the connection after auth failure
      sockets = []

      # First 5 auth attempts should get invalid_credentials (not rate limited)
      sockets =
        for i <- 1..5, reduce: sockets do
          acc ->
            {decoded, socket} = attempt_auth("user#{i}", "wrongpass")
            assert decoded["s"] == "err", "Attempt #{i} should fail auth"

            assert decoded["reason"] == "invalid_credentials",
                   "Attempt #{i} should get invalid_credentials, got: #{decoded["reason"]}"

            [socket | acc]
        end

      # 6th attempt should be rate limited
      {decoded, socket} = attempt_auth("user6", "wrongpass")
      sockets = [socket | sockets]

      assert decoded["s"] == "err"
      assert decoded["reason"] == "rate_limit_exceeded"

      # Cleanup
      Enum.each(sockets, fn s -> :gen_tcp.close(s) end)

      # Reset rate limit bucket for the test IP
      Malachi.RateLimiter.reset_bucket("127.0.0.1", :auth)
    end
  end

  describe "publish rate limiting" do
    test "blocks excessive publish requests" do
      # Publish rate limit is 10 per 1 second window
      {:ok, socket} = connect_and_auth("producer", "producer123")

      queue_name = "test_rate_limit_queue_#{:rand.uniform(1_000_000)}"

      # Publish up to limit (10)
      for i <- 1..10 do
        publish_msg =
          Jason.encode!(%{
            "action" => "publish",
            "queue_name" => queue_name,
            "payload" => "message #{i}"
          }) <> "\n"

        :gen_tcp.send(socket, publish_msg)
        {:ok, response} = :gen_tcp.recv(socket, 0, 5000)
        decoded = Jason.decode!(String.trim(response))
        assert decoded["s"] == "ok", "Message #{i} should succeed, got: #{inspect(decoded)}"
      end

      # Next publish should be rate limited
      publish_msg =
        Jason.encode!(%{
          "action" => "publish",
          "queue_name" => queue_name,
          "payload" => "message 11"
        }) <> "\n"

      :gen_tcp.send(socket, publish_msg)
      {:ok, response} = :gen_tcp.recv(socket, 0, 5000)
      decoded = Jason.decode!(String.trim(response))

      assert decoded["s"] == "err"
      assert decoded["reason"] == "rate_limit_exceeded"
      assert is_integer(decoded["retry_after_ms"])

      :gen_tcp.close(socket)

      # Reset rate limit bucket
      Malachi.RateLimiter.reset_bucket("producer", :publish)
    end

    test "publish rate limit resets after window" do
      {:ok, socket} = connect_and_auth("producer", "producer123")

      queue_name = "test_reset_queue_#{:rand.uniform(1_000_000)}"

      # Exhaust limit (10)
      for i <- 1..10 do
        msg =
          Jason.encode!(%{
            "action" => "publish",
            "queue_name" => queue_name,
            "payload" => "msg#{i}"
          }) <> "\n"

        :gen_tcp.send(socket, msg)
        {:ok, _} = :gen_tcp.recv(socket, 0, 5000)
      end

      # Should be rate limited
      msg =
        Jason.encode!(%{
          "action" => "publish",
          "queue_name" => queue_name,
          "payload" => "blocked"
        }) <> "\n"

      :gen_tcp.send(socket, msg)
      {:ok, response} = :gen_tcp.recv(socket, 0, 5000)
      assert Jason.decode!(String.trim(response))["reason"] == "rate_limit_exceeded"

      # Wait for window to pass (1 second + buffer)
      Process.sleep(1500)

      # Should work again
      msg =
        Jason.encode!(%{
          "action" => "publish",
          "queue_name" => queue_name,
          "payload" => "allowed"
        }) <> "\n"

      :gen_tcp.send(socket, msg)
      {:ok, response} = :gen_tcp.recv(socket, 0, 5000)
      decoded = Jason.decode!(String.trim(response))
      assert decoded["s"] == "ok"

      :gen_tcp.close(socket)

      # Reset
      Malachi.RateLimiter.reset_bucket("producer", :publish)
    end
  end

  describe "subscribe rate limiting" do
    test "blocks excessive subscribe requests" do
      # Subscribe rate limit is 5 per 60s window
      {:ok, socket} = connect_and_auth("consumer", "consumer123")

      # Subscribe to different queues up to limit (5)
      for i <- 1..5 do
        queue_name = "sub_queue_#{i}_#{:rand.uniform(1_000_000)}"

        subscribe_msg =
          Jason.encode!(%{
            "action" => "subscribe",
            "queue_name" => queue_name
          }) <> "\n"

        :gen_tcp.send(socket, subscribe_msg)
        {:ok, response} = :gen_tcp.recv(socket, 0, 5000)
        decoded = Jason.decode!(String.trim(response))
        assert decoded["s"] == "ok", "Subscribe #{i} should succeed, got: #{inspect(decoded)}"
      end

      # 6th subscribe should be rate limited
      subscribe_msg =
        Jason.encode!(%{
          "action" => "subscribe",
          "queue_name" => "sub_queue_6_#{:rand.uniform(1_000_000)}"
        }) <> "\n"

      :gen_tcp.send(socket, subscribe_msg)
      {:ok, response} = :gen_tcp.recv(socket, 0, 5000)
      decoded = Jason.decode!(String.trim(response))

      assert decoded["s"] == "err"
      assert decoded["reason"] == "rate_limit_exceeded"

      :gen_tcp.close(socket)

      # Reset
      Malachi.RateLimiter.reset_bucket("consumer", :subscribe)
    end
  end

  describe "connection limiting" do
    test "blocks connections exceeding per-IP limit" do
      # Max connections per IP set to 20 in setup
      # Raise auth rate limit so 20+ successful auths aren't blocked
      Application.put_env(:malachi, :auth_rate_limit, 100)
      Malachi.RateLimiter.reset_bucket("127.0.0.1", :auth)

      # Create 20 connections
      sockets =
        for i <- 1..20 do
          {:ok, socket} =
            :gen_tcp.connect(
              {127, 0, 0, 1},
              @tcp_port,
              [:binary, active: false, packet: :line],
              5000
            )

          # Authenticate each
          auth_msg =
            Jason.encode!(%{
              "action" => "auth",
              "username" => "app",
              "password" => "app123"
            }) <> "\n"

          :gen_tcp.send(socket, auth_msg)
          {:ok, response} = :gen_tcp.recv(socket, 0, 5000)

          assert Jason.decode!(String.trim(response))["s"] == "ok",
                 "Connection #{i} auth failed"

          socket
        end

      # 21st connection should be rejected
      case :gen_tcp.connect(
             {127, 0, 0, 1},
             @tcp_port,
             [:binary, active: false, packet: :line],
             2000
           ) do
        {:ok, socket} ->
          # Connection accepted at TCP level, but should get error on auth or before
          auth_msg =
            Jason.encode!(%{
              "action" => "auth",
              "username" => "app",
              "password" => "app123"
            }) <> "\n"

          :gen_tcp.send(socket, auth_msg)

          case :gen_tcp.recv(socket, 0, 2000) do
            {:ok, response} ->
              decoded = Jason.decode!(String.trim(response))
              assert decoded["s"] == "err"

              assert decoded["reason"] in [
                       "connection_limit_exceeded",
                       "global_limit_exceeded"
                     ]

            {:error, :closed} ->
              # Connection closed immediately - acceptable behavior
              :ok
          end

          :gen_tcp.close(socket)

        {:error, _} ->
          # Connection refused - acceptable behavior
          :ok
      end

      # Cleanup
      Enum.each(sockets, &:gen_tcp.close/1)
    end
  end

  describe "metrics tracking" do
    test "tracks rate limit blocks in metrics" do
      initial_metrics = Malachi.Metrics.get_system_metrics()
      initial_auth_blocks = get_in(initial_metrics, [:rate_limiting, :auth_blocked]) || 0

      # Exhaust auth limit (5 attempts) using separate connections
      sockets =
        for i <- 1..5 do
          {_decoded, socket} = attempt_auth("metricsuser#{i}", "wrong")
          socket
        end

      # Trigger 3 blocks (attempts 6-8)
      block_sockets =
        for i <- 6..8 do
          {decoded, socket} = attempt_auth("metricsuser#{i}", "wrong")

          assert decoded["reason"] == "rate_limit_exceeded",
                 "Attempt #{i} should be rate limited, got: #{inspect(decoded)}"

          socket
        end

      # Check metrics increased
      new_metrics = Malachi.Metrics.get_system_metrics()
      new_auth_blocks = get_in(new_metrics, [:rate_limiting, :auth_blocked]) || 0

      assert new_auth_blocks > initial_auth_blocks

      # Cleanup
      Enum.each(sockets ++ block_sockets, fn s -> :gen_tcp.close(s) end)
      Malachi.RateLimiter.reset_bucket("127.0.0.1", :auth)
    end
  end

  describe "dashboard /rate_limits endpoint" do
    test "returns top blocked identifiers" do
      dashboard_port = Application.get_env(:malachi, :dashboard_port, 4041)

      # Trigger some auth blocks first (5 to exhaust, then extras to block)
      sockets =
        for i <- 1..7 do
          {_decoded, socket} = attempt_auth("dashuser#{i}", "wrong")
          socket
        end

      Enum.each(sockets, fn s -> :gen_tcp.close(s) end)

      # Small delay to let metrics update
      Process.sleep(100)

      # Query dashboard endpoint
      {:ok, dash_socket} =
        :gen_tcp.connect({127, 0, 0, 1}, dashboard_port, [:binary, active: false], 5000)

      request = "GET /rate_limits HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
      :gen_tcp.send(dash_socket, request)

      # Read response (may come in multiple chunks)
      response = read_full_response(dash_socket, "", 5000)

      assert String.contains?(response, "top_blocked")
      assert String.contains?(response, "enabled")

      :gen_tcp.close(dash_socket)

      # Reset
      Malachi.RateLimiter.reset_bucket("127.0.0.1", :auth)
    end
  end

  # Helper: read full HTTP response, accumulating chunks
  defp read_full_response(socket, acc, timeout) do
    case :gen_tcp.recv(socket, 0, timeout) do
      {:ok, data} ->
        read_full_response(socket, acc <> data, timeout)

      {:error, :closed} ->
        acc

      {:error, :timeout} ->
        acc
    end
  end
end
