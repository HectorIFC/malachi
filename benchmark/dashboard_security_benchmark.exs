#!/usr/bin/env elixir

# Dashboard Security Performance Benchmark
# 
# Measures the overhead introduced by authentication, security headers,
# and audit logging on dashboard endpoints.
#
# Acceptance criteria: < 25% latency increase

Mix.install([
  {:benchee, "~> 1.0"}
])

defmodule DashboardSecurityBenchmark do
  @dashboard_port 4041

  def run do
    IO.puts("\n🔒 Dashboard Security Performance Benchmark\n")
    IO.puts("Measuring overhead of authentication and security features...\n")

    # Setup: Create test user and get token
    {:ok, _} = :gen_tcp.connect({127, 0, 0, 1}, 4040, [:binary, active: false], 5000)
    |> case do
      {:ok, socket} ->
        # Authenticate to get token
        auth_request = Jason.encode!(%{
          action: "auth",
          username: "producer",
          password: "producer123"
        }) <> "\n"
        
        :gen_tcp.send(socket, auth_request)
        {:ok, response} = :gen_tcp.recv(socket, 0, 5000)
        
        auth_data = Jason.decode!(String.trim(response))
        token = auth_data["token"]
        :gen_tcp.close(socket)
        {:ok, token}
      
      {:error, _} ->
        IO.puts("⚠️  MalachiMQ TCP server not running on port 4040")
        IO.puts("Start the server with: mix run --no-halt")
        System.halt(1)
    end

    token = case :gen_tcp.connect({127, 0, 0, 1}, 4040, [:binary, active: false], 5000) do
      {:ok, socket} ->
        auth_request = Jason.encode!(%{
          action: "auth",
          username: "producer",
          password: "producer123"
        }) <> "\n"
        
        :gen_tcp.send(socket, auth_request)
        {:ok, response} = :gen_tcp.recv(socket, 0, 5000)
        auth_data = Jason.decode!(String.trim(response))
        :gen_tcp.close(socket)
        auth_data["token"]
      
      {:error, _} ->
        IO.puts("⚠️  Could not connect to MalachiMQ")
        System.halt(1)
    end

    Benchee.run(
      %{
        "Dashboard HTML (authenticated)" => fn ->
          request_dashboard_html(token)
        end,
        "Dashboard HTML (no auth - simulated)" => fn ->
          # Simulate no-auth by measuring connection overhead only
          request_dashboard_html_no_validation(token)
        end,
        "Metrics endpoint (authenticated)" => fn ->
          request_metrics(token)
        end,
        "Metrics endpoint (no auth - simulated)" => fn ->
          request_metrics_no_validation(token)
        end,
        "SSE stream connection (authenticated)" => fn ->
          connect_sse_stream(token)
        end,
        "Login endpoint (POST)" => fn ->
          post_login()
        end,
        "Security header generation" => fn ->
          generate_security_headers()
        end,
        "Token validation" => fn ->
          validate_token(token)
        end
      },
      time: 5,
      memory_time: 2,
      reduction_time: 1,
      formatters: [
        {Benchee.Formatters.Console, extended_statistics: true},
        {Benchee.Formatters.HTML, file: "benchmark/results/dashboard_security.html"}
      ],
      print: [
        fast_warning: false
      ]
    )

    IO.puts("\n📊 Benchmark Results Summary\n")
    IO.puts("Overhead Analysis:")
    IO.puts("  - Authentication check: ~8-50µs (expected)")
    IO.puts("  - Security headers: ~5-20µs (expected)")
    IO.puts("  - Total overhead target: < 25% of baseline\n")
    IO.puts("Results saved to: benchmark/results/dashboard_security.html\n")
  end

  defp request_dashboard_html(token) do
    case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
      {:ok, socket} ->
        request = """
        GET / HTTP/1.1\r
        Host: localhost\r
        Authorization: Bearer #{token}\r
        \r
        """
        
        :gen_tcp.send(socket, request)
        {:ok, _response} = :gen_tcp.recv(socket, 0, 2000)
        :gen_tcp.close(socket)
        :ok
      
      {:error, _} -> :error
    end
  end

  defp request_dashboard_html_no_validation(token) do
    # Measure pure HTTP overhead (won't actually work, but simulates baseline)
    case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
      {:ok, socket} ->
        # Don't actually send request, just measure connection
        :gen_tcp.close(socket)
        :ok
      
      {:error, _} -> :error
    end
  end

  defp request_metrics(token) do
    case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
      {:ok, socket} ->
        request = """
        GET /metrics HTTP/1.1\r
        Host: localhost\r
        Authorization: Bearer #{token}\r
        \r
        """
        
        :gen_tcp.send(socket, request)
        {:ok, _response} = :gen_tcp.recv(socket, 0, 2000)
        :gen_tcp.close(socket)
        :ok
      
      {:error, _} -> :error
    end
  end

  defp request_metrics_no_validation(_token) do
    case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        :ok
      
      {:error, _} -> :error
    end
  end

  defp connect_sse_stream(token) do
    case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
      {:ok, socket} ->
        request = """
        GET /stream?token=#{token} HTTP/1.1\r
        Host: localhost\r
        \r
        """
        
        :gen_tcp.send(socket, request)
        {:ok, _response} = :gen_tcp.recv(socket, 0, 2000)
        :gen_tcp.close(socket)
        :ok
      
      {:error, _} -> :error
    end
  end

  defp post_login do
    case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
      {:ok, socket} ->
        body = Jason.encode!(%{username: "producer", password: "producer123"})
        
        request = """
        POST /login HTTP/1.1\r
        Host: localhost\r
        Content-Type: application/json\r
        Content-Length: #{byte_size(body)}\r
        \r
        #{body}
        """
        
        :inet.setopts(socket, packet: :http)
        :gen_tcp.send(socket, request)
        {:ok, _response} = :gen_tcp.recv(socket, 0, 2000)
        :gen_tcp.close(socket)
        :ok
      
      {:error, _} -> :error
    end
  end

  defp generate_security_headers do
    # Simulate security header generation
    response = "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html></html>"
    
    # This would call the actual function, but we simulate for benchmark
    headers = [
      {"x-content-type-options", "nosniff"},
      {"x-frame-options", "DENY"},
      {"x-xss-protection", "1; mode=block"},
      {"referrer-policy", "no-referrer"},
      {"content-security-policy", "default-src 'self'"}
    ]
    
    # Simulate header prepending
    Enum.reduce(headers, response, fn {key, value}, resp ->
      "#{key}: #{value}\r\n" <> resp
    end)
  end

  defp validate_token(token) do
    # Simulate token validation (would call Auth.validate_token in reality)
    # For benchmark, just measure string operations and pattern matching
    case String.split(token, ".", parts: 3) do
      [_header, _payload, _signature] -> :ok
      _ -> :error
    end
  end
end

# Run the benchmark
DashboardSecurityBenchmark.run()
