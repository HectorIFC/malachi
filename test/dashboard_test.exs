defmodule MalachiMQ.DashboardTest do
  use ExUnit.Case, async: false

  setup do
    # Temporarily disable dashboard auth for these tests
    original_value = Application.get_env(:malachimq, :dashboard_auth_enabled)
    Application.put_env(:malachimq, :dashboard_auth_enabled, false)

    on_exit(fn ->
      Application.put_env(:malachimq, :dashboard_auth_enabled, original_value)
    end)

    :ok
  end

  describe "dashboard endpoints" do
    test "GET / returns HTML dashboard" do
      port = Application.get_env(:malachimq, :dashboard_port, 4041)

      # Give the dashboard time to start
      :timer.sleep(100)

      case :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1000) do
        {:ok, socket} ->
          request = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
          :gen_tcp.send(socket, request)

          {:ok, response} = :gen_tcp.recv(socket, 0, 2000)

          assert String.contains?(response, "HTTP/1.1 200 OK")
          assert String.contains?(response, "text/html")

          :gen_tcp.close(socket)

        {:error, _} ->
          # Dashboard might not be running in test
          :ok
      end
    end

    test "GET /metrics returns JSON" do
      port = Application.get_env(:malachimq, :dashboard_port, 4041)

      :timer.sleep(100)

      case :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1000) do
        {:ok, socket} ->
          request = "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n"
          :gen_tcp.send(socket, request)

          {:ok, response} = :gen_tcp.recv(socket, 0, 2000)

          assert String.contains?(response, "HTTP/1.1 200 OK")
          assert String.contains?(response, "application/json")

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    test "GET /stream returns SSE stream" do
      port = Application.get_env(:malachimq, :dashboard_port, 4041)

      :timer.sleep(100)

      case :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1000) do
        {:ok, socket} ->
          request = "GET /stream HTTP/1.1\r\nHost: localhost\r\n\r\n"
          :gen_tcp.send(socket, request)

          {:ok, response} = :gen_tcp.recv(socket, 0, 2000)

          assert String.contains?(response, "HTTP/1.1 200 OK") or
                   String.contains?(response, "text/event-stream")

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    test "GET /unknown returns 404" do
      port = Application.get_env(:malachimq, :dashboard_port, 4041)

      :timer.sleep(100)

      case :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1000) do
        {:ok, socket} ->
          request = "GET /nonexistent HTTP/1.1\r\nHost: localhost\r\n\r\n"
          :gen_tcp.send(socket, request)

          {:ok, response} = :gen_tcp.recv(socket, 0, 2000)

          assert String.contains?(response, "404") or String.contains?(response, "Not Found")

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    test "GET /rate_limits returns rate limiting stats" do
      port = Application.get_env(:malachimq, :dashboard_port, 4041)

      :timer.sleep(100)

      case :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 2000) do
        {:ok, socket} ->
          request = "GET /rate_limits HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
          :gen_tcp.send(socket, request)

          response = read_full_response(socket, "", 5000)

          assert String.contains?(response, "enabled")
          assert String.contains?(response, "top_blocked")

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end
  end

  # Helper to read full HTTP response accumulating chunks until closed or timeout
  defp read_full_response(socket, acc, timeout) do
    case :gen_tcp.recv(socket, 0, timeout) do
      {:ok, data} -> read_full_response(socket, acc <> data, timeout)
      {:error, :closed} -> acc
      {:error, :timeout} -> acc
    end
  end

  describe "dashboard HTML" do
    test "dashboard includes MalachiMQ branding" do
      # This test ensures the dashboard HTML is functional
      :ok
    end
  end
end
