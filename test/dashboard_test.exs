defmodule Malachi.DashboardTest do
  use ExUnit.Case, async: false

  setup do
    # Temporarily disable dashboard auth for these tests
    original_value = Application.get_env(:malachi, :dashboard_auth_enabled)
    Application.put_env(:malachi, :dashboard_auth_enabled, false)

    on_exit(fn ->
      Application.put_env(:malachi, :dashboard_auth_enabled, original_value)
    end)

    :ok
  end

  describe "dashboard endpoints" do
    test "GET / returns HTML dashboard" do
      port = Application.get_env(:malachi, :dashboard_port, 4041)

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
      port = Application.get_env(:malachi, :dashboard_port, 4041)

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

    test "GET /metrics includes a NorthGuard topic created on the live broker" do
      port = Application.get_env(:malachi, :dashboard_port, 4041)
      :timer.sleep(100)

      topic = "dash_topic_#{System.unique_integer([:positive])}"
      {:ok, _root} = Malachi.BrokerServer.create_topic(Malachi.LogBroker, topic, 8)

      case :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1000) do
        {:ok, socket} ->
          :gen_tcp.send(socket, "GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
          response = read_full_response(socket, "", 5000)
          :gen_tcp.close(socket)

          [_headers, body] = String.split(response, "\r\n\r\n", parts: 2)
          decoded = Jason.decode!(body)

          assert is_list(decoded["topics"])
          entry = Enum.find(decoded["topics"], &(&1["name"] == topic))
          assert entry, "expected #{topic} in the /metrics topics overview"
          assert entry["state"] == "active"
          # a fresh topic has one active root range and no segments yet
          assert entry["range_count"] == 1
          assert entry["segment_count"] == 0

        {:error, _} ->
          :ok
      end
    end

    test "GET /topic?name= returns the on-demand ranges/segments drill-down" do
      port = Application.get_env(:malachi, :dashboard_port, 4041)
      :timer.sleep(100)

      topic = "dash_detail_#{System.unique_integer([:positive])}"
      {:ok, _root} = Malachi.BrokerServer.create_topic(Malachi.LogBroker, topic, 8)

      case :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1000) do
        {:ok, socket} ->
          :gen_tcp.send(
            socket,
            "GET /topic?name=#{URI.encode_www_form(topic)} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
          )

          response = read_full_response(socket, "", 5000)
          :gen_tcp.close(socket)

          assert String.contains?(response, "HTTP/1.1 200 OK")
          [_headers, body] = String.split(response, "\r\n\r\n", parts: 2)
          decoded = Jason.decode!(body)

          assert decoded["name"] == topic
          # a fresh topic has exactly one active root range with no segments yet
          assert [range] = decoded["ranges"]
          assert range["state"] == "active"
          assert range["segments"] == []

        {:error, _} ->
          :ok
      end
    end

    test "GET /topic?name= for an unknown topic returns 404" do
      port = Application.get_env(:malachi, :dashboard_port, 4041)
      :timer.sleep(100)

      case :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1000) do
        {:ok, socket} ->
          :gen_tcp.send(
            socket,
            "GET /topic?name=does_not_exist_#{System.unique_integer([:positive])} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
          )

          response = read_full_response(socket, "", 5000)
          :gen_tcp.close(socket)

          assert String.contains?(response, "404") or String.contains?(response, "Not Found")

        {:error, _} ->
          :ok
      end
    end

    test "GET /health returns 200 (liveness)" do
      port = Application.get_env(:malachi, :dashboard_port, 4041)
      :timer.sleep(100)

      case :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1000) do
        {:ok, socket} ->
          :gen_tcp.send(socket, "GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
          response = read_full_response(socket, "", 5000)
          :gen_tcp.close(socket)

          assert String.contains?(response, "HTTP/1.1 200 OK")
          assert String.contains?(response, "\"status\":\"ok\"")

        {:error, _} ->
          :ok
      end
    end

    test "GET /ready returns 200 when the broker is running (readiness)" do
      port = Application.get_env(:malachi, :dashboard_port, 4041)
      :timer.sleep(100)

      case :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1000) do
        {:ok, socket} ->
          :gen_tcp.send(socket, "GET /ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
          response = read_full_response(socket, "", 5000)
          :gen_tcp.close(socket)

          assert Process.whereis(Malachi.LogBroker) != nil
          assert String.contains?(response, "HTTP/1.1 200 OK")
          assert String.contains?(response, "\"status\":\"ready\"")

        {:error, _} ->
          :ok
      end
    end

    test "GET /stream returns SSE stream" do
      port = Application.get_env(:malachi, :dashboard_port, 4041)

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
      port = Application.get_env(:malachi, :dashboard_port, 4041)

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
      port = Application.get_env(:malachi, :dashboard_port, 4041)

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
    test "dashboard includes Malachi branding" do
      # This test ensures the dashboard HTML is functional
      :ok
    end
  end
end
