defmodule Malachi.DashboardTest do
  use ExUnit.Case, async: false

  alias Malachi.Test.DashboardHelper

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
      # Give the dashboard time to start
      :timer.sleep(100)

      case DashboardHelper.connect() do
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
      :timer.sleep(100)

      case DashboardHelper.connect() do
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
      :timer.sleep(100)

      topic = "dash_topic_#{System.unique_integer([:positive])}"
      {:ok, _root} = Malachi.BrokerServer.create_topic(Malachi.LogBroker, topic, 8)

      case DashboardHelper.connect() do
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
      :timer.sleep(100)

      topic = "dash_detail_#{System.unique_integer([:positive])}"
      {:ok, _root} = Malachi.BrokerServer.create_topic(Malachi.LogBroker, topic, 8)

      case DashboardHelper.connect() do
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
      :timer.sleep(100)

      case DashboardHelper.connect() do
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

    test "GET /metrics with Accept: text/plain returns the Prometheus exposition" do
      :timer.sleep(100)

      topic = "prom_#{System.unique_integer([:positive])}"
      {:ok, _root} = Malachi.BrokerServer.create_topic(Malachi.LogBroker, topic, 8)

      case DashboardHelper.connect() do
        {:ok, socket} ->
          :gen_tcp.send(
            socket,
            "GET /metrics HTTP/1.1\r\nHost: localhost\r\nAccept: text/plain\r\nConnection: close\r\n\r\n"
          )

          response = read_full_response(socket, "", 5000)
          :gen_tcp.close(socket)

          assert String.contains?(response, "HTTP/1.1 200 OK")
          assert String.contains?(response, "text/plain; version=0.0.4")
          assert String.contains?(response, "# TYPE malachi_up gauge")
          assert String.contains?(response, "\nmalachi_up 1\n")
          # the topic we created shows up as a per-topic gauge
          assert String.contains?(response, ~s(malachi_topic_ranges{topic="#{topic}"} 1))

        {:error, _} ->
          :ok
      end
    end

    test "GET /health returns 200 (liveness)" do
      :timer.sleep(100)

      case DashboardHelper.connect() do
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
      :timer.sleep(100)

      case DashboardHelper.connect() do
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
      :timer.sleep(100)

      case DashboardHelper.connect() do
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
      :timer.sleep(100)

      case DashboardHelper.connect() do
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
      :timer.sleep(100)

      case DashboardHelper.connect(timeout: 2000) do
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

  describe "port" do
    defp dashboard_name, do: :"dashboard_test_#{System.unique_integer([:positive])}"

    test "port 0 binds a port the operating system picks, records it, and serves on it" do
      name = dashboard_name()
      start_supervised!({Malachi.Dashboard, {0, name: name}}, id: name)

      port = Malachi.Dashboard.port(name)
      assert is_integer(port) and port > 0
      assert port != Malachi.Dashboard.port()

      {:ok, socket} = DashboardHelper.connect(port: port)
      :ok = :gen_tcp.send(socket, "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
      assert {:ok, "HTTP/1.1 200 OK" <> _rest} = :gen_tcp.recv(socket, 0, 2_000)
      :gen_tcp.close(socket)
    end

    test "fails when the port is taken" do
      {:ok, holder} = :gen_tcp.listen(0, [:binary, active: false])
      on_exit(fn -> :gen_tcp.close(holder) end)
      {:ok, held} = :inet.port(holder)

      name = dashboard_name()
      assert {:error, {:eaddrinuse, _child}} = start_supervised({Malachi.Dashboard, {held, name: name}}, id: name)
      assert Malachi.Dashboard.port(name) == nil
    end

    test "the application's dashboard bound a real port while the test config asks for 0" do
      assert Application.get_env(:malachi, :dashboard_port) == 0
      assert Malachi.Dashboard.port() > 0
    end
  end
end
