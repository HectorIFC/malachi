defmodule Malachi.ProtocolFuzzingTest do
  @moduledoc """
  Fuzzing tests for the TCP protocol layer.

  Sends malformed, random, and adversarial binary data over TCP to verify
  the server does not crash, leak memory, or behave unexpectedly.
  Unlike tcp_protocol_test.exs (which tests happy paths) and
  injection_attack_test.exs (which tests validators in isolation),
  this file sends malicious data over actual TCP sockets.
  """
  use ExUnit.Case, async: false

  # SKIP (B3a): exercises the JSON queue/log protocol via socket; to be rewritten against the
  # binary Malachi.Wire protocol in B1b. The underlying infra (Auth/RateLimiter/Validator) stays
  # covered by its own unit tests.
  @moduletag :skip

  alias Malachi.Test.{SecurityHelper, TCPHelper}

  @moduletag :security

  setup do
    # Brief pause between tests to let the server recover resources
    Process.sleep(50)
    :ok
  end

  describe "random binary data before auth" do
    test "server handles random binary without crashing" do
      for _ <- 1..20 do
        {:ok, socket} = TCPHelper.connect()
        data = SecurityHelper.random_binary(:rand.uniform(512))
        :gen_tcp.send(socket, data)

        # Server should close or timeout - not crash
        case :gen_tcp.recv(socket, 0, 2000) do
          {:ok, _response} -> :ok
          {:error, :closed} -> :ok
          {:error, :timeout} -> :ok
          {:error, _other} -> :ok
        end

        :gen_tcp.close(socket)
      end

      # Verify server is still alive by authenticating normally
      {:ok, socket} = TCPHelper.connect()
      {:ok, _token} = TCPHelper.authenticate(socket, "admin", "admin123")
      :gen_tcp.close(socket)
    end

    test "large random binary does not crash server" do
      {:ok, socket} = TCPHelper.connect()

      # Send 64KB of random data
      data = SecurityHelper.random_binary(65_536)
      :gen_tcp.send(socket, data)

      case :gen_tcp.recv(socket, 0, 3000) do
        {:ok, _response} -> :ok
        {:error, _reason} -> :ok
      end

      :gen_tcp.close(socket)

      # Server must still be operational
      {:ok, socket} = TCPHelper.connect()
      {:ok, _token} = TCPHelper.authenticate(socket, "admin", "admin123")
      :gen_tcp.close(socket)
    end
  end

  describe "malformed JSON" do
    test "truncated JSON" do
      {:ok, socket} = TCPHelper.connect()
      TCPHelper.send_line(socket, ~s({"action":"auth"))

      case TCPHelper.recv_line(socket, timeout: 3000) do
        {:ok, response} ->
          assert String.contains?(response, "err") or String.contains?(response, "error")

        {:error, :closed} ->
          :ok

        {:error, :timeout} ->
          :ok
      end

      :gen_tcp.close(socket)
    end

    test "invalid JSON syntax variants" do
      invalid_jsons = [
        ~s({broken json}),
        ~s({"key": }),
        ~s({"key":: "value"}),
        ~s({"key" "value"}),
        ~s([1, 2, 3]),
        ~s("just a string"),
        ~s(null),
        ~s(42),
        ~s(true),
        ~s({,}),
        ~s({"a":{"b":{"c":) <> String.duplicate("{", 50)
      ]

      for json <- invalid_jsons do
        {:ok, socket} = TCPHelper.connect()
        TCPHelper.send_line(socket, json)

        case TCPHelper.recv_line(socket, timeout: 2000) do
          {:ok, _response} -> :ok
          {:error, _reason} -> :ok
        end

        :gen_tcp.close(socket)
      end

      # Server still alive
      {:ok, socket} = TCPHelper.connect()
      {:ok, _token} = TCPHelper.authenticate(socket, "admin", "admin123")
      :gen_tcp.close(socket)
    end

    test "deeply nested JSON" do
      # Create JSON with 200 levels of nesting
      prefix = String.duplicate(~s({"a":), 200)
      suffix = String.duplicate("}", 200)
      # The innermost value
      deep_json = prefix <> ~s("value") <> suffix

      {:ok, socket} = TCPHelper.connect()
      TCPHelper.send_line(socket, deep_json)

      case TCPHelper.recv_line(socket, timeout: 3000) do
        {:ok, _response} -> :ok
        {:error, _reason} -> :ok
      end

      :gen_tcp.close(socket)
    end

    test "JSON with trailing garbage" do
      {:ok, socket} = TCPHelper.connect()

      json_with_garbage =
        ~s({"action":"auth","username":"admin","password":"admin123"}GARBAGE_DATA_HERE)

      TCPHelper.send_line(socket, json_with_garbage)

      case TCPHelper.recv_line(socket, timeout: 3000) do
        {:ok, response} ->
          # Should either parse successfully (ignoring trailing data) or error
          assert is_binary(response)

        {:error, _reason} ->
          :ok
      end

      :gen_tcp.close(socket)
    end
  end

  describe "oversized messages" do
    @tag timeout: 30_000
    test "1MB message line" do
      {:ok, socket} = TCPHelper.connect()
      huge_line = String.duplicate("A", 1_048_576)
      :gen_tcp.send(socket, huge_line <> "\n")

      case :gen_tcp.recv(socket, 0, 10_000) do
        {:ok, _response} -> :ok
        {:error, _reason} -> :ok
      end

      :gen_tcp.close(socket)

      # Server still alive
      {:ok, socket} = TCPHelper.connect()
      {:ok, _token} = TCPHelper.authenticate(socket, "admin", "admin123")
      :gen_tcp.close(socket)
    end

    test "message with many newlines (line splitting attack)" do
      {:ok, socket} = TCPHelper.connect()

      # Send many tiny lines rapidly
      for _ <- 1..1000 do
        :gen_tcp.send(socket, "x\n")
      end

      Process.sleep(500)

      :gen_tcp.close(socket)

      # Server still alive
      {:ok, socket} = TCPHelper.connect()
      {:ok, _token} = TCPHelper.authenticate(socket, "admin", "admin123")
      :gen_tcp.close(socket)
    end
  end

  describe "protocol state attacks" do
    test "publish before authentication is rejected" do
      {:ok, socket} = TCPHelper.connect()

      publish_msg =
        Jason.encode!(%{
          "action" => "publish",
          "queue_name" => "test_queue",
          "payload" => "unauthorized_message"
        })

      TCPHelper.send_line(socket, publish_msg)

      case TCPHelper.recv_line(socket, timeout: 3000) do
        {:ok, response} ->
          decoded = Jason.decode!(String.trim(response))
          # Should indicate error - not authenticated
          assert decoded["s"] == "err" or Map.has_key?(decoded, "error")

        {:error, :closed} ->
          # Server closed the connection (also acceptable)
          :ok
      end

      :gen_tcp.close(socket)
    end

    test "subscribe before authentication is rejected" do
      {:ok, socket} = TCPHelper.connect()

      subscribe_msg =
        Jason.encode!(%{
          "action" => "subscribe",
          "queue_name" => "test_queue"
        })

      TCPHelper.send_line(socket, subscribe_msg)

      case TCPHelper.recv_line(socket, timeout: 3000) do
        {:ok, response} ->
          decoded = Jason.decode!(String.trim(response))
          assert decoded["s"] == "err" or Map.has_key?(decoded, "error")

        {:error, :closed} ->
          :ok
      end

      :gen_tcp.close(socket)
    end

    test "double authentication on same socket" do
      {:ok, socket} = TCPHelper.connect()

      # First auth
      {:ok, token1} = TCPHelper.authenticate(socket, "admin", "admin123")
      assert is_binary(token1)

      # Second auth on same socket
      auth_msg =
        Jason.encode!(%{
          "action" => "auth",
          "username" => "producer",
          "password" => "producer123"
        })

      TCPHelper.send_line(socket, auth_msg)

      case TCPHelper.recv_line(socket, timeout: 3000) do
        {:ok, response} ->
          # Should either succeed (re-auth) or error (already authenticated)
          assert is_binary(response)

        {:error, _} ->
          :ok
      end

      :gen_tcp.close(socket)
    end

    test "unknown action is rejected" do
      {:ok, socket} = TCPHelper.connect()
      {:ok, _token} = TCPHelper.authenticate(socket, "admin", "admin123")

      unknown_msg = Jason.encode!(%{"action" => "hack_the_planet", "data" => "evil"})
      TCPHelper.send_line(socket, unknown_msg)

      case TCPHelper.recv_line(socket, timeout: 3000) do
        {:ok, response} ->
          decoded = Jason.decode!(String.trim(response))
          assert decoded["s"] == "err" or Map.has_key?(decoded, "error")

        {:error, _} ->
          :ok
      end

      :gen_tcp.close(socket)
    end
  end

  describe "slowloris-style partial data" do
    @tag timeout: 45_000
    test "partial JSON message eventually times out" do
      {:ok, socket} = TCPHelper.connect()

      # Send partial data
      :gen_tcp.send(socket, ~s({"action":"au))

      # Wait for server timeout (typically 30s + buffer)
      Process.sleep(35_000)

      # Connection should be closed by server timeout
      case :gen_tcp.send(socket, ~s(th"})) do
        {:error, :closed} -> :ok
        {:error, _} -> :ok
        # If send succeeds, the server may still be accepting data
        :ok -> :ok
      end

      :gen_tcp.close(socket)

      # Server must still be alive
      {:ok, socket} = TCPHelper.connect()
      {:ok, _token} = TCPHelper.authenticate(socket, "admin", "admin123")
      :gen_tcp.close(socket)
    end

    test "multiple partial connections don't exhaust resources" do
      # Open 20 connections and send partial data
      sockets =
        for _ <- 1..20 do
          case TCPHelper.connect(timeout: 2000) do
            {:ok, socket} ->
              :gen_tcp.send(socket, ~s({"act))
              socket

            {:error, _} ->
              nil
          end
        end
        |> Enum.filter(& &1)

      # Server should still accept new connections
      {:ok, socket} = TCPHelper.connect()
      {:ok, _token} = TCPHelper.authenticate(socket, "admin", "admin123")
      :gen_tcp.close(socket)

      # Cleanup partial connections
      Enum.each(sockets, &:gen_tcp.close/1)
    end
  end

  describe "null bytes and control characters via TCP" do
    test "null bytes in auth username" do
      {:ok, socket} = TCPHelper.connect()

      auth_msg =
        Jason.encode!(%{
          "action" => "auth",
          "username" => "admin\x00evil",
          "password" => "admin123"
        })

      TCPHelper.send_line(socket, auth_msg)

      case TCPHelper.recv_line(socket, timeout: 3000) do
        {:ok, response} ->
          decoded = Jason.decode!(String.trim(response))
          # Should either fail auth or sanitize the username
          assert is_map(decoded)

        {:error, _} ->
          :ok
      end

      :gen_tcp.close(socket)
    end

    test "control characters in queue name via TCP" do
      {:ok, socket} = TCPHelper.connect()
      {:ok, _token} = TCPHelper.authenticate(socket, "admin", "admin123")

      # Try to publish with control chars in queue name
      publish_msg =
        Jason.encode!(%{
          "action" => "publish",
          "queue_name" => "queue\x01\x02\x03",
          "payload" => "test"
        })

      TCPHelper.send_line(socket, publish_msg)

      case TCPHelper.recv_line(socket, timeout: 3000) do
        {:ok, response} ->
          decoded = Jason.decode!(String.trim(response))
          # Should be rejected by validation
          assert is_map(decoded)

        {:error, _} ->
          :ok
      end

      :gen_tcp.close(socket)
    end

    test "binary payload with embedded null bytes is handled" do
      {:ok, socket} = TCPHelper.connect()
      {:ok, _token} = TCPHelper.authenticate(socket, "admin", "admin123")

      queue = SecurityHelper.unique_queue_name("nulltest")

      publish_msg =
        Jason.encode!(%{
          "action" => "publish",
          "queue_name" => queue,
          "payload" => "before\x00after"
        })

      TCPHelper.send_line(socket, publish_msg)

      case TCPHelper.recv_line(socket, timeout: 3000) do
        {:ok, response} ->
          # JSON payloads are strings, null bytes may cause issues
          assert is_binary(response)

        {:error, _} ->
          :ok
      end

      :gen_tcp.close(socket)
    end
  end
end
