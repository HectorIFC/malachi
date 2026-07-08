defmodule Malachi.ValidationIntegrationTest do
  use ExUnit.Case, async: false

  # SKIP (B3a): exercises the JSON queue/log protocol via socket; to be rewritten against the
  # binary Malachi.Wire protocol in B1b. The underlying infra (Auth/RateLimiter/Validator) stays
  # covered by its own unit tests.
  @moduletag :skip

  alias Malachi.Test.TCPHelper

  @tcp_port Application.compile_env(:malachi, :tcp_port, 4040)

  setup do
    # Give time for app to start
    Process.sleep(100)

    # Connect and authenticate
    {:ok, socket} = TCPHelper.connect(port: @tcp_port)

    auth_request =
      Jason.encode!(%{
        "action" => "auth",
        "username" => "producer",
        "password" => "producer123"
      })

    TCPHelper.send_line(socket, auth_request)
    {:ok, _response} = TCPHelper.recv_line(socket, timeout: 2000)

    on_exit(fn ->
      :gen_tcp.close(socket)
    end)

    %{socket: socket}
  end

  describe "queue name validation via TCP" do
    test "accepts valid queue name", %{socket: socket} do
      request =
        Jason.encode!(%{
          "action" => "publish",
          "queue_name" => "valid.queue-name_123",
          "payload" => "test"
        })

      TCPHelper.send_line(socket, request)
      {:ok, response} = TCPHelper.recv_line(socket, timeout: 1000)

      {:ok, decoded} = Jason.decode(response)
      assert decoded["s"] == "ok"
    end

    test "rejects queue name with spaces", %{socket: socket} do
      request =
        Jason.encode!(%{
          "action" => "publish",
          "queue_name" => "my queue",
          "payload" => "test"
        })

      TCPHelper.send_line(socket, request)
      {:ok, response} = TCPHelper.recv_line(socket, timeout: 1000)

      {:ok, decoded} = Jason.decode(response)
      assert decoded["s"] == "err"
      assert decoded["reason"] == "invalid_queue_name_invalid_characters"
    end

    test "rejects queue name exceeding 255 characters", %{socket: socket} do
      long_name = String.duplicate("x", 256)

      request =
        Jason.encode!(%{
          "action" => "publish",
          "queue_name" => long_name,
          "payload" => "test"
        })

      TCPHelper.send_line(socket, request)
      {:ok, response} = TCPHelper.recv_line(socket, timeout: 1000)

      {:ok, decoded} = Jason.decode(response)
      assert decoded["s"] == "err"
      assert decoded["reason"] == "invalid_queue_name_too_long"
    end

    test "rejects reserved queue name", %{socket: socket} do
      request =
        Jason.encode!(%{
          "action" => "publish",
          "queue_name" => "system",
          "payload" => "test"
        })

      TCPHelper.send_line(socket, request)
      {:ok, response} = TCPHelper.recv_line(socket, timeout: 1000)

      {:ok, decoded} = Jason.decode(response)
      assert decoded["s"] == "err"
      assert decoded["reason"] == "invalid_queue_name_reserved"
    end

    test "rejects queue name with underscore prefix", %{socket: socket} do
      request =
        Jason.encode!(%{
          "action" => "publish",
          "queue_name" => "_internal",
          "payload" => "test"
        })

      TCPHelper.send_line(socket, request)
      {:ok, response} = TCPHelper.recv_line(socket, timeout: 1000)

      {:ok, decoded} = Jason.decode(response)
      assert decoded["s"] == "err"
      assert decoded["reason"] == "invalid_queue_name_reserved"
    end
  end

  describe "payload validation via TCP" do
    test "accepts empty payload", %{socket: socket} do
      request =
        Jason.encode!(%{
          "action" => "publish",
          "queue_name" => "test_queue",
          "payload" => ""
        })

      TCPHelper.send_line(socket, request)
      {:ok, response} = TCPHelper.recv_line(socket, timeout: 1000)

      {:ok, decoded} = Jason.decode(response)
      assert decoded["s"] == "ok"
    end

    test "accepts 1MB payload", %{socket: socket} do
      payload = String.duplicate("x", 1_048_576)

      request =
        Jason.encode!(%{
          "action" => "publish",
          "queue_name" => "test_queue",
          "payload" => payload
        })

      TCPHelper.send_line(socket, request)
      {:ok, response} = TCPHelper.recv_line(socket, timeout: 2000)

      {:ok, decoded} = Jason.decode(response)
      assert decoded["s"] == "ok"
    end

    # Note: Testing 10MB+ payloads via JSON is impractical due to encoding overhead
    # This is covered in the unit tests with raw binary data
  end

  describe "headers validation via TCP" do
    test "accepts valid headers", %{socket: socket} do
      request =
        Jason.encode!(%{
          "action" => "publish",
          "queue_name" => "test_queue",
          "payload" => "test",
          "headers" => %{
            "priority" => 1,
            "type" => "order",
            "urgent" => true
          }
        })

      TCPHelper.send_line(socket, request)
      {:ok, response} = TCPHelper.recv_line(socket, timeout: 1000)

      {:ok, decoded} = Jason.decode(response)
      assert decoded["s"] == "ok"
    end

    test "rejects headers with nested maps", %{socket: socket} do
      request =
        Jason.encode!(%{
          "action" => "publish",
          "queue_name" => "test_queue",
          "payload" => "test",
          "headers" => %{
            "nested" => %{"x" => 1}
          }
        })

      TCPHelper.send_line(socket, request)
      {:ok, response} = TCPHelper.recv_line(socket, timeout: 1000)

      {:ok, decoded} = Jason.decode(response)
      assert decoded["s"] == "err"
      assert String.contains?(decoded["reason"], "invalid_headers")
    end

    test "rejects headers with arrays", %{socket: socket} do
      request =
        Jason.encode!(%{
          "action" => "publish",
          "queue_name" => "test_queue",
          "payload" => "test",
          "headers" => %{
            "tags" => ["a", "b", "c"]
          }
        })

      TCPHelper.send_line(socket, request)
      {:ok, response} = TCPHelper.recv_line(socket, timeout: 1000)

      {:ok, decoded} = Jason.decode(response)
      assert decoded["s"] == "err"
      assert String.contains?(decoded["reason"], "invalid_headers")
    end

    test "rejects headers with nil values", %{socket: socket} do
      request =
        Jason.encode!(%{
          "action" => "publish",
          "queue_name" => "test_queue",
          "payload" => "test",
          "headers" => %{
            "optional" => nil
          }
        })

      TCPHelper.send_line(socket, request)
      {:ok, response} = TCPHelper.recv_line(socket, timeout: 1000)

      {:ok, decoded} = Jason.decode(response)
      assert decoded["s"] == "err"
      assert String.contains?(decoded["reason"], "invalid_headers")
    end

    test "rejects more than 50 headers", %{socket: socket} do
      headers = Map.new(1..51, fn i -> {"key#{i}", "value"} end)

      request =
        Jason.encode!(%{
          "action" => "publish",
          "queue_name" => "test_queue",
          "payload" => "test",
          "headers" => headers
        })

      TCPHelper.send_line(socket, request)
      {:ok, response} = TCPHelper.recv_line(socket, timeout: 1000)

      {:ok, decoded} = Jason.decode(response)
      assert decoded["s"] == "err"
      assert decoded["reason"] == "invalid_headers_too_many"
    end
  end

  describe "subscribe validation via TCP" do
    test "accepts valid queue name for subscribe", %{socket: _socket} do
      # Need consumer permission
      {:ok, consumer_socket} = TCPHelper.connect(port: @tcp_port)

      auth =
        Jason.encode!(%{
          "action" => "auth",
          "username" => "consumer",
          "password" => "consumer123"
        })

      TCPHelper.send_line(consumer_socket, auth)
      {:ok, _auth_response} = TCPHelper.recv_line(consumer_socket, timeout: 1000)

      request =
        Jason.encode!(%{
          "action" => "subscribe",
          "queue_name" => "valid_queue"
        })

      TCPHelper.send_line(consumer_socket, request)
      {:ok, response} = TCPHelper.recv_line(consumer_socket, timeout: 1000)

      {:ok, decoded} = Jason.decode(response)
      assert decoded["s"] == "ok"

      :gen_tcp.close(consumer_socket)
    end

    test "rejects invalid queue name for subscribe", %{socket: _socket} do
      {:ok, consumer_socket} = TCPHelper.connect(port: @tcp_port)

      auth =
        Jason.encode!(%{
          "action" => "auth",
          "username" => "consumer",
          "password" => "consumer123"
        })

      TCPHelper.send_line(consumer_socket, auth)
      {:ok, _auth_response} = TCPHelper.recv_line(consumer_socket, timeout: 1000)

      request =
        Jason.encode!(%{
          "action" => "subscribe",
          "queue_name" => "invalid queue name"
        })

      TCPHelper.send_line(consumer_socket, request)
      {:ok, response} = TCPHelper.recv_line(consumer_socket, timeout: 1000)

      {:ok, decoded} = Jason.decode(response)
      assert decoded["s"] == "err"
      assert String.contains?(decoded["reason"], "invalid_queue_name")

      :gen_tcp.close(consumer_socket)
    end
  end

  describe "channel validation via TCP" do
    test "accepts valid channel name for publish", %{socket: socket} do
      request =
        Jason.encode!(%{
          "action" => "channel_publish",
          "channel_name" => "valid.channel",
          "payload" => "test"
        })

      TCPHelper.send_line(socket, request)
      {:ok, response} = TCPHelper.recv_line(socket, timeout: 1000)

      {:ok, decoded} = Jason.decode(response)
      assert decoded["s"] == "ok"
    end

    test "rejects invalid channel name", %{socket: socket} do
      request =
        Jason.encode!(%{
          "action" => "channel_publish",
          "channel_name" => "invalid/channel",
          "payload" => "test"
        })

      TCPHelper.send_line(socket, request)
      {:ok, response} = TCPHelper.recv_line(socket, timeout: 1000)

      {:ok, decoded} = Jason.decode(response)
      assert decoded["s"] == "err"
      assert String.contains?(decoded["reason"], "invalid_channel_name")
    end
  end

  describe "regression - valid operations still work" do
    test "normal publish/subscribe flow works", %{socket: socket} do
      # Create consumer
      {:ok, consumer_socket} = TCPHelper.connect(port: @tcp_port)

      auth =
        Jason.encode!(%{
          "action" => "auth",
          "username" => "consumer",
          "password" => "consumer123"
        })

      TCPHelper.send_line(consumer_socket, auth)
      {:ok, _} = TCPHelper.recv_line(consumer_socket, timeout: 1000)

      # Subscribe
      subscribe =
        Jason.encode!(%{
          "action" => "subscribe",
          "queue_name" => "regression_test"
        })

      TCPHelper.send_line(consumer_socket, subscribe)
      {:ok, sub_response} = TCPHelper.recv_line(consumer_socket, timeout: 1000)
      {:ok, sub_decoded} = Jason.decode(sub_response)
      assert sub_decoded["s"] == "ok"

      # Publish
      publish =
        Jason.encode!(%{
          "action" => "publish",
          "queue_name" => "regression_test",
          "payload" => "test_data",
          "headers" => %{"type" => "test"}
        })

      TCPHelper.send_line(socket, publish)
      {:ok, pub_response} = TCPHelper.recv_line(socket, timeout: 1000)
      {:ok, pub_decoded} = Jason.decode(pub_response)
      assert pub_decoded["s"] == "ok"

      :gen_tcp.close(consumer_socket)
    end
  end
end
