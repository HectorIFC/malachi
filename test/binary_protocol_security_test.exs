defmodule Malachi.BinaryProtocolSecurityTest do
  # Security surface of the binary Malachi.Wire log protocol over the real TCP server: authentication,
  # permissions, malformed frames, and fuzzing/robustness. Replaces the old JSON queue/channel protocol
  # security suites (removed in B3a) with coverage of the protocol that actually ships (B1b-ii). The
  # underlying infra (Auth/RateLimiter/Validator/LockoutManager) keeps its own unit tests.
  use ExUnit.Case, async: false

  alias Malachi.Test.TCPHelper
  alias Malachi.Wire

  @port Application.compile_env(:malachi, :tcp_port, 4040)

  defp uniq, do: System.unique_integer([:positive])

  defp connect do
    {:ok, socket} = TCPHelper.connect(port: @port)
    socket
  end

  defp connect_auth(user \\ "app", pass \\ "app123") do
    socket = connect()
    {:ok, _token} = TCPHelper.authenticate_wire(socket, user, pass)
    socket
  end

  # decodes an error response payload (the reason travels as a length-prefixed string)
  defp reason(payload), do: Wire.decode_auth_resp(payload)

  # a valid request on a fresh authenticated connection must still succeed — proof the server survived
  defp assert_server_alive do
    socket = connect_auth()
    {code, _} = TCPHelper.request(socket, Wire.create_topic_key(), 1, Wire.encode_create_topic_req("surv_#{uniq()}", 8))
    assert code == Wire.ok_code()
    :gen_tcp.close(socket)
  end

  describe "authentication" do
    test "rejects invalid credentials" do
      socket = connect()
      assert {:error, _reason} = TCPHelper.authenticate_wire(socket, "app", "wrong-password")
      :gen_tcp.close(socket)
    end

    test "requires auth as the first frame — a non-auth frame is rejected" do
      socket = connect()
      # a produce frame before authenticating
      :ok = :gen_tcp.send(socket, Wire.encode_request(Wire.produce_key(), 7, <<>>))

      assert {:ok, body} = TCPHelper.recv_frame(socket, timeout: 1_000)
      assert {7, code, payload} = Wire.decode_response(body)
      assert code == Wire.error_code()
      assert reason(payload) == "auth_required"
      :gen_tcp.close(socket)
    end

    test "a malformed first frame does not crash the server" do
      socket = connect()
      # a frame whose body is too short to be a request envelope
      :ok = :gen_tcp.send(socket, Wire.encode_frame(<<0, 1>>))
      # the server answers an error and/or closes; either way it must stay up
      _ = TCPHelper.recv_frame(socket, timeout: 500)
      :gen_tcp.close(socket)

      assert_server_alive()
    end
  end

  describe "permissions" do
    test "produce requires :produce" do
      socket = connect_auth("consumer", "consumer123")
      {code, payload} = TCPHelper.request(socket, Wire.produce_key(), 1, Wire.encode_produce_req("t_#{uniq()}", []))
      assert code == Wire.error_code()
      assert reason(payload) == "permission_denied"
      :gen_tcp.close(socket)
    end

    test "fetch requires :consume" do
      socket = connect_auth("producer", "producer123")

      {code, payload} =
        TCPHelper.request(socket, Wire.fetch_key(), 1, Wire.encode_fetch_req("t_#{uniq()}", nil, nil, 100, 0))

      assert code == Wire.error_code()
      assert reason(payload) == "permission_denied"
      :gen_tcp.close(socket)
    end
  end

  describe "malformed frames (authenticated)" do
    test "an unknown api_key errors and the connection keeps serving" do
      socket = connect_auth()

      {code, payload} = TCPHelper.request(socket, 99, 7, <<>>)
      assert code == Wire.error_code()
      assert reason(payload) == "unknown_api_key"

      # the same connection still serves a valid request (framing/state intact)
      {ok_code, _} =
        TCPHelper.request(socket, Wire.create_topic_key(), 8, Wire.encode_create_topic_req("surv_#{uniq()}", 8))

      assert ok_code == Wire.ok_code()
      :gen_tcp.close(socket)
    end

    test "a truncated payload errors (malformed_request) with the correlation id preserved" do
      socket = connect_auth()

      # a produce payload that claims 5 records but carries none: put_str("t") <> <<count::32>>
      truncated = <<1::8, 1::32, "t", 5::32>>
      {code, payload} = TCPHelper.request(socket, Wire.produce_key(), 4242, truncated)

      assert code == Wire.error_code()
      assert reason(payload) == "malformed_request"
      :gen_tcp.close(socket)
    end
  end

  describe "fuzzing / robustness" do
    test "random bytes and a lying length prefix do not crash the server" do
      socket = connect()
      # garbage that is not a valid frame, then a header claiming 1MB but sending 3 bytes
      :ok = :gen_tcp.send(socket, :crypto.strong_rand_bytes(64))
      :ok = :gen_tcp.send(socket, <<1_000_000::32, 1, 2, 3>>)
      :gen_tcp.close(socket)

      assert_server_alive()
    end
  end

  # A hostile client can announce a huge frame length and dribble bytes; without a cap the server would
  # buffer unbounded memory. The length prefix is rejected before the body is accumulated.
  describe "frame size limit (DoS guard)" do
    test "an oversized frame on an authenticated connection is rejected, not buffered" do
      socket = connect_auth()
      # claim a 100 MB frame (over the 16 MiB default) and send only a few body bytes
      :ok = :gen_tcp.send(socket, <<100_000_000::32, 0, 1, 2>>)

      assert {:ok, body} = TCPHelper.recv_frame(socket, timeout: 1_000)
      assert {_corr, code, payload} = Wire.decode_response(body)
      assert code == Wire.error_code()
      assert reason(payload) == "frame_too_large"

      :gen_tcp.close(socket)
      assert_server_alive()
    end

    test "an oversized first (auth) frame is rejected at the handshake" do
      socket = connect()
      :ok = :gen_tcp.send(socket, <<100_000_000::32, 0, 1, 2>>)

      # the server answers an error frame and/or closes; either way it must stay up
      _ = TCPHelper.recv_frame(socket, timeout: 1_000)
      :gen_tcp.close(socket)

      assert_server_alive()
    end

    test "the boundary: a frame at the cap is processed, one over is rejected" do
      original = Application.get_env(:malachi, :max_frame_size)

      on_exit(fn ->
        if original,
          do: Application.put_env(:malachi, :max_frame_size, original),
          else: Application.delete_env(:malachi, :max_frame_size)
      end)

      socket = connect_auth()
      # shrink the cap after auth (the auth frame used the default) so we can exercise the boundary cheaply
      Application.put_env(:malachi, :max_frame_size, 64)

      # an envelope (6 bytes) + 58-byte payload = a 64-byte body, exactly at the cap: decoded and
      # dispatched (unknown api_key), not a size rejection
      {_c1, at_cap} = TCPHelper.request(socket, 99, 1, :binary.copy(<<0>>, 58))
      assert reason(at_cap) == "unknown_api_key"

      # a 65-byte body is one over the cap: rejected at the length prefix
      :ok = :gen_tcp.send(socket, Wire.encode_frame(:binary.copy(<<0>>, 65)))
      assert {:ok, body} = TCPHelper.recv_frame(socket, timeout: 1_000)
      assert {_corr, code, payload} = Wire.decode_response(body)
      assert code == Wire.error_code()
      assert reason(payload) == "frame_too_large"

      :gen_tcp.close(socket)
    end
  end
end
