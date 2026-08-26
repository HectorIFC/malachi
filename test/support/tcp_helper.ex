defmodule Malachi.Test.TCPHelper do
  @moduledoc """
  TCP helper utilities for testing.

  Provides convenience functions for creating TCP connections in tests with
  proper configuration to avoid packet size limitations on OTP 28+.

  ## Socket Mode

  Uses `packet: 0` (raw mode) so the binary `Malachi.Wire` protocol frames messages itself. The helpers
  are `connect/1`, `authenticate_wire/3`, `request/4` (send a Wire request, read its response) and
  `recv_frame/2` (read one length-prefixed frame).
  """

  alias Malachi.Wire

  @doc """
  Connects to the Malachi TCP server with test-appropriate settings.

  Uses `packet: 0` to avoid line length limitations and handles framing manually.

  ## Options

  - `:timeout` - Connection timeout in ms (default: 1000)
  - `:port` - Server port (default: from application config)

  ## Examples

      {:ok, socket} = TCPHelper.connect()
      {:ok, socket} = TCPHelper.connect(timeout: 5000)
      {:ok, socket} = TCPHelper.connect(port: 4040)
  """
  def connect(opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 1000)
    port = Keyword.get(opts, :port, Application.get_env(:malachi, :tcp_port, 4040))

    :gen_tcp.connect(
      {127, 0, 0, 1},
      port,
      [:binary, packet: 0, active: false],
      timeout
    )
  end

  # ---- binary Wire protocol helpers ----

  @doc "Reads one length-prefixed Wire frame body from the socket (accumulating until complete)."
  def recv_frame(socket, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5000)
    recv_frame_loop(socket, "", timeout, :os.system_time(:millisecond))
  end

  defp recv_frame_loop(socket, buffer, timeout, start_time) do
    case Wire.decode_frame(buffer) do
      {:ok, body, _rest} ->
        {:ok, body}

      :incomplete ->
        remaining = max(timeout - (:os.system_time(:millisecond) - start_time), 0)

        if remaining == 0 do
          {:error, :timeout}
        else
          case :gen_tcp.recv(socket, 0, remaining) do
            {:ok, data} -> recv_frame_loop(socket, buffer <> data, timeout, start_time)
            {:error, _} = error -> error
          end
        end
    end
  end

  @doc """
  Sends a Wire request frame and reads its response frame; returns `{error_code, payload}`. Asserts the
  response `correlation_id` matches the request's.
  """
  def request(socket, api_key, correlation_id, payload) do
    :ok = :gen_tcp.send(socket, Wire.encode_request(api_key, correlation_id, payload))
    {:ok, frame_body} = recv_frame(socket)
    {^correlation_id, error_code, resp_payload} = Wire.decode_response(frame_body)
    {error_code, resp_payload}
  end

  @doc "Authenticates over the binary protocol; returns `{:ok, token}` or `{:error, reason}`."
  def authenticate_wire(socket, username, password) do
    {error_code, payload} = request(socket, Wire.auth_key(), 1, Wire.encode_auth_req(username, password))

    if error_code == Wire.ok_code() do
      {:ok, Wire.decode_auth_resp(payload)}
    else
      {:error, Wire.decode_auth_resp(payload)}
    end
  end

  # ---- streaming helpers ----

  @doc "Sends a subscribe frame (opens a push stream); no immediate response is expected."
  def subscribe(socket, topic, group, window, max, correlation_id) do
    subscribe(socket, topic, group, nil, window, max, correlation_id)
  end

  @doc "Sends a subscribe frame for a consumer-group `member` (server-scoped push stream)."
  def subscribe(socket, topic, group, member, window, max, correlation_id) do
    payload = Wire.encode_subscribe_req(topic, group, member, window, max)
    :ok = :gen_tcp.send(socket, Wire.encode_request(Wire.subscribe_key(), correlation_id, payload))
  end

  @doc """
  Reads one server push and asserts it carries `correlation_id` (the subscribe's). Returns
  `{records, next_cursor}` decoded from the fetch-response payload.
  """
  def recv_push(socket, correlation_id, opts \\ []) do
    {:ok, frame_body} = recv_frame(socket, opts)
    {^correlation_id, error_code, payload} = Wire.decode_response(frame_body)
    ^error_code = Wire.ok_code()
    Wire.decode_fetch_resp(payload)
  end

  @doc "Sends a stream_ack frame (durable commit + window credit); fire-and-forget, no response."
  def stream_ack(socket, topic, group, cursor, count, correlation_id) do
    stream_ack(socket, topic, group, nil, cursor, count, correlation_id)
  end

  @doc "Sends a stream_ack frame for a consumer-group `member` (heartbeat + refresh + commit + credit)."
  def stream_ack(socket, topic, group, member, cursor, count, correlation_id) do
    payload = Wire.encode_stream_ack_req(topic, group, member, cursor, count)
    :ok = :gen_tcp.send(socket, Wire.encode_request(Wire.stream_ack_key(), correlation_id, payload))
  end

  @doc """
  Creates a test socket listener for local testing.

  Useful for testing socket operations without connecting to the server.

  ## Examples

      {:ok, listen_socket, port} = create_test_listener()
  """
  def create_test_listener do
    {:ok, listen_socket} = :gen_tcp.listen(0, [:binary, packet: 0, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen_socket)
    {:ok, listen_socket, port}
  end
end
