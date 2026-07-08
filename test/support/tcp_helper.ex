defmodule Malachi.Test.TCPHelper do
  @moduledoc """
  TCP helper utilities for testing.

  Provides convenience functions for creating TCP connections in tests with
  proper configuration to avoid packet size limitations on OTP 28+.

  ## Socket Mode

  Uses `packet: 0` (raw mode) instead of `packet: :line` to avoid 64KB line
  length limits. Messages are manually framed with newlines.

  The binary log protocol (`Malachi.Wire`) helpers are `request/4`, `recv_frame/2` and
  `authenticate_wire/3`; the JSON `send_line`/`recv_line`/`authenticate` remain for the skipped queue
  tests until they are rewritten (B1b-ii).
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

  @doc """
  Sends a message through the socket with newline framing.

  Automatically appends a newline if not present.

  ## Examples

      send_line(socket, "{\"action\": \"auth\"}")
      send_line(socket, "message\\n")  # Already has newline
  """
  def send_line(socket, message) do
    data = if String.ends_with?(message, "\n"), do: message, else: message <> "\n"
    :gen_tcp.send(socket, data)
  end

  @doc """
  Receives a single line from the socket.

  Reads data until a newline is encountered or timeout occurs.

  ## Options

  - `:timeout` - Read timeout in ms (default: 5000)

  ## Examples

      {:ok, line} = recv_line(socket)
      {:ok, line} = recv_line(socket, timeout: 1000)
  """
  def recv_line(socket, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5000)
    recv_line_loop(socket, "", timeout, :os.system_time(:millisecond))
  end

  defp recv_line_loop(socket, buffer, timeout, start_time) do
    elapsed = :os.system_time(:millisecond) - start_time
    remaining = max(timeout - elapsed, 0)

    if remaining == 0 do
      {:error, :timeout}
    else
      case :gen_tcp.recv(socket, 0, remaining) do
        {:ok, data} ->
          new_buffer = buffer <> data

          case String.split(new_buffer, "\n", parts: 2) do
            [line, _rest] ->
              {:ok, line <> "\n"}

            [partial] ->
              recv_line_loop(socket, partial, timeout, start_time)
          end

        {:error, _} = error ->
          error
      end
    end
  end

  @doc """
  Authenticates with the server and returns the auth token.

  ## Examples

      {:ok, token} = authenticate(socket, "admin", "admin123")
  """
  def authenticate(socket, username, password) do
    auth_msg =
      Jason.encode!(%{
        "action" => "auth",
        "username" => username,
        "password" => password
      })

    :ok = send_line(socket, auth_msg)

    case recv_line(socket) do
      {:ok, response} ->
        case Jason.decode(String.trim(response)) do
          {:ok, %{"s" => "ok", "token" => token}} ->
            {:ok, token}

          {:ok, %{"s" => "err"} = error} ->
            {:error, error}

          {:error, _} = error ->
            error
        end

      error ->
        error
    end
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
