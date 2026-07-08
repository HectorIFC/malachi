defmodule Malachi.TCPAcceptor do
  @moduledoc """
  One acceptor of the `Malachi.TCPAcceptorPool`: a `GenServer` that owns a listen socket on a port and
  accepts client connections in a loop, one at a time.

  Each accepted connection is completed — a TLS handshake for `:ssl` (recording the negotiated version
  and success/failure metrics), or the raw socket for `:gen_tcp` — and handed to a fresh process that
  runs `Malachi.TCPProtocol` for that client. The accept loop self-schedules with an idle backoff: the
  poll timeout grows as consecutive accepts time out (capped at 30s), so an idle server does not
  busy-wait. Several acceptors share the same port (`reuseport`), spreading accepts across schedulers.
  """
  use GenServer
  require Logger
  alias Malachi.Auth.LockoutManager
  alias Malachi.I18n
  alias Malachi.TCPProtocol
  alias Malachi.Wire

  @doc """
  Starts an acceptor for `{port, opts, id, transport}`: `opts` are the `:gen_tcp`/`:ssl` listen
  options, `id` labels this acceptor in logs, and `transport` is `:gen_tcp` or `:ssl`.
  """
  def start_link({port, opts, id, transport}) do
    GenServer.start_link(__MODULE__, {port, opts, id, transport})
  end

  @impl true
  def init({port, opts, id, transport}) do
    listen_result =
      case transport do
        :ssl -> :ssl.listen(port, opts)
        :gen_tcp -> :gen_tcp.listen(port, opts)
      end

    case listen_result do
      {:ok, socket} ->
        Logger.info(I18n.t(:acceptor_started, id: id))
        send(self(), :accept)
        {:ok, %{socket: socket, id: id, connections: 0, idle_count: 0, transport: transport}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_info(:accept, %{socket: socket, idle_count: idle_count, transport: transport} = state) do
    timeout = min(100 + idle_count * 50, 30_000)

    accept_result =
      case transport do
        :ssl -> :ssl.transport_accept(socket, timeout)
        :gen_tcp -> :gen_tcp.accept(socket, timeout)
      end

    case accept_result do
      {:ok, client} ->
        case establish_client_socket(client, transport, timeout) do
          nil -> :ok
          client_socket -> hand_off_client(client_socket, transport)
        end

        send(self(), :accept)
        {:noreply, %{state | connections: state.connections + 1, idle_count: 0}}

      {:error, :timeout} ->
        send(self(), :accept)
        {:noreply, %{state | idle_count: min(idle_count + 1, 20)}}

      {:error, reason} ->
        Logger.error(I18n.t(:accept_error, reason: inspect(reason)))
        Process.sleep(10)
        send(self(), :accept)
        {:noreply, state}
    end
  end

  # Completes the accepted client socket. For TLS, performs the handshake (recording success/failure
  # metrics and the negotiated version) and returns the TLS socket, or nil when the handshake fails;
  # for plain TCP, returns the accepted socket as-is.
  defp establish_client_socket(client, :gen_tcp, _timeout), do: client

  defp establish_client_socket(client, :ssl, timeout) do
    case :ssl.handshake(client, timeout) do
      {:ok, tls_socket} ->
        Malachi.Metrics.increment_tls_handshake_success()
        record_negotiated_tls_version(tls_socket)
        tls_socket

      {:error, reason} ->
        Malachi.Metrics.increment_tls_handshake_failed()
        Logger.warning(I18n.t(:tls_handshake_failed, reason: inspect(reason)))
        :ssl.close(client)
        nil
    end
  end

  # Records the negotiated TLS protocol version in metrics (best-effort).
  defp record_negotiated_tls_version(tls_socket) do
    case :ssl.connection_information(tls_socket, [:protocol]) do
      {:ok, [{:protocol, version}]} -> Malachi.Metrics.record_tls_version(version)
      _ -> :ok
    end
  end

  # Hands the connected client socket to a fresh process, transferring socket ownership to it and then
  # signaling that the transfer is complete so it can start serving.
  defp hand_off_client(client_socket, transport) do
    pid =
      spawn(fn ->
        # Wait for socket ownership transfer before proceeding
        receive do
          :socket_ready -> handle_client(client_socket, transport)
        after
          5000 -> :timeout
        end
      end)

    case transport do
      :ssl -> :ssl.controlling_process(client_socket, pid)
      :gen_tcp -> :gen_tcp.controlling_process(client_socket, pid)
    end

    send(pid, :socket_ready)
  end

  defp handle_client(socket, transport) do
    # Extract client IP address
    client_ip = get_client_ip(socket, transport)

    # Check connection limits
    case Malachi.ConnectionLimiter.register_connection(self(), client_ip) do
      :ok ->
        # Register connection for graceful shutdown tracking
        Malachi.ConnectionRegistry.register(self(), socket, transport)

        try do
          # Create initial state
          state = %{
            socket: socket,
            transport: transport,
            client_ip: client_ip,
            session: nil,
            buffer: ""
          }

          case authenticate_client(state) do
            {:ok, updated_state} ->
              receive_loop(updated_state)

            # the auth flow already answered an error frame (with the real correlation id) where it
            # could; here we just close the connection.
            {:error, _reason} ->
              close_socket(socket, transport)
          end
        after
          # Unregister on exit (normal or error)
          Malachi.ConnectionRegistry.unregister(self())
          Malachi.ConnectionLimiter.unregister_connection(self())
        end

      {:error, :connection_limit_exceeded} ->
        Malachi.Metrics.increment_connection_limit_blocked()
        send_error(socket, :connection_limit_exceeded, transport)
        close_socket(socket, transport)

      {:error, :global_limit_exceeded} ->
        Malachi.Metrics.increment_connection_limit_blocked()
        send_error(socket, :global_limit_exceeded, transport)
        close_socket(socket, transport)
    end
  end

  # The auth handshake is a binary frame: read one, and (if it is an auth request) validate it. Frames may
  # arrive split across recvs, so an incomplete buffer loops for more bytes.
  defp authenticate_client(%{socket: socket, transport: transport, buffer: buffer} = state) do
    set_socket_opts(socket, transport, active: false)
    recv_timeout = Application.get_env(:malachi, :auth_timeout_ms, 10_000)

    case recv_data(socket, transport, 0, recv_timeout) do
      {:ok, data} ->
        case Wire.decode_frame(buffer <> data) do
          {:ok, frame_body, rest} -> process_auth_frame(frame_body, %{state | buffer: rest})
          :incomplete -> authenticate_client(%{state | buffer: buffer <> data})
        end

      {:error, _} ->
        {:error, :connection_error}
    end
  end

  # The first frame must be an auth request; any other api_key or a malformed frame is rejected.
  defp process_auth_frame(frame_body, %{socket: socket, transport: transport} = state) do
    {api_key, correlation_id, payload} = Wire.decode_request(frame_body)

    if api_key == Wire.auth_key() do
      {username, password} = Wire.decode_auth_req(payload)
      validate_and_authenticate(username, password, correlation_id, state)
    else
      transport.send(socket, Wire.encode_error(correlation_id, :auth_required))
      {:error, :auth_required}
    end
  rescue
    _malformed ->
      transport.send(socket, Wire.encode_error(0, :auth_required))
      {:error, :auth_required}
  end

  defp validate_and_authenticate(
         username,
         password,
         correlation_id,
         %{socket: socket, transport: transport, client_ip: client_ip} = state
       ) do
    # STEP 1: Check account lockout (OWASP: most specific control first)
    case LockoutManager.locked?(username, client_ip) do
      {:locked, _time_remaining_ms} ->
        Malachi.Metrics.increment_account_lockout_blocked()
        transport.send(socket, Wire.encode_error(correlation_id, :account_locked))
        {:error, :account_locked}

      :not_locked ->
        # STEP 2: Check rate limit (network-level control)
        auth_limit = Application.get_env(:malachi, :auth_rate_limit, 10)
        auth_window_ms = Application.get_env(:malachi, :auth_rate_window_ms, 60_000)

        case Malachi.RateLimiter.check_limit(client_ip, :auth, %{limit: auth_limit, window_ms: auth_window_ms}) do
          :ok ->
            # STEP 3: Perform authentication
            case Malachi.Auth.authenticate(username, password, client_ip) do
              {:ok, token} ->
                LockoutManager.record_successful_auth(username, client_ip)
                validate_token_and_respond(token, correlation_id, state, client_ip)

              {:error, _reason} ->
                LockoutManager.record_failed_attempt(username, client_ip)
                transport.send(socket, Wire.encode_error(correlation_id, :invalid_credentials))
                {:error, :invalid_credentials}
            end

          {:error, :rate_limit_exceeded, _retry_after_ms} ->
            Malachi.Metrics.increment_rate_limit_blocked(:auth)
            transport.send(socket, Wire.encode_error(correlation_id, :rate_limit_exceeded))
            {:error, :rate_limit_exceeded}
        end
    end
  end

  defp validate_token_and_respond(token, correlation_id, %{socket: socket, transport: transport} = state, client_ip) do
    case Malachi.Auth.validate_token(token, client_ip) do
      {:ok, session} ->
        transport.send(socket, Wire.encode_ok(correlation_id, Wire.encode_auth_resp(token)))
        {:ok, %{state | session: session}}

      {:error, _} ->
        {:error, :invalid_token}
    end
  end

  # Reads request frames and dispatches each; loops until the socket closes or errors. (B2 will add a
  # streaming subscribe mode on top of this.)
  defp receive_loop(%{socket: socket, transport: transport, buffer: buffer} = state) do
    recv_timeout = Application.get_env(:malachi, :tcp_recv_timeout, 30_000)

    case recv_data(socket, transport, 0, recv_timeout) do
      {:ok, data} ->
        remaining = process_buffered_frames(buffer <> data, state)
        receive_loop(%{state | buffer: remaining})

      {:error, _} ->
        close_socket(socket, transport)
    end
  end

  # Processes each complete frame and returns the trailing partial data (kept in the buffer).
  defp process_buffered_frames(buffer, state) do
    case Wire.decode_frame(buffer) do
      {:ok, frame_body, rest} ->
        process_authenticated(frame_body, state)
        process_buffered_frames(rest, state)

      :incomplete ->
        buffer
    end
  end

  @compile {:inline, process_authenticated: 2}
  defp process_authenticated(frame_body, %{socket: socket, session: session, transport: transport}) do
    TCPProtocol.process_frame(socket, frame_body, session, transport)
  end

  defp close_socket(socket, :ssl), do: :ssl.close(socket)
  defp close_socket(socket, :gen_tcp), do: :gen_tcp.close(socket)

  # Pre-auth errors (limits) have no correlation id yet, so they use 0.
  defp send_error(socket, reason, transport) do
    transport.send(socket, Wire.encode_error(0, reason))
  end

  # Socket helper functions
  defp recv_data(socket, :ssl, length, timeout), do: :ssl.recv(socket, length, timeout)
  defp recv_data(socket, :gen_tcp, length, timeout), do: :gen_tcp.recv(socket, length, timeout)

  defp set_socket_opts(socket, :ssl, opts), do: :ssl.setopts(socket, opts)
  defp set_socket_opts(socket, :gen_tcp, opts), do: :inet.setopts(socket, opts)

  # IP address formatting helpers
  defp get_client_ip(socket, transport) do
    case transport do
      :ssl ->
        case :ssl.peername(socket) do
          {:ok, {address, _port}} -> format_ip(address)
          {:error, _} -> "unknown"
        end

      :gen_tcp ->
        case :inet.peername(socket) do
          {:ok, {address, _port}} -> format_ip(address)
          {:error, _} -> "unknown"
        end
    end
  end

  defp format_ip({a, b, c, d}), do: "#{a}.#{b}.#{c}.#{d}"

  defp format_ip({a, b, c, d, e, f, g, h}),
    do:
      "#{Integer.to_string(a, 16)}:#{Integer.to_string(b, 16)}:#{Integer.to_string(c, 16)}:#{Integer.to_string(d, 16)}:#{Integer.to_string(e, 16)}:#{Integer.to_string(f, 16)}:#{Integer.to_string(g, 16)}:#{Integer.to_string(h, 16)}"

  defp format_ip(_), do: "unknown"
end
