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
  alias Malachi.Auth.MtlsProvider
  alias Malachi.Auth.SessionManager
  alias Malachi.I18n
  alias Malachi.LogApi
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
        case Wire.decode_frame(buffer <> data, max_frame_size()) do
          {:ok, frame_body, rest} ->
            process_auth_frame(frame_body, %{state | buffer: rest})

          :incomplete ->
            authenticate_client(%{state | buffer: buffer <> data})

          {:error, :frame_too_large} ->
            send_oversized_error(socket, transport)
            {:error, :frame_too_large}
        end

      {:error, _} ->
        {:error, :connection_error}
    end
  end

  # The first frame must be an auth request — password (`auth`) or mTLS-identity (`mtls_auth`); any other
  # api_key or a malformed frame is rejected.
  defp process_auth_frame(frame_body, %{socket: socket, transport: transport} = state) do
    {api_key, correlation_id, payload} = Wire.decode_request(frame_body)

    cond do
      api_key == Wire.auth_key() ->
        {username, password} = Wire.decode_auth_req(payload)
        validate_and_authenticate(username, password, correlation_id, state)

      api_key == Wire.mtls_auth_key() ->
        authenticate_mtls(correlation_id, state)

      true ->
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
        case check_auth_rate_limit(client_ip) do
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

          {:error, :rate_limit_exceeded} ->
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

  # mTLS-identity auth (P4): authenticate the client from the certificate it presented at the TLS handshake
  # instead of a password. Gated so an unverified certificate can never authenticate (see `ensure_mtls_allowed/0`).
  # On success the session is minted exactly like the password path (SessionManager.create_session). Failed
  # attempts are not fed to the lockout manager (there is no password to brute-force); the rate limit still
  # applies for DoS protection.
  defp authenticate_mtls(correlation_id, %{socket: socket, transport: transport, client_ip: client_ip} = state) do
    with :ok <- ensure_mtls_allowed(),
         :ok <- check_auth_rate_limit(client_ip),
         {:ok, der} <- peer_certificate(transport, socket),
         {:ok, %{username: username, permissions: permissions}} <-
           MtlsProvider.authenticate(der, %{policy: mtls_identity_policy()}) do
      {:ok, token} = SessionManager.create_session(username, permissions, client_ip, "")
      Logger.info(I18n.t(:auth_success, username: username))
      Malachi.AuditLog.log_event(:auth_success, %{username: username, ip: client_ip}, "mtls_authenticate", :success, %{})
      validate_token_and_respond(token, correlation_id, state, client_ip)
    else
      {:error, reason} ->
        Malachi.AuditLog.log_event(:auth_failure, %{ip: client_ip}, "mtls_authenticate", :failure, %{reason: reason})
        transport.send(socket, Wire.encode_error(correlation_id, mtls_client_error(reason)))
        {:error, reason}
    end
  end

  # Applies the per-IP auth rate limit (network-level control), shared by the password and mTLS handshakes.
  defp check_auth_rate_limit(client_ip) do
    auth_limit = Application.get_env(:malachi, :auth_rate_limit, 10)
    auth_window_ms = Application.get_env(:malachi, :auth_rate_window_ms, 60_000)

    case Malachi.RateLimiter.check_limit(client_ip, :auth, %{limit: auth_limit, window_ms: auth_window_ms}) do
      :ok ->
        :ok

      {:error, :rate_limit_exceeded, _retry_after_ms} ->
        Malachi.Metrics.increment_rate_limit_blocked(:auth)
        {:error, :rate_limit_exceeded}
    end
  end

  # mTLS auth is honored only when explicitly enabled AND the listener verifies peer certs — under
  # verify_none a client could present a forged certificate, so it must never authenticate.
  defp ensure_mtls_allowed do
    cond do
      not Application.get_env(:malachi, :mtls_auth, false) -> {:error, :mtls_auth_disabled}
      not tls_verify_peer?() -> {:error, :mtls_auth_unavailable}
      true -> :ok
    end
  end

  defp tls_verify_peer? do
    Application.get_env(:malachi, :tls_verify, "verify_none") in ["verify_peer", :verify_peer]
  end

  defp mtls_identity_policy do
    Application.get_env(:malachi, :mtls_identity_policy, :cn)
  end

  # The DER peer certificate, or an error. Only a TLS transport can carry one.
  defp peer_certificate(:ssl, socket) do
    case :ssl.peercert(socket) do
      {:ok, der} -> {:ok, der}
      {:error, _reason} -> {:error, :no_peer_certificate}
    end
  end

  defp peer_certificate(_transport, _socket), do: {:error, :no_peer_certificate}

  # An identity-mapping failure collapses to :invalid_credentials so we never reveal which certificate
  # identities are provisioned; config/protocol errors (feature off, no cert, rate limit) are surfaced as-is.
  defp mtls_client_error(reason) when reason in [:malformed_certificate, :no_identity, :unknown_identity] do
    :invalid_credentials
  end

  defp mtls_client_error(reason), do: reason

  # Reads request frames and dispatches each; loops until the socket closes or errors. A `subscribe` frame
  # switches the connection into `stream_loop/2` (server-push streaming) for the rest of its life.
  defp receive_loop(%{socket: socket, transport: transport, buffer: buffer} = state) do
    recv_timeout = Application.get_env(:malachi, :tcp_recv_timeout, 30_000)

    case recv_data(socket, transport, 0, recv_timeout) do
      {:ok, data} ->
        case process_buffered_frames(buffer <> data, state) do
          {:more, remaining} -> receive_loop(%{state | buffer: remaining})
          {:stream, sub_corr, remaining} -> stream_loop(%{state | buffer: remaining}, sub_corr)
          {:close, _reason} -> close_oversized(state)
        end

      {:error, _} ->
        close_socket(socket, transport)
    end
  end

  # Processes complete frames until the buffer is exhausted (`{:more, trailing}`), a frame opens a stream
  # (`{:stream, sub_corr, trailing}`, carrying any frames buffered after the subscribe), or an oversized
  # frame is seen (`{:close, reason}`).
  defp process_buffered_frames(buffer, state) do
    case Wire.decode_frame(buffer, max_frame_size()) do
      {:ok, frame_body, rest} ->
        case process_authenticated(frame_body, state) do
          :ok -> process_buffered_frames(rest, state)
          {:stream, sub_corr} -> {:stream, sub_corr, rest}
        end

      :incomplete ->
        {:more, buffer}

      {:error, :frame_too_large} ->
        {:close, :frame_too_large}
    end
  end

  @compile {:inline, process_authenticated: 2}
  defp process_authenticated(frame_body, %{socket: socket, session: session, transport: transport}) do
    TCPProtocol.process_frame(socket, frame_body, session, transport)
  end

  # A subscribed connection becomes a one-stream duplex: switched to active mode so a single `receive`
  # handles both the broker's `{:log_records, ...}` pushes (forwarded to the client as response frames
  # tagged with the subscribe's correlation id) and the client's inbound ack frames. Any frames buffered
  # alongside the subscribe are drained first. The stream ends when the socket closes; the broker drops
  # the subscriber via the connection process's `:DOWN`.
  defp stream_loop(%{buffer: buffer} = state, sub_corr) do
    case handle_stream_buffer(buffer, state) do
      {:more, remaining} ->
        arm_active(state)
        stream_recv(%{state | buffer: remaining}, sub_corr)

      {:close, _reason} ->
        close_oversized(state)
    end
  end

  defp stream_recv(%{socket: socket, transport: transport, buffer: buffer} = state, sub_corr) do
    receive do
      {:log_records, _topic, records, positions} ->
        frame = Wire.encode_ok(sub_corr, Wire.encode_fetch_resp(records, LogApi.encode_cursor(positions)))
        transport.send(socket, frame)
        stream_recv(state, sub_corr)

      {tag, ^socket, data} when tag in [:tcp, :ssl] ->
        case handle_stream_buffer(buffer <> data, state) do
          {:more, remaining} ->
            arm_active(state)
            stream_recv(%{state | buffer: remaining}, sub_corr)

          {:close, _reason} ->
            close_oversized(state)
        end

      {tag, ^socket} when tag in [:tcp_closed, :ssl_closed] ->
        close_socket(socket, transport)

      {tag, ^socket, _reason} when tag in [:tcp_error, :ssl_error] ->
        close_socket(socket, transport)
    end
  end

  # Applies each complete inbound frame (an ack, per `TCPProtocol.process_stream_frame/3`) and returns the
  # trailing partial data to keep buffered, or `{:close, reason}` on an oversized frame.
  defp handle_stream_buffer(buffer, %{socket: socket, transport: transport} = state) do
    case Wire.decode_frame(buffer, max_frame_size()) do
      {:ok, frame_body, rest} ->
        TCPProtocol.process_stream_frame(socket, frame_body, transport)
        handle_stream_buffer(rest, state)

      :incomplete ->
        {:more, buffer}

      {:error, :frame_too_large} ->
        {:close, :frame_too_large}
    end
  end

  # The largest request frame the server will accept. A declared length beyond this is rejected as soon as
  # the length prefix arrives, bounding the memory a single connection can force the server to buffer.
  defp max_frame_size, do: Application.get_env(:malachi, :max_frame_size, 16_777_216)

  # Answers an oversized frame with a single error response (no correlation id is trustworthy on a hostile
  # frame, so 0), best-effort — the caller then closes the connection.
  defp send_oversized_error(socket, transport), do: transport.send(socket, Wire.encode_error(0, :frame_too_large))

  defp close_oversized(%{socket: socket, transport: transport}) do
    send_oversized_error(socket, transport)
    close_socket(socket, transport)
  end

  defp arm_active(%{socket: socket, transport: transport}), do: set_socket_opts(socket, transport, active: :once)

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
