defmodule Malachi.TCPAcceptor do
  use GenServer
  require Logger
  alias Malachi.Auth.LockoutManager
  alias Malachi.I18n
  alias Malachi.TCPProtocol

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
        client_socket =
          if transport == :ssl do
            case :ssl.handshake(client, timeout) do
              {:ok, tls_socket} ->
                Malachi.Metrics.increment_tls_handshake_success()

                # Extract and record negotiated TLS version
                case :ssl.connection_information(tls_socket, [:protocol]) do
                  {:ok, [{:protocol, version}]} ->
                    Malachi.Metrics.record_tls_version(version)

                  _ ->
                    :ok
                end

                tls_socket

              {:error, reason} ->
                Malachi.Metrics.increment_tls_handshake_failed()
                Logger.warning(I18n.t(:tls_handshake_failed, reason: inspect(reason)))
                :ssl.close(client)
                nil
            end
          else
            client
          end

        if client_socket do
          pid =
            spawn(fn ->
              # Wait for socket ownership transfer before proceeding
              receive do
                :socket_ready -> handle_client(client_socket, transport)
              after
                5000 -> :timeout
              end
            end)

          # Transfer socket ownership to the spawned process
          case transport do
            :ssl -> :ssl.controlling_process(client_socket, pid)
            :gen_tcp -> :gen_tcp.controlling_process(client_socket, pid)
          end

          # Signal that transfer is complete
          send(pid, :socket_ready)
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
            subscribed: false,
            buffer: ""
          }

          case authenticate_client(state) do
            {:ok, updated_state} ->
              receive_loop(updated_state)

            {:error, reason} ->
              send_error(socket, reason, transport)
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

  defp authenticate_client(%{socket: socket, transport: transport} = state) do
    set_socket_opts(socket, transport, active: false)
    recv_timeout = Application.get_env(:malachi, :auth_timeout_ms, 10_000)

    case recv_data(socket, transport, 0, recv_timeout) do
      {:ok, data} ->
        process_auth_data(data, state)

      {:error, _} ->
        {:error, :connection_error}
    end
  end

  defp process_auth_data(data, %{socket: _socket, transport: _transport} = state) do
    case Jason.decode(data) do
      {:ok, %{"action" => "auth", "username" => username, "password" => password}} ->
        validate_and_authenticate(username, password, state)

      _ ->
        {:error, :auth_required}
    end
  end

  defp validate_and_authenticate(
         username,
         password,
         %{socket: socket, transport: transport, client_ip: client_ip} = state
       ) do
    # STEP 1: Check account lockout (OWASP: most specific control first)
    case LockoutManager.locked?(username, client_ip) do
      {:locked, time_remaining_ms} ->
        # Account is locked - reject immediately
        Malachi.Metrics.increment_account_lockout_blocked()

        TCPProtocol.send_error(
          socket,
          :account_locked,
          %{
            "time_remaining_ms" => time_remaining_ms,
            "locked_until" => System.system_time(:millisecond) + time_remaining_ms
          },
          transport
        )

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
                # Success - clear lockout data
                LockoutManager.record_successful_auth(username, client_ip)
                validate_token_and_respond(token, state, client_ip)

              {:error, _reason} ->
                # Failed - record attempt (may trigger lockout)
                LockoutManager.record_failed_attempt(username, client_ip)
                {:error, :invalid_credentials}
            end

          {:error, :rate_limit_exceeded, retry_after_ms} ->
            # Rate limit exceeded
            Malachi.Metrics.increment_rate_limit_blocked(:auth)
            TCPProtocol.send_error(socket, :rate_limit_exceeded, %{"retry_after_ms" => retry_after_ms}, transport)
            {:error, :rate_limit_exceeded}
        end
    end
  end

  defp validate_token_and_respond(token, state, client_ip) do
    %{socket: socket, transport: transport} = state

    case Malachi.Auth.validate_token(token, client_ip) do
      {:ok, session} ->
        response = Jason.encode!(%{"s" => "ok", "token" => token})
        transport.send(socket, response <> "\n")
        {:ok, %{state | session: session}}

      {:error, _} ->
        {:error, :invalid_token}
    end
  end

  # New state-based receive_loop
  defp receive_loop(%{subscribed: false} = state) do
    # Passive mode: only receive socket data
    %{socket: socket, transport: transport, buffer: buffer, session: _session} = state
    recv_timeout = Application.get_env(:malachi, :tcp_recv_timeout, 30_000)

    case recv_data(socket, transport, 0, recv_timeout) do
      {:ok, data} ->
        new_buffer = buffer <> data

        case process_buffered_lines(new_buffer, state) do
          {:subscribed, remaining, updated_state} ->
            receive_loop(%{updated_state | subscribed: true, buffer: remaining})

          {:ok, remaining, updated_state} ->
            receive_loop(%{updated_state | buffer: remaining})
        end

      {:error, _} ->
        close_socket(socket, transport)
    end
  end

  defp receive_loop(%{subscribed: true} = state) do
    # Active mode: receive both socket data and queue messages
    %{socket: socket, transport: transport} = state
    set_socket_opts(socket, transport, active: :once)
    receive_active_loop(state)
  end

  # Process complete lines from buffer, return remaining incomplete data
  defp process_buffered_lines(buffer, state) do
    case String.split(buffer, "\n", parts: 2) do
      [complete_line, rest] when complete_line != "" ->
        case process_authenticated(complete_line, state) do
          :subscribed ->
            {:subscribed, rest, state}

          :ok ->
            process_buffered_lines(rest, state)

          _ ->
            process_buffered_lines(rest, state)
        end

      [incomplete] ->
        # No complete line yet, keep buffering
        {:ok, incomplete, state}

      ["", rest] ->
        # Empty line (double newline), skip it
        process_buffered_lines(rest, state)
    end
  end

  defp receive_active_loop(%{socket: socket, session: _session, transport: transport, buffer: buffer} = state) do
    receive do
      # TCP data received
      {:tcp, ^socket, data} ->
        new_buffer = buffer <> data
        remaining = process_active_buffered_lines(new_buffer, state)
        set_socket_opts(socket, transport, active: :once)
        receive_active_loop(%{state | buffer: remaining})

      # SSL data received
      {:ssl, ^socket, data} ->
        new_buffer = buffer <> data
        remaining = process_active_buffered_lines(new_buffer, state)
        set_socket_opts(socket, transport, active: :once)
        receive_active_loop(%{state | buffer: remaining})

      # Queue message to forward to client
      {:queue_message, message} ->
        TCPProtocol.send_queue_message(socket, message, transport)
        set_socket_opts(socket, transport, active: :once)
        receive_active_loop(state)

      # Channel message to forward to client
      {:channel_message, message} ->
        TCPProtocol.send_channel_message(socket, message, transport)
        set_socket_opts(socket, transport, active: :once)
        receive_active_loop(state)

      # Connection closed
      {:tcp_closed, ^socket} ->
        :ok

      {:ssl_closed, ^socket} ->
        :ok

      # Connection error
      {:tcp_error, ^socket, _reason} ->
        close_socket(socket, transport)

      {:ssl_error, ^socket, _reason} ->
        close_socket(socket, transport)
    end
  end

  defp process_active_buffered_lines(buffer, state) do
    case String.split(buffer, "\n", parts: 2) do
      [complete_line, rest] when complete_line != "" ->
        process_authenticated(complete_line, state)
        process_active_buffered_lines(rest, state)

      [incomplete] ->
        incomplete

      ["", rest] ->
        process_active_buffered_lines(rest, state)
    end
  end

  @compile {:inline, process_authenticated: 2}
  defp process_authenticated(
         data,
         %{socket: socket, session: session, transport: transport, client_ip: client_ip} = _state
       ) do
    TCPProtocol.process_message(socket, data, session, transport, client_ip)
  end

  defp close_socket(socket, :ssl), do: :ssl.close(socket)
  defp close_socket(socket, :gen_tcp), do: :gen_tcp.close(socket)

  defp send_error(socket, reason, transport) do
    TCPProtocol.send_error(socket, reason, transport)
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
