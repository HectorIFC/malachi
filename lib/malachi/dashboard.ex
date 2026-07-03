defmodule Malachi.Dashboard do
  use GenServer
  require Logger
  alias Malachi.AuditLog
  alias Malachi.Auth
  alias Malachi.ConnectionRegistry
  alias Malachi.Dashboard.SecurityHeaders
  alias Malachi.I18n
  alias Malachi.Metrics
  alias Malachi.RateLimiter

  def start_link(port) do
    GenServer.start_link(__MODULE__, port, name: __MODULE__)
  end

  @impl true
  def init(port) do
    opts = [:binary, packet: :http, active: false, reuseaddr: true]

    case :gen_tcp.listen(port, opts) do
      {:ok, socket} ->
        Logger.info(I18n.t(:dashboard_started, port: port))
        send(self(), :accept)
        {:ok, %{socket: socket, port: port}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_info(:accept, %{socket: socket} = state) do
    case :gen_tcp.accept(socket) do
      {:ok, client} ->
        spawn(fn -> handle_http(client) end)
        send(self(), :accept)
        {:noreply, state}

      {:error, _} ->
        send(self(), :accept)
        {:noreply, state}
    end
  end

  defp handle_http(socket) do
    # Get client IP for logging and authentication
    {:ok, {client_ip, _port}} = :inet.peername(socket)

    case :gen_tcp.recv(socket, 0) do
      {:ok, {:http_request, method, {:abs_path, path}, _version}} ->
        headers = parse_headers(socket, %{})
        path_string = to_string(path)
        handle_route_with_auth(socket, %{method: method, path: path_string}, headers, client_ip)

      {:ok, {:http_request, method, :*, _version}} ->
        headers = parse_headers(socket, %{})
        handle_route_with_auth(socket, %{method: method, path: "*"}, headers, client_ip)

      {:ok, _other} ->
        :gen_tcp.close(socket)

      {:error, _} ->
        :gen_tcp.close(socket)
    end
  end

  defp parse_headers(socket, headers_map) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, :http_eoh} ->
        headers_map

      {:ok, {:http_header, _, header_name, _, value}} ->
        # Convert header name to lowercase string
        key =
          case header_name do
            :Authorization -> "authorization"
            :Origin -> "origin"
            :Content_Type -> "content-type"
            :Content_Length -> "content-length"
            atom when is_atom(atom) -> atom |> to_string() |> String.downcase()
            string when is_binary(string) -> String.downcase(string)
            charlist when is_list(charlist) -> charlist |> to_string() |> String.downcase()
          end

        parse_headers(socket, Map.put(headers_map, key, to_string(value)))

      _ ->
        headers_map
    end
  end

  defp handle_route_with_auth(socket, request, headers, client_ip) do
    auth_enabled = Application.get_env(:malachi, :dashboard_auth_enabled, true)

    # Public routes that don't require authentication
    is_public_route =
      request.path in ["/login", "/logout", "/logo.jpeg"] or request.method == :OPTIONS

    if not auth_enabled or is_public_route do
      # Authentication disabled or public route - allow all requests
      handle_route(socket, request, headers, client_ip, nil)
    else
      # Strip query string from path (e.g. /stream?foo=bar → /stream)
      path = request.path |> String.split("?", parts: 2) |> List.first()

      # Authenticate via Cookie (primary, for browser) or Authorization header (fallback, for API)
      auth_result = authenticate_request(headers, client_ip, path)

      case auth_result do
        {:ok, session} ->
          # Log successful access
          AuditLog.log_event(
            :dashboard_access,
            %{username: session.username, ip: client_ip},
            "http_#{request.method}_#{path}",
            :success,
            %{path: path, method: request.method}
          )

          handle_route(socket, %{request | path: path}, headers, client_ip, session)

        {:error, :authentication_required} ->
          # For HTML page routes, redirect to login page instead of returning JSON 401
          if html_route?(path) do
            send_redirect_to_login(socket)
          else
            send_auth_required(socket, path)
          end

        {:error, :rate_limit_exceeded, retry_after_ms} ->
          Metrics.increment_dashboard_auth_blocked()

          AuditLog.log_event(
            :dashboard_auth_failure,
            %{ip: client_ip},
            "http_#{request.method}_#{request.path}",
            :rate_limited,
            %{path: request.path, retry_after_ms: retry_after_ms}
          )

          send_rate_limited(socket, retry_after_ms)

        {:error, reason} ->
          Metrics.increment_dashboard_auth_failed()

          AuditLog.log_event(
            :dashboard_auth_failure,
            %{ip: client_ip},
            "http_#{request.method}_#{request.path}",
            :failure,
            %{path: request.path, reason: reason}
          )

          send_forbidden(socket, reason)
      end
    end
  end

  defp html_route?(path), do: path in ["/", "/stream"]

  defp authenticate_request(headers, client_ip, path) do
    # Try Cookie first (browser navigation + EventSource), then Authorization header (API/curl)
    token = extract_token_from_cookie(headers) || extract_bearer_token(headers)

    case token do
      nil -> {:error, :authentication_required}
      token_value -> validate_token_with_rate_limit(token_value, client_ip, path)
    end
  end

  defp extract_token_from_cookie(headers) do
    case Map.get(headers, "cookie") do
      nil ->
        nil

      cookie_string ->
        cookie_string
        |> String.split(";")
        |> Enum.find_value(fn cookie ->
          case cookie |> String.trim() |> String.split("=", parts: 2) do
            ["malachi_token", value] -> String.trim(value)
            _ -> nil
          end
        end)
    end
  end

  defp extract_bearer_token(headers) do
    case Map.get(headers, "authorization") do
      "Bearer " <> token -> token
      _ -> nil
    end
  end

  defp validate_token_with_rate_limit(token, client_ip, path) do
    # Check rate limit before validating token (prevent brute force)
    rate_limit = Application.get_env(:malachi, :dashboard_auth_rate_limit, 10)
    rate_window = Application.get_env(:malachi, :dashboard_auth_rate_window_ms, 60_000)

    client_ip_string = format_ip_for_rate_limit(client_ip)

    case RateLimiter.check_limit(client_ip_string, :dashboard_auth, %{
           limit: rate_limit,
           window_ms: rate_window
         }) do
      :ok ->
        # Validate token
        case Auth.validate_token(token, client_ip) do
          {:ok, session_data} ->
            # Check permissions based on path
            if has_required_permission?(session_data.permissions, path) do
              {:ok, session_data}
            else
              {:error, :insufficient_permissions}
            end

          {:error, reason} ->
            {:error, reason}
        end

      {:error, :rate_limit_exceeded, retry_after_ms} ->
        {:error, :rate_limit_exceeded, retry_after_ms}
    end
  end

  defp has_required_permission?(permissions, path) do
    require_admin = Application.get_env(:malachi, :dashboard_require_admin_for_html, true)

    cond do
      # Admin has access to everything
      :admin in permissions ->
        true

      # HTML dashboard and SSE stream require admin (if configured)
      path in ["/", "/stream"] and require_admin ->
        false

      # Metrics endpoint accessible to any authenticated user
      path in ["/metrics", "/rate_limits"] ->
        true

      # Login endpoint is public
      path == "/login" ->
        true

      # Default: deny
      true ->
        false
    end
  end

  # The ip here is always a tuple (or an :inet error atom, caught by the catch-all), so dialyzer flags
  # the is_binary/1 clause as unreachable. Keep it as defense in case a caller ever passes a string ip
  # and suppress the warning rather than drop the clause.
  @dialyzer {:nowarn_function, format_ip_for_rate_limit: 1}
  defp format_ip_for_rate_limit(ip) when is_tuple(ip) do
    ip |> :inet.ntoa() |> to_string()
  end

  defp format_ip_for_rate_limit(ip) when is_binary(ip), do: ip
  defp format_ip_for_rate_limit(_), do: "unknown"

  defp send_redirect_to_login(socket) do
    response = "HTTP/1.1 302 Found\r\nLocation: /login\r\nCache-Control: no-store\r\nContent-Length: 0\r\n\r\n"

    :gen_tcp.send(socket, response)
    :gen_tcp.close(socket)
  end

  defp send_login_success(socket, token) do
    response_body = Jason.encode!(%{"s" => "ok", "token" => token})

    secure_flag =
      if Application.get_env(:malachi, :enable_tls), do: "; Secure", else: ""

    cookie_header =
      "Set-Cookie: malachi_token=#{token}; HttpOnly; Path=/; SameSite=Strict#{secure_flag}"

    response = """
    HTTP/1.1 200 OK\r
    Content-Type: application/json\r
    #{cookie_header}\r
    Content-Length: #{byte_size(response_body)}\r
    \r
    #{response_body}
    """

    response_with_headers =
      SecurityHeaders.add_security_headers(response, "/login")

    :gen_tcp.send(socket, response_with_headers)
    :gen_tcp.close(socket)
  end

  defp send_auth_required(socket, path) do
    body = Jason.encode!(%{"s" => "err", "reason" => "authentication_required"})

    response = """
    HTTP/1.1 401 Unauthorized\r
    WWW-Authenticate: Bearer realm="Malachi Dashboard"\r
    Content-Type: application/json\r
    Content-Length: #{byte_size(body)}\r
    \r
    #{body}
    """

    response_with_headers =
      SecurityHeaders.add_security_headers(response, path)

    :gen_tcp.send(socket, response_with_headers)
    :gen_tcp.close(socket)
  end

  defp send_forbidden(socket, reason) do
    body = Jason.encode!(%{"s" => "err", "reason" => to_string(reason)})

    response = """
    HTTP/1.1 403 Forbidden\r
    Content-Type: application/json\r
    Content-Length: #{byte_size(body)}\r
    \r
    #{body}
    """

    response_with_headers =
      SecurityHeaders.add_security_headers(response, "/forbidden")

    :gen_tcp.send(socket, response_with_headers)
    :gen_tcp.close(socket)
  end

  defp send_rate_limited(socket, retry_after_ms) do
    body = Jason.encode!(%{"s" => "err", "reason" => "rate_limit_exceeded", "retry_after_ms" => retry_after_ms})

    response = """
    HTTP/1.1 429 Too Many Requests\r
    Content-Type: application/json\r
    Retry-After: #{div(retry_after_ms, 1000)}\r
    Content-Length: #{byte_size(body)}\r
    \r
    #{body}
    """

    response_with_headers =
      SecurityHeaders.add_security_headers(response, "/rate_limited")

    :gen_tcp.send(socket, response_with_headers)
    :gen_tcp.close(socket)
  end

  defp serve_cors_preflight(socket, headers) do
    origin = Map.get(headers, "origin", "*")

    response = """
    HTTP/1.1 204 No Content\r
    Access-Control-Allow-Origin: #{origin}\r
    Access-Control-Allow-Methods: GET, POST, OPTIONS\r
    Access-Control-Allow-Headers: Authorization, Content-Type\r
    Access-Control-Max-Age: 86400\r
    Content-Length: 0\r
    \r
    """

    :gen_tcp.send(socket, response)
    :gen_tcp.close(socket)
  end

  defp handle_login(socket, headers, client_ip) do
    content_length = Map.get(headers, "content-length", "0") |> String.to_integer()

    # Read request body
    body =
      if content_length > 0 do
        :inet.setopts(socket, packet: :raw)

        case :gen_tcp.recv(socket, content_length, 5000) do
          {:ok, data} -> data
          _ -> ""
        end
      else
        ""
      end

    # Parse JSON body
    case Jason.decode(body) do
      {:ok, %{"username" => username, "password" => password}} ->
        # Rate limit check
        rate_limit = Application.get_env(:malachi, :dashboard_auth_rate_limit, 10)
        rate_window = Application.get_env(:malachi, :dashboard_auth_rate_window_ms, 60_000)
        client_ip_string = format_ip_for_rate_limit(client_ip)

        case RateLimiter.check_limit(client_ip_string, :dashboard_auth, %{
               limit: rate_limit,
               window_ms: rate_window
             }) do
          :ok ->
            # Authenticate
            case Auth.authenticate(username, password, client_ip) do
              {:ok, token} ->
                Metrics.increment_dashboard_auth_success()

                AuditLog.log_event(
                  :dashboard_login_success,
                  %{username: username, ip: client_ip},
                  "login",
                  :success,
                  %{}
                )

                send_login_success(socket, token)

              {:error, _reason} ->
                Metrics.increment_dashboard_auth_failed()

                AuditLog.log_event(
                  :dashboard_auth_failure,
                  %{username: username, ip: client_ip},
                  "login",
                  :failure,
                  %{reason: :invalid_credentials}
                )

                send_forbidden(socket, :invalid_credentials)
            end

          {:error, :rate_limit_exceeded, retry_after_ms} ->
            Metrics.increment_dashboard_auth_blocked()
            send_rate_limited(socket, retry_after_ms)
        end

      _ ->
        send_forbidden(socket, :invalid_request)
    end
  end

  defp serve_login_page(socket) do
    html = login_page_html()

    response = """
    HTTP/1.1 200 OK\r
    Content-Type: text/html; charset=utf-8\r
    Content-Length: #{byte_size(html)}\r
    Cache-Control: no-store, no-cache, must-revalidate\r
    \r
    #{html}
    """

    response_with_headers =
      SecurityHeaders.add_security_headers(response, "/login")

    :gen_tcp.send(socket, response_with_headers)
    :gen_tcp.close(socket)
  end

  defp handle_route(socket, %{method: :GET, path: "/"}, _headers, _client_ip, _session),
    do: serve_html(socket)

  defp handle_route(socket, %{method: :GET, path: "/login"}, _headers, _client_ip, _session),
    do: serve_login_page(socket)

  defp handle_route(socket, %{method: :POST, path: "/login"}, headers, client_ip, _session),
    do: handle_login(socket, headers, client_ip)

  defp handle_route(socket, %{method: :GET, path: "/metrics"}, _headers, _client_ip, _session),
    do: serve_metrics(socket)

  defp handle_route(socket, %{method: :GET, path: "/stream"}, _headers, _client_ip, _session),
    do: serve_sse(socket)

  defp handle_route(socket, %{method: :GET, path: "/rate_limits"}, _headers, _client_ip, _session),
    do: serve_rate_limits(socket)

  defp handle_route(socket, %{method: :GET, path: "/logout"}, headers, _client_ip, _session),
    do: serve_logout(socket, headers)

  defp handle_route(socket, %{method: :GET, path: "/logo.jpeg"}, _headers, _client_ip, _session),
    do: serve_logo(socket)

  defp handle_route(socket, %{method: :OPTIONS}, headers, _client_ip, _session),
    do: serve_cors_preflight(socket, headers)

  defp handle_route(socket, _, _headers, _client_ip, _session), do: serve_404(socket)

  defp serve_html(socket) do
    html = dashboard_html()

    response = """
    HTTP/1.1 200 OK\r
    Content-Type: text/html; charset=utf-8\r
    Content-Length: #{byte_size(html)}\r
    Cache-Control: no-store, no-cache, must-revalidate\r
    \r
    #{html}
    """

    response_with_headers =
      SecurityHeaders.add_security_headers(response, "/")

    :gen_tcp.send(socket, response_with_headers)
    :gen_tcp.close(socket)
  end

  defp serve_metrics(socket) do
    queue_metrics = Metrics.get_all_metrics()

    # Enrich each queue with producer/consumer IPs
    enriched_queues =
      Enum.map(queue_metrics, fn queue ->
        producers = ConnectionRegistry.list_producers_by_queue(queue.queue)
        consumers = ConnectionRegistry.list_consumers_by_queue(queue.queue)

        Map.merge(queue, %{
          producer_ips: producers,
          consumer_ips: consumers
        })
      end)

    channel_metrics = Metrics.get_all_channel_metrics()

    # Enrich each channel with subscriber and publisher IPs
    enriched_channels =
      Enum.map(channel_metrics, fn channel ->
        subscribers = ConnectionRegistry.list_subscribers_by_channel(channel.channel)
        publishers = ConnectionRegistry.list_publishers_by_channel(channel.channel)

        Map.merge(channel, %{
          subscriber_ips: subscribers,
          publisher_ips: publishers
        })
      end)

    metrics = %{
      queues: enriched_queues,
      channels: enriched_channels,
      system: Metrics.get_system_metrics()
    }

    json = Jason.encode!(metrics)

    response = """
    HTTP/1.1 200 OK\r
    Content-Type: application/json\r
    Content-Length: #{byte_size(json)}\r
    Cache-Control: no-cache\r
    \r
    #{json}
    """

    response_with_headers =
      SecurityHeaders.add_security_headers(response, "/metrics")

    :gen_tcp.send(socket, response_with_headers)
    :gen_tcp.close(socket)
  end

  defp serve_sse(socket) do
    response = """
    HTTP/1.1 200 OK\r
    Content-Type: text/event-stream\r
    Cache-Control: no-cache\r
    Connection: keep-alive\r
    \r
    """

    response_with_headers =
      SecurityHeaders.add_security_headers(response, "/stream")

    :gen_tcp.send(socket, response_with_headers)
    :inet.setopts(socket, packet: :raw)

    stream_metrics(socket)
  end

  defp stream_metrics(socket) do
    queue_metrics = Metrics.get_all_metrics()

    # Enrich each queue with producer/consumer IPs
    enriched_queues =
      Enum.map(queue_metrics, fn queue ->
        producers = ConnectionRegistry.list_producers_by_queue(queue.queue)
        consumers = ConnectionRegistry.list_consumers_by_queue(queue.queue)

        Map.merge(queue, %{
          producer_ips: producers,
          consumer_ips: consumers
        })
      end)

    channel_metrics = Metrics.get_all_channel_metrics()

    # Enrich each channel with subscriber and publisher IPs
    enriched_channels =
      Enum.map(channel_metrics, fn channel ->
        subscribers = ConnectionRegistry.list_subscribers_by_channel(channel.channel)
        publishers = ConnectionRegistry.list_publishers_by_channel(channel.channel)

        Map.merge(channel, %{
          subscriber_ips: subscribers,
          publisher_ips: publishers
        })
      end)

    metrics = %{
      queues: enriched_queues,
      channels: enriched_channels,
      system: Metrics.get_system_metrics()
    }

    json = Jason.encode!(metrics)
    event = "data: #{json}\n\n"

    update_interval = Application.get_env(:malachi, :dashboard_update_interval_ms, 1000)

    case :gen_tcp.send(socket, event) do
      :ok ->
        Process.sleep(update_interval)
        stream_metrics(socket)

      {:error, _} ->
        :gen_tcp.close(socket)
    end
  end

  defp serve_rate_limits(socket) do
    rate_limits = %{
      enabled: Application.get_env(:malachi, :rate_limit_enabled, true),
      top_blocked: %{
        auth: format_top_blocked(RateLimiter.get_top_blocked(:auth, 20)),
        publish: format_top_blocked(RateLimiter.get_top_blocked(:publish, 20)),
        subscribe: format_top_blocked(RateLimiter.get_top_blocked(:subscribe, 20)),
        channel_publish: format_top_blocked(RateLimiter.get_top_blocked(:channel_publish, 20)),
        channel_subscribe: format_top_blocked(RateLimiter.get_top_blocked(:channel_subscribe, 20))
      },
      config: %{
        auth: %{
          limit: Application.get_env(:malachi, :auth_rate_limit, 10),
          window_ms: Application.get_env(:malachi, :auth_rate_window_ms, 60_000)
        },
        publish: %{
          limit: Application.get_env(:malachi, :publish_rate_limit, 1_000),
          window_ms: Application.get_env(:malachi, :publish_rate_window_ms, 1_000)
        },
        subscribe: %{
          limit: Application.get_env(:malachi, :subscribe_rate_limit, 100),
          window_ms: Application.get_env(:malachi, :subscribe_rate_window_ms, 60_000)
        }
      }
    }

    json = Jason.encode!(rate_limits)

    response = """
    HTTP/1.1 200 OK\r
    Content-Type: application/json\r
    Content-Length: #{byte_size(json)}\r
    Cache-Control: no-cache\r
    \r
    #{json}
    """

    response_with_headers =
      SecurityHeaders.add_security_headers(response, "/rate_limits")

    :gen_tcp.send(socket, response_with_headers)
    :gen_tcp.close(socket)
  end

  defp serve_logout(socket, headers) do
    # Revoke the session if a cookie token is present
    case extract_token_from_cookie(headers) do
      nil -> :ok
      token -> Auth.logout(token)
    end

    secure_flag =
      if Application.get_env(:malachi, :enable_tls), do: "; Secure", else: ""

    response =
      "HTTP/1.1 302 Found\r\nLocation: /login\r\nSet-Cookie: malachi_token=; HttpOnly; Path=/; SameSite=Strict; Max-Age=0#{secure_flag}\r\nCache-Control: no-store\r\nContent-Length: 0\r\n\r\n"

    :gen_tcp.send(socket, response)
    :gen_tcp.close(socket)
  end

  defp serve_logo(socket) do
    logo_path = Path.join(:code.priv_dir(:malachi), "static/logo.jpeg")

    case File.read(logo_path) do
      {:ok, data} ->
        header =
          "HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nContent-Length: #{byte_size(data)}\r\nCache-Control: public, max-age=86400\r\n\r\n"

        :gen_tcp.send(socket, [header, data])
        :gen_tcp.close(socket)

      {:error, _} ->
        serve_404(socket)
    end
  end

  defp serve_404(socket) do
    body = Jason.encode!(%{"s" => "err", "reason" => "not_found"})

    response = """
    HTTP/1.1 404 Not Found\r
    Content-Type: application/json\r
    Content-Length: #{byte_size(body)}\r
    \r
    #{body}
    """

    response_with_headers =
      SecurityHeaders.add_security_headers(response, "/404")

    :gen_tcp.send(socket, response_with_headers)
    :gen_tcp.close(socket)
  end

  # Convert list of {identifier, count} tuples to JSON-encodable list of maps
  defp format_top_blocked(entries) do
    Enum.map(entries, fn {identifier, count} ->
      %{"identifier" => to_string(identifier), "count" => count}
    end)
  end

  defp dashboard_html do
    """
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="utf-8">
      <title>Malachi Dashboard</title>
      <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
          font-family: -apple-system, sans-serif;
          background: #0a0e27;
          color: #e0e0e0;
          padding: 20px;
        }
        h1 { color: #00d9ff; margin-bottom: 0; }
        h2 { color: #00d9ff; margin-bottom: 15px; font-size: 1.2em; }
        .dashboard-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: 20px;
        }
        .logout-btn {
          background: transparent;
          color: #e0e0e0;
          border: 1px solid #2a3f5f;
          padding: 8px 18px;
          border-radius: 6px;
          cursor: pointer;
          font-size: 0.9em;
          text-decoration: none;
          transition: all 0.2s;
        }
        .logout-btn:hover {
          background: #2a3f5f;
          border-color: #ff4757;
          color: #ff4757;
        }
        .card {
          background: #1a1f3a;
          border: 1px solid #2a3f5f;
          border-radius: 8px;
          padding: 20px;
          margin-bottom: 20px;
        }
        .metric { display: flex; justify-content: space-between; padding: 8px 0; }
        .metric-value { color: #00ff88; font-family: monospace; }
        .grid-container {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
          gap: 20px;
          margin-bottom: 20px;
        }
        .connection-list {
          max-height: 200px;
          overflow-y: auto;
          margin-top: 10px;
          padding: 5px;
        }
        .connection-item {
          background: #0f1428;
          border-left: 3px solid #00d9ff;
          padding: 8px;
          margin-bottom: 6px;
          border-radius: 4px;
          font-size: 0.85em;
        }
        .connection-item .ip {
          color: #00ff88;
          font-family: monospace;
          font-weight: bold;
          font-size: 0.9em;
        }
        .connection-item .time {
          color: #888;
          font-size: 0.8em;
        }
        .pagination {
          display: flex;
          justify-content: center;
          gap: 10px;
          margin-top: 15px;
        }
        .pagination button {
          background: #2a3f5f;
          border: 1px solid #3a4f6f;
          color: #e0e0e0;
          padding: 8px 15px;
          border-radius: 5px;
          cursor: pointer;
          font-size: 0.9em;
        }
        .pagination button:hover:not(:disabled) {
          background: #3a4f6f;
        }
        .pagination button:disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }
        .pagination .page-info {
          display: flex;
          align-items: center;
          color: #888;
        }
        .empty-state {
          text-align: center;
          color: #666;
          padding: 30px;
          font-style: italic;
        }
        .queue-card {
          background: #1a1f3a;
          border: 1px solid #2a3f5f;
          border-left: 4px solid #00d9ff;
          border-radius: 8px;
          padding: 15px;
          margin-bottom: 15px;
        }
        .channel-card {
          background: #1a1f3a;
          border: 1px solid #2a3f5f;
          border-left: 4px solid #a855f7;
          border-radius: 8px;
          padding: 15px;
          margin-bottom: 15px;
        }
        .queue-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: 12px;
          padding-bottom: 12px;
          border-bottom: 1px solid #2a3f5f;
        }
        .queue-name {
          color: #00ff88;
          font-size: 1.1em;
          font-weight: bold;
          font-family: monospace;
        }
        .queue-metrics {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
          gap: 10px;
          margin-bottom: 15px;
          font-size: 0.9em;
        }
        .queue-metric-item {
          padding: 5px;
        }
        .connections-grid {
          display: grid;
          grid-template-columns: 1fr 1fr;
          gap: 15px;
          margin-top: 12px;
        }
        .connection-section {
          background: #0f1428;
          border-radius: 6px;
          padding: 10px;
        }
        .connection-section h3 {
          color: #00d9ff;
          font-size: 0.95em;
          margin-bottom: 8px;
          display: flex;
          align-items: center;
          gap: 8px;
        }
        .connection-count {
          background: #2a3f5f;
          color: #00ff88;
          padding: 2px 8px;
          border-radius: 12px;
          font-size: 0.85em;
        }
        .pressure-badge {
          display: inline-block;
          padding: 3px 10px;
          border-radius: 12px;
          font-size: 0.8em;
          font-weight: bold;
          text-transform: uppercase;
        }
        .pressure-low { background: #00ff88; color: #0a0e27; }
        .pressure-medium { background: #ffd93d; color: #0a0e27; }
        .pressure-high { background: #ff9500; color: #fff; }
        .pressure-full { background: #ff4757; color: #fff; animation: pulse 2s infinite; }
        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.7; }
        }
        .utilization-bar {
          width: 100%;
          height: 6px;
          background: #0f1428;
          border-radius: 3px;
          overflow: hidden;
          margin-top: 4px;
        }
        .utilization-fill {
          height: 100%;
          transition: width 0.3s ease, background-color 0.3s ease;
        }
        .util-low { background: linear-gradient(90deg, #00ff88, #00d9ff); }
        .util-medium { background: linear-gradient(90deg, #ffd93d, #ffaa00); }
        .util-high { background: linear-gradient(90deg, #ff9500, #ff6b6b); }
        .util-full { background: linear-gradient(90deg, #ff4757, #ff1744); }
      </style>
    </head>
    <body>
      <div class="dashboard-header">
        <h1><img src="/logo.jpeg" alt="Malachi" style="height: 42px; vertical-align: middle; margin-right: 10px;">Malachi Dashboard</h1>
        <a href="/logout" class="logout-btn">Logout</a>
      </div>
      <div class="card">
        <h2>System Status</h2>
        <div class="metric">
          <span>Processes:</span>
          <span class="metric-value" id="processes">-</span>
        </div>
        <div class="metric">
          <span>Memory:</span>
          <span class="metric-value" id="memory">-</span>
        </div>
      </div>

      <div class="card">
        <h2>Queues</h2>
        <div id="queues">Loading...</div>
      </div>

      <div class="card">
        <h2>Channels</h2>
        <div id="channels">Loading...</div>
      </div>
      <script>
        // HTML escape function to prevent XSS attacks
        function escapeHtml(unsafe) {
          return unsafe
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#039;");
        }

        // Pagination state - one state per queue
        const queuePaginationState = {};

        function formatTime(timestamp) {
          const now = Date.now();
          const diff = now - timestamp;
          const seconds = Math.floor(diff / 1000);
          const minutes = Math.floor(seconds / 60);
          const hours = Math.floor(minutes / 60);
          const days = Math.floor(hours / 24);

          if (days > 0) return `${days}d ago`;
          if (hours > 0) return `${hours}h ago`;
          if (minutes > 0) return `${minutes}m ago`;
          return `${seconds}s ago`;
        }

        function renderConnectionList(connections, type, queueName) {
          if (!queuePaginationState[queueName]) {
            queuePaginationState[queueName] = {
              producerPage: 1,
              consumerPage: 1,
              subscriberPage: 1,
              publisherPage: 1,
              perPage: 10
            };
          }

          const page = type === 'producer' 
            ? queuePaginationState[queueName].producerPage 
            : type === 'consumer'
            ? queuePaginationState[queueName].consumerPage
            : type === 'publisher'
            ? queuePaginationState[queueName].publisherPage
            : queuePaginationState[queueName].subscriberPage;
          const perPage = queuePaginationState[queueName].perPage;
          const start = (page - 1) * perPage;
          const end = start + perPage;
          const pageData = connections.slice(start, end);
          const totalPages = Math.ceil(connections.length / perPage);

          if (connections.length === 0) {
            return `
              <div class="connection-section">
                <h3>${type === 'producer' ? '📤 Producers' : type === 'consumer' ? '📥 Consumers' : type === 'publisher' ? '📤 Publishers' : '📡 Subscribers'} <span class="connection-count">0</span></h3>
                <div class="empty-state" style="padding: 15px; font-size: 0.85em;">No ${type}s</div>
              </div>
            `;
          }

          const itemsHtml = pageData.map(item => {
            const time = formatTime(item.connected_at);
            return `
              <div class="connection-item">
                <div class="ip">${escapeHtml(item.ip)}</div>
                <div class="time">Connected ${time}</div>
              </div>
            `;
          }).join('');

          const paginationHtml = totalPages > 1 ? `
            <div class="pagination">
              <button onclick="changeQueuePage('${escapeHtml(queueName)}', '${type}', -1)" ${page <= 1 ? 'disabled' : ''}>← Prev</button>
              <span class="page-info">Page ${page} of ${totalPages}</span>
              <button onclick="changeQueuePage('${escapeHtml(queueName)}', '${type}', 1)" ${page >= totalPages ? 'disabled' : ''}>Next →</button>
            </div>
          ` : '';

          return `
            <div class="connection-section">
              <h3>${type === 'producer' ? '📤 Producers' : type === 'consumer' ? '📥 Consumers' : type === 'publisher' ? '📤 Publishers' : '📡 Subscribers'} <span class="connection-count">${connections.length}</span></h3>
              <div class="connection-list">
                ${itemsHtml}
              </div>
              ${paginationHtml}
            </div>
          `;
        }

        function changeQueuePage(queueName, type, direction) {
          if (!queuePaginationState[queueName]) return;

          const currentPage = type === 'producer' 
            ? queuePaginationState[queueName].producerPage 
            : type === 'consumer'
            ? queuePaginationState[queueName].consumerPage
            : type === 'publisher'
            ? queuePaginationState[queueName].publisherPage
            : queuePaginationState[queueName].subscriberPage;
          const newPage = currentPage + direction;

          if (newPage >= 1) {
            if (type === 'producer') {
              queuePaginationState[queueName].producerPage = newPage;
            } else if (type === 'consumer') {
              queuePaginationState[queueName].consumerPage = newPage;
            } else if (type === 'publisher') {
              queuePaginationState[queueName].publisherPage = newPage;
            } else {
              queuePaginationState[queueName].subscriberPage = newPage;
            }
            // Force re-render by triggering update
            const lastData = window.lastMetricsData;
            if (lastData) {
              renderQueues(lastData.queues);
              renderChannels(lastData.channels);
            }
          }
        }

        function renderQueues(queues) {
          if (!queues || queues.length === 0) {
            document.getElementById('queues').innerHTML = '<div class="empty-state">No queues</div>';
            return;
          }

          const queuesHtml = queues.map(q => {
            const producers = q.producer_ips || [];
            const consumers = q.consumer_ips || [];

            const producerList = renderConnectionList(producers, 'producer', q.queue);
            const consumerList = renderConnectionList(consumers, 'consumer', q.queue);

            return `
              <div class="queue-card">
                <div class="queue-header">
                  <div class="queue-name">${escapeHtml(q.queue)}</div>
                </div>
                
                <div class="queue-metrics">
                  <div class="queue-metric-item">
                    <span style="color: #888;">Producers:</span> <span style="color: #00ff88;">${q.queue_stats.producers || 0}</span>
                  </div>
                  <div class="queue-metric-item">
                    <span style="color: #888;">Consumers:</span> <span style="color: #00ff88;">${q.queue_stats.consumers || 0}</span>
                  </div>
                  <div class="queue-metric-item">
                    <span style="color: #00ff88;">✓ Acked:</span> ${q.acked || 0}
                  </div>
                  <div class="queue-metric-item">
                    <span style="color: #ff6b6b;">✗ Nacked:</span> ${q.nacked || 0}
                  </div>
                  <div class="queue-metric-item">
                    <span style="color: #ffd93d;">⏳ Pending:</span> ${q.pending_ack || 0}
                  </div>
                  <div class="queue-metric-item">
                    <span style="color: #6bcfff;">↻ Retried:</span> ${q.retried || 0}
                  </div>
                  <div class="queue-metric-item">
                    <span style="color: #ff4757;">☠ DLQ:</span> ${q.dead_lettered || 0}
                  </div>
                  <div class="queue-metric-item">
                    <span style="color: #888;">Processed:</span> ${q.processed}
                  </div>
                  <div class="queue-metric-item">
                    <span style="color: #888;">Buffered:</span> ${q.queue_stats.buffered}/${q.max_buffer_size || 10000}
                  </div>
                  <div class="queue-metric-item">
                    <span style="color: #888;">Latency:</span> ${q.latency_us.avg ? (q.latency_us.avg/1000).toFixed(2) + 'ms' : '-'}
                  </div>
                  <div class="queue-metric-item" style="grid-column: span 2;">
                    <span style="color: #888;">Buffer Utilization:</span> <span style="color: #00ff88;">${(q.buffer_utilization_pct || 0).toFixed(1)}%</span>
                    <div class="utilization-bar">
                      <div class="utilization-fill ${
                        q.buffer_utilization_pct >= 100 ? 'util-full' :
                        q.buffer_utilization_pct >= 80 ? 'util-high' :
                        q.buffer_utilization_pct >= 50 ? 'util-medium' : 'util-low'
                      }" style="width: ${Math.min(q.buffer_utilization_pct || 0, 100)}%"></div>
                    </div>
                  </div>
                  <div class="queue-metric-item">
                    <span style="color: #888;">🔒 Blocked:</span> <span style="color: ${q.blocked_producers_count > 0 ? '#ff9500' : '#888'};">${q.blocked_producers_count || 0}</span>
                  </div>
                  <div class="queue-metric-item">
                    <span style="color: #888;">Rejected:</span> <span style="color: #ff6b6b;">${q.rejected || 0}</span>
                  </div>
                  <div class="queue-metric-item">
                    <span style="color: #888;">Dropped:</span> <span style="color: #ff9500;">${q.dropped || 0}</span>
                  </div>
                  <div class="queue-metric-item">
                    <span style="color: #888;">Overflow:</span> <span style="color: #00d9ff;">${escapeHtml(q.overflow_behavior || 'drop_newest')}</span>
                  </div>
                  <div class="queue-metric-item" style="grid-column: span 2;">
                    <span style="color: #888;">Pressure Status:</span> 
                    <span class="pressure-badge pressure-${
                      q.backpressure_status === 'full' ? 'full' :
                      q.backpressure_status === 'high_pressure' ? 'high' :
                      q.backpressure_status === 'medium_pressure' ? 'medium' : 'low'
                    }">${escapeHtml(q.backpressure_status || 'low_pressure').replace('_', ' ')}</span>
                  </div>
                </div>

                <div class="connections-grid">
                  ${producerList}
                  ${consumerList}
                </div>
              </div>
            `;
          }).join('');

          document.getElementById('queues').innerHTML = queuesHtml;
        }

        function renderChannels(channels) {
          if (!channels || channels.length === 0) {
            document.getElementById('channels').innerHTML = '<div class="empty-state">No channels</div>';
            return;
          }

          const channelsHtml = channels.map(c => {
            const subscribers = c.subscriber_ips || [];
            const publishers = c.publisher_ips || [];
            const subscriberList = renderConnectionList(subscribers, 'subscriber', c.channel);
            const publisherList = renderConnectionList(publishers, 'publisher', c.channel);
            
            // Calculate delivery and discard rates
            // Expected deliveries = published * subscribers (each message should go to all subscribers)
            const expectedDeliveries = c.published * (c.subscribers || 0);
            const deliveryRate = expectedDeliveries > 0 ? Math.min(((c.delivered / expectedDeliveries) * 100), 100).toFixed(2) : '0.00';
            const discardRate = c.published > 0 ? Math.min(((c.dropped / c.published) * 100), 100).toFixed(2) : '0.00';

            return `
              <div class="channel-card">
                <div class="queue-header">
                  <div class="queue-name">${escapeHtml(c.channel)}</div>
                </div>
                
                <div class="queue-metrics">
                  <div class="queue-metric-item">
                    <span style="color: #888;">Subscribers:</span> <span style="color: #00ff88;">${c.subscribers || 0}</span>
                  </div>
                  <div class="queue-metric-item">
                    <span style="color: #888;">Published:</span> ${c.published || 0}
                  </div>
                  <div class="queue-metric-item">
                    <span style="color: #00ff88;">✓ Delivered:</span> ${c.delivered || 0}
                  </div>
                  <div class="queue-metric-item">
                    <span style="color: #ff6b6b;">✗ Dropped:</span> ${c.dropped || 0}
                  </div>
                  <div class="queue-metric-item">
                    <span style="color: #a855f7;">📊 Delivery Rate:</span> ${deliveryRate}%
                  </div>
                  <div class="queue-metric-item">
                    <span style="color: #ff4757;">📉 Discard Rate:</span> ${discardRate}%
                  </div>
                </div>

                <div style="display: flex; gap: 24px; flex-wrap: wrap;">
                  ${publishers.length > 0 ? `
                    <div style="margin-top: 12px; flex: 1; min-width: 250px;">
                      ${publisherList}
                    </div>
                  ` : ''}
                  ${subscribers.length > 0 ? `
                    <div style="margin-top: 12px; flex: 1; min-width: 250px;">
                      ${subscriberList}
                    </div>
                  ` : ''}
                </div>
              </div>
            `;
          }).join('');

          document.getElementById('channels').innerHTML = channelsHtml;
        }

        // Auth is handled via HttpOnly cookies — sent automatically by the browser.
        const source = new EventSource('/stream');
        source.onmessage = (event) => {
          const data = JSON.parse(event.data);
          window.lastMetricsData = data;
          
          document.getElementById('processes').textContent = data.system.process_count;
          document.getElementById('memory').textContent = data.system.memory.total_mb.toFixed(2) + ' MB';

          renderQueues(data.queues);
          renderChannels(data.channels || []);
        };

        source.onerror = (error) => {
          console.error('EventSource error:', error);
          // Session likely expired - redirect to login
          if (source.readyState === EventSource.CLOSED) {
            window.location.href = '/login';
          }
        };
      </script>
    </body>
    </html>
    """
  end

  defp login_page_html do
    """
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="utf-8">
      <title>Malachi - Login</title>
      <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
          font-family: -apple-system, sans-serif;
          background: linear-gradient(135deg, #0a0e27 0%, #1a1f3a 100%);
          color: #e0e0e0;
          display: flex;
          align-items: center;
          justify-content: center;
          min-height: 100vh;
          padding: 20px;
        }
        .login-container {
          background: #1a1f3a;
          border: 1px solid #2a3f5f;
          border-radius: 12px;
          padding: 40px;
          width: 100%;
          max-width: 400px;
          box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        }
        h1 {
          color: #00d9ff;
          margin-bottom: 30px;
          text-align: center;
          font-size: 2em;
        }
        .form-group {
          margin-bottom: 20px;
        }
        label {
          display: block;
          color: #00d9ff;
          margin-bottom: 8px;
          font-size: 0.9em;
          font-weight: 500;
        }
        input {
          width: 100%;
          padding: 12px;
          background: #0a0e27;
          border: 1px solid #2a3f5f;
          border-radius: 6px;
          color: #e0e0e0;
          font-size: 1em;
          transition: border-color 0.3s;
        }
        input:focus {
          outline: none;
          border-color: #00d9ff;
        }
        button {
          width: 100%;
          padding: 14px;
          background: linear-gradient(135deg, #00d9ff 0%, #0099cc 100%);
          border: none;
          border-radius: 6px;
          color: #0a0e27;
          font-size: 1em;
          font-weight: bold;
          cursor: pointer;
          transition: transform 0.2s, box-shadow 0.2s;
        }
        button:hover {
          transform: translateY(-2px);
          box-shadow: 0 4px 12px rgba(0, 217, 255, 0.4);
        }
        button:active {
          transform: translateY(0);
        }
        button:disabled {
          opacity: 0.6;
          cursor: not-allowed;
          transform: none;
        }
        .error {
          background: rgba(255, 71, 87, 0.2);
          border: 1px solid #ff4757;
          border-radius: 6px;
          padding: 12px;
          margin-bottom: 20px;
          color: #ff4757;
          font-size: 0.9em;
          display: none;
        }
        .error.show {
          display: block;
        }
      </style>
    </head>
    <body>
      <div class="login-container">
        <h1><img src="/logo.jpeg" alt="Malachi" style="height: 48px; vertical-align: middle; margin-right: 10px;">Malachi</h1>
        <div id="error" class="error"></div>
        <form id="loginForm">
          <div class="form-group">
            <label for="username">Username</label>
            <input type="text" id="username" name="username" autocomplete="username" required autofocus>
          </div>
          <div class="form-group">
            <label for="password">Password</label>
            <input type="password" id="password" name="password" autocomplete="current-password" required>
          </div>
          <button type="submit" id="loginBtn">Login</button>
        </form>
      </div>
      <script>
        const form = document.getElementById('loginForm');
        const errorDiv = document.getElementById('error');
        const loginBtn = document.getElementById('loginBtn');

        form.addEventListener('submit', async (e) => {
          e.preventDefault();

          const username = document.getElementById('username').value;
          const password = document.getElementById('password').value;

          errorDiv.classList.remove('show');
          loginBtn.disabled = true;
          loginBtn.textContent = 'Logging in...';

          try {
            const response = await fetch('/login', {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json'
              },
              body: JSON.stringify({ username, password })
            });

            const data = await response.json();

            if (data.s === 'ok') {
              // Cookie is set automatically by the server (HttpOnly)
              // Redirect to dashboard
              window.location.href = '/';
            } else {
              throw new Error(data.reason || 'Login failed');
            }
          } catch (error) {
            errorDiv.textContent = getErrorMessage(error.message);
            errorDiv.classList.add('show');
            loginBtn.disabled = false;
            loginBtn.textContent = 'Login';
          }
        });

        function getErrorMessage(reason) {
          const messages = {
            'invalid_credentials': 'Invalid username or password',
            'rate_limit_exceeded': 'Too many attempts. Please try again later.',
            'authentication_required': 'Authentication required',
            'insufficient_permissions': 'Insufficient permissions'
          };
          return messages[reason] || 'An error occurred. Please try again.';
        }

      </script>
    </body>
    </html>
    """
  end
end
