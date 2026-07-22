defmodule Malachi.Dashboard do
  @moduledoc """
  The built-in HTTP dashboard: a `GenServer` that listens on a port and serves the operational UI and
  metrics endpoints over plain HTTP.

  It accepts connections on its listen socket and handles each request in a spawned process, wrapping
  the routed handlers with authentication (`Malachi.Auth`), per-IP rate limiting
  (`Malachi.RateLimiter`), security headers (`Malachi.Dashboard.SecurityHeaders`), and audit logging
  (`Malachi.AuditLog`).
  """
  use GenServer
  require Logger
  alias Malachi.AuditLog
  alias Malachi.Auth
  alias Malachi.BrokerServer
  alias Malachi.Dashboard.SecurityHeaders
  alias Malachi.I18n
  alias Malachi.Metadata
  alias Malachi.Metrics
  alias Malachi.Metrics.Prometheus
  alias Malachi.RateLimiter

  @doc "Starts the dashboard HTTP server listening on `port` (registered under the module name)."
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

    # Split the query string off the path (e.g. /topic?name=x → path "/topic", query "name=x"), so routes
    # match on the bare path and a handler can still read its query.
    {path, query} =
      case String.split(request.path, "?", parts: 2) do
        [path, query] -> {path, query}
        [path] -> {path, ""}
      end

    request = request |> Map.put(:path, path) |> Map.put(:query, query)

    # Public routes that don't require authentication (health/readiness probes never send credentials).
    is_public_route =
      path in ["/login", "/logout", "/logo.svg", "/health", "/ready"] or request.method == :OPTIONS

    if not auth_enabled or is_public_route do
      # Authentication disabled or public route - allow all requests
      handle_route(socket, request, headers, client_ip, nil)
    else
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

          handle_route(socket, request, headers, client_ip, session)

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

      # Read-only data endpoints accessible to any authenticated user
      path in ["/metrics", "/rate_limits", "/topic"] ->
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
    case read_json_body(socket, headers) do
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

  # --- admin user management (P3): REST CRUD over the replicated user store. The auth stage already gated
  # these to the :admin permission (has_required_permission?), so the handlers run only for admins. Passwords
  # arrive in the JSON body in the clear, so run the dashboard over TLS in production. ---

  defp handle_list_users(socket) do
    users =
      Enum.map(Auth.list_users(), fn %{username: u, permissions: perms} ->
        %{"username" => u, "permissions" => Enum.map(perms, &to_string/1)}
      end)

    send_json(socket, "200 OK", %{"s" => "ok", "users" => users})
  end

  defp handle_create_user(socket, headers) do
    case read_json_body(socket, headers) do
      {:ok, %{"username" => username, "password" => password} = body} ->
        case Auth.parse_permissions(Map.get(body, "permissions", ["produce", "consume"])) do
          {:ok, permissions} ->
            respond_user_result(socket, Auth.add_user(username, password, permissions), "201 Created")

          :error ->
            send_json(socket, "400 Bad Request", %{"s" => "err", "reason" => "invalid_permissions"})
        end

      _malformed ->
        send_json(socket, "400 Bad Request", %{"s" => "err", "reason" => "invalid_request"})
    end
  end

  # PUT /users/:username/password, rotate a user's password.
  defp handle_user_password(socket, rest, headers) do
    case String.split(rest, "/") do
      [username, "password"] when username != "" -> handle_change_password(socket, username, headers)
      _other -> serve_404(socket)
    end
  end

  defp handle_change_password(socket, username, headers) do
    case read_json_body(socket, headers) do
      {:ok, %{"password" => password}} ->
        respond_user_result(socket, Auth.change_password(username, password), "200 OK")

      _malformed ->
        send_json(socket, "400 Bad Request", %{"s" => "err", "reason" => "invalid_request"})
    end
  end

  defp handle_delete_user(socket, username), do: respond_user_result(socket, Auth.remove_user(username), "200 OK")

  # --- per-topic ACL management (P5): /users/:username/acls, admin-gated by has_required_permission?. ---

  # Routes a /users/<rest> request: `acl_fun.(username)` when `rest` is `"<username>/acls"`, else `fallback`.
  defp route_acl(_socket, rest, acl_fun, fallback) do
    case acl_username(rest) do
      {:ok, username} -> acl_fun.(username)
      :error -> fallback.()
    end
  end

  defp acl_username(rest) do
    case String.split(rest, "/") do
      [username, "acls"] when username != "" -> {:ok, username}
      _other -> :error
    end
  end

  defp handle_list_acls(socket, username) do
    acls =
      Enum.map(Auth.list_acls(username), fn %{operation: operation, resource: resource} ->
        %{"operation" => to_string(operation), "resource" => resource}
      end)

    send_json(socket, "200 OK", %{"s" => "ok", "acls" => acls})
  end

  defp handle_grant_acl(socket, username, headers) do
    with_acl_body(socket, headers, fn operation, pattern ->
      respond_user_result(socket, Auth.grant_acl(username, operation, pattern), "201 Created")
    end)
  end

  defp handle_revoke_acl(socket, username, headers) do
    with_acl_body(socket, headers, fn operation, pattern ->
      respond_user_result(socket, Auth.revoke_acl(username, operation, pattern), "200 OK")
    end)
  end

  # Reads `{operation, pattern}` from the JSON body, parsing the operation string; runs `fun` or answers 400.
  defp with_acl_body(socket, headers, fun) do
    case read_json_body(socket, headers) do
      {:ok, %{"operation" => operation, "pattern" => pattern}} when is_binary(pattern) ->
        case Auth.parse_acl_operation(operation) do
          {:ok, op} -> fun.(op, pattern)
          :error -> send_json(socket, "400 Bad Request", %{"s" => "err", "reason" => "invalid_operation"})
        end

      _malformed ->
        send_json(socket, "400 Bad Request", %{"s" => "err", "reason" => "invalid_request"})
    end
  end

  defp respond_user_result(socket, :ok, ok_status), do: send_json(socket, ok_status, %{"s" => "ok"})

  defp respond_user_result(socket, {:error, reason}, _ok_status) do
    status =
      case reason do
        :user_exists -> "409 Conflict"
        :user_not_found -> "404 Not Found"
        _other -> "400 Bad Request"
      end

    send_json(socket, status, %{"s" => "err", "reason" => to_string(reason)})
  end

  # Reads the request body (bounded by Content-Length) and JSON-decodes it: `{:ok, map}` or `{:error, _}`.
  defp read_json_body(socket, headers) do
    content_length = Map.get(headers, "content-length", "0") |> String.to_integer()

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

    Jason.decode(body)
  end

  defp send_json(socket, status, body_map) do
    body = Jason.encode!(body_map)

    response = """
    HTTP/1.1 #{status}\r
    Content-Type: application/json\r
    Content-Length: #{byte_size(body)}\r
    \r
    #{body}
    """

    :gen_tcp.send(socket, SecurityHeaders.add_security_headers(response, "/users"))
    :gen_tcp.close(socket)
  end

  defp handle_route(socket, %{method: :GET, path: "/"}, _headers, _client_ip, _session),
    do: serve_html(socket)

  defp handle_route(socket, %{method: :GET, path: "/login"}, _headers, _client_ip, _session),
    do: serve_login_page(socket)

  defp handle_route(socket, %{method: :POST, path: "/login"}, headers, client_ip, _session),
    do: handle_login(socket, headers, client_ip)

  defp handle_route(socket, %{method: :GET, path: "/health"}, _headers, _client_ip, _session),
    do: serve_health(socket)

  defp handle_route(socket, %{method: :GET, path: "/ready"}, _headers, _client_ip, _session),
    do: serve_ready(socket)

  # /metrics serves the Prometheus text exposition to a scraper (Accept: text/plain/openmetrics) and the
  # JSON dashboard payload otherwise: same auth (any authenticated user), one conventional path.
  defp handle_route(socket, %{method: :GET, path: "/metrics"}, headers, _client_ip, _session) do
    if prometheus_scrape?(headers), do: serve_prometheus(socket), else: serve_metrics(socket)
  end

  defp handle_route(socket, %{method: :GET, path: "/topic"} = request, _headers, _client_ip, _session),
    do: serve_topic_detail(socket, request.query)

  defp handle_route(socket, %{method: :GET, path: "/stream"}, _headers, _client_ip, _session),
    do: serve_sse(socket)

  defp handle_route(socket, %{method: :GET, path: "/rate_limits"}, _headers, _client_ip, _session),
    do: serve_rate_limits(socket)

  defp handle_route(socket, %{method: :GET, path: "/logout"}, headers, _client_ip, _session),
    do: serve_logout(socket, headers)

  defp handle_route(socket, %{method: :GET, path: "/logo.svg"}, _headers, _client_ip, _session),
    do: serve_logo(socket)

  defp handle_route(socket, %{method: :OPTIONS}, headers, _client_ip, _session),
    do: serve_cors_preflight(socket, headers)

  # Admin user management (P3): gated to :admin by the auth stage above.
  defp handle_route(socket, %{method: :GET, path: "/users"}, _headers, _client_ip, _session),
    do: handle_list_users(socket)

  defp handle_route(socket, %{method: :POST, path: "/users"}, headers, _client_ip, _session),
    do: handle_create_user(socket, headers)

  defp handle_route(socket, %{method: :PUT, path: "/users/" <> rest}, headers, _client_ip, _session),
    do: handle_user_password(socket, rest, headers)

  # Per-topic ACL management (P5): /users/:username/acls: GET lists, POST grants, DELETE revokes. A DELETE
  # on a bare /users/:username (no /acls suffix) falls back to deleting the user.
  defp handle_route(socket, %{method: :GET, path: "/users/" <> rest}, _headers, _client_ip, _session),
    do: route_acl(socket, rest, &handle_list_acls(socket, &1), fn -> serve_404(socket) end)

  defp handle_route(socket, %{method: :POST, path: "/users/" <> rest}, headers, _client_ip, _session),
    do: route_acl(socket, rest, &handle_grant_acl(socket, &1, headers), fn -> serve_404(socket) end)

  defp handle_route(socket, %{method: :DELETE, path: "/users/" <> rest}, headers, _client_ip, _session),
    do: route_acl(socket, rest, &handle_revoke_acl(socket, &1, headers), fn -> handle_delete_user(socket, rest) end)

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

  # The light payload served by both /metrics (one-shot) and /stream (per-tick): the BEAM/security system
  # snapshot plus a per-topic log-stack summary (counts/bytes/groups). The per-range/segment drill-down is
  # NOT here: it is fetched on demand per topic via /topic, so the stream stays small as segments grow.
  defp dashboard_metrics do
    %{system: Metrics.get_system_metrics(), topics: topics_overview()}
  end

  # A read-only summary of the live log stack (each topic annotated with its failure-domain violation
  # count), or [] when the broker is not running (e.g. a minimal test boot) so the dashboard degrades
  # gracefully instead of crashing the connection.
  defp topics_overview do
    case Process.whereis(Malachi.LogBroker) do
      nil -> []
      _pid -> BrokerServer.topics_overview(Malachi.LogBroker)
    end
  end

  defp serve_metrics(socket) do
    serve_json(socket, "/metrics", dashboard_metrics())
  end

  # True when the caller wants the Prometheus exposition format rather than the JSON dashboard payload.
  defp prometheus_scrape?(headers) do
    accept = Map.get(headers, "accept", "")
    String.contains?(accept, "text/plain") or String.contains?(accept, "openmetrics")
  end

  defp serve_prometheus(socket) do
    text =
      Prometheus.export(Metrics.get_system_metrics(), topics_overview())
      |> IO.iodata_to_binary()

    response = """
    HTTP/1.1 200 OK\r
    Content-Type: #{Prometheus.content_type()}\r
    Content-Length: #{byte_size(text)}\r
    Cache-Control: no-cache\r
    \r
    #{text}
    """

    :gen_tcp.send(socket, SecurityHeaders.add_security_headers(response, "/metrics"))
    :gen_tcp.close(socket)
  end

  # On-demand drill-down for one topic (its ranges and segments), read from `?name=`. 404 for an unknown
  # or missing topic. Keeps the per-second stream light: segment detail is fetched only on expand.
  defp serve_topic_detail(socket, query) do
    name = query |> URI.decode_query() |> Map.get("name")
    detail = name && with_metadata(nil, &Metadata.topic_detail(&1, name))

    if detail do
      serve_json(socket, "/topic", detail)
    else
      serve_404(socket)
    end
  end

  # Runs `fun` against the live broker's metadata, or returns `default` when the broker is not running.
  defp with_metadata(default, fun) do
    case Process.whereis(Malachi.LogBroker) do
      nil -> default
      _pid -> Malachi.LogBroker |> BrokerServer.metadata() |> fun.()
    end
  end

  defp serve_json(socket, route, data) do
    json = Jason.encode!(data)

    response = """
    HTTP/1.1 200 OK\r
    Content-Type: application/json\r
    Content-Length: #{byte_size(json)}\r
    Cache-Control: no-cache\r
    \r
    #{json}
    """

    :gen_tcp.send(socket, SecurityHeaders.add_security_headers(response, route))
    :gen_tcp.close(socket)
  end

  # Liveness: the HTTP server answered, so the node is up. Always 200, unauthenticated (probes).
  defp serve_health(socket), do: serve_status(socket, "/health", 200, "ok")

  # Readiness: 200 once the log broker is running (ready to serve produce/consume), else 503, so a load
  # balancer / k8s stops routing to a node that is still booting or has lost its broker.
  defp serve_ready(socket) do
    if Process.whereis(Malachi.LogBroker),
      do: serve_status(socket, "/ready", 200, "ready"),
      else: serve_status(socket, "/ready", 503, "not_ready")
  end

  defp serve_status(socket, route, code, status) do
    json = Jason.encode!(%{status: status})
    reason = if code == 200, do: "OK", else: "Service Unavailable"

    response = """
    HTTP/1.1 #{code} #{reason}\r
    Content-Type: application/json\r
    Content-Length: #{byte_size(json)}\r
    Cache-Control: no-store\r
    \r
    #{json}
    """

    :gen_tcp.send(socket, SecurityHeaders.add_security_headers(response, route))
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
    json = Jason.encode!(dashboard_metrics())
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
    logo_path = Path.join(:code.priv_dir(:malachi), "static/logo.svg")

    case File.read(logo_path) do
      {:ok, data} ->
        header =
          "HTTP/1.1 200 OK\r\nContent-Type: image/svg+xml; charset=utf-8\r\nContent-Length: #{byte_size(data)}\r\nCache-Control: public, max-age=86400\r\n\r\n"

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
        .empty-state { text-align: center; color: #666; padding: 20px; font-style: italic; }
        .topic-card {
          background: #0f1428;
          border: 1px solid #2a3f5f;
          border-left: 4px solid #00d9ff;
          border-radius: 6px;
          margin-bottom: 10px;
        }
        .topic-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          gap: 10px;
          padding: 12px 14px;
          cursor: pointer;
          user-select: none;
        }
        .topic-header:hover { background: #141a33; }
        .topic-name { color: #00ff88; font-family: monospace; font-weight: bold; }
        .topic-summary { color: #8a97b1; font-size: 0.85em; font-family: monospace; }
        .badge {
          display: inline-block;
          padding: 2px 8px;
          border-radius: 10px;
          font-size: 0.75em;
          font-weight: bold;
          text-transform: uppercase;
        }
        .badge-active { background: #00ff88; color: #0a0e27; }
        .badge-sealed { background: #6b7280; color: #f0f0f0; }
        .badge-warn { background: #f59e0b; color: #0a0e27; }
        .topic-detail { padding: 0 14px 12px 14px; }
        .range-block { margin-top: 10px; border-top: 1px solid #202844; padding-top: 8px; }
        .range-head { color: #00d9ff; font-family: monospace; font-size: 0.85em; margin-bottom: 4px; }
        .seg-row {
          display: grid;
          grid-template-columns: 1fr 1fr 1fr auto;
          gap: 8px;
          color: #b8c2d9;
          font-family: monospace;
          font-size: 0.8em;
          padding: 2px 0 2px 14px;
        }
        .seg-empty { color: #666; font-size: 0.8em; padding: 2px 0 2px 14px; }
        .group-chip {
          display: inline-block;
          background: #2a3f5f;
          color: #00ff88;
          padding: 1px 8px;
          border-radius: 10px;
          font-size: 0.75em;
          font-family: monospace;
          margin: 2px 4px 0 0;
        }
      </style>
    </head>
    <body>
      <div class="dashboard-header">
        <h1><img src="/logo.svg" alt="Malachi" style="height: 42px; vertical-align: middle; margin-right: 10px;">Malachi Dashboard</h1>
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
        <h2>Topics</h2>
        <div id="topics">Loading...</div>
      </div>
      <script>
        // Topic names are user-controlled, so every value interpolated into HTML is escaped.
        function escapeHtml(unsafe) {
          return String(unsafe)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#039;");
        }

        function formatBytes(n) {
          if (!n) return '0 B';
          const units = ['B', 'KB', 'MB', 'GB', 'TB'];
          let i = 0, v = n;
          while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
          return (i === 0 ? v : v.toFixed(1)) + ' ' + units[i];
        }

        // Drill-down state, keyed by topic name, survives the per-second summary re-render. The heavy
        // per-range/segment detail is NOT in the stream: it is fetched once, on expand, into topicDetails.
        const expandedTopics = new Set();
        const topicDetails = {};
        let lastTopics = [];

        function toggleTopic(i) {
          const t = lastTopics[i];
          if (!t) return;
          if (expandedTopics.has(t.name)) {
            expandedTopics.delete(t.name);
            delete topicDetails[t.name];
            renderTopics(lastTopics);
          } else {
            expandedTopics.add(t.name);
            renderTopics(lastTopics); // show the header expanded + a loading row immediately
            loadTopicDetail(t.name);
          }
        }

        // Fetch one topic's ranges/segments on demand; re-render only if it is still expanded when it lands.
        function loadTopicDetail(name) {
          fetch('/topic?name=' + encodeURIComponent(name))
            .then(resp => resp.ok ? resp.json() : null)
            .then(detail => {
              if (detail && expandedTopics.has(name)) {
                topicDetails[name] = detail;
                renderTopics(lastTopics);
              }
            })
            .catch(() => {});
        }

        function renderSegments(segments) {
          if (!segments || segments.length === 0) return '<div class="seg-empty">no segments</div>';
          return segments.map(s => `
            <div class="seg-row">
              <span>off ${s.start_offset}</span>
              <span>len ${s.length == null ? '-' : s.length}</span>
              <span>${formatBytes(s.byte_size)}</span>
              <span class="badge badge-${escapeHtml(s.state)}">${escapeHtml(s.state)}</span>
            </div>
          `).join('');
        }

        function renderRanges(ranges) {
          return (ranges || []).map(r => `
            <div class="range-block">
              <div class="range-head">
                range #${r.seq} · keys [${r.key_start}, ${r.key_end}) ·
                <span class="badge badge-${escapeHtml(r.state)}">${escapeHtml(r.state)}</span> ·
                ${r.segments.length} seg${r.segments.length === 1 ? '' : 's'}
              </div>
              ${renderSegments(r.segments)}
            </div>
          `).join('');
        }

        function renderTopicCard(t, i) {
          const open = expandedTopics.has(t.name);
          const groups = (t.groups || []).map(g => `<span class="group-chip">${escapeHtml(g)}</span>`).join('');
          let detail = '';
          if (open) {
            const d = topicDetails[t.name];
            const body = d ? renderRanges(d.ranges) : '<div class="seg-empty">loading…</div>';
            detail = `
            <div class="topic-detail">
              ${groups ? '<div style="margin-bottom:6px;">groups: ' + groups + '</div>' : '<div class="seg-empty">no consumer groups</div>'}
              ${body}
            </div>`;
          }
          return `
            <div class="topic-card">
              <div class="topic-header" onclick="toggleTopic(${i})">
                <span class="topic-name">${open ? '▾' : '▸'} ${escapeHtml(t.name)} <span class="badge badge-${escapeHtml(t.state)}">${escapeHtml(t.state)}</span></span>
                <span class="topic-summary">${t.active_range_count}/${t.range_count} ranges · ${t.active_segment_count}/${t.segment_count} segs · ${formatBytes(t.total_bytes)} · ${(t.groups || []).length} grp${t.domain_violations > 0 ? ' · <span class="badge badge-warn">⚠ ' + t.domain_violations + ' HA</span>' : ''}</span>
              </div>
              ${detail}
            </div>`;
        }

        function renderTopics(topics) {
          lastTopics = topics || [];
          const el = document.getElementById('topics');
          if (lastTopics.length === 0) {
            el.innerHTML = '<div class="empty-state">No topics</div>';
            return;
          }
          el.innerHTML = lastTopics.map(renderTopicCard).join('');
        }

        // Auth is handled via HttpOnly cookies, sent automatically by the browser.
        const source = new EventSource('/stream');
        source.onmessage = (event) => {
          const data = JSON.parse(event.data);
          document.getElementById('processes').textContent = data.system.process_count;
          document.getElementById('memory').textContent = data.system.memory.total_mb.toFixed(2) + ' MB';
          renderTopics(data.topics);
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
        <h1><img src="/logo.svg" alt="Malachi" style="height: 48px; vertical-align: middle; margin-right: 10px;">Malachi</h1>
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
