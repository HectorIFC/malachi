defmodule Malachi.Dashboard.SecurityHeaders do
  @moduledoc """
  Security headers for dashboard HTTP responses.

  Implements defense-in-depth security controls:
  - Content Security Policy (CSP)
  - HTTP Strict Transport Security (HSTS)
  - X-Frame-Options (clickjacking prevention)
  - X-Content-Type-Options (MIME sniffing prevention)
  - X-XSS-Protection
  - Referrer-Policy
  - CORS (Cross-Origin Resource Sharing)
  """

  @doc """
  Adds security headers to an HTTP response string.

  ## Parameters

  - `response` - HTTP response string (must include status line and headers)
  - `request_path` - Path being accessed (e.g., "/", "/metrics", "/stream")

  ## Returns

  Modified response string with security headers prepended.

  ## Examples

      iex> response = "HTTP/1.1 200 OK\\r\\nContent-Type: text/html\\r\\n\\r\\n<html>..."
      iex> SecurityHeaders.add_security_headers(response, "/")
      "HTTP/1.1 200 OK\\r\\nX-Frame-Options: DENY\\r\\nContent-Type: text/html\\r\\n\\r\\n<html>..."
  """
  def add_security_headers(response, request_path, request_origin \\ nil) do
    base_headers = [
      {"x-content-type-options", "nosniff"},
      {"x-frame-options", "DENY"},
      {"x-xss-protection", "1; mode=block"},
      {"referrer-policy", "no-referrer"}
    ]

    csp_header = build_csp_header()
    hsts_header = build_hsts_header()
    cors_headers = build_cors_headers(request_path, request_origin)

    all_headers = base_headers ++ csp_header ++ hsts_header ++ cors_headers

    prepend_headers(response, all_headers)
  end

  @doc """
  Builds Content Security Policy header.

  Default policy allows 'unsafe-inline' for compatibility with current dashboard.
  Can be customized via MALACHI_DASHBOARD_CSP environment variable.

  ## Future Hardening

  For production environments, consider removing 'unsafe-inline' and using nonces:

      export MALACHI_DASHBOARD_CSP="default-src 'self'; script-src 'self' 'nonce-RANDOM'; style-src 'self' 'nonce-RANDOM'"

  This requires refactoring dashboard HTML to use nonce attributes.
  """
  def build_csp_header do
    csp =
      Application.get_env(:malachi, :dashboard_csp) ||
        "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'"

    [{"content-security-policy", csp}]
  end

  @doc """
  Builds HTTP Strict Transport Security header.

  Only included when TLS is enabled. Instructs browsers to only connect via HTTPS
  for the specified duration (default: 1 year).
  """
  def build_hsts_header do
    tls_enabled = Application.get_env(:malachi, :enable_tls, false)
    hsts_enabled = Application.get_env(:malachi, :hsts_enabled, true)

    if tls_enabled and hsts_enabled do
      max_age = Application.get_env(:malachi, :hsts_max_age, 31_536_000)
      include_subdomains = Application.get_env(:malachi, :hsts_include_subdomains, true)

      hsts_value =
        if include_subdomains do
          "max-age=#{max_age}; includeSubDomains"
        else
          "max-age=#{max_age}"
        end

      [{"strict-transport-security", hsts_value}]
    else
      []
    end
  end

  @doc """
  Builds CORS headers for API endpoints.

  CORS is only enabled for `/metrics` and `/stream` endpoints when explicitly configured. With the default
  wildcard whitelist every origin gets `*`; with an explicit whitelist the request's own `Origin` is echoed
  back, and only when it is listed, since `Access-Control-Allow-Origin` accepts a single origin. An origin
  outside the whitelist (or a request with no `Origin`) gets no allow header, which the browser reads as not
  allowed, but still gets `Vary: origin` so a cache cannot reuse that denial for a whitelisted origin.

  Returns a list of `{name, value}` tuples, empty when CORS does not apply at all. `Malachi.Dashboard` uses
  it for both the real responses and the `OPTIONS` preflight, so a preflight can never advertise a permission
  the actual request would not receive.

  ## Configuration

      export MALACHI_DASHBOARD_CORS_ENABLED=true
      export MALACHI_DASHBOARD_CORS_ORIGINS="https://app.example.com,https://admin.example.com"
  """
  def build_cors_headers(request_path, request_origin \\ nil) do
    # CORS only for API endpoints, and only when turned on
    if request_path in ["/metrics", "/stream"] and
         Application.get_env(:malachi, :dashboard_cors_enabled, false) do
      origins = Application.get_env(:malachi, :dashboard_cors_origins, ["*"])

      cors_headers_for(origins, allowed_origin(origins, request_origin))
    else
      []
    end
  end

  defp cors_headers_for(origins, allowed) do
    # Under a wildcard every origin gets the same answer, so the response does not vary. Under an explicit
    # whitelist it does, and `Vary` has to be there even when the origin is refused: otherwise a cache can
    # store the header-less denial and later replay it to a whitelisted origin.
    vary = if "*" in origins, do: [], else: [{"vary", "origin"}]

    allow =
      case allowed do
        nil ->
          []

        origin ->
          [
            {"access-control-allow-origin", origin},
            {"access-control-allow-methods", "GET, OPTIONS"},
            {"access-control-allow-headers", "Authorization, Content-Type"},
            {"access-control-max-age", "86400"}
          ]
      end

    allow ++ vary
  end

  # A wildcard whitelist answers "*" to everyone. Otherwise the request's own Origin is echoed back, and only
  # when it is whitelisted: `Access-Control-Allow-Origin` takes a single origin, so listing several of them
  # (the previous behavior) produced a header browsers reject, which silently broke a multi-origin whitelist.
  # nil means not allowed: no Origin on the request, or one outside the list.
  defp allowed_origin(origins, request_origin) do
    cond do
      "*" in origins -> "*"
      request_origin in origins -> request_origin
      true -> nil
    end
  end

  @doc """
  Prepends headers to an HTTP response string.

  Inserts headers after the status line, before existing headers.

  ## Examples

      iex> response = "HTTP/1.1 200 OK\\r\\nContent-Type: text/html\\r\\n\\r\\nBody"
      iex> headers = [{"x-frame-options", "DENY"}]
      iex> prepend_headers(response, headers)
      "HTTP/1.1 200 OK\\r\\nX-Frame-Options: DENY\\r\\nContent-Type: text/html\\r\\n\\r\\nBody"
  """
  def prepend_headers(response, headers) do
    # Split response into status line and rest
    case String.split(response, "\r\n", parts: 2) do
      [status_line, rest] ->
        # Build header strings
        header_strings =
          Enum.map(headers, fn {key, value} ->
            # Capitalize header names (X-Frame-Options, not x-frame-options)
            formatted_key =
              key
              |> String.split("-")
              |> Enum.map_join("-", &String.capitalize/1)

            "#{formatted_key}: #{value}\r\n"
          end)

        # Reconstruct response with security headers first
        status_line <> "\r\n" <> Enum.join(header_strings, "") <> rest

      _ ->
        # Malformed response, return as-is
        response
    end
  end
end
