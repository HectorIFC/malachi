defmodule Malachi.DashboardSecurityTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Malachi.Auth.UserStore
  alias Malachi.Dashboard.SecurityHeaders
  alias Malachi.Test.DashboardHelper

  # These tests require dashboard authentication and rate limiting enabled in config/test.exs

  setup do
    # Reset rate limiter BEFORE tests (not just on_exit)
    Malachi.RateLimiter.reset_bucket("127.0.0.1", :dashboard_auth)

    # Wait for application to be fully started
    :timer.sleep(200)

    # Remove users if they exist from previous tests
    _ = Malachi.Auth.remove_user("dashboard_admin")
    _ = Malachi.Auth.remove_user("producer_user")

    # Create test user with admin permission
    :ok = Malachi.Auth.add_user("dashboard_admin", "admin_pass_123", [:admin])
    {:ok, admin_token} = Malachi.Auth.authenticate("dashboard_admin", "admin_pass_123", {127, 0, 0, 1})

    # Create test user with only produce permission
    :ok = Malachi.Auth.add_user("producer_user", "prod_pass_123", [:produce])
    {:ok, producer_token} = Malachi.Auth.authenticate("producer_user", "prod_pass_123", {127, 0, 0, 1})

    on_exit(fn ->
      _ = Malachi.Auth.remove_user("dashboard_admin")
      _ = Malachi.Auth.remove_user("producer_user")
      # Reset rate limiter for this IP after each test
      Malachi.RateLimiter.reset_bucket("127.0.0.1", :dashboard_auth)
    end)

    {:ok, admin_token: admin_token, producer_token: producer_token}
  end

  describe "authentication" do
    test "GET / without token redirects to /login", %{} do
      case DashboardHelper.connect() do
        {:ok, socket} ->
          request = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
          :gen_tcp.send(socket, request)
          {:ok, response} = :gen_tcp.recv(socket, 0, 2000)

          assert String.contains?(response, "302 Found") or
                   String.contains?(response, "302")

          assert String.contains?(response, "Location: /login")
          # The redirect must still carry the security headers every response gets.
          assert String.downcase(response) =~ "x-frame-options: deny"
          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    test "GET / with invalid token redirects to login and clears the cookie", %{} do
      {:ok, socket} = DashboardHelper.connect()

      {:ok, response} =
        DashboardHelper.request(socket, :GET, "/", headers: %{"Cookie" => "malachi_token=invalid_token_12345"})

      # Access is refused either way. On a page route the refusal sends the user to the login form and
      # expires the bad cookie, instead of leaving the browser replaying it against a JSON 403.
      refute String.contains?(response, "200 OK")
      assert String.contains?(response, "302 Found")
      assert String.contains?(response, "Location: /login")
      assert String.downcase(response) =~ "set-cookie: malachi_token=;"
      # The redirect replaced a 403 that carried the security headers, so it must carry them too.
      assert String.downcase(response) =~ "x-frame-options: deny"

      :gen_tcp.close(socket)
    end

    test "GET / with producer token (non-admin) returns 403", %{producer_token: token} do
      case DashboardHelper.connect() do
        {:ok, socket} ->
          request = """
          GET / HTTP/1.1\r
          Host: localhost\r
          Cookie: malachi_token=#{token}\r
          \r
          """

          :gen_tcp.send(socket, request)
          {:ok, response} = :gen_tcp.recv(socket, 0, 2000)

          assert String.contains?(response, "403") or
                   String.contains?(response, "insufficient_permissions")

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    test "GET / with admin token returns 200", %{admin_token: token} do
      case DashboardHelper.connect() do
        {:ok, socket} ->
          request = """
          GET / HTTP/1.1\r
          Host: localhost\r
          Cookie: malachi_token=#{token}\r
          \r
          """

          :gen_tcp.send(socket, request)
          {:ok, response} = :gen_tcp.recv(socket, 0, 5000)

          assert String.contains?(response, "200 OK")
          assert String.contains?(response, "text/html")
          assert String.contains?(response, "Malachi Dashboard")

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    # A wire permission is not a console role (#228): a producer no longer reads the operational state.
    test "GET /metrics with producer token is refused for lack of a console role", %{producer_token: token} do
      case DashboardHelper.connect() do
        {:ok, socket} ->
          request = """
          GET /metrics HTTP/1.1\r
          Host: localhost\r
          Cookie: malachi_token=#{token}\r
          \r
          """

          :gen_tcp.send(socket, request)
          {:ok, response} = :gen_tcp.recv(socket, 0, 2000)

          assert String.contains?(response, "403 Forbidden")
          assert String.contains?(response, "application/problem+json")
          assert String.contains?(response, ~s("required_role":"viewer"))

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    test "GET / with Bearer token fallback returns 200", %{admin_token: token} do
      case DashboardHelper.connect() do
        {:ok, socket} ->
          request = """
          GET / HTTP/1.1\r
          Host: localhost\r
          Authorization: Bearer #{token}\r
          \r
          """

          :gen_tcp.send(socket, request)
          {:ok, response} = :gen_tcp.recv(socket, 0, 5000)

          assert String.contains?(response, "200 OK")
          assert String.contains?(response, "text/html")

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end
  end

  describe "login endpoint" do
    test "POST /login with valid credentials returns token and Set-Cookie" do
      case DashboardHelper.connect() do
        {:ok, socket} ->
          body = Jason.encode!(%{"username" => "dashboard_admin", "password" => "admin_pass_123"})

          request =
            "POST /login HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: #{byte_size(body)}\r\n\r\n#{body}"

          :gen_tcp.send(socket, request)
          {:ok, response} = :gen_tcp.recv(socket, 0, 2000)

          assert String.contains?(response, "200 OK") or
                   String.contains?(response, "200")

          assert String.contains?(response, "token")
          assert String.contains?(response, "Set-Cookie: malachi_token=")
          assert String.contains?(response, "HttpOnly")

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    test "POST /login with invalid credentials returns 401" do
      case DashboardHelper.connect() do
        {:ok, socket} ->
          body = Jason.encode!(%{"username" => "dashboard_admin", "password" => "wrong_password"})

          request =
            "POST /login HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: #{byte_size(body)}\r\n\r\n#{body}"

          :gen_tcp.send(socket, request)
          {:ok, response} = :gen_tcp.recv(socket, 0, 2000)

          assert String.contains?(response, "401 Unauthorized")
          assert String.contains?(response, "errors.auth.invalid_credentials")

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    test "GET /login returns HTML login page" do
      case DashboardHelper.connect() do
        {:ok, socket} ->
          request = "GET /login HTTP/1.1\r\nHost: localhost\r\n\r\n"
          :gen_tcp.send(socket, request)
          {:ok, response} = :gen_tcp.recv(socket, 0, 2000)

          assert String.contains?(response, "200 OK")
          assert String.contains?(response, "text/html")
          assert String.contains?(response, "Malachi")
          assert String.contains?(response, "loginForm")

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end
  end

  describe "malformed Content-Length" do
    test "non-numeric Content-Length does not crash the handler" do
      case DashboardHelper.connect() do
        {:ok, socket} ->
          # Before the fix, String.to_integer("abc") raised and killed the handler, so the socket closed
          # with no response. The server must instead answer (any HTTP status) without crashing.
          request =
            "POST /login HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: abc\r\n\r\n"

          :gen_tcp.send(socket, request)
          assert {:ok, response} = :gen_tcp.recv(socket, 0, 2000)
          assert String.contains?(response, "HTTP/1.1")

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    test "negative Content-Length does not crash the handler" do
      case DashboardHelper.connect() do
        {:ok, socket} ->
          request =
            "POST /login HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: -5\r\n\r\n"

          :gen_tcp.send(socket, request)
          assert {:ok, response} = :gen_tcp.recv(socket, 0, 2000)
          assert String.contains?(response, "HTTP/1.1")

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end
  end

  describe "read timeout (slowloris)" do
    test "an idle connection is closed after the recv timeout" do
      original = Application.get_env(:malachi, :dashboard_recv_timeout_ms)
      Application.put_env(:malachi, :dashboard_recv_timeout_ms, 300)

      on_exit(fn ->
        if original,
          do: Application.put_env(:malachi, :dashboard_recv_timeout_ms, original),
          else: Application.delete_env(:malachi, :dashboard_recv_timeout_ms)
      end)

      case DashboardHelper.connect() do
        {:ok, socket} ->
          # Connect and send nothing. Before the fix, recv(socket, 0) blocked forever and the client would
          # only see its own recv timeout ({:error, :timeout}); now the server closes the idle socket once
          # its 300ms read timeout elapses, which the client observes as {:error, :closed} well before 2s.
          assert {:error, :closed} = :gen_tcp.recv(socket, 0, 2000)
          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end
  end

  describe "header bounds" do
    # Before the bound, parse_headers/2 recursed on every header line until the blank line, so the only limit
    # on how many headers a request could carry was the receive timeout, and a line longer than the socket
    # buffer ended the parse early and routed the request with whatever headers had been read so far.

    test "fifty header lines are served and the fifty-first is refused with 431" do
      assert status_code(raw_request(header_lines(50))) == 200

      response = raw_request(header_lines(51))
      assert status_code(response) == 431
      assert response =~ "431 Request Header Fields Too Large"
      assert {:ok, %{"type" => "errors.http.header_fields_too_large"}} = json_body(response)
    end

    test "a repeated name counts once per line" do
      lines = ["Host: localhost" | List.duplicate("Cookie: a=b", 50)]
      assert status_code(raw_request(lines)) == 431
    end

    test "a 10000 byte line is read whole and a 10001 byte one closes the connection", %{admin_token: token} do
      # The padding goes first, so the cookie after it only authenticates the request if the long line was
      # read rather than ending the parse. The line length counts the name, the separator and the CRLF. Past
      # it the socket driver closes the connection itself, so there is no answer to read.
      cookie = "Cookie: malachi_token=#{token}"

      assert status_code(raw_request(["Host: localhost", padded_line(10_000), cookie], "/metrics")) == 200
      assert raw_request(["Host: localhost", padded_line(10_001), cookie], "/metrics") == ""
    end

    test "a repeated name adds its bytes on every line, not only the value kept" do
      # 41 lines is under the count, and only one cookie survives in the map, but the 40 lines carry about
      # 36 KB: the total counts what was read, not what was kept.
      lines = ["Host: localhost" | List.duplicate("Cookie: " <> String.duplicate("a", 900), 40)]
      assert status_code(raw_request(lines)) == 431
    end

    test "a 10000 byte request line is read and a 10001 byte one closes the connection" do
      assert status_code(raw_request_line(10_000)) > 0
      assert raw_request_line(10_001) == ""
    end

    test "32768 bytes of names and values are served and one more is refused" do
      # "host" + "localhost" is 13 bytes and each "x-tN" name is 4, so the values carry the rest.
      assert status_code(raw_request(total_lines(32_768))) == 200
      assert status_code(raw_request(total_lines(32_769))) == 431
    end

    test "a client that keeps sending headers is cut off at the deadline" do
      put_recv_timeout(300)
      {:ok, socket} = DashboardHelper.connect()
      :ok = :gen_tcp.send(socket, "GET /health HTTP/1.1\r\nHost: localhost\r\n")

      # Each header lands well inside the per-read timeout, so only a deadline over the whole block ends this.
      dribbler =
        spawn(fn ->
          for i <- 1..30 do
            Process.sleep(100)
            :gen_tcp.send(socket, "X-D#{i}: v\r\n")
          end
        end)

      started = System.monotonic_time(:millisecond)
      assert {:error, reason} = :gen_tcp.recv(socket, 0, 2_000)
      assert reason in [:closed, :econnreset]
      assert System.monotonic_time(:millisecond) - started < 1_500

      Process.exit(dribbler, :kill)
      :gen_tcp.close(socket)
    end

    test "a request whose headers stop arriving is closed, not routed with the headers read so far" do
      put_recv_timeout(300)
      {:ok, socket} = DashboardHelper.connect()
      :ok = :gen_tcp.send(socket, "GET /health HTTP/1.1\r\nHost: localhost\r\n")

      assert {:error, :closed} = :gen_tcp.recv(socket, 0, 2_000)
      :gen_tcp.close(socket)
    end
  end

  describe "request line forms" do
    # Characterization of the shapes the bounded parse still dispatches exactly as before it.

    test "an asterisk-form request is parsed and answered" do
      {:ok, socket} = DashboardHelper.connect()
      :ok = :gen_tcp.send(socket, "OPTIONS * HTTP/1.1\r\nHost: localhost\r\n\r\n")
      assert status_code(read_until_closed(socket, "")) > 0
      :gen_tcp.close(socket)
    end

    test "an absolute-form request is closed unanswered" do
      {:ok, socket} = DashboardHelper.connect()
      :ok = :gen_tcp.send(socket, "GET http://localhost/health HTTP/1.1\r\nHost: localhost\r\n\r\n")
      assert read_until_closed(socket, "") == ""
      :gen_tcp.close(socket)
    end

    test "a malformed header line ends the parse and the request is routed with the headers before it",
         %{admin_token: token} do
      # /metrics needs the cookie, so a 200 shows the header read before the malformed line was kept.
      lines = ["Host: localhost", "Cookie: malachi_token=#{token}", "NoColonHere"]
      assert status_code(raw_request(lines, "/metrics")) == 200
    end
  end

  describe "configured header bounds" do
    setup do
      keys = [:dashboard_max_header_count, :dashboard_max_header_line_size, :dashboard_max_header_size]
      originals = Map.new(keys, &{&1, Application.fetch_env(:malachi, &1)})

      on_exit(fn ->
        Enum.each(originals, fn
          {key, {:ok, value}} -> Application.put_env(:malachi, key, value)
          {key, :error} -> Application.delete_env(:malachi, key)
        end)
      end)

      :ok
    end

    test "each configured limit is served at its value and refused one past it" do
      Application.put_env(:malachi, :dashboard_max_header_count, 5)
      Application.put_env(:malachi, :dashboard_max_header_line_size, 512)
      Application.put_env(:malachi, :dashboard_max_header_size, 1_024)
      port = start_dashboard()

      assert status_code(raw_request(header_lines(5), "/health", port)) == 200
      assert status_code(raw_request(header_lines(6), "/health", port)) == 431

      assert status_code(raw_request(["Host: localhost", padded_line(512)], "/health", port)) == 200
      assert raw_request(["Host: localhost", padded_line(513)], "/health", port) == ""
      assert status_code(raw_request_line(512, port)) > 0
      assert raw_request_line(513, port) == ""

      # 13 bytes for the Host header and 3 for each "x-a" style name leave 1002 bytes for three values.
      assert status_code(raw_request(small_total_lines(1_024), "/health", port)) == 200
      assert status_code(raw_request(small_total_lines(1_025), "/health", port)) == 431
    end

    test "a limit that is not a positive integer falls back to its default and says so" do
      Application.put_env(:malachi, :dashboard_max_header_count, 0)

      {port, log} = with_log(fn -> start_dashboard() end)
      assert log =~ "dashboard_max_header_count"

      assert status_code(raw_request(header_lines(50), "/health", port)) == 200
      assert status_code(raw_request(header_lines(51), "/health", port)) == 431
    end

    test "a line limit is accepted up to 1048576 and a larger one falls back to the default" do
      Application.put_env(:malachi, :dashboard_max_header_line_size, 1_048_576)
      port = start_dashboard()
      assert status_code(raw_request(["Host: localhost", padded_line(20_000)], "/health", port)) == 200

      Application.put_env(:malachi, :dashboard_max_header_line_size, 1_048_577)
      {port, log} = with_log(fn -> start_dashboard() end)
      assert log =~ "dashboard_max_header_line_size"

      assert status_code(raw_request(["Host: localhost", padded_line(10_000)], "/health", port)) == 200
      assert raw_request(["Host: localhost", padded_line(10_001)], "/health", port) == ""
    end
  end

  describe "security headers" do
    test "responses include security headers", %{admin_token: token} do
      case DashboardHelper.connect() do
        {:ok, socket} ->
          request = """
          GET / HTTP/1.1\r
          Host: localhost\r
          Cookie: malachi_token=#{token}\r
          \r
          """

          :gen_tcp.send(socket, request)
          {:ok, response} = :gen_tcp.recv(socket, 0, 5000)

          # Check for security headers
          assert String.contains?(response, "X-Content-Type-Options: nosniff") or
                   String.contains?(response, "X-Content-Type-Options")

          assert String.contains?(response, "X-Frame-Options: DENY") or
                   String.contains?(response, "X-Frame-Options")

          assert String.contains?(response, "Content-Security-Policy") or
                   String.contains?(response, "content-security-policy")

          # Case-insensitive, like the CORS assertion below: the header casing is the responder's business.
          # The exact policy value is pinned in dashboard_security_headers_test.exs, which runs whether or
          # not the dashboard is listening.
          assert String.downcase(response) =~ "permissions-policy: "

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    test "CORS headers present on /metrics", %{admin_token: token} do
      case DashboardHelper.connect() do
        {:ok, socket} ->
          request = """
          GET /metrics HTTP/1.1\r
          Host: localhost\r
          Cookie: malachi_token=#{token}\r
          \r
          """

          :gen_tcp.send(socket, request)
          {:ok, response} = :gen_tcp.recv(socket, 0, 2000)

          # CORS headers should be present if enabled
          # (may not be present in test env if CORS disabled)
          assert String.contains?(response, "200 OK")

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    test "OPTIONS preflight echoes the whitelisted Origin" do
      # Asserting the echoed value (rather than just that some Access-Control header exists) is what pins the
      # "origin" header key: Origin is the one header the HTTP decoder does not hand back as an atom, so it
      # is the only coverage of the binary/charlist branch of the header-name normalization.
      original_enabled = Application.get_env(:malachi, :dashboard_cors_enabled, false)
      original_origins = Application.get_env(:malachi, :dashboard_cors_origins, ["*"])
      Application.put_env(:malachi, :dashboard_cors_enabled, true)
      Application.put_env(:malachi, :dashboard_cors_origins, ["https://example.com"])

      on_exit(fn ->
        Application.put_env(:malachi, :dashboard_cors_enabled, original_enabled)
        Application.put_env(:malachi, :dashboard_cors_origins, original_origins)
      end)

      case DashboardHelper.connect() do
        {:ok, socket} ->
          request = """
          OPTIONS /metrics HTTP/1.1\r
          Host: localhost\r
          Origin: https://example.com\r
          \r
          """

          :gen_tcp.send(socket, request)
          {:ok, response} = :gen_tcp.recv(socket, 0, 2000)

          assert String.contains?(response, "204 No Content")
          # Case-insensitive: the header name casing is an implementation detail of the responder.
          assert String.downcase(response) =~ "access-control-allow-origin: https://example.com"

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end
  end

  describe "stale session on a page route" do
    test "a rejected session clears the cookie and redirects instead of stranding the user on a 403" do
      {:ok, socket} = DashboardHelper.connect()

      # A token the server does not know is the same shape of failure a legitimate user hits when session
      # binding rejects them (a changed IP, or a changed User-Agent with UA binding on). On a page route the
      # browser would otherwise keep replaying the bad cookie against a JSON 403 forever.
      {:ok, response} =
        DashboardHelper.request(socket, :GET, "/", headers: %{"Cookie" => "malachi_token=not_a_real_token"})

      assert status_code(response) == 302
      assert String.contains?(response, "Location: /login")
      assert String.downcase(response) =~ "set-cookie: malachi_token=;"
      assert String.contains?(response, "Max-Age=0")
      :gen_tcp.close(socket)
    end

    test "a valid session lacking permission still gets a 403 and keeps its cookie", %{producer_token: token} do
      {:ok, socket} = DashboardHelper.connect()

      # /users is admin-only. The session is fine, so this is a permission failure, not a stale session: it
      # must not log the user out.
      {:ok, response} = DashboardHelper.authenticated_request(socket, :GET, "/users", token)

      assert status_code(response) == 403
      refute String.downcase(response) =~ "set-cookie: malachi_token=;"
      :gen_tcp.close(socket)
    end
  end

  # Every test here sets `:enable_tls` to the opposite of the cookie policy, and that opposition is the
  # assertion: the flag used to be read from `:enable_tls`, which describes the broker listener on 4040,
  # while the cookie is issued on 4041, which has no TLS path at all. Pinning them against each other is
  # what proves the broker's setting no longer participates.
  describe "session cookie Secure flag" do
    test "is set when the dashboard is configured for it, with the broker's TLS off" do
      response = with_cookie_policy(true, false, &login_response/0)

      assert String.contains?(response, "Set-Cookie: malachi_token=")
      assert String.contains?(response, "; Secure")
    end

    test "is absent when the dashboard is not configured for it, with the broker's TLS on" do
      # The regression. Marking the cookie Secure over plain HTTP makes the browser refuse to store it,
      # so the login form posts, the server answers 200, and nothing happens. Only localhost escapes it,
      # because browsers treat that origin as trustworthy.
      response = with_cookie_policy(false, true, &login_response/0)

      assert String.contains?(response, "Set-Cookie: malachi_token=")
      refute String.contains?(response, "; Secure")
    end

    test "the cookie-clearing redirect follows the same policy as the login cookie" do
      secure = with_cookie_policy(true, false, &clearing_response/0)
      plain = with_cookie_policy(false, true, &clearing_response/0)

      # They share secure_cookie_flag/0 precisely so a policy change cannot make them disagree: a clear
      # whose attributes do not match the cookie that was set is a clear the browser can ignore.
      assert String.downcase(secure) =~ "set-cookie: malachi_token=;"
      assert String.contains?(secure, "; Secure")
      assert String.downcase(plain) =~ "set-cookie: malachi_token=;"
      refute String.contains?(plain, "; Secure")
    end

    test "warns when a forwarded protocol says the cookie will be dropped" do
      log =
        capture_log(fn ->
          with_cookie_policy(true, false, fn -> login_response(%{"X-Forwarded-Proto" => "http"}) end)
        end)

      assert log =~ "X-Forwarded-Proto"
    end

    test "warns when the request arrived over HTTPS but the cookie is not Secure" do
      log =
        capture_log(fn ->
          with_cookie_policy(false, true, fn -> login_response(%{"X-Forwarded-Proto" => "https"}) end)
        end)

      assert log =~ "X-Forwarded-Proto"
    end

    test "stays quiet when the forwarded protocol agrees, and when there is none" do
      # A proxy that does not forward the header is ordinary, so its absence cannot be a warning without
      # becoming noise. Silence here is what keeps the two warnings above worth reading.
      agreeing =
        capture_log(fn ->
          with_cookie_policy(true, false, fn -> login_response(%{"X-Forwarded-Proto" => "https"}) end)
        end)

      absent = capture_log(fn -> with_cookie_policy(true, false, &login_response/0) end)

      refute agreeing =~ "X-Forwarded-Proto"
      refute absent =~ "X-Forwarded-Proto"
    end
  end

  # CORS is off by default, so each test sets exactly the configuration it exercises. The preflight must
  # agree with what a real request would get: it answers from the same builder.
  describe "CORS preflight" do
    setup do
      original_enabled = Application.get_env(:malachi, :dashboard_cors_enabled, false)
      original_origins = Application.get_env(:malachi, :dashboard_cors_origins, ["*"])

      on_exit(fn ->
        Application.put_env(:malachi, :dashboard_cors_enabled, original_enabled)
        Application.put_env(:malachi, :dashboard_cors_origins, original_origins)
      end)

      :ok
    end

    test "sends no CORS headers when CORS is disabled" do
      Application.put_env(:malachi, :dashboard_cors_enabled, false)

      response = preflight_response("/metrics", "https://example.com")

      assert String.contains?(response, "204 No Content")
      refute String.downcase(response) =~ "access-control-allow-origin"
    end

    test "echoes a whitelisted origin and varies on it" do
      Application.put_env(:malachi, :dashboard_cors_enabled, true)
      Application.put_env(:malachi, :dashboard_cors_origins, ["https://a.example", "https://b.example"])

      lower = String.downcase(preflight_response("/metrics", "https://b.example"))

      assert lower =~ "access-control-allow-origin: https://b.example"
      # Exactly the requesting origin, never the whole whitelist joined into one invalid header.
      refute lower =~ "https://a.example"
      assert lower =~ "vary: origin"
    end

    test "refuses an origin outside the whitelist but still varies" do
      Application.put_env(:malachi, :dashboard_cors_enabled, true)
      Application.put_env(:malachi, :dashboard_cors_origins, ["https://allowed.example"])

      lower = String.downcase(preflight_response("/metrics", "https://evil.example"))

      assert lower =~ "204 no content"
      refute lower =~ "access-control-allow-origin"
      # Without Vary a cache could replay this denial to the whitelisted origin.
      assert lower =~ "vary: origin"
    end

    test "answers the wildcard when the whitelist is *" do
      Application.put_env(:malachi, :dashboard_cors_enabled, true)
      Application.put_env(:malachi, :dashboard_cors_origins, ["*"])

      lower = String.downcase(preflight_response("/metrics", "https://anything.example"))

      assert lower =~ "access-control-allow-origin: *"
      refute lower =~ "vary: origin"
    end

    test "sends no CORS headers on a non-CORS path even when enabled" do
      Application.put_env(:malachi, :dashboard_cors_enabled, true)
      Application.put_env(:malachi, :dashboard_cors_origins, ["https://example.com"])

      response = preflight_response("/", "https://example.com")

      assert String.contains?(response, "204 No Content")
      refute String.downcase(response) =~ "access-control-allow-origin"
    end
  end

  # The preflight is only half the contract: the real response has to carry the same CORS headers, including
  # on an error, or the browser turns the failure into an opaque network error the caller cannot inspect.
  describe "CORS on real responses" do
    setup do
      original_enabled = Application.get_env(:malachi, :dashboard_cors_enabled, false)
      original_origins = Application.get_env(:malachi, :dashboard_cors_origins, ["*"])
      Application.put_env(:malachi, :dashboard_cors_enabled, true)
      Application.put_env(:malachi, :dashboard_cors_origins, ["https://app.example"])

      on_exit(fn ->
        Application.put_env(:malachi, :dashboard_cors_enabled, original_enabled)
        Application.put_env(:malachi, :dashboard_cors_origins, original_origins)
      end)

      :ok
    end

    test "an authenticated GET /metrics echoes the whitelisted origin", %{admin_token: token} do
      {:ok, socket} = DashboardHelper.connect()

      {:ok, response} =
        DashboardHelper.authenticated_request(socket, :GET, "/metrics", token,
          headers: %{"Origin" => "https://app.example"}
        )

      assert status_code(response) == 200
      assert String.downcase(response) =~ "access-control-allow-origin: https://app.example"
      :gen_tcp.close(socket)
    end

    test "a 401 on /metrics still carries the CORS headers" do
      {:ok, socket} = DashboardHelper.connect()

      # No credentials, so this is the authentication_required path. It must stay readable cross-origin.
      {:ok, response} =
        DashboardHelper.request(socket, :GET, "/metrics", headers: %{"Origin" => "https://app.example"})

      assert status_code(response) == 401
      assert String.downcase(response) =~ "access-control-allow-origin: https://app.example"
      :gen_tcp.close(socket)
    end

    test "an unauthenticated cross-origin /stream gets a readable 401, not a redirect" do
      {:ok, socket} = DashboardHelper.connect()

      # /stream is an HTML route for same-origin navigation (302 to the login page), but a cross-origin
      # EventSource cannot follow that into an HTML page, so it must get the JSON 401 with CORS headers.
      {:ok, response} =
        DashboardHelper.request(socket, :GET, "/stream", headers: %{"Origin" => "https://app.example"})

      assert status_code(response) == 401
      assert String.downcase(response) =~ "access-control-allow-origin: https://app.example"
      :gen_tcp.close(socket)
    end

    test "an unauthenticated same-origin /stream still redirects to the login page" do
      {:ok, socket} = DashboardHelper.connect()

      # No Origin header means a same-origin navigation, which keeps the redirect.
      {:ok, response} = DashboardHelper.request(socket, :GET, "/stream")

      assert status_code(response) == 302
      assert String.contains?(response, "Location: /login")
      :gen_tcp.close(socket)
    end

    @tag :slow
    test "a 429 on /metrics still carries the CORS headers" do
      Malachi.RateLimiter.reset_bucket("127.0.0.1", :dashboard_auth)

      # The dashboard auth limit is 10 per minute per IP, so a burst of token validations trips it. The 429
      # has to stay readable cross-origin for the same reason the 401 does.
      rate_limited =
        Enum.reduce_while(1..20, nil, fn _i, _acc ->
          {:ok, socket} = DashboardHelper.connect()

          {:ok, response} =
            DashboardHelper.authenticated_request(socket, :GET, "/metrics", "bogus_token",
              headers: %{"Origin" => "https://app.example"}
            )

          :gen_tcp.close(socket)

          if status_code(response) == 429, do: {:halt, response}, else: {:cont, nil}
        end)

      assert rate_limited, "expected the dashboard auth rate limit to trip within 20 requests"
      assert String.downcase(rate_limited) =~ "access-control-allow-origin: https://app.example"
    end

    test "a response to a non-whitelisted origin carries no CORS headers", %{admin_token: token} do
      {:ok, socket} = DashboardHelper.connect()

      {:ok, response} =
        DashboardHelper.authenticated_request(socket, :GET, "/metrics", token,
          headers: %{"Origin" => "https://evil.example"}
        )

      assert status_code(response) == 200
      refute String.downcase(response) =~ "access-control-allow-origin"
      :gen_tcp.close(socket)
    end
  end

  describe "SecurityHeaders.build_cors_headers/2" do
    setup do
      original_enabled = Application.get_env(:malachi, :dashboard_cors_enabled, false)
      original_origins = Application.get_env(:malachi, :dashboard_cors_origins, ["*"])
      Application.put_env(:malachi, :dashboard_cors_enabled, true)

      on_exit(fn ->
        Application.put_env(:malachi, :dashboard_cors_enabled, original_enabled)
        Application.put_env(:malachi, :dashboard_cors_origins, original_origins)
      end)

      :ok
    end

    test "a multi-origin whitelist answers the requesting origin, not the joined list" do
      Application.put_env(:malachi, :dashboard_cors_origins, ["https://a.example", "https://b.example"])

      headers = SecurityHeaders.build_cors_headers("/metrics", "https://a.example")

      assert {"access-control-allow-origin", "https://a.example"} in headers
      assert {"vary", "origin"} in headers
    end

    test "an origin outside the whitelist gets no allow header, only Vary" do
      Application.put_env(:malachi, :dashboard_cors_origins, ["https://a.example"])

      # Vary alone: the response depends on Origin even when refused, so a cache must not reuse it.
      assert SecurityHeaders.build_cors_headers("/metrics", "https://evil.example") == [{"vary", "origin"}]
    end

    test "a request with no Origin gets no allow header, only Vary" do
      Application.put_env(:malachi, :dashboard_cors_origins, ["https://a.example"])

      assert SecurityHeaders.build_cors_headers("/metrics", nil) == [{"vary", "origin"}]
    end

    test "the wildcard whitelist answers * and does not vary" do
      Application.put_env(:malachi, :dashboard_cors_origins, ["*"])

      headers = SecurityHeaders.build_cors_headers("/metrics", "https://anything.example")

      assert {"access-control-allow-origin", "*"} in headers
      refute Enum.any?(headers, fn {name, _value} -> name == "vary" end)
    end

    test "a path outside /metrics and /stream gets no headers" do
      Application.put_env(:malachi, :dashboard_cors_origins, ["*"])

      assert SecurityHeaders.build_cors_headers("/", "https://a.example") == []
    end

    test "CORS disabled beats any whitelist" do
      Application.put_env(:malachi, :dashboard_cors_enabled, false)
      Application.put_env(:malachi, :dashboard_cors_origins, ["*"])

      assert SecurityHeaders.build_cors_headers("/metrics", "https://a.example") == []
    end
  end

  describe "rate limiting" do
    @tag :slow
    test "excessive login attempts trigger rate limit" do
      # Wait for other tests to finish their rate limited operations
      :timer.sleep(100)

      # Reset rate limiter for this IP to start fresh
      Malachi.RateLimiter.reset_bucket("127.0.0.1", :dashboard_auth)

      # Give it a moment to settle
      :timer.sleep(100)

      # Make 25 failed login attempts (limit is 10, so 11th+ should definitely be blocked)
      results =
        for _i <- 1..25 do
          case DashboardHelper.connect() do
            {:ok, socket} ->
              body = Jason.encode!(%{"username" => "nonexistent", "password" => "wrong"})

              request =
                "POST /login HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: #{byte_size(body)}\r\n\r\n#{body}"

              :gen_tcp.send(socket, request)

              result =
                case :gen_tcp.recv(socket, 0, 2000) do
                  {:ok, response} ->
                    cond do
                      String.contains?(response, "429") -> :rate_limited
                      String.contains?(response, "403") -> :forbidden
                      true -> :other
                    end

                  _ ->
                    :error
                end

              :gen_tcp.close(socket)
              result

            {:error, _} ->
              :error
          end
        end

      # At least one request should have been rate limited
      assert :rate_limited in results
    end
  end

  describe "audit logging" do
    test "successful dashboard access is logged", %{admin_token: token} do
      # Access dashboard
      case DashboardHelper.connect() do
        {:ok, socket} ->
          request = """
          GET / HTTP/1.1\r
          Host: localhost\r
          Cookie: malachi_token=#{token}\r
          \r
          """

          :gen_tcp.send(socket, request)
          {:ok, _response} = :gen_tcp.recv(socket, 0, 5000)
          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end

      # Wait for async logging
      :timer.sleep(100)

      # Check audit log
      events = Malachi.AuditLog.get_events_by_type(:dashboard_access, 10)
      assert events != []

      recent_event = List.first(events)
      assert recent_event.event_type == :dashboard_access
      assert recent_event.username == "dashboard_admin"
    end

    test "failed authentication is logged" do
      case DashboardHelper.connect() do
        {:ok, socket} ->
          request = """
          GET / HTTP/1.1\r
          Host: localhost\r
          Cookie: malachi_token=invalid_token\r
          \r
          """

          :gen_tcp.send(socket, request)
          {:ok, _response} = :gen_tcp.recv(socket, 0, 2000)
          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end

      # Wait for async logging
      :timer.sleep(100)

      # Check audit log
      events = Malachi.AuditLog.get_events_by_type(:dashboard_auth_failure, 10)
      assert events != []
    end
  end

  describe "logout" do
    test "GET /logout clears cookie and redirects to /login" do
      case DashboardHelper.connect() do
        {:ok, socket} ->
          request = """
          GET /logout HTTP/1.1\r
          Host: localhost\r
          Cookie: malachi_token=some_token\r
          \r
          """

          :gen_tcp.send(socket, request)
          {:ok, response} = :gen_tcp.recv(socket, 0, 2000)

          assert String.contains?(response, "302 Found") or
                   String.contains?(response, "302")

          assert String.contains?(response, "Location: /login")
          assert String.contains?(response, "Max-Age=0")
          assert String.downcase(response) =~ "x-frame-options: deny"
          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    test "GET /logout works without cookie" do
      case DashboardHelper.connect() do
        {:ok, socket} ->
          request = "GET /logout HTTP/1.1\r\nHost: localhost\r\n\r\n"
          :gen_tcp.send(socket, request)
          {:ok, response} = :gen_tcp.recv(socket, 0, 2000)

          assert String.contains?(response, "302 Found") or
                   String.contains?(response, "302")

          assert String.contains?(response, "Location: /login")
          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end
  end

  describe "cookie authentication flow" do
    test "login sets cookie, cookie grants access to dashboard" do
      # Step 1: Login and capture Set-Cookie
      case DashboardHelper.connect() do
        {:ok, socket} ->
          body = Jason.encode!(%{"username" => "dashboard_admin", "password" => "admin_pass_123"})

          request =
            "POST /login HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: #{byte_size(body)}\r\n\r\n#{body}"

          :gen_tcp.send(socket, request)
          {:ok, login_response} = :gen_tcp.recv(socket, 0, 2000)
          :gen_tcp.close(socket)

          assert String.contains?(login_response, "Set-Cookie: malachi_token=")

          # Extract token from Set-Cookie header
          token = DashboardHelper.extract_set_cookie(login_response)
          assert token != nil

          # Step 2: Use cookie to access dashboard
          case DashboardHelper.connect() do
            {:ok, socket2} ->
              request2 = """
              GET / HTTP/1.1\r
              Host: localhost\r
              Cookie: malachi_token=#{token}\r
              \r
              """

              :gen_tcp.send(socket2, request2)
              {:ok, response2} = :gen_tcp.recv(socket2, 0, 5000)

              assert String.contains?(response2, "200 OK")
              assert String.contains?(response2, "Malachi Dashboard")
              :gen_tcp.close(socket2)

            {:error, _} ->
              :ok
          end

        {:error, _} ->
          :ok
      end
    end

    test "GET /metrics without token returns 401 (non-HTML route)" do
      case DashboardHelper.connect() do
        {:ok, socket} ->
          request = "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n"
          :gen_tcp.send(socket, request)
          {:ok, response} = :gen_tcp.recv(socket, 0, 2000)

          # /metrics is NOT an HTML route, so it should return 401 instead of 302
          assert String.contains?(response, "401 Unauthorized") or
                   String.contains?(response, "401")

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    test "GET /health and /ready are public: 200 without a token even when auth is enabled" do
      for path <- ["/health", "/ready"] do
        case DashboardHelper.connect() do
          {:ok, socket} ->
            :gen_tcp.send(socket, "GET #{path} HTTP/1.1\r\nHost: localhost\r\n\r\n")
            {:ok, response} = :gen_tcp.recv(socket, 0, 2000)
            # probes never authenticate, so these must not 401/redirect
            assert String.contains?(response, "HTTP/1.1 200 OK"), "#{path} should be public"
            refute String.contains?(response, "401")
            :gen_tcp.close(socket)

          {:error, _} ->
            :ok
        end
      end
    end
  end

  describe "user management (P3-3)" do
    test "admin lists users (with permissions, no hashes)", %{admin_token: token} do
      {:ok, socket} = DashboardHelper.connect()
      {:ok, response} = DashboardHelper.authenticated_request(socket, :GET, "/users", token)
      :gen_tcp.close(socket)

      assert status_code(response) == 200
      {:ok, body} = json_body(response)
      usernames = Enum.map(body["users"], & &1["username"])
      assert "dashboard_admin" in usernames
      # the response carries permissions but never a password/hash field
      refute Enum.any?(body["users"], &Map.has_key?(&1, "password"))
    end

    test "admin creates a user that can then authenticate", %{admin_token: token} do
      username = "dashuser_#{System.unique_integer([:positive])}"
      on_exit(fn -> Malachi.Auth.remove_user(username) end)

      {:ok, socket} = DashboardHelper.connect()
      body = Jason.encode!(%{username: username, password: "Dash-Pass-123", permissions: ["consume"]})
      {:ok, response} = DashboardHelper.authenticated_request(socket, :POST, "/users", token, body: body)
      :gen_tcp.close(socket)

      assert status_code(response) == 201
      assert {:ok, _token} = Malachi.Auth.authenticate(username, "Dash-Pass-123", {127, 0, 0, 1})
    end

    test "admin rotates a password: the new one works, the old does not", %{admin_token: token} do
      username = "dashpw_#{System.unique_integer([:positive])}"
      on_exit(fn -> Malachi.Auth.remove_user(username) end)
      :ok = Malachi.Auth.add_user(username, "Old-Pass-111", [:consume])

      {:ok, socket} = DashboardHelper.connect()
      body = Jason.encode!(%{password: "New-Pass-222"})

      {:ok, response} =
        DashboardHelper.authenticated_request(socket, :PUT, "/users/#{username}/password", token, body: body)

      :gen_tcp.close(socket)

      assert status_code(response) == 200
      assert {:ok, _token} = Malachi.Auth.authenticate(username, "New-Pass-222", {127, 0, 0, 1})
      assert {:error, _reason} = Malachi.Auth.authenticate(username, "Old-Pass-111", {127, 0, 0, 1})
    end

    test "admin deletes a user", %{admin_token: token} do
      username = "dashdel_#{System.unique_integer([:positive])}"
      :ok = Malachi.Auth.add_user(username, "Del-Pass-1", [:consume])

      {:ok, socket} = DashboardHelper.connect()
      {:ok, response} = DashboardHelper.authenticated_request(socket, :DELETE, "/users/#{username}", token)
      :gen_tcp.close(socket)

      assert status_code(response) == 200
      assert {:error, :user_not_found} = UserStore.get_user(username)
    end

    test "creating a duplicate is a 409; an unknown permission is a 400", %{admin_token: token} do
      username = "dashdup_#{System.unique_integer([:positive])}"
      on_exit(fn -> Malachi.Auth.remove_user(username) end)
      :ok = Malachi.Auth.add_user(username, "p", [:consume])

      {:ok, s1} = DashboardHelper.connect()
      dup = Jason.encode!(%{username: username, password: "p2", permissions: ["consume"]})
      {:ok, r1} = DashboardHelper.authenticated_request(s1, :POST, "/users", token, body: dup)
      :gen_tcp.close(s1)
      assert status_code(r1) == 409

      {:ok, s2} = DashboardHelper.connect()
      bad = Jason.encode!(%{username: "dashbad_x", password: "p", permissions: ["superuser"]})
      {:ok, r2} = DashboardHelper.authenticated_request(s2, :POST, "/users", token, body: bad)
      :gen_tcp.close(s2)
      assert status_code(r2) == 400
      assert {:error, :user_not_found} = UserStore.get_user("dashbad_x")
    end

    test "a non-admin is forbidden and an unauthenticated request is unauthorized", %{producer_token: token} do
      # non-admin token -> 403
      {:ok, s1} = DashboardHelper.connect()
      {:ok, r1} = DashboardHelper.authenticated_request(s1, :GET, "/users", token)
      :gen_tcp.close(s1)
      assert status_code(r1) == 403

      # no token -> 401
      {:ok, s2} = DashboardHelper.connect()
      {:ok, r2} = DashboardHelper.request(s2, :GET, "/users")
      :gen_tcp.close(s2)
      assert status_code(r2) == 401
    end
  end

  describe "per-topic ACL management (P5-4b)" do
    setup do
      username = "dashacl_#{System.unique_integer([:positive])}"
      :ok = Malachi.Auth.add_user(username, "Acl-Pass-1", [:produce])
      on_exit(fn -> Malachi.Auth.remove_user(username) end)
      {:ok, acl_user: username}
    end

    defp acl_req(method, path, token, body \\ nil) do
      {:ok, socket} = DashboardHelper.connect()
      opts = if body, do: [body: Jason.encode!(body)], else: []
      {:ok, response} = DashboardHelper.authenticated_request(socket, method, path, token, opts)
      :gen_tcp.close(socket)
      response
    end

    test "admin grants an ACL, lists it, then revokes it", %{admin_token: token, acl_user: user} do
      grant = acl_req(:POST, "/users/#{user}/acls", token, %{operation: "produce", pattern: "orders.*"})
      assert status_code(grant) == 201

      list = acl_req(:GET, "/users/#{user}/acls", token)
      assert status_code(list) == 200
      {:ok, body} = json_body(list)
      assert body["acls"] == [%{"operation" => "produce", "resource" => "orders.*"}]

      revoke = acl_req(:DELETE, "/users/#{user}/acls", token, %{operation: "produce", pattern: "orders.*"})
      assert status_code(revoke) == 200

      after_list = acl_req(:GET, "/users/#{user}/acls", token)
      {:ok, after_body} = json_body(after_list)
      assert after_body["acls"] == []
    end

    test "an invalid operation is a 400", %{admin_token: token, acl_user: user} do
      response = acl_req(:POST, "/users/#{user}/acls", token, %{operation: "superuser", pattern: "t.*"})
      assert status_code(response) == 400
      {:ok, body} = json_body(response)
      assert body["type"] == "errors.acls.invalid_operation"
    end

    test "a non-admin is forbidden (403)", %{producer_token: token, acl_user: user} do
      response = acl_req(:GET, "/users/#{user}/acls", token)
      assert status_code(response) == 403
    end
  end

  # End-to-end coverage that the User-Agent is actually read from the HTTP headers and threaded into the
  # session binding (the unit tests in attack_simulation_test.exs pass the UA directly to SessionManager).
  describe "user-agent binding (end-to-end)" do
    setup do
      original = Application.get_env(:malachi, :session_ua_binding, false)
      Application.put_env(:malachi, :session_ua_binding, true)
      on_exit(fn -> Application.put_env(:malachi, :session_ua_binding, original) end)
      :ok
    end

    test "a token used with a different User-Agent than login is rejected", %{admin_token: token} do
      case DashboardHelper.connect() do
        {:ok, socket} ->
          # The setup token was minted with an empty User-Agent; a request carrying one mismatches, so ua
          # binding rejects the session (401) before the role check runs.
          {:ok, response} =
            DashboardHelper.authenticated_request(socket, :GET, "/metrics", token,
              headers: %{"User-Agent" => "Mozilla/5.0 (attacker)"}
            )

          assert status_code(response) == 401
          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    test "a token used with the same (empty) User-Agent is accepted", %{admin_token: token} do
      case DashboardHelper.connect() do
        {:ok, socket} ->
          # No User-Agent header resolves to "", matching the token's stored "", so validation passes.
          {:ok, response} = DashboardHelper.authenticated_request(socket, :GET, "/metrics", token)

          assert status_code(response) == 200
          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    test "the login captures the User-Agent, so a later request with a different UA is rejected" do
      # This proves the login side reads the UA from the HTTP headers (not just the validation side): the
      # token is minted over HTTP with a User-Agent, so a hardcoded "" at login would make this fail.
      case DashboardHelper.connect() do
        {:ok, login_socket} ->
          body = Jason.encode!(%{"username" => "dashboard_admin", "password" => "admin_pass_123"})

          {:ok, login_resp} =
            DashboardHelper.request(login_socket, :POST, "/login",
              body: body,
              headers: %{"User-Agent" => "AgentX/1.0"}
            )

          :gen_tcp.close(login_socket)
          token = DashboardHelper.extract_set_cookie(login_resp)
          assert is_binary(token) and token != ""

          # Same UA as the login is accepted.
          {:ok, s1} = DashboardHelper.connect()

          {:ok, ok_resp} =
            DashboardHelper.authenticated_request(s1, :GET, "/metrics", token, headers: %{"User-Agent" => "AgentX/1.0"})

          assert status_code(ok_resp) == 200
          :gen_tcp.close(s1)

          # A different UA than the login is rejected, proving the login-captured UA is what is enforced.
          {:ok, s2} = DashboardHelper.connect()

          {:ok, bad_resp} =
            DashboardHelper.authenticated_request(s2, :GET, "/metrics", token, headers: %{"User-Agent" => "AgentY/2.0"})

          assert status_code(bad_resp) == 401
          :gen_tcp.close(s2)

        {:error, _} ->
          :ok
      end
    end
  end

  # Sends an OPTIONS preflight for `path` carrying `origin` and returns the raw response. Deliberately
  # strict: the dashboard runs for this suite, so a connect or recv failure is a real failure, not a reason
  # to skip the assertions and report green.
  defp preflight_response(path, origin) do
    {:ok, socket} = DashboardHelper.connect()
    :gen_tcp.send(socket, "OPTIONS #{path} HTTP/1.1\r\nHost: localhost\r\nOrigin: #{origin}\r\n\r\n")
    {:ok, response} = :gen_tcp.recv(socket, 0, 2000)
    :gen_tcp.close(socket)

    response
  end

  # Applies a cookie policy for the duration of `fun` and restores both settings afterwards, including on
  # a failing assertion: leaking either of these would silently change what the rest of the suite tests.
  # `enable_tls` is passed explicitly rather than left alone because these tests exist to show it has no
  # say, which only means something when it is set against the expected outcome.
  defp with_cookie_policy(secure_cookie?, enable_tls?, fun) do
    previous = {
      Application.get_env(:malachi, :dashboard_secure_cookie),
      Application.get_env(:malachi, :enable_tls)
    }

    Application.put_env(:malachi, :dashboard_secure_cookie, secure_cookie?)
    Application.put_env(:malachi, :enable_tls, enable_tls?)

    try do
      fun.()
    after
      {previous_cookie, previous_tls} = previous
      Application.put_env(:malachi, :dashboard_secure_cookie, previous_cookie)
      Application.put_env(:malachi, :enable_tls, previous_tls)
    end
  end

  # Both of these bind the socket with a match rather than a case: a connection that fails has to fail the
  # test, not pass it quietly.
  defp login_response(extra_headers \\ %{}) do
    {:ok, socket} = DashboardHelper.connect()
    body = Jason.encode!(%{"username" => "dashboard_admin", "password" => "admin_pass_123"})

    {:ok, response} =
      DashboardHelper.request(socket, :POST, "/login", body: body, headers: extra_headers)

    :gen_tcp.close(socket)
    response
  end

  defp clearing_response do
    {:ok, socket} = DashboardHelper.connect()

    {:ok, response} =
      DashboardHelper.request(socket, :GET, "/", headers: %{"Cookie" => "malachi_token=not_a_real_token"})

    :gen_tcp.close(socket)
    response
  end

  # Sends a request made of `lines` (header lines without their CRLF) and reads the answer until the
  # server closes the connection.
  defp raw_request(lines, path \\ "/health", port \\ Malachi.Dashboard.port()) do
    {:ok, socket} = DashboardHelper.connect(port: port)
    head = Enum.map_join(lines, &(&1 <> "\r\n"))
    :ok = :gen_tcp.send(socket, "GET #{path} HTTP/1.1\r\n#{head}\r\n")
    response = read_until_closed(socket, "")
    :gen_tcp.close(socket)
    response
  end

  # A GET whose request line, CRLF included, is exactly `length` bytes long.
  defp raw_request_line(length, port \\ Malachi.Dashboard.port()) do
    {:ok, socket} = DashboardHelper.connect(port: port)
    target = "/" <> String.duplicate("a", length - byte_size("GET / HTTP/1.1\r\n"))
    :ok = :gen_tcp.send(socket, "GET #{target} HTTP/1.1\r\nHost: localhost\r\n\r\n")
    response = read_until_closed(socket, "")
    :gen_tcp.close(socket)
    response
  end

  defp read_until_closed(socket, acc) do
    case :gen_tcp.recv(socket, 0, 3_000) do
      {:ok, data} -> read_until_closed(socket, acc <> data)
      {:error, _} -> acc
    end
  end

  # `count` header lines in all, the Host header among them.
  defp header_lines(count), do: ["Host: localhost" | Enum.map(2..count//1, &"X-H#{&1}: v")]

  # One "X-Pad" header line exactly `length` bytes long, CRLF included.
  defp padded_line(length), do: "X-Pad: " <> String.duplicate("a", length - byte_size("X-Pad: ") - 2)

  # Host plus four "X-TN" headers whose names and values add up to `total` bytes.
  defp total_lines(total), do: ["Host: localhost" | split_values(["X-T1", "X-T2", "X-T3", "X-T4"], total - 13 - 16)]

  # Host plus three "X-A" style headers whose names and values add up to `total` bytes.
  defp small_total_lines(total), do: ["Host: localhost" | split_values(["X-A", "X-B", "X-C"], total - 13 - 9)]

  defp split_values(names, value_bytes) do
    share = div(value_bytes, length(names))
    extra = value_bytes - share * length(names)

    names
    |> Enum.with_index()
    |> Enum.map(fn {name, index} ->
      size = if index == 0, do: share + extra, else: share
      "#{name}: " <> String.duplicate("v", size)
    end)
  end

  defp start_dashboard do
    name = :"header_bounds_dashboard_#{System.unique_integer([:positive])}"
    start_supervised!({Malachi.Dashboard, {0, name: name}}, id: name)
    Malachi.Dashboard.port(name)
  end

  defp put_recv_timeout(ms) do
    original = Application.fetch_env(:malachi, :dashboard_recv_timeout_ms)
    Application.put_env(:malachi, :dashboard_recv_timeout_ms, ms)

    on_exit(fn ->
      case original do
        {:ok, value} -> Application.put_env(:malachi, :dashboard_recv_timeout_ms, value)
        :error -> Application.delete_env(:malachi, :dashboard_recv_timeout_ms)
      end
    end)
  end

  # Extracts the numeric status from an HTTP response, and its JSON body.
  defp status_code(response) do
    case Regex.run(~r"HTTP/1\.1 (\d{3})", response) do
      [_, code] -> String.to_integer(code)
      _ -> 0
    end
  end

  defp json_body(response) do
    case String.split(response, "\r\n\r\n", parts: 2) do
      [_headers, body] -> body |> String.trim() |> Jason.decode()
      _ -> :error
    end
  end
end
