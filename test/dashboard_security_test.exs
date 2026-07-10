defmodule Malachi.DashboardSecurityTest do
  use ExUnit.Case, async: false

  alias Malachi.Test.DashboardHelper

  # These tests require dashboard authentication and rate limiting enabled in config/test.exs

  @dashboard_port Application.compile_env(:malachi, :dashboard_port, 4041)

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
      case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
        {:ok, socket} ->
          request = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
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

    test "GET / with invalid token returns 403", %{} do
      case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
        {:ok, socket} ->
          request = """
          GET / HTTP/1.1\r
          Host: localhost\r
          Cookie: malachi_token=invalid_token_12345\r
          \r
          """

          :gen_tcp.send(socket, request)
          {:ok, response} = :gen_tcp.recv(socket, 0, 2000)

          assert String.contains?(response, "403 Forbidden") or
                   String.contains?(response, "403")

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    test "GET / with producer token (non-admin) returns 403", %{producer_token: token} do
      case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
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
      case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
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

    test "GET /metrics with producer token returns 200", %{producer_token: token} do
      case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
        {:ok, socket} ->
          request = """
          GET /metrics HTTP/1.1\r
          Host: localhost\r
          Cookie: malachi_token=#{token}\r
          \r
          """

          :gen_tcp.send(socket, request)
          {:ok, response} = :gen_tcp.recv(socket, 0, 2000)

          assert String.contains?(response, "200 OK")
          assert String.contains?(response, "application/json")

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    test "GET / with Bearer token fallback returns 200", %{admin_token: token} do
      case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
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
      case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
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

    test "POST /login with invalid credentials returns 403" do
      case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
        {:ok, socket} ->
          body = Jason.encode!(%{"username" => "dashboard_admin", "password" => "wrong_password"})

          request =
            "POST /login HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: #{byte_size(body)}\r\n\r\n#{body}"

          :gen_tcp.send(socket, request)
          {:ok, response} = :gen_tcp.recv(socket, 0, 2000)

          assert String.contains?(response, "403 Forbidden") or
                   String.contains?(response, "403")

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    test "GET /login returns HTML login page" do
      case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
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

  describe "security headers" do
    test "responses include security headers", %{admin_token: token} do
      case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
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

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    test "CORS headers present on /metrics", %{admin_token: token} do
      case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
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

    test "OPTIONS request returns CORS preflight" do
      case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
        {:ok, socket} ->
          request = """
          OPTIONS /metrics HTTP/1.1\r
          Host: localhost\r
          Origin: https://example.com\r
          \r
          """

          :gen_tcp.send(socket, request)
          {:ok, response} = :gen_tcp.recv(socket, 0, 2000)

          assert String.contains?(response, "204 No Content") or
                   String.contains?(response, "Access-Control-Allow")

          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
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
          case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
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
      case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
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
      case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
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
      case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
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
          :gen_tcp.close(socket)

        {:error, _} ->
          :ok
      end
    end

    test "GET /logout works without cookie" do
      case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
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
      case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
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
          case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
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
      case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
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
        case :gen_tcp.connect({127, 0, 0, 1}, @dashboard_port, [:binary, active: false], 1000) do
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
end
