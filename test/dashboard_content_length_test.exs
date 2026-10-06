defmodule Malachi.DashboardContentLengthTest do
  use ExUnit.Case, async: false

  alias Malachi.Test.DashboardHelper

  # Every response is read by its framing (DashboardHelper.recv_framed/2): exactly Content-Length bytes of
  # body, and then nothing but the server closing the socket. A byte after the declared body is harmless
  # while the socket closes after each response, and becomes the first byte of the next response the moment
  # a connection is reused, so each route that answers with a body has its own row here.

  @admin "content_length_admin"
  @password "content_length_pass_123"

  setup do
    Malachi.RateLimiter.reset_bucket("127.0.0.1", :dashboard_auth)
    _ = Malachi.Auth.remove_user(@admin)
    :ok = Malachi.Auth.add_user(@admin, @password, [:admin])
    {:ok, token} = Malachi.Auth.authenticate(@admin, @password, {127, 0, 0, 1})

    # The cookie rows below expect no Secure attribute. config/runtime.exs reads the policy from
    # MALACHI_DASHBOARD_SECURE_COOKIE even under test, so it is pinned here rather than inherited from the
    # shell that runs the suite.
    secure_cookie = Application.get_env(:malachi, :dashboard_secure_cookie)
    Application.put_env(:malachi, :dashboard_secure_cookie, false)

    on_exit(fn ->
      Application.put_env(:malachi, :dashboard_secure_cookie, secure_cookie)
      _ = Malachi.Auth.remove_user(@admin)
      Malachi.RateLimiter.reset_bucket("127.0.0.1", :dashboard_auth)
    end)

    {:ok, token: token}
  end

  @json %{"content-type" => "application/json"}
  @json_no_cache Map.put(@json, "cache-control", "no-cache")
  @problem %{"content-type" => "application/problem+json"}
  @problem_401 Map.put(@problem, "www-authenticate", ~s(Bearer realm="Malachi"))
  @html %{"content-type" => "text/html; charset=utf-8", "cache-control" => "no-store, no-cache, must-revalidate"}
  @to_login %{"location" => "/login", "cache-control" => "no-store"}

  # {label, method, path, credentials, extra request headers, request body, expected status, expected
  # response headers}. The label names the sender under test, so a failure points at the function to look
  # at. The expected headers are the ones each sender sets itself, which moved from a heredoc into a list
  # when the framing moved into one helper: a header value is matched exactly, {:wraps, prefix, suffix}
  # matches its start and end around a value that changes per request (the session token), and :integer
  # asks for a non-negative integer. The cookies expect the Secure policy off, which setup pins.
  @framed [
    {"serve_login_page", :GET, "/login", :none, %{}, nil, 200, @html},
    {"send_login_success", :POST, "/login", :none, %{}, :valid_login, 200,
     Map.put(@json, "set-cookie", {:wraps, "malachi_token=", "; HttpOnly; Path=/; SameSite=Strict"})},
    {"send_problem, wrong password", :POST, "/login", :none, %{}, :wrong_login, 401, @problem_401},
    {"send_problem, invalid token", :GET, "/metrics", :bogus_bearer, %{}, nil, 401, @problem_401},
    {"send_problem, no credentials", :GET, "/metrics", :none, %{}, nil, 401, @problem_401},
    {"serve_html", :GET, "/", :cookie, %{}, nil, 200, @html},
    {"serve_json", :GET, "/metrics", :cookie, %{}, nil, 200, @json_no_cache},
    {"serve_prometheus", :GET, "/metrics", :cookie, %{"Accept" => "text/plain"}, nil, 200,
     %{"content-type" => Malachi.Metrics.Prometheus.content_type(), "cache-control" => "no-cache"}},
    {"serve_status, liveness", :GET, "/health", :none, %{}, nil, 200, Map.put(@json, "cache-control", "no-store")},
    {"serve_status, readiness", :GET, "/ready", :none, %{}, nil, 200, Map.put(@json, "cache-control", "no-store")},
    {"serve_rate_limits", :GET, "/rate_limits", :cookie, %{}, nil, 200, @json_no_cache},
    {"send_json", :GET, "/users", :cookie, %{}, nil, 200, @json},
    {"serve_404", :GET, "/no-such-route", :cookie, %{}, nil, 404, @problem},
    # The senders below were already framed correctly; they stay here so none of them regresses.
    {"redirect to login", :GET, "/", :none, %{}, nil, 302, @to_login},
    {"cookie clearing redirect", :GET, "/logout", :none, %{}, nil, 302,
     Map.put(@to_login, "set-cookie", "malachi_token=; HttpOnly; Path=/; SameSite=Strict; Max-Age=0")},
    {"CORS preflight", :OPTIONS, "/metrics", :none, %{"Origin" => "https://app.example"}, nil, 204, %{}},
    {"serve_logo", :GET, "/logo.svg", :none, %{}, nil, 200,
     %{"content-type" => "image/svg+xml; charset=utf-8", "cache-control" => "public, max-age=86400"}}
  ]

  for {label, method, path, credentials, headers, body, status, expected_headers} <- @framed do
    test "#{method} #{path} (#{label}) sends exactly the Content-Length it declares", %{token: token} do
      response =
        request(
          unquote(method),
          unquote(path),
          unquote(credentials),
          unquote(Macro.escape(headers)),
          unquote(body),
          token
        )

      assert_framed(response, unquote(status))
      assert_headers(response, unquote(Macro.escape(expected_headers)))
    end
  end

  test "GET /stream declares no Content-Length, since the stream is framed by its events", %{token: token} do
    {:ok, socket} = DashboardHelper.connect()
    :ok = DashboardHelper.send_request(socket, :GET, "/stream", headers: %{"Cookie" => "malachi_token=#{token}"})
    {:ok, response} = DashboardHelper.recv_framed(socket)
    :gen_tcp.close(socket)

    assert response.status == 200
    refute List.keymember?(response.headers, "content-length", 0)
  end

  @tag :slow
  test "a 429 sends exactly the Content-Length it declares, as does every 401 before it" do
    limit = Application.get_env(:malachi, :dashboard_auth_rate_limit, 10)

    rate_limited =
      Enum.find_value(1..(limit * 2), fn _attempt ->
        response = request(:GET, "/metrics", :bogus_bearer, %{}, nil, nil)
        assert_framed(response, response.status)
        if response.status == 429, do: response
      end)

    assert rate_limited, "expected the dashboard auth rate limit to trip within #{limit * 2} requests"
    assert_headers(rate_limited, Map.put(@problem, "retry-after", :integer))
  end

  defp request(method, path, credentials, headers, body, token) do
    {:ok, socket} = DashboardHelper.connect()

    :ok =
      DashboardHelper.send_request(socket, method, path,
        headers: Map.merge(headers, credential_headers(credentials, token)),
        body: request_body(body)
      )

    {:ok, response} = DashboardHelper.recv_framed(socket)
    :gen_tcp.close(socket)
    response
  end

  defp credential_headers(:none, _token), do: %{}
  defp credential_headers(:cookie, token), do: %{"Cookie" => "malachi_token=#{token}"}
  defp credential_headers(:bogus_bearer, _token), do: %{"Authorization" => "Bearer bogus_token"}

  defp request_body(nil), do: nil
  defp request_body(:valid_login), do: Jason.encode!(%{"username" => @admin, "password" => @password})
  defp request_body(:wrong_login), do: Jason.encode!(%{"username" => @admin, "password" => "wrong"})

  defp assert_framed(response, expected_status) do
    assert response.status == expected_status

    lengths = for {"content-length", value} <- response.headers, do: value
    assert [_one] = lengths, "expected exactly one Content-Length header, got #{inspect(lengths)}"

    assert response.trailing == "",
           "#{byte_size(response.trailing)} byte(s) after the declared body: #{inspect(response.trailing)}"
  end

  defp assert_headers(response, expected) do
    for {name, expectation} <- expected do
      values = for {^name, value} <- response.headers, do: value
      assert [value] = values, "expected exactly one #{name} header, got #{inspect(values)}"
      assert header_matches?(value, expectation), "#{name}: #{inspect(value)} does not match #{inspect(expectation)}"
    end
  end

  defp header_matches?(value, :integer), do: match?({n, ""} when n >= 0, Integer.parse(value))

  defp header_matches?(value, {:wraps, prefix, suffix}) do
    byte_size(value) > byte_size(prefix) + byte_size(suffix) and String.starts_with?(value, prefix) and
      String.ends_with?(value, suffix)
  end

  defp header_matches?(value, exact), do: value == exact
end
