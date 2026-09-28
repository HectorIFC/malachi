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

    on_exit(fn ->
      _ = Malachi.Auth.remove_user(@admin)
      Malachi.RateLimiter.reset_bucket("127.0.0.1", :dashboard_auth)
    end)

    {:ok, token: token}
  end

  # {label, method, path, credentials, extra request headers, request body, expected status}. The label
  # names the sender under test, so a failure points at the function to look at.
  @framed [
    {"serve_login_page", :GET, "/login", :none, %{}, nil, 200},
    {"send_login_success", :POST, "/login", :none, %{}, :valid_login, 200},
    {"send_forbidden, wrong password", :POST, "/login", :none, %{}, :wrong_login, 403},
    {"send_forbidden, invalid token", :GET, "/metrics", :bogus_bearer, %{}, nil, 403},
    {"send_auth_required", :GET, "/metrics", :none, %{}, nil, 401},
    {"serve_html", :GET, "/", :cookie, %{}, nil, 200},
    {"serve_json", :GET, "/metrics", :cookie, %{}, nil, 200},
    {"serve_prometheus", :GET, "/metrics", :cookie, %{"Accept" => "text/plain"}, nil, 200},
    {"serve_status, liveness", :GET, "/health", :none, %{}, nil, 200},
    {"serve_status, readiness", :GET, "/ready", :none, %{}, nil, 200},
    {"serve_rate_limits", :GET, "/rate_limits", :cookie, %{}, nil, 200},
    {"send_json", :GET, "/users", :cookie, %{}, nil, 200},
    {"serve_404", :GET, "/no-such-route", :cookie, %{}, nil, 404},
    # The senders below were already framed correctly; they stay here so none of them regresses.
    {"redirect to login", :GET, "/", :none, %{}, nil, 302},
    {"cookie clearing redirect", :GET, "/logout", :none, %{}, nil, 302},
    {"CORS preflight", :OPTIONS, "/metrics", :none, %{"Origin" => "https://app.example"}, nil, 204},
    {"serve_logo", :GET, "/logo.svg", :none, %{}, nil, 200}
  ]

  for {label, method, path, credentials, headers, body, status} <- @framed do
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
  test "a 429 (send_rate_limited) sends exactly the Content-Length it declares, as does every 403 before it" do
    limit = Application.get_env(:malachi, :dashboard_auth_rate_limit, 10)

    rate_limited =
      Enum.find_value(1..(limit * 2), fn _attempt ->
        response = request(:GET, "/metrics", :bogus_bearer, %{}, nil, nil)
        assert_framed(response, response.status)
        if response.status == 429, do: response
      end)

    assert rate_limited, "expected the dashboard auth rate limit to trip within #{limit * 2} requests"
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
end
