defmodule Malachi.DashboardApiRateLimitTest do
  # Authenticated dashboard requests spend their own session keyed `:dashboard_api` bucket, not the login
  # bucket `:dashboard_auth`, which stays keyed by IP and is spent only by logins and by tokens that do not
  # validate. async: false: the limiter, the audit log and the application env are node global.
  use ExUnit.Case, async: false

  alias Malachi.AuditLog
  alias Malachi.Auth
  alias Malachi.Metrics
  alias Malachi.RateLimiter
  alias Malachi.Test.DashboardHelper

  @client_ip "127.0.0.1"
  @admin "api_rate_admin"
  @producer "api_rate_producer"
  @password "api_rate_pass_123"
  @origin "https://app.example"

  @env_keys [
    :dashboard_api_rate_limit,
    :dashboard_api_rate_window_ms,
    :dashboard_auth_rate_limit,
    :dashboard_auth_rate_window_ms,
    :session_ua_binding,
    :dashboard_update_interval_ms,
    :dashboard_cors_enabled,
    :dashboard_cors_origins
  ]

  setup do
    original = for key <- @env_keys, into: %{}, do: {key, Application.fetch_env(:malachi, key)}
    # Only the login bucket is reset: it is keyed by the one address every test here shares. The API
    # buckets need no reset, because each test mints fresh sessions and each session has its own bucket.
    RateLimiter.reset_bucket(@client_ip, :dashboard_auth)

    for user <- [@admin, @producer], do: _ = Auth.remove_user(user)
    :ok = Auth.add_user(@admin, @password, [:admin])
    :ok = Auth.add_user(@producer, @password, [:produce])

    on_exit(fn ->
      for {key, value} <- original do
        case value do
          {:ok, v} -> Application.put_env(:malachi, key, v)
          :error -> Application.delete_env(:malachi, key)
        end
      end

      for user <- [@admin, @producer], do: _ = Auth.remove_user(user)
      RateLimiter.reset_bucket(@client_ip, :dashboard_auth)
    end)

    :ok
  end

  describe "the authenticated path no longer spends the login bucket" do
    test "fifty authenticated requests in a second from one address are all served" do
      token = session(@admin)

      statuses = for _ <- 1..50, do: status(get(token, "/metrics"))

      assert Enum.all?(statuses, &(&1 == 200)), "statuses: #{inspect(Enum.frequencies(statuses))}"
    end

    test "a valid session keeps working while its address has exhausted the login bucket" do
      token = session(@admin)
      exhaust_login_bucket()

      assert status(get(token, "/metrics")) == 200
    end

    test "two sessions from one address have independent budgets" do
      limit_api(3)
      first = session(@admin)
      second = session(@admin)

      assert for(_ <- 1..4, do: status(get(first, "/metrics"))) == [200, 200, 200, 429]
      assert status(get(second, "/metrics")) == 200
    end

    test "a limit of zero turns the API bucket off" do
      # Spent first, so a zero that silently fell back to some default budget would still answer 429.
      limit_api(1)
      token = session(@admin)
      assert status(get(token, "/metrics")) == 200
      assert status(get(token, "/metrics")) == 429

      limit_api(0)

      assert status(get(token, "/metrics")) == 200
    end

    test "a request refused for lack of permission still spends the session's budget" do
      limit_api(1)
      token = session(@producer)

      assert status(get(token, "/")) == 403
      assert status(get(token, "/metrics")) == 429
    end

    test "/stream spends one token when it opens and none per event" do
      limit_api(2)
      Application.put_env(:malachi, :dashboard_update_interval_ms, 20)
      token = session(@admin)

      {:ok, socket} = DashboardHelper.connect()
      :ok = :gen_tcp.send(socket, "GET /stream HTTP/1.1\r\nHost: localhost\r\nCookie: malachi_token=#{token}\r\n\r\n")
      assert read_events(socket, 3, "") >= 3
      :gen_tcp.close(socket)

      assert status(get(token, "/metrics")) == 200
      assert status(get(token, "/metrics")) == 429
    end
  end

  describe "an API limited response" do
    setup do
      Application.put_env(:malachi, :dashboard_cors_enabled, true)
      Application.put_env(:malachi, :dashboard_cors_origins, [@origin])
      :ok
    end

    test "is the usual 429, readable cross-origin" do
      limit_api(1)
      token = session(@admin)
      assert status(get(token, "/metrics")) == 200

      response = get(token, "/metrics", %{"Origin" => @origin})

      assert status(response) == 429
      assert %{"type" => "errors.http.rate_limited", "status" => 429, "retry_after_ms" => retry} = json_body(response)
      assert is_integer(retry) and retry > 0
      assert response =~ ~r/\r\nRetry-After: \d+\r\n/
      assert String.downcase(response) =~ "access-control-allow-origin: #{@origin}"
    end

    test "is counted and audited apart from login throttling, under the session digest" do
      limit_api(1)
      token = session(@admin)
      digest = digest(token)
      blocked_before = Metrics.get_system_metrics().rate_limiting.dashboard_api_blocked
      auth_blocked_before = Metrics.get_system_metrics().security.dashboard.auth_blocked

      assert status(get(token, "/metrics")) == 200
      assert status(get(token, "/metrics")) == 429

      assert Metrics.get_system_metrics().rate_limiting.dashboard_api_blocked == blocked_before + 1
      assert Metrics.get_system_metrics().security.dashboard.auth_blocked == auth_blocked_before

      :ok = AuditLog.flush()

      assert Enum.any?(
               AuditLog.get_events_by_type(:dashboard_api_rate_limited, 50),
               &(&1.username == @admin and &1.metadata.session == digest)
             )
    end

    test "shows in /rate_limits by digest, and the token itself is stored nowhere" do
      limit_api(1)
      token = session(@admin)
      assert status(get(token, "/metrics")) == 200
      assert status(get(token, "/metrics")) == 429

      response = get(session(@admin), "/rate_limits")
      rate_limits = json_body(response)

      assert %{"identifier" => digest(token), "count" => 1} in rate_limits["top_blocked"]["dashboard_api"]
      assert is_list(rate_limits["top_blocked"]["dashboard_auth"])
      assert rate_limits["config"]["dashboard_api"] == %{"limit" => 1, "window_ms" => 60_000}
      assert %{"limit" => _, "window_ms" => _} = rate_limits["config"]["dashboard_auth"]

      refute response =~ token
      refute inspect(:ets.tab2list(:malachi_rate_limits), limit: :infinity) =~ token
    end
  end

  describe "a token that does not validate" do
    test "still spends the login bucket, and is refused once the address is out of it" do
      exhaust_login_bucket()

      response = get("bogus_token", "/metrics")

      assert status(response) == 429
      :ok = AuditLog.flush()

      assert Enum.any?(
               AuditLog.get_events_by_type(:dashboard_auth_failure, 50),
               &(&1.status == :rate_limited and &1.metadata.path == "/metrics")
             )
    end

    test "cannot write more hijack events than the login bucket admits" do
      # A stolen token replayed from the wrong client used to be capped by the login bucket, which was
      # checked before validation. Validation is where the hijack event is written, so a token that does
      # not validate must still be charged to the address before it is validated.
      Application.put_env(:malachi, :session_ua_binding, true)
      Application.put_env(:malachi, :dashboard_auth_rate_limit, 5)
      Application.put_env(:malachi, :dashboard_auth_rate_window_ms, 60_000)
      {:ok, token} = Auth.authenticate(@admin, @password, {127, 0, 0, 1}, "ua-original")

      statuses = for _ <- 1..25, do: status(get(token, "/metrics", %{"User-Agent" => "ua-replayed"}))

      :ok = AuditLog.flush()
      prefix = String.slice(token, 0, 8)

      hijacks =
        Enum.count(AuditLog.get_events_by_type(:session_hijack_attempt, 1_000), &(&1.metadata.token_prefix == prefix))

      assert hijacks <= 6, "#{hijacks} hijack events for 25 replays with a login budget of 5"
      assert Enum.count(statuses, &(&1 == 429)) >= 19
    end
  end

  # --- helpers ---

  defp session(user) do
    {:ok, token} = Auth.authenticate(user, @password, {127, 0, 0, 1})
    token
  end

  defp limit_api(limit) do
    Application.put_env(:malachi, :dashboard_api_rate_limit, limit)
    Application.put_env(:malachi, :dashboard_api_rate_window_ms, 60_000)
  end

  # Spends the address's login bucket to the last token, the way a burst of wrong passwords would, without
  # paying for the password hashing of real login attempts.
  defp exhaust_login_bucket do
    config = %{
      limit: Application.get_env(:malachi, :dashboard_auth_rate_limit, 10),
      window_ms: Application.get_env(:malachi, :dashboard_auth_rate_window_ms, 60_000)
    }

    Enum.find(1..(config.limit + 1), fn _ ->
      match?({:error, :rate_limit_exceeded, _}, RateLimiter.check_limit(@client_ip, :dashboard_auth, config))
    end) || flunk("the login bucket never ran out")
  end

  # The digest the API bucket is keyed by: part of the contract, since it is what an operator reads in
  # /rate_limits and in the audit event.
  defp digest(token), do: :sha256 |> :crypto.hash(token) |> binary_part(0, 16) |> Base.url_encode64(padding: false)

  defp get(token, path, headers \\ %{}) do
    {:ok, socket} = DashboardHelper.connect()
    header_lines = Enum.map_join(headers, fn {k, v} -> "#{k}: #{v}\r\n" end)

    :ok =
      :gen_tcp.send(
        socket,
        "GET #{path} HTTP/1.1\r\nHost: localhost\r\nCookie: malachi_token=#{token}\r\n#{header_lines}\r\n"
      )

    read_until_closed(socket, "")
  end

  defp read_until_closed(socket, acc) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, data} -> read_until_closed(socket, acc <> data)
      {:error, _closed} -> acc
    end
  end

  defp read_events(socket, wanted, acc) do
    events = acc |> String.split("data: ") |> length() |> Kernel.-(1)

    if events >= wanted do
      events
    else
      case :gen_tcp.recv(socket, 0, 2_000) do
        {:ok, data} -> read_events(socket, wanted, acc <> data)
        {:error, _} -> events
      end
    end
  end

  defp status("HTTP/1.1 " <> <<code::binary-size(3), _rest::binary>>), do: String.to_integer(code)

  defp json_body(response) do
    [_head, body] = String.split(response, "\r\n\r\n", parts: 2)
    Jason.decode!(body)
  end
end
