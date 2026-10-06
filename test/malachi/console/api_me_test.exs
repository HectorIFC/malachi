defmodule Malachi.Console.ApiMeTest do
  use ExUnit.Case, async: false

  alias Malachi.Test.AccessHelper

  setup do
    Malachi.RateLimiter.reset_bucket("127.0.0.1", :dashboard_auth)
    :ok
  end

  defp me(token, opts \\ []), do: AccessHelper.call(:console, "GET", "/api/v1/me", token, opts)

  describe "GET /api/v1/me" do
    test "a viewer, an editor and an admin are told their role and what it lets them do" do
      for {role, capabilities} <- [
            viewer: ["read_cluster"],
            editor: ["read_cluster", "manage_policies"],
            admin: ["read_cluster", "manage_policies", "manage_users", "manage_acls", "diagnostics"]
          ] do
        token = AccessHelper.account!("me_#{role}", [], role)
        response = me(token)

        assert response.status == 200
        assert AccessHelper.header(response, "content-type") =~ "application/json"
        assert AccessHelper.header(response, "cache-control") == "no-store"

        assert %{
                 "username" => username,
                 "authenticated" => true,
                 "role" => role_name,
                 "capabilities" => ^capabilities,
                 "permissions" => [],
                 "locale" => "en_US"
               } = response.json

        assert username == "me_#{role}"
        assert role_name == Atom.to_string(role)
      end
    end

    test "an account with only wire permissions is answered, with no role and no capabilities" do
      token = AccessHelper.account!("me_producer", [:produce], nil)

      assert %{status: 200, json: %{"role" => nil, "capabilities" => [], "permissions" => ["produce"]}} = me(token)
    end

    test "a wire admin is a console admin" do
      token = AccessHelper.account!("me_wire_admin", [:admin], nil)
      assert %{status: 200, json: %{"role" => "admin", "permissions" => ["admin"]}} = me(token)
    end

    test "the session cookie works when no Bearer header is sent" do
      token = AccessHelper.account!("me_cookie", [], :viewer)
      response = me(nil, headers: [{"Cookie", "malachi_token=#{token}"}])
      assert %{status: 200, json: %{"username" => "me_cookie"}} = response
    end

    test "no credentials is a 401 problem asking for a Bearer token" do
      response = me(nil)

      assert response.status == 401
      assert AccessHelper.header(response, "content-type") == "application/problem+json"
      assert AccessHelper.header(response, "www-authenticate") =~ "Bearer"
      assert response.json == %{"type" => "errors.auth.unauthenticated", "status" => 401}
    end

    test "an invalid token is a 401 problem" do
      assert %{status: 401, json: %{"type" => "errors.auth.session_invalid"}} = me("not-a-session")
    end

    test "the cluster identity comes from configuration, null when unset" do
      token = AccessHelper.account!("me_identity", [], :viewer)

      assert %{json: %{"cluster" => %{"name" => nil, "color" => nil, "icon" => nil}}} = me(token)

      Application.put_env(:malachi, :cluster_identity, %{name: "prod-eu", color: "#A1B2C3", icon: "globe"})
      on_exit(fn -> Application.delete_env(:malachi, :cluster_identity) end)

      assert %{json: %{"cluster" => %{"name" => "prod-eu", "color" => "#A1B2C3", "icon" => "globe"}}} = me(token)
    end

    test "the locale is the node's" do
      token = AccessHelper.account!("me_locale", [], :viewer)
      Application.put_env(:malachi, :locale, "pt_BR")
      on_exit(fn -> Application.put_env(:malachi, :locale, "en_US") end)

      assert %{json: %{"locale" => "pt_BR"}} = me(token)
    end

    test "with authentication disabled it reports the anonymous admin" do
      Application.put_env(:malachi, :dashboard_auth_enabled, false)
      on_exit(fn -> Application.put_env(:malachi, :dashboard_auth_enabled, true) end)

      assert %{status: 200, json: %{"username" => nil, "authenticated" => false, "role" => "admin"}} = me(nil)
    end

    test "it spends the session's API budget" do
      Application.put_env(:malachi, :dashboard_api_rate_limit, 2)
      Application.put_env(:malachi, :dashboard_api_rate_window_ms, 60_000)

      on_exit(fn ->
        Application.delete_env(:malachi, :dashboard_api_rate_limit)
        Application.delete_env(:malachi, :dashboard_api_rate_window_ms)
      end)

      token = AccessHelper.account!("me_budget", [], :viewer)
      statuses = for _request <- 1..4, do: me(token).status

      assert List.last(statuses) == 429
      assert %{json: %{"type" => "errors.http.rate_limited", "retry_after_ms" => _}} = me(token)
    end
  end

  describe "the /api/v1 namespace" do
    test "is matched before the single page application fallback" do
      # The console under test has no bundle, so anything the static pipeline answered would be a 503.
      assert %{status: 401} = me(nil)

      assert %{status: 404, json: %{"type" => "errors.http.not_found"}} =
               AccessHelper.call(:console, "GET", "/api/v1/nope", nil)
    end

    test "another method on a route is a 405 problem with Allow, before any credential is read" do
      response = AccessHelper.call(:console, "POST", "/api/v1/me", nil)

      assert response.status == 405
      assert AccessHelper.header(response, "allow") == "GET"
      assert response.json == %{"type" => "errors.http.method_not_allowed", "status" => 405}
    end
  end
end
