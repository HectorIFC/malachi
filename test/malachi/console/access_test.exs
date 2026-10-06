defmodule Malachi.Console.AccessTest do
  # Resolution reads the shared session table, the user registry and the rate limiter.
  use ExUnit.Case, async: false

  alias Malachi.Auth
  alias Malachi.Auth.UserStore
  alias Malachi.Console.Access
  alias Malachi.Test.AccessHelper

  doctest Access

  @request %{ip: "127.0.0.1", user_agent: "", method: "GET", path: "/metrics"}

  describe "required_role/2" do
    test "public routes need no credentials" do
      for {method, path} <- [
            {"GET", "/login"},
            {"POST", "/login"},
            {"GET", "/logout"},
            {"GET", "/health"},
            {"GET", "/ready"},
            {"GET", "/logo.svg"},
            {"OPTIONS", "/metrics"},
            {"OPTIONS", "/anything/at/all"}
          ],
          do: assert(Access.required_role(method, path) == :public, "#{method} #{path}")
    end

    test "reads need a viewer, storage policy writes an editor, users and ACLs an admin" do
      assert Access.required_role("GET", "/") == :viewer
      assert Access.required_role("GET", "/stream") == :viewer
      assert Access.required_role("GET", "/topic") == :viewer
      assert Access.required_role("GET", "/rate_limits") == :viewer
      assert Access.required_role("GET", "/policies") == :viewer
      assert Access.required_role("GET", "/topics/orders/policy") == :viewer
      assert Access.required_role("PUT", "/policies/hot") == :editor
      assert Access.required_role("DELETE", "/topics/orders/policy") == :editor
      assert Access.required_role("GET", "/users") == :admin
      assert Access.required_role("PUT", "/users/alice/role") == :admin
      assert Access.required_role("DELETE", "/users/alice/acls") == :admin
      assert Access.required_role("GET", "/api/v1/me") == :authenticated
    end

    test "a path no row matches requires admin, so a forgotten row denies" do
      assert Access.required_role("GET", "/topics/orders") == :admin
      assert Access.required_role("POST", "/metrics") == :admin
      assert Access.required_role("GET", "/api/v1/unknown") == :admin
      assert Access.required_role("PUT", "/policies/") == :admin
      assert Access.required_role("PUT", "/topics//policy") == :admin
    end

    test "every row has a known method and level" do
      for {method, pattern, level} <- Access.routes() do
        assert method in ~w(GET POST PUT DELETE OPTIONS)
        assert pattern == :any or is_list(pattern)
        assert level in [:public, :authenticated, :viewer, :editor, :admin]
      end
    end
  end

  describe "effective_role/2 and capabilities/1" do
    test "wire permissions grant no console role except the superuser" do
      assert Access.effective_role([:produce, :consume], nil) == nil
      assert Access.effective_role([:produce], :viewer) == :viewer
      assert Access.effective_role([:admin], nil) == :admin
      assert Access.effective_role([:admin, :produce], :editor) == :admin
    end

    test "capabilities accumulate up the nesting" do
      assert Access.capabilities(:viewer) == [:read_cluster]
      assert Access.capabilities(:editor) == [:read_cluster, :manage_policies]

      assert Access.capabilities(:admin) ==
               [:read_cluster, :manage_policies, :manage_users, :manage_acls, :diagnostics]
    end
  end

  describe "authorize/2" do
    test "names what is missing" do
      viewer = %{Access.anonymous() | role: :viewer, authenticated: true}

      assert Access.authorize(nil, :public) == :ok
      assert Access.authorize(nil, :authenticated) == {:error, :authentication_required}
      assert Access.authorize(nil, :viewer) == {:error, :authentication_required}
      assert Access.authorize(%{viewer | role: nil}, :authenticated) == :ok
      assert Access.authorize(%{viewer | role: nil}, :viewer) == {:error, {:missing_role, :viewer, nil}}
      assert Access.authorize(viewer, :viewer) == :ok
      assert Access.authorize(viewer, :editor) == {:error, {:missing_role, :editor, :viewer}}
      assert Access.authorize(Access.anonymous(), :admin) == :ok
    end
  end

  describe "token/2" do
    test "Bearer first, then the cookie, and nothing empty" do
      assert Access.token("Bearer abc", "malachi_token=def") == "abc"
      assert Access.token(nil, "theme=dark; malachi_token=def") == "def"
      assert Access.token("Basic xyz", "malachi_token=def") == "def"
      assert Access.token("Bearer ", nil) == nil
      assert Access.token(nil, "malachi_token=") == nil
      assert Access.token(nil, "theme=dark") == nil
      assert Access.token(nil, nil) == nil
    end
  end

  describe "resolve/3 and admit/3" do
    setup do
      Malachi.RateLimiter.reset_bucket("127.0.0.1", :dashboard_auth)
      :ok
    end

    test "no token is authentication_required" do
      assert Access.resolve(nil, "127.0.0.1", "") == {:error, :authentication_required}
      assert Access.admit(nil, :viewer, @request) == {:error, :authentication_required}
    end

    test "the role comes from the registry on every request, not from the session" do
      token = AccessHelper.account!("access_fresh", [:produce], nil)

      assert {:ok, %{username: "access_fresh", role: nil, permissions: [:produce], authenticated: true}} =
               Access.resolve(token, "127.0.0.1", "")

      :ok = Auth.set_role("access_fresh", :editor, "test")
      assert {:ok, %{role: :editor}} = Access.resolve(token, "127.0.0.1", "")
      assert {:ok, %{role: :editor}} = Access.admit(token, :editor, @request)

      :ok = Auth.set_role("access_fresh", nil, "test")
      assert Access.admit(token, :viewer, @request) == {:error, {:missing_role, :viewer, nil}}
    end

    test "a session whose user is gone from the registry is an invalid session" do
      token = AccessHelper.account!("access_gone", [], :viewer)
      # Delete from the store only, as a removal on another node leaves this node's session in place.
      :ok = UserStore.delete_user("access_gone")

      assert Access.resolve(token, "127.0.0.1", "") == {:error, :invalid_session}
    end

    test "an unknown token is refused and spends the address's login bucket" do
      Malachi.RateLimiter.reset_bucket("10.9.9.9", :dashboard_auth)
      assert {:error, _reason} = Access.resolve("not-a-session", "10.9.9.9", "")

      limit = Access.login_bucket_config().limit

      results = for _attempt <- 1..limit, do: Access.resolve("not-a-session", "10.9.9.9", "")
      assert {:error, :rate_limit_exceeded, _retry_after_ms} = List.last(results)
    end

    test "with authentication disabled every request is the anonymous admin" do
      Application.put_env(:malachi, :dashboard_auth_enabled, false)
      on_exit(fn -> Application.put_env(:malachi, :dashboard_auth_enabled, true) end)

      assert Access.admit(nil, :admin, @request) == {:ok, Access.anonymous()}
      assert %{username: nil, role: :admin, authenticated: false} = Access.anonymous()
    end
  end
end
