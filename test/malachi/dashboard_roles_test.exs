defmodule Malachi.DashboardRolesTest do
  @moduledoc """
  The legacy dashboard under the console roles (#228): a wire permission no longer reads the cluster's
  operational state, a viewer reads every read only route and is refused every mutation with the role it
  lacks, an admin is unchanged, and the console role is managed over HTTP.
  """

  use ExUnit.Case, async: false

  alias Malachi.Auth
  alias Malachi.Auth.UserStore
  alias Malachi.Test.AccessHelper

  setup do
    Malachi.RateLimiter.reset_bucket("127.0.0.1", :dashboard_auth)
    :ok
  end

  defp get(path, token), do: AccessHelper.call(:dashboard, "GET", path, token)

  describe "a wire permission is not a console role" do
    test "a :produce account is refused /metrics, /topic and /rate_limits" do
      token = AccessHelper.account!("roles_producer", [:produce], nil)

      for path <- ["/metrics", "/topic?name=orders", "/rate_limits"] do
        response = get(path, token)
        assert response.status == 403, path
        assert AccessHelper.header(response, "content-type") == "application/problem+json"

        assert response.json == %{
                 "type" => "errors.auth.missing_role",
                 "status" => 403,
                 "required_role" => "viewer",
                 "role" => nil
               }
      end
    end

    test "a Prometheus scrape with a :produce account is refused too" do
      token = AccessHelper.account!("roles_scraper", [:consume], nil)
      response = AccessHelper.call(:dashboard, "GET", "/metrics", token, headers: [{"Accept", "text/plain"}])
      assert response.status == 403
    end
  end

  describe "a viewer" do
    test "reads every read only route" do
      token = AccessHelper.account!("roles_viewer", [], :viewer)

      for path <- ["/", "/metrics", "/topic?name=roles_absent", "/rate_limits", "/policies"] do
        refute get(path, token).status in [401, 403], path
      end
    end

    test "is refused every mutation, with the role it lacks named" do
      token = AccessHelper.account!("roles_viewer_mut", [], :viewer)

      for {method, path, required} <- [
            {"PUT", "/policies/roles_p", "editor"},
            {"PUT", "/topics/roles_t/policy", "editor"},
            {"POST", "/users", "admin"},
            {"PUT", "/users/roles_viewer_mut/role", "admin"},
            {"DELETE", "/users/roles_viewer_mut", "admin"}
          ] do
        response = AccessHelper.call(:dashboard, method, path, token, body: %{"role" => "admin"})
        assert response.status == 403, "#{method} #{path}"
        assert response.json["required_role"] == required
      end

      # It did not promote itself.
      assert {:ok, %{role: :viewer}} = UserStore.get_principal("roles_viewer_mut")
    end
  end

  test "an editor writes storage policies but not users" do
    token = AccessHelper.account!("roles_editor", [], :editor)

    refute AccessHelper.call(:dashboard, "DELETE", "/policies/roles_absent_policy", token).status in [401, 403]
    assert AccessHelper.call(:dashboard, "GET", "/users", token).status == 403
  end

  test "an :admin account is unchanged: it reaches users, policies and reads" do
    token = AccessHelper.account!("roles_wire_admin", [:admin], nil)

    for path <- ["/", "/metrics", "/users", "/policies"], do: assert(get(path, token).status == 200, path)
  end

  describe "managing the console role over HTTP" do
    setup do
      %{admin: AccessHelper.account!("roles_manager", [], :admin)}
    end

    test "PUT /users/:u/role sets and removes it, and the change applies to the user's next request", %{admin: admin} do
      user = AccessHelper.account!("roles_target", [:produce], nil)
      assert get("/metrics", user).status == 403

      put = &AccessHelper.call(:dashboard, "PUT", "/users/roles_target/role", admin, body: %{"role" => &1})

      assert %{status: 200} = put.("viewer")
      assert get("/metrics", user).status == 200

      assert %{status: 200} = put.(nil)
      assert get("/metrics", user).status == 403
    end

    test "an unknown role is a 400 problem and creates no atom", %{admin: admin} do
      AccessHelper.account!("roles_target2", [], nil)
      unseen = "role_never_seen_#{System.unique_integer([:positive])}"

      response = AccessHelper.call(:dashboard, "PUT", "/users/roles_target2/role", admin, body: %{"role" => unseen})

      assert %{status: 400, json: %{"type" => "errors.users.invalid_role"}} = response
      assert_raise ArgumentError, fn -> String.to_existing_atom(unseen) end
    end

    test "an unknown user is a 404 problem, a body without a role a 400", %{admin: admin} do
      assert %{status: 404, json: %{"type" => "errors.users.not_found"}} =
               AccessHelper.call(:dashboard, "PUT", "/users/roles_nobody/role", admin, body: %{"role" => "viewer"})

      assert %{status: 400, json: %{"type" => "errors.http.invalid_request"}} =
               AccessHelper.call(:dashboard, "PUT", "/users/roles_nobody/role", admin, body: %{"other" => 1})
    end

    test "POST /users takes a role and GET /users lists it", %{admin: admin} do
      on_exit(fn -> Auth.remove_user("roles_created") end)

      body = %{"username" => "roles_created", "password" => "pw-roles-created", "permissions" => [], "role" => "editor"}
      assert %{status: 201} = AccessHelper.call(:dashboard, "POST", "/users", admin, body: body)

      %{json: %{"users" => users}} = get("/users", admin)
      assert %{"username" => "roles_created", "permissions" => [], "role" => "editor"} in users

      bad = %{body | "username" => "roles_bad", "role" => "root"}

      assert %{status: 400, json: %{"type" => "errors.users.invalid_role"}} =
               AccessHelper.call(:dashboard, "POST", "/users", admin, body: bad)
    end

    test "a role change is audited with who made it", %{admin: admin} do
      AccessHelper.account!("roles_audited", [], nil)
      AccessHelper.call(:dashboard, "PUT", "/users/roles_audited/role", admin, body: %{"role" => "viewer"})

      Malachi.AuditLog.flush()

      assert Enum.any?(Malachi.AuditLog.get_events(), fn event ->
               event.event_type == :user_role_changed and event.username == "roles_manager" and
                 event.metadata[:target] == "roles_audited"
             end)
    end
  end

  test "a session whose user was removed is answered 401, not served" do
    token = AccessHelper.account!("roles_removed", [], :viewer)
    :ok = UserStore.delete_user("roles_removed")

    assert %{status: 401, json: %{"type" => "errors.auth.session_invalid"}} = get("/metrics", token)
  end

  test "with authentication disabled every route is served" do
    Application.put_env(:malachi, :dashboard_auth_enabled, false)
    on_exit(fn -> Application.put_env(:malachi, :dashboard_auth_enabled, true) end)

    for path <- ["/metrics", "/users"], do: assert(get(path, nil).status == 200, path)
  end

  test "a refusal for a missing role is audited under the user and counted as an auth failure" do
    token = AccessHelper.account!("roles_refused_audit", [], :viewer)
    before = auth_failed_count()

    assert %{status: 403} = get("/users", token)
    Malachi.AuditLog.flush()

    assert Enum.any?(Malachi.AuditLog.get_events_by_type(:dashboard_auth_failure), fn event ->
             event.username == "roles_refused_audit" and event.metadata[:reason] == {:missing_role, :admin, :viewer}
           end)

    assert auth_failed_count() == before + 1
  end

  # Without its local replica of the user store a node cannot know anyone's role, so it answers 503, and a
  # browser keeps its cookie: the session is still good once the replica is back.
  test "a node whose user store replica is down answers 503 on both endpoints and logs no one out" do
    token = AccessHelper.account!("roles_store_down", [], :viewer)
    server_id = {UserStore.cluster_name(), node()}
    :ok = :ra.stop_server(:default, server_id)

    try do
      page = get("/", token)
      assert %{status: 503, json: %{"type" => "errors.auth.unavailable"}} = page
      assert AccessHelper.header(page, "set-cookie") == nil

      assert %{status: 503, json: %{"type" => "errors.auth.unavailable"}} =
               AccessHelper.call(:console, "GET", "/api/v1/me", token)
    after
      :ok = :ra.restart_server(:default, server_id)
    end

    # Once the replica is back the same session is served again.
    assert eventually(fn -> get("/metrics", token).status == 200 end)
  end

  defp auth_failed_count do
    case :ets.lookup(:malachi_metrics, :dashboard_auth_failed) do
      [{_key, count}] -> count
      [] -> 0
    end
  end

  defp eventually(fun, attempts \\ 50) do
    cond do
      fun.() -> true
      attempts == 0 -> false
      true -> Process.sleep(100) && eventually(fun, attempts - 1)
    end
  end
end
