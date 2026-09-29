defmodule Malachi.DashboardPolicyRoutesTest do
  # The storage policy routes (#194) over real HTTP against the dashboard the application started.
  # async: false: users, the limiter, the policy store and the application env are node global.
  use ExUnit.Case, async: false

  alias Malachi.Auth
  alias Malachi.Cluster.PolicyStore
  alias Malachi.Dashboard
  alias Malachi.DataPlaneRouter
  alias Malachi.LogApi
  alias Malachi.RateLimiter
  alias Malachi.Test.DashboardHelper

  @admin "policy_route_admin"
  @producer "policy_route_producer"
  @password "policy_route_pass_123"

  setup do
    RateLimiter.reset_bucket("127.0.0.1", :dashboard_auth)

    original =
      for key <- [:dashboard_api_rate_limit, :dashboard_api_rate_window_ms],
          into: %{},
          do: {key, Application.fetch_env(:malachi, key)}

    for user <- [@admin, @producer], do: _ = Auth.remove_user(user)
    :ok = Auth.add_user(@admin, @password, [:admin])
    :ok = Auth.add_user(@producer, @password, [:produce])
    {:ok, admin} = Auth.authenticate(@admin, @password, {127, 0, 0, 1})
    {:ok, producer} = Auth.authenticate(@producer, @password, {127, 0, 0, 1})

    suffix = System.unique_integer([:positive])
    name = "route_policy_#{suffix}"
    topic = "route-topic-#{suffix}"
    :ok = LogApi.create_topic(DataPlaneRouter.shard_for(topic), topic)

    on_exit(fn ->
      PolicyStore.delete(name)
      for user <- [@admin, @producer], do: _ = Auth.remove_user(user)
      RateLimiter.reset_bucket("127.0.0.1", :dashboard_auth)

      for {key, value} <- original do
        case value do
          {:ok, v} -> Application.put_env(:malachi, key, v)
          :error -> Application.delete_env(:malachi, key)
        end
      end
    end)

    %{
      admin: admin,
      producer: producer,
      name: name,
      topic: topic,
      topic_path: "/topics/#{URI.encode_www_form(topic)}/policy"
    }
  end

  defp req(method, path, token, body \\ nil) do
    {:ok, socket} = DashboardHelper.connect()
    opts = if body, do: [body: Jason.encode!(body)], else: []
    {:ok, response} = DashboardHelper.authenticated_request(socket, method, path, token, opts)
    :gen_tcp.close(socket)
    {status_code(response), json_body(response)}
  end

  defp status_code(response) do
    [_, code] = Regex.run(~r"HTTP/1\.1 (\d{3})", response)
    String.to_integer(code)
  end

  defp json_body(response) do
    [_headers, body] = String.split(response, "\r\n\r\n", parts: 2)

    case Jason.decode(String.trim(body)) do
      {:ok, decoded} -> decoded
      {:error, _not_json} -> body
    end
  end

  test "an admin defines, lists, binds, reads back, and deletes, with in-use refused until forced", ctx do
    fields = %{"retention.max_bytes" => 0, "retention.max_age_ms" => nil}
    assert {200, %{"s" => "ok"}} = req(:PUT, "/policies/#{ctx.name}", ctx.admin, %{fields: fields})

    assert {200, %{"policies" => policies}} = req(:GET, "/policies", ctx.admin)
    assert %{"name" => ctx.name, "fields" => fields} in policies

    assert {200, _} = req(:PUT, ctx.topic_path, ctx.admin, %{policy: ctx.name})
    assert {200, read_back} = req(:GET, ctx.topic_path, ctx.admin)

    assert %{"topic" => topic, "policy" => policy, "resolution" => "resolved", "definition" => ^fields} = read_back
    assert {topic, policy} == {ctx.topic, ctx.name}
    assert %{"field" => "retention.max_bytes", "value" => 0, "origin" => "policy"} in read_back["effective"]
    assert %{"field" => "retention.max_age_ms", "value" => nil, "origin" => "policy"} in read_back["effective"]

    assert {409, %{"reason" => "policy_in_use: " <> in_use}} = req(:DELETE, "/policies/#{ctx.name}", ctx.admin)
    assert in_use == ctx.topic
    assert {200, _} = req(:DELETE, "/policies/#{ctx.name}?force=true", ctx.admin)

    assert {200, %{"resolution" => "unresolved"}} = req(:GET, ctx.topic_path, ctx.admin)
    assert {200, _} = req(:DELETE, ctx.topic_path, ctx.admin)
    assert {200, %{"policy" => nil, "resolution" => "none", "definition" => nil}} = req(:GET, ctx.topic_path, ctx.admin)
  end

  test "refusals are named and mapped to a status", ctx do
    assert {400, %{"reason" => "unknown_policy_field: retention.ms"}} =
             req(:PUT, "/policies/#{ctx.name}", ctx.admin, %{fields: %{"retention.ms" => 1}})

    assert {400, %{"reason" => "invalid_request"}} = req(:PUT, "/policies/#{ctx.name}", ctx.admin, %{nope: 1})
    assert {400, %{"reason" => "invalid_request"}} = req(:PUT, ctx.topic_path, ctx.admin, %{policy: 1})
    assert {404, %{"reason" => "no_such_policy"}} = req(:PUT, ctx.topic_path, ctx.admin, %{policy: ctx.name})

    assert {404, %{"reason" => "no_such_topic"}} =
             req(:GET, "/topics/ghost-#{System.unique_integer([:positive])}/policy", ctx.admin)

    assert {404, _} = req(:GET, "/topics/x/other", ctx.admin)
    assert {404, _} = req(:PUT, "/policies/", ctx.admin, %{fields: %{}})
  end

  test "a non-admin is forbidden on every policy route", ctx do
    for {method, path} <- [
          {:GET, "/policies"},
          {:PUT, "/policies/#{ctx.name}"},
          {:DELETE, "/policies/#{ctx.name}"},
          {:GET, ctx.topic_path},
          {:PUT, ctx.topic_path},
          {:DELETE, ctx.topic_path}
        ] do
      assert {403, _} = req(method, path, ctx.producer, %{fields: %{}}), "#{method} #{path}"
    end
  end

  test "the routes spend the session's own API budget (#223)", ctx do
    Application.put_env(:malachi, :dashboard_api_rate_limit, 2)
    Application.put_env(:malachi, :dashboard_api_rate_window_ms, 60_000)
    {:ok, fresh} = Auth.authenticate(@admin, @password, {127, 0, 0, 1})

    assert for(_ <- 1..3, do: elem(req(:GET, "/policies", fresh), 0)) == [200, 200, 429]
    # Another session keeps its own budget.
    assert {200, _} = req(:GET, "/policies", ctx.admin)
  end

  test "a policy store that cannot be read is a 503 on the list, never an empty list", ctx do
    server_id = {PolicyStore.cluster_name(), node()}
    :ok = :ra.stop_server(:default, server_id)

    try do
      assert {503, %{"s" => "err"}} = req(:GET, "/policies", ctx.admin)
    after
      :ok = :ra.restart_server(:default, server_id)
    end
  end

  test "with dashboard authentication off, a change is audited as the dashboard's", ctx do
    original = Application.fetch_env(:malachi, :dashboard_auth_enabled)
    Application.put_env(:malachi, :dashboard_auth_enabled, false)

    on_exit(fn ->
      case original do
        {:ok, value} -> Application.put_env(:malachi, :dashboard_auth_enabled, value)
        :error -> Application.delete_env(:malachi, :dashboard_auth_enabled)
      end
    end)

    assert {200, _} = req(:PUT, "/policies/#{ctx.name}", "unused", %{fields: %{}})
    _ = :sys.get_state(Malachi.AuditLog)

    assert Enum.any?(
             Malachi.AuditLog.get_events_by_user("dashboard"),
             &match?(%{event_type: :policy_defined, metadata: %{policy: name}} when name == ctx.name, &1)
           )
  end

  test "policy_status/1 maps every refusal" do
    assert Dashboard.policy_status(:no_such_topic) == "404 Not Found"
    assert Dashboard.policy_status({:policy_in_use, ["t"]}) == "409 Conflict"
    assert Dashboard.policy_status({:unsupported_command, {:bind_topic_policy, 3}, 4, 3}) == "409 Conflict"
    assert Dashboard.policy_status({:unsupported_policy_field, "x", 5, 4}) == "409 Conflict"
    assert Dashboard.policy_status(:invalid_topic) == "400 Bad Request"
    assert Dashboard.policy_status({:duplicate_policy_field, "x"}) == "400 Bad Request"
    assert Dashboard.policy_status(:migrating) == "503 Service Unavailable"
    assert Dashboard.policy_status(:timeout) == "503 Service Unavailable"
  end
end
