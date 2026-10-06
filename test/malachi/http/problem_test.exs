defmodule Malachi.HTTP.ProblemTest do
  use ExUnit.Case, async: true

  alias Malachi.HTTP.Problem

  doctest Problem

  describe "build/2" do
    test "every key builds an RFC 9457 body whose type is the key and status the key's status" do
      for {type, status} <- Problem.types() do
        {^status, headers, body} = Problem.build(type, %{"extra" => 1})
        assert {"Content-Type", "application/problem+json"} in headers
        assert Jason.decode!(body) == %{"type" => type, "status" => status, "extra" => 1}
      end
    end

    test "no body carries translated prose" do
      for {type, _status} <- Problem.types() do
        {_status, _headers, body} = Problem.build(type)
        decoded = Jason.decode!(body)
        refute Map.has_key?(decoded, "title")
        refute Map.has_key?(decoded, "detail")
      end
    end

    test "every key is a dotted translation key" do
      for {type, _status} <- Problem.types(), do: assert(type =~ ~r/\Aerrors\.[a-z]+\.[a-z_]+\z/)
    end

    test "a 401 asks for a Bearer token and a 429 says when to retry, rounded up to whole seconds" do
      {401, headers, _body} = Problem.build("errors.auth.unauthenticated")
      assert {"WWW-Authenticate", ~s(Bearer realm="Malachi")} in headers

      {429, headers, _body} = Problem.build("errors.http.rate_limited", %{"retry_after_ms" => 1_001})
      assert {"Retry-After", "2"} in headers

      {429, headers, _body} = Problem.build("errors.http.rate_limited", %{"retry_after_ms" => 0})
      assert {"Retry-After", "0"} in headers
    end

    test "an unknown key raises rather than answering with a made up status" do
      assert_raise KeyError, fn -> Problem.build("errors.nope.nope") end
    end
  end

  describe "from_error/1" do
    test "session and role refusals" do
      assert {401, "errors.auth.unauthenticated", %{}} = Problem.from_error(:authentication_required)
      assert {401, "errors.auth.invalid_credentials", _} = Problem.from_error(:invalid_credentials)
      assert {401, "errors.auth.session_expired", _} = Problem.from_error(:session_expired)
      assert {401, "errors.auth.session_rejected", _} = Problem.from_error(:session_hijack_attempt)
      assert {401, "errors.auth.session_invalid", _} = Problem.from_error(:invalid_session)
      assert {503, "errors.auth.unavailable", _} = Problem.from_error(:principal_unavailable)

      assert {403, "errors.auth.missing_role", %{"required_role" => "viewer", "role" => nil}} =
               Problem.from_error({:missing_role, :viewer, nil})
    end

    test "transport refusals" do
      assert {400, "errors.http.invalid_request", _} = Problem.from_error(:invalid_request)
      assert {404, "errors.http.not_found", _} = Problem.from_error(:not_found)
      assert {405, "errors.http.method_not_allowed", _} = Problem.from_error(:method_not_allowed)
      assert {431, "errors.http.header_fields_too_large", _} = Problem.from_error(:header_fields_too_large)
      assert {429, "errors.http.rate_limited", %{"retry_after_ms" => 5}} = Problem.from_error({:rate_limited, 5})
    end

    test "user and ACL writes" do
      assert {409, "errors.users.exists", _} = Problem.from_error(:user_exists)
      assert {404, "errors.users.not_found", _} = Problem.from_error(:user_not_found)
      assert {400, "errors.users.invalid_permissions", _} = Problem.from_error(:invalid_permissions)

      assert {400, "errors.users.invalid_role", %{"roles" => ["viewer", "editor", "admin"]}} =
               Problem.from_error(:invalid_role)

      assert {503, "errors.users.persist_failed", _} = Problem.from_error(:persist_failed)
      assert {400, "errors.acls.invalid_operation", _} = Problem.from_error(:invalid_operation)
      assert {400, "errors.acls.invalid", _} = Problem.from_error(:invalid_acl)
    end

    test "a machine version refusal is a 409 naming both versions" do
      assert {409, "errors.cluster.upgrade_pending", %{"introduced" => 5, "effective" => 4}} =
               Problem.from_error({:unsupported_command, {:set_role, 3}, 5, 4})

      assert {409, "errors.cluster.upgrade_pending", %{"introduced" => nil, "effective" => 4}} =
               Problem.from_error({:unknown_command, {:set_role, 3}, 4})
    end

    test "policy refusals keep their data in extension members" do
      assert {404, "errors.policies.no_such_policy", _} = Problem.from_error(:no_such_policy)
      assert {404, "errors.policies.no_such_topic", _} = Problem.from_error(:no_such_topic)
      assert {400, "errors.policies.invalid_policy_name", _} = Problem.from_error(:invalid_policy_name)
      assert {400, "errors.policies.invalid_topic", _} = Problem.from_error(:invalid_topic)
      assert {400, "errors.policies.invalid_policy", _} = Problem.from_error(:invalid_policy)
      assert {503, "errors.policies.timeout", _} = Problem.from_error(:timeout)

      assert {409, "errors.policies.policy_in_use", %{"topics" => ["a", "b"]}} =
               Problem.from_error({:policy_in_use, ["a", "b"]})

      assert {409, "errors.policies.unsupported_policy_field", %{"field" => "f", "introduced" => 4, "effective" => 3}} =
               Problem.from_error({:unsupported_policy_field, "f", 4, 3})

      for reason <- [:unknown_policy_field, :invalid_policy_field, :duplicate_policy_field] do
        assert {400, type, %{"field" => "f"}} = Problem.from_error({reason, "f"})
        assert type == "errors.policies.#{reason}"
      end
    end

    test "anything else is a 503 the same request may pass later" do
      assert {503, "errors.http.unavailable", %{}} = Problem.from_error({:bindings_unavailable, :nodedown})
      assert {503, "errors.http.unavailable", %{}} = Problem.from_error(:noproc)
    end

    test "for_error/1 builds what from_error/1 classifies" do
      assert {403, _headers, body} = Problem.for_error({:missing_role, :admin, :editor})
      assert %{"type" => "errors.auth.missing_role", "required_role" => "admin"} = Jason.decode!(body)
    end
  end
end
