defmodule Malachi.Auth.SessionClassifyTest do
  @moduledoc """
  `SessionManager.classify_session/3` answers what `validate_session/3` would decide, with none of its
  effects. The dashboard relies on that to charge a token that does not validate to the client address
  before validating it, so a replayed token cannot write more audit events than the address's budget.
  """
  use ExUnit.Case, async: false

  alias Malachi.Auth
  alias Malachi.Auth.SessionManager

  @sessions :malachi_sessions

  setup do
    keys = [:session_ip_binding, :session_ua_binding, :trusted_proxy_ranges]
    original = for key <- keys, into: %{}, do: {key, Application.fetch_env(:malachi, key)}
    Application.put_env(:malachi, :session_ip_binding, true)
    Application.put_env(:malachi, :session_ua_binding, true)
    Application.put_env(:malachi, :trusted_proxy_ranges, [])

    on_exit(fn ->
      for {key, value} <- original do
        case value do
          {:ok, v} -> Application.put_env(:malachi, key, v)
          :error -> Application.delete_env(:malachi, key)
        end
      end
    end)

    {:ok, token} =
      SessionManager.create_session("classify_#{System.unique_integer([:positive])}", [:admin], "10.0.0.1", "ua")

    on_exit(fn -> SessionManager.revoke_session(token) end)
    {:ok, token: token}
  end

  test "a matching live session is valid", %{token: token} do
    assert {:valid, %{user_agent: "ua"}} = SessionManager.classify_session(token, "10.0.0.1", "ua")
    assert Auth.session_valid?(token, "10.0.0.1", "ua")
  end

  test "a binding that does not match names the dimensions", %{token: token} do
    assert {:mismatch, _session, [:ip]} = SessionManager.classify_session(token, "10.0.0.2", "ua")
    assert {:mismatch, _session, [:ip, :user_agent]} = SessionManager.classify_session(token, "10.0.0.2", "other")
    refute Auth.session_valid?(token, "10.0.0.1", "other")
  end

  test "expiry is reported ahead of the binding, with the binding verdict kept", %{token: token} do
    expire(token)

    assert {:expired, _session, []} = SessionManager.classify_session(token, "10.0.0.1", "ua")
    assert {:expired, _session, [:ip]} = SessionManager.classify_session(token, "10.0.0.2", "ua")
    refute Auth.session_valid?(token, "10.0.0.1", "ua")
  end

  test "a token with no session is unknown" do
    assert SessionManager.classify_session("no_such_token", "10.0.0.1", "ua") == :unknown
    refute Auth.session_valid?("no_such_token", "10.0.0.1", "ua")
  end

  test "classifying changes nothing, where validating does", %{token: token} do
    expire(token)
    before = :ets.lookup(@sessions, token)
    audit_before = audit_count()

    for _ <- 1..5, do: SessionManager.classify_session(token, "10.0.0.2", "other")

    assert :ets.lookup(@sessions, token) == before
    assert audit_count() == audit_before

    # The same token through validate_session/3 does delete and audit, which is what classifying spares.
    assert {:error, :session_expired} = SessionManager.validate_session(token, "10.0.0.2", "other")
    assert :ets.lookup(@sessions, token) == []
    assert audit_count() > audit_before
  end

  test "validating a live session still records its activity", %{token: token} do
    [{^token, session}] = :ets.lookup(@sessions, token)
    :ets.insert(@sessions, {token, %{session | last_activity: 0}})

    assert {:ok, _session} = SessionManager.validate_session(token, "10.0.0.1", "ua")
    assert [{^token, %{last_activity: activity}}] = :ets.lookup(@sessions, token)
    assert activity > 0
  end

  defp expire(token) do
    [{^token, session}] = :ets.lookup(@sessions, token)
    :ets.insert(@sessions, {token, %{session | expires_at: System.system_time(:second) - 1}})
  end

  defp audit_count do
    :ok = Malachi.AuditLog.flush()
    length(Malachi.AuditLog.get_events(100_000))
  end
end
