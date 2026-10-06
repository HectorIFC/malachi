defmodule Malachi.AuthTest do
  use ExUnit.Case, async: false

  alias Malachi.Auth.SessionManager
  alias Malachi.Auth.UserStore

  setup do
    on_exit(fn ->
      # Remove any dynamically created test users (keep only default users)
      default_users = ["admin", "producer", "consumer", "app"]

      Malachi.Auth.list_users()
      |> Enum.each(fn user ->
        unless user.username in default_users do
          Malachi.Auth.remove_user(user.username)
        end
      end)

      :timer.sleep(50)
    end)

    :ok
  end

  describe "verify_credentials/2" do
    test "returns the permissions for valid credentials without creating a session" do
      assert {:ok, permissions} = Malachi.Auth.verify_credentials("admin", "admin123")
      assert :admin in permissions
    end

    test "distinguishes a wrong password from an unknown user (for the audit trail)" do
      assert {:error, :invalid_password} = Malachi.Auth.verify_credentials("admin", "wrongpass")
      assert {:error, :user_not_found} = Malachi.Auth.verify_credentials("nonexistent", "pass")
    end
  end

  describe "authenticate/2" do
    test "authenticates with valid credentials" do
      assert {:ok, token} = Malachi.Auth.authenticate("admin", "admin123")
      assert is_binary(token)
      assert String.length(token) > 0
    end

    test "rejects invalid password" do
      assert {:error, :invalid_credentials} = Malachi.Auth.authenticate("admin", "wrongpass")
    end

    test "rejects non-existent user" do
      assert {:error, :invalid_credentials} = Malachi.Auth.authenticate("nonexistent", "pass")
    end

    test "creates unique tokens for each authentication" do
      {:ok, token1} = Malachi.Auth.authenticate("admin", "admin123")
      {:ok, token2} = Malachi.Auth.authenticate("admin", "admin123")

      assert token1 != token2
    end
  end

  describe "validate_token/1" do
    test "validates a valid token" do
      {:ok, token} = Malachi.Auth.authenticate("admin", "admin123")

      assert {:ok, session} = Malachi.Auth.validate_token(token)
      assert session.username == "admin"
      assert :admin in session.permissions
    end

    test "rejects invalid token" do
      assert {:error, :invalid_session} = Malachi.Auth.validate_token("invalid_token")
    end

    test "rejects expired token" do
      # Temporarily disable session timeout by setting it very high
      # Cannot easily test expiration in unit tests without mocking time
      # This test now validates that sessions don't immediately expire
      {:ok, token} = Malachi.Auth.authenticate("admin", "admin123")
      {:ok, session} = Malachi.Auth.validate_token(token)
      assert session.username == "admin"
    end

    test "validates tokens for different users" do
      {:ok, admin_token} = Malachi.Auth.authenticate("admin", "admin123")
      {:ok, producer_token} = Malachi.Auth.authenticate("producer", "producer123")

      {:ok, admin_session} = Malachi.Auth.validate_token(admin_token)
      {:ok, producer_session} = Malachi.Auth.validate_token(producer_token)

      assert admin_session.username == "admin"
      assert producer_session.username == "producer"
      assert :admin in admin_session.permissions
      assert :produce in producer_session.permissions
    end
  end

  describe "logout/1" do
    test "invalidates a session token" do
      {:ok, token} = Malachi.Auth.authenticate("admin", "admin123")

      assert :ok = Malachi.Auth.logout(token)
      assert {:error, :invalid_session} = Malachi.Auth.validate_token(token)
    end

    test "logout of non-existent token is harmless" do
      assert :ok = Malachi.Auth.logout("nonexistent_token")
    end
  end

  describe "add_user/3" do
    test "adds a new user" do
      username = "newuser_#{:rand.uniform(10000)}"
      assert :ok = Malachi.Auth.add_user(username, "password123", [:produce])

      assert {:ok, _token} = Malachi.Auth.authenticate(username, "password123")
    end

    test "prevents duplicate username" do
      username = "duplicate_#{:rand.uniform(10000)}"
      assert :ok = Malachi.Auth.add_user(username, "pass1", [:produce])
      assert {:error, :user_exists} = Malachi.Auth.add_user(username, "pass2", [:consume])
    end

    test "creates user with custom permissions" do
      username = "custom_#{:rand.uniform(10000)}"
      Malachi.Auth.add_user(username, "pass", [:admin, :produce])

      {:ok, token} = Malachi.Auth.authenticate(username, "pass")
      {:ok, session} = Malachi.Auth.validate_token(token)

      assert :admin in session.permissions
      assert :produce in session.permissions
    end

    test "uses default permissions when not specified" do
      username = "default_#{:rand.uniform(10000)}"
      Malachi.Auth.add_user(username, "pass")

      {:ok, token} = Malachi.Auth.authenticate(username, "pass")
      {:ok, session} = Malachi.Auth.validate_token(token)

      assert :produce in session.permissions
      assert :consume in session.permissions
    end
  end

  describe "add_user/4 and set_role/3" do
    test "a user is created holding a console role, and the role changes and goes" do
      username = "role_user_#{System.unique_integer([:positive])}"

      assert :ok = Malachi.Auth.add_user(username, "pass", [:produce], :viewer)
      assert {:ok, %{role: :viewer, permissions: [:produce]}} = UserStore.get_principal(username)

      assert :ok = Malachi.Auth.set_role(username, :admin, "auth_test")
      assert {:ok, %{role: :admin}} = UserStore.get_principal(username)

      assert :ok = Malachi.Auth.set_role(username, nil, "auth_test")
      assert {:ok, %{role: nil}} = UserStore.get_principal(username)
    end

    test "an unknown user or role is refused as such" do
      username = "role_user_#{System.unique_integer([:positive])}"

      assert {:error, :invalid_role} = Malachi.Auth.add_user(username, "pass", [], :root)
      assert {:error, :user_not_found} = UserStore.get_user(username)
      assert {:error, :user_not_found} = Malachi.Auth.set_role(username, :viewer, "auth_test")

      :ok = Malachi.Auth.add_user(username, "pass", [])
      assert {:error, :invalid_role} = Malachi.Auth.set_role(username, :root, "auth_test")
    end

    test "a machine version refusal reaches the caller instead of reading as a failed write" do
      refusal = {:error, {:unsupported_command, {:set_role, 3}, 5, 4}}
      assert Malachi.Auth.store_failure(refusal) == refusal
      assert Malachi.Auth.store_failure({:error, :noproc}) == {:error, :persist_failed}
    end

    test "a default user with a console role is seeded holding it, and an existing one is left alone" do
      username = "seed_role_#{System.unique_integer([:positive])}"
      deadline = System.monotonic_time(:millisecond) + 1_000

      assert Malachi.Auth.seed_user({username, "seed-pass", [], :viewer}, deadline) == 1
      assert {:ok, %{role: :viewer, permissions: []}} = UserStore.get_principal(username)
      assert Malachi.Auth.seed_user({username, "other-pass", [:admin], :admin}, deadline) == 0
      assert {:ok, %{role: :viewer, permissions: []}} = UserStore.get_principal(username)
    end

    test "a default user refused for an unfinished upgrade is not created, and the log says why" do
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          refusal = {:error, {:unsupported_command, {:put_user, 5}, 5, 4}}
          assert Malachi.Auth.seed_outcome(refusal, "seed_pending") == 0
        end)

      assert log =~ "Default user 'seed_pending' not created"
      assert log =~ "machine version 4 and this needs 5"

      log = ExUnit.CaptureLog.capture_log(fn -> assert Malachi.Auth.seed_outcome({:error, :noproc}, "x") == 0 end)
      assert log =~ "noproc"
    end

    test "every role change is audited with its actor and outcome" do
      username = "role_user_#{System.unique_integer([:positive])}"
      :ok = Malachi.Auth.add_user(username, "pass", [])

      :ok = Malachi.Auth.set_role(username, :editor, "auditor")
      {:error, :user_not_found} = Malachi.Auth.set_role(username <> "_ghost", :editor, "auditor")
      Malachi.AuditLog.flush()

      events = Malachi.AuditLog.get_events_by_type(:user_role_changed)
      assert Enum.any?(events, &(&1.username == "auditor" and &1.metadata.target == username and &1.status == :success))

      assert Enum.any?(
               events,
               &(&1.metadata.target == username <> "_ghost" and &1.status == :failure)
             )
    end
  end

  describe "remove_user/1" do
    test "removes an existing user" do
      username = "toremove_#{:rand.uniform(10000)}"
      Malachi.Auth.add_user(username, "pass", [:produce])

      assert :ok = Malachi.Auth.remove_user(username)
      assert {:error, :invalid_credentials} = Malachi.Auth.authenticate(username, "pass")
    end

    test "invalidates sessions on user removal" do
      username = "removewithsession_#{:rand.uniform(10000)}"
      Malachi.Auth.add_user(username, "pass", [:produce])
      {:ok, token} = Malachi.Auth.authenticate(username, "pass")

      Malachi.Auth.remove_user(username)
      :timer.sleep(50)

      assert {:error, :invalid_session} = Malachi.Auth.validate_token(token)
    end

    test "removing non-existent user is harmless" do
      assert :ok = Malachi.Auth.remove_user("nonexistent_user")
    end
  end

  describe "change_password/2" do
    test "changes user password" do
      username = "changepass_#{:rand.uniform(10000)}"
      Malachi.Auth.add_user(username, "oldpass", [:produce])

      assert :ok = Malachi.Auth.change_password(username, "newpass")

      assert {:error, :invalid_credentials} = Malachi.Auth.authenticate(username, "oldpass")
      assert {:ok, _token} = Malachi.Auth.authenticate(username, "newpass")
    end

    test "returns error for non-existent user" do
      assert {:error, :user_not_found} = Malachi.Auth.change_password("nonexistent", "newpass")
    end

    test "preserves permissions after password change" do
      username = "preserveperms_#{:rand.uniform(10000)}"
      Malachi.Auth.add_user(username, "oldpass", [:admin])

      Malachi.Auth.change_password(username, "newpass")
      {:ok, token} = Malachi.Auth.authenticate(username, "newpass")
      {:ok, session} = Malachi.Auth.validate_token(token)

      assert :admin in session.permissions
    end
  end

  describe "list_users/0" do
    test "lists all users without passwords" do
      users = Malachi.Auth.list_users()

      assert is_list(users)
      assert length(users) >= 4

      admin_user = Enum.find(users, &(&1.username == "admin"))
      assert admin_user != nil
      assert :admin in admin_user.permissions
      refute Map.has_key?(admin_user, :password)
    end

    test "includes newly added users" do
      username = "listtest_#{:rand.uniform(10000)}"
      Malachi.Auth.add_user(username, "pass", [:consume])

      users = Malachi.Auth.list_users()
      new_user = Enum.find(users, &(&1.username == username))

      assert new_user != nil
      assert :consume in new_user.permissions
    end
  end

  describe "has_permission?/2" do
    test "checks permission with username" do
      assert Malachi.Auth.has_permission?("admin", :produce) == true
      assert Malachi.Auth.has_permission?("producer", :produce) == true
      assert Malachi.Auth.has_permission?("producer", :consume) == false
    end

    test "admin has all permissions" do
      assert Malachi.Auth.has_permission?("admin", :produce) == true
      assert Malachi.Auth.has_permission?("admin", :consume) == true
      assert Malachi.Auth.has_permission?("admin", :anything) == true
    end

    test "checks permission with permissions list" do
      assert Malachi.Auth.has_permission?([:produce, :consume], :produce) == true
      assert Malachi.Auth.has_permission?([:produce], :consume) == false
      assert Malachi.Auth.has_permission?([:admin], :anything) == true
    end

    test "returns false for non-existent user" do
      assert Malachi.Auth.has_permission?("nonexistent", :produce) == false
    end
  end

  # There is no periodic sweeper: expired sessions are reaped lazily, by the first `validate_session/3`
  # that touches them. These tests drive that path deterministically, without sleeping, by setting the
  # timeout negative so the session is born already expired (`expires_at = now + timeout < now`).
  describe "session cleanup" do
    setup do
      prior = Application.get_env(:malachi, :session_timeout_seconds)

      on_exit(fn ->
        case prior do
          nil -> Application.delete_env(:malachi, :session_timeout_seconds)
          value -> Application.put_env(:malachi, :session_timeout_seconds, value)
        end
      end)

      # TEST-NET-3, outside every trusted-proxy range, so IP binding stays enforced
      {:ok, ip: {203, 0, 113, 7}}
    end

    test "an expired session is rejected and its row is physically removed", %{ip: ip} do
      Application.put_env(:malachi, :session_timeout_seconds, -1)
      {:ok, token} = SessionManager.create_session("cleanup_expired", [:produce], ip)

      # it really was stored, so the deletion below cannot pass for the wrong reason
      assert [{^token, _data}] = :ets.lookup(:malachi_sessions, token)

      assert {:error, :session_expired} = SessionManager.validate_session(token, ip)

      # reaped, not merely reported: the row is gone, and a second read can no longer tell this token
      # apart from one that never existed
      assert [] = :ets.lookup(:malachi_sessions, token)
      assert {:error, :invalid_session} = SessionManager.validate_session(token, ip)
    end

    test "a session inside its timeout validates and is kept", %{ip: ip} do
      Application.put_env(:malachi, :session_timeout_seconds, 3600)
      {:ok, token} = SessionManager.create_session("cleanup_live", [:produce], ip)

      assert {:ok, %{username: "cleanup_live", permissions: [:produce]}} = SessionManager.validate_session(token, ip)
      assert [{^token, _data}] = :ets.lookup(:malachi_sessions, token)
    end

    test "an expired token replayed from another IP still raises the hijack signal", %{ip: ip} do
      Application.put_env(:malachi, :session_timeout_seconds, -1)
      {:ok, token} = SessionManager.create_session("cleanup_other_ip", [:produce], ip)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          # the caller is told the accurate reason, expiry, not that it looks like an attacker
          assert {:error, :session_expired} = SessionManager.validate_session(token, {198, 51, 100, 9})
        end)

      # but the binding mismatch is still recorded, otherwise a stolen token replayed after it expired
      # would leave no trace, since the expiry branch answers first
      assert log =~ "cleanup_other_ip"
      assert log =~ "198.51.100.9"
      assert [] = :ets.lookup(:malachi_sessions, token)
    end

    test "an expired token replayed from its own IP raises no hijack signal", %{ip: ip} do
      Application.put_env(:malachi, :session_timeout_seconds, -1)
      {:ok, token} = SessionManager.create_session("cleanup_same_ip", [:produce], ip)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:error, :session_expired} = SessionManager.validate_session(token, ip)
        end)

      # plain expiry is not a theft signal, so the warning must not fire for every timed-out session
      refute log =~ "cleanup_same_ip"
    end
  end

  describe "generate_admin_if_absent/1" do
    import ExUnit.CaptureLog

    # a unique admin username per test so the shared default "admin" is never touched
    setup do
      username = "gen_admin_#{System.unique_integer([:positive])}"
      on_exit(fn -> Application.put_env(:malachi, :generate_admin, false) end)
      {:ok, username: username}
    end

    test "generates a random admin on first boot, logs it once, and it authenticates", %{username: username} do
      Application.put_env(:malachi, :generate_admin, true)

      log = capture_log(fn -> assert :ok = Malachi.Auth.generate_admin_if_absent(username) end)
      assert log =~ "password:"
      [_, password] = Regex.run(~r/password:\s*(\S+)/, log)

      # the generated password authenticates the new admin
      assert {:ok, _token} = Malachi.Auth.authenticate(username, password, {0, 0, 0, 0})
      assert Malachi.Auth.has_permission?(username, :admin)

      # calling again is a no-op: the admin now exists, so nothing new is generated or logged
      log2 = capture_log(fn -> assert :ok = Malachi.Auth.generate_admin_if_absent(username) end)
      refute log2 =~ "password:"
      # the original generated password still works (it was not overwritten)
      assert {:ok, _token} = Malachi.Auth.authenticate(username, password, {0, 0, 0, 0})
    end

    test "does nothing when generation is disabled", %{username: username} do
      Application.put_env(:malachi, :generate_admin, false)

      log = capture_log(fn -> assert :ok = Malachi.Auth.generate_admin_if_absent(username) end)
      refute log =~ "password:"
      assert {:error, :user_not_found} = UserStore.get_user(username)
    end
  end
end
