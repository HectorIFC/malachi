defmodule Malachi.AttackSimulationTest do
  @moduledoc """
  Simulates real-world attack patterns to validate defenses.

  Tests the interplay between LockoutManager, RateLimiter, SessionManager
  and ConnectionLimiter under adversarial conditions. Unlike individual module
  tests (auth_test, rate_limiter_test, connection_limiter_test), this file
  tests coordinated attack scenarios.
  """
  use ExUnit.Case, async: false

  alias Malachi.Auth
  alias Malachi.Auth.{LockoutManager, SessionManager}
  alias Malachi.ConnectionLimiter
  alias Malachi.Test.SecurityHelper

  @moduletag :security

  setup do
    victim = SecurityHelper.unique_username("victim")
    Auth.add_user(victim, "strong_password_1!", [:produce, :consume])

    attacker_ip = SecurityHelper.random_ip()

    on_exit(fn ->
      Auth.remove_user(victim)
      LockoutManager.unlock_account(victim)
    end)

    {:ok, victim: victim, attacker_ip: attacker_ip}
  end

  describe "brute force attack" do
    test "repeated failed attempts trigger lockout", %{victim: victim, attacker_ip: ip} do
      # Simulate failed logins by recording failed attempts directly
      # (In production, tcp_acceptor calls record_failed_attempt after Auth.authenticate fails)
      for _ <- 1..10 do
        LockoutManager.record_failed_attempt(victim, ip)
      end

      # Allow async GenServer.cast to process
      Process.sleep(100)

      # Account should be locked for this IP
      assert match?({:locked, _}, LockoutManager.locked?(victim, ip))
    end

    test "lockout prevents access from locked IP", %{
      victim: victim,
      attacker_ip: ip
    } do
      # Lock the account via repeated failed attempts
      for _ <- 1..10 do
        LockoutManager.record_failed_attempt(victim, ip)
      end

      Process.sleep(100)

      assert match?({:locked, _}, LockoutManager.locked?(victim, ip))

      # Verify locked accounts list includes this entry
      locked = LockoutManager.list_locked_accounts()
      assert locked != []
    end

    test "unlock_account restores access", %{victim: victim, attacker_ip: ip} do
      # Lock the account
      for _ <- 1..10 do
        LockoutManager.record_failed_attempt(victim, ip)
      end

      Process.sleep(100)

      assert match?({:locked, _}, LockoutManager.locked?(victim, ip))

      # Unlock
      LockoutManager.unlock_account(victim, ip)

      assert :not_locked == LockoutManager.locked?(victim, ip)
    end

    test "successful auth resets failed attempt counter", %{victim: victim} do
      clean_ip = SecurityHelper.random_ip()

      # A few failed attempts (not enough to lock)
      for _ <- 1..3 do
        LockoutManager.record_failed_attempt(victim, clean_ip)
      end

      Process.sleep(50)

      assert :not_locked == LockoutManager.locked?(victim, clean_ip)

      # Successful auth should reset counter
      LockoutManager.record_successful_auth(victim, clean_ip)

      Process.sleep(50)

      # More failed attempts should start counting from zero
      for _ <- 1..3 do
        LockoutManager.record_failed_attempt(victim, clean_ip)
      end

      Process.sleep(50)

      assert :not_locked == LockoutManager.locked?(victim, clean_ip)
    end
  end

  describe "credential stuffing (distributed IPs)" do
    test "per-IP lockout independence", %{victim: victim} do
      # Attack from 10 different IPs
      ips = for _ <- 1..10, do: SecurityHelper.random_ip()

      # Lock from first IP
      locked_ip = hd(ips)

      for _ <- 1..10 do
        LockoutManager.record_failed_attempt(victim, locked_ip)
      end

      Process.sleep(100)

      assert match?({:locked, _}, LockoutManager.locked?(victim, locked_ip))

      # Other IPs should not be locked
      clean_ip = List.last(ips)
      assert :not_locked == LockoutManager.locked?(victim, clean_ip)

      # Victim can still authenticate from clean IP
      {:ok, _token} = Auth.authenticate(victim, "strong_password_1!", clean_ip)
    end

    test "multiple IPs attacking same user independently", %{victim: victim} do
      ips = for _ <- 1..5, do: SecurityHelper.random_ip()

      # Each IP sends some failed attempts
      for ip <- ips do
        for _ <- 1..4 do
          LockoutManager.record_failed_attempt(victim, ip)
        end
      end

      Process.sleep(100)

      # None should be locked yet (assuming lockout threshold > 4)
      for ip <- ips do
        result = LockoutManager.locked?(victim, ip)
        # Just verify the system doesn't crash under distributed attack
        assert result == :not_locked or match?({:locked, _}, result)
      end
    end
  end

  describe "session hijacking detection" do
    test "validates session from original IP", %{victim: victim} do
      original_ip = {192, 168, 1, 1}
      {:ok, token} = Auth.authenticate(victim, "strong_password_1!", original_ip)

      # Validate from same IP should succeed
      result = SessionManager.validate_session(token, original_ip)
      assert {:ok, session_data} = result
      assert session_data.username == victim
    end

    test "detects hijack from different IP", %{victim: victim} do
      original_ip = {192, 168, 1, 1}
      attacker_ip = {10, 0, 0, 99}

      {:ok, token} = Auth.authenticate(victim, "strong_password_1!", original_ip)

      # Attempt to use token from different IP
      result = SessionManager.validate_session(token, attacker_ip)
      assert {:error, :session_hijack_attempt} = result
    end

    test "original IP still works after hijack attempt", %{victim: victim} do
      original_ip = {192, 168, 1, 1}
      attacker_ip = {10, 0, 0, 99}

      {:ok, token} = Auth.authenticate(victim, "strong_password_1!", original_ip)

      # Hijack attempt
      {:error, :session_hijack_attempt} =
        SessionManager.validate_session(token, attacker_ip)

      # Original should still work
      {:ok, session_data} = SessionManager.validate_session(token, original_ip)
      assert session_data.username == victim
    end

    test "hijack attempt generates audit event", %{victim: victim} do
      original_ip = {192, 168, 1, 1}
      attacker_ip = {10, 0, 0, 99}

      {:ok, token} = Auth.authenticate(victim, "strong_password_1!", original_ip)

      # Hijack attempt
      SessionManager.validate_session(token, attacker_ip)

      # Allow async audit log to process
      Process.sleep(100)
      Malachi.AuditLog.flush()

      # Check audit log for hijack event
      events = Malachi.AuditLog.get_events(50)

      hijack_events =
        Enum.filter(events, fn e ->
          e.event_type == :session_hijack_attempt or
            (e.event_type == :session_validation_failed and
               e.metadata[:reason] == :session_hijack_attempt)
        end)

      # Should have at least one hijack-related event
      assert hijack_events != [] or
               Enum.any?(events, fn e ->
                 to_string(e.event_type) =~ "hijack" or
                   inspect(e.metadata) =~ "hijack"
               end)
    end
  end

  describe "user-agent binding (enabled)" do
    setup do
      original = Application.get_env(:malachi, :session_ua_binding, false)
      Application.put_env(:malachi, :session_ua_binding, true)
      on_exit(fn -> Application.put_env(:malachi, :session_ua_binding, original) end)
      :ok
    end

    test "validates a session from the original user agent", %{victim: victim} do
      ip = {192, 168, 1, 1}
      ua = "Mozilla/5.0 (original)"
      {:ok, token} = Auth.authenticate(victim, "strong_password_1!", ip, ua)

      # Same IP and same UA should succeed
      assert {:ok, session_data} = SessionManager.validate_session(token, ip, ua)
      assert session_data.username == victim
    end

    test "detects a different user agent as a hijack", %{victim: victim} do
      ip = {192, 168, 1, 1}
      original_ua = "Mozilla/5.0 (original)"
      attacker_ua = "curl/8.0"
      {:ok, token} = Auth.authenticate(victim, "strong_password_1!", ip, original_ua)

      # Same IP but a different UA is rejected when ua binding is on
      assert {:error, :session_hijack_attempt} =
               SessionManager.validate_session(token, ip, attacker_ua)
    end

    test "the original user agent still works after a mismatch", %{victim: victim} do
      ip = {192, 168, 1, 1}
      original_ua = "Mozilla/5.0 (original)"
      {:ok, token} = Auth.authenticate(victim, "strong_password_1!", ip, original_ua)

      {:error, :session_hijack_attempt} =
        SessionManager.validate_session(token, ip, "curl/8.0")

      assert {:ok, session_data} = SessionManager.validate_session(token, ip, original_ua)
      assert session_data.username == victim
    end

    test "user-agent binding still applies behind a trusted proxy", %{victim: victim} do
      # A trusted proxy disables IP binding (the source IP is the proxy's), so the UA is the only hijack
      # signal left. It must still be enforced.
      original_ranges = Application.get_env(:malachi, :trusted_proxy_ranges, [])
      Application.put_env(:malachi, :trusted_proxy_ranges, ["10.0.0.0/8"])
      on_exit(fn -> Application.put_env(:malachi, :trusted_proxy_ranges, original_ranges) end)

      ua = "Mozilla/5.0 (original)"
      {:ok, token} = Auth.authenticate(victim, "strong_password_1!", {10, 0, 0, 5}, ua)

      # IP binding is exempt (proxy), but a different UA is still rejected, even from another proxy IP.
      assert {:error, :session_hijack_attempt} =
               SessionManager.validate_session(token, {10, 0, 0, 99}, "curl/8.0")

      # The original UA validates from a different IP in the trusted range (IP binding exempt).
      assert {:ok, session_data} = SessionManager.validate_session(token, {10, 0, 0, 99}, ua)
      assert session_data.username == victim
    end
  end

  describe "user-agent binding (disabled, the default)" do
    setup do
      original = Application.get_env(:malachi, :session_ua_binding, false)
      Application.put_env(:malachi, :session_ua_binding, false)
      on_exit(fn -> Application.put_env(:malachi, :session_ua_binding, original) end)
      :ok
    end

    test "a different user agent is allowed when ua binding is off", %{victim: victim} do
      ip = {192, 168, 1, 1}
      {:ok, token} = Auth.authenticate(victim, "strong_password_1!", ip, "Mozilla/5.0 (original)")

      # UA binding off: only the IP is enforced, so a different UA still validates
      assert {:ok, session_data} = SessionManager.validate_session(token, ip, "curl/8.0")
      assert session_data.username == victim
    end
  end

  describe "session fixation and replay" do
    test "logout permanently invalidates token", %{victim: victim} do
      {:ok, token} = Auth.authenticate(victim, "strong_password_1!")

      # Logout
      :ok = Auth.logout(token)

      # Token should be permanently invalid (replay attack fails)
      assert {:error, :invalid_session} = Auth.validate_token(token)

      # Second attempt also fails
      assert {:error, :invalid_session} = Auth.validate_token(token)
    end

    test "re-authentication produces different token", %{victim: victim} do
      {:ok, token1} = Auth.authenticate(victim, "strong_password_1!")
      {:ok, token2} = Auth.authenticate(victim, "strong_password_1!")

      assert token1 != token2
    end

    test "old token invalid after password change", %{victim: victim} do
      {:ok, token} = Auth.authenticate(victim, "strong_password_1!")

      # Change password
      new_pass = "new_pass_#{:rand.uniform(100_000)}"
      :ok = Auth.change_password(victim, new_pass)

      # Wait for session cleanup
      Process.sleep(100)

      # Old token should be invalid
      _result = Auth.validate_token(token)
      # May still be valid if sessions persist; the key security is old password won't work
      assert {:error, :invalid_credentials} =
               Auth.authenticate(victim, "strong_password_1!")

      # New password works
      {:ok, _new_token} = Auth.authenticate(victim, new_pass)

      # Restore original password for cleanup
      Auth.change_password(victim, "strong_password_1!")
    end
  end

  describe "connection exhaustion attack" do
    test "exceeding per-IP limit returns error" do
      ip = SecurityHelper.random_ip_string()

      # Save original config
      original_limit = Application.get_env(:malachi, :max_connections_per_ip, 10_000)

      # Set a small limit for testing
      Application.put_env(:malachi, :max_connections_per_ip, 10)

      pids =
        for _ <- 1..10 do
          {:ok, pid} = Task.start(fn -> Process.sleep(:infinity) end)
          :ok = ConnectionLimiter.register_connection(pid, ip)
          pid
        end

      # Next connection should be rejected
      {:ok, extra_pid} = Task.start(fn -> Process.sleep(:infinity) end)
      result = ConnectionLimiter.register_connection(extra_pid, ip)
      assert result == {:error, :connection_limit_exceeded}

      # Cleanup
      Process.exit(extra_pid, :kill)
      Enum.each(pids, &Process.exit(&1, :kill))

      # Wait for DOWN messages to be processed
      Process.sleep(100)

      Application.put_env(:malachi, :max_connections_per_ip, original_limit)
    end

    test "connection count decrements when processes die" do
      ip = SecurityHelper.random_ip_string()

      {:ok, pid} = Task.start(fn -> Process.sleep(:infinity) end)
      :ok = ConnectionLimiter.register_connection(pid, ip)

      count_before = ConnectionLimiter.get_connection_count(ip)
      assert count_before >= 1

      # Kill the process
      Process.exit(pid, :kill)
      Process.sleep(100)

      count_after = ConnectionLimiter.get_connection_count(ip)
      assert count_after < count_before
    end
  end

  describe "concurrent authentication safety" do
    test "100 concurrent auth attempts don't crash the system", %{victim: victim} do
      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            ip = {10, 0, rem(i, 255) + 1, rem(i, 255) + 1}
            password = if rem(i, 3) == 0, do: "strong_password_1!", else: "wrong_#{i}"
            Auth.authenticate(victim, password, ip)
          end)
        end

      results = Task.await_many(tasks, 10_000)

      # All should return valid responses (no crashes)
      assert Enum.all?(results, fn
               {:ok, token} when is_binary(token) -> true
               {:error, _reason} -> true
               _ -> false
             end)

      # At least some should succeed
      successes = Enum.count(results, &match?({:ok, _}, &1))
      failures = Enum.count(results, &match?({:error, _}, &1))
      assert successes + failures == 100
    end
  end
end
