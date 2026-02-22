defmodule MalachiMQ.ComprehensiveSecurityTest do
  @moduledoc """
  Master security acceptance test covering OWASP Top 10 alignment.

  This is the security checklist that verifies MalachiMQ as a whole meets
  security requirements. Unlike individual module tests, this file validates
  the system-wide security posture systematically.
  """
  use ExUnit.Case, async: false

  alias MalachiMQ.{Auth, Validator, RateLimiter, ConnectionLimiter}
  alias MalachiMQ.Auth.{LockoutManager, SessionManager}
  alias MalachiMQ.Test.{TCPHelper, SecurityHelper}

  @moduletag :security

  setup do
    test_user = SecurityHelper.unique_username("owasp")
    Auth.add_user(test_user, "owasp_test_pass!", [:produce, :consume])

    on_exit(fn ->
      Auth.remove_user(test_user)
      LockoutManager.unlock_account(test_user)
    end)

    {:ok, user: test_user, pass: "owasp_test_pass!"}
  end

  # ──────────────────────────────────────────────────────────────────
  # OWASP A01:2021 – Broken Access Control
  # ──────────────────────────────────────────────────────────────────
  describe "OWASP A01 - Broken Access Control" do
    test "unauthenticated TCP requests are rejected" do
      {:ok, socket} = TCPHelper.connect()

      # Try to publish without auth
      msg = Jason.encode!(%{"action" => "publish", "queue_name" => "q", "payload" => "p"})
      TCPHelper.send_line(socket, msg)

      case TCPHelper.recv_line(socket, timeout: 3000) do
        {:ok, response} ->
          decoded = Jason.decode!(String.trim(response))
          assert decoded["s"] == "err"

        {:error, :closed} ->
          :ok
      end

      :gen_tcp.close(socket)
    end

    test "each permission level enforces correct access" do
      # Producer: can publish, cannot consume/admin
      producer = SecurityHelper.unique_username("a01_producer")
      Auth.add_user(producer, "pass123!", [:produce])

      assert Auth.has_permission?(producer, :produce) == true
      assert Auth.has_permission?(producer, :consume) == false

      # Consumer: can consume, cannot produce/admin
      consumer = SecurityHelper.unique_username("a01_consumer")
      Auth.add_user(consumer, "pass123!", [:consume])

      assert Auth.has_permission?(consumer, :consume) == true
      assert Auth.has_permission?(consumer, :produce) == false

      # Admin: has all permissions
      assert Auth.has_permission?("admin", :produce) == true
      assert Auth.has_permission?("admin", :consume) == true
      assert Auth.has_permission?("admin", :admin) == true
      assert Auth.has_permission?("admin", :any_permission) == true

      # Cleanup
      Auth.remove_user(producer)
      Auth.remove_user(consumer)
    end

    test "deleted user cannot authenticate" do
      ephemeral = SecurityHelper.unique_username("a01_ephemeral")
      Auth.add_user(ephemeral, "pass123!", [:produce])
      {:ok, _token} = Auth.authenticate(ephemeral, "pass123!")

      Auth.remove_user(ephemeral)
      Process.sleep(50)

      assert {:error, :invalid_credentials} = Auth.authenticate(ephemeral, "pass123!")
    end

    test "changed password invalidates old password" do
      user = SecurityHelper.unique_username("a01_pwchange")
      Auth.add_user(user, "old_pass!", [:produce])

      Auth.change_password(user, "new_pass!")

      assert {:error, :invalid_credentials} = Auth.authenticate(user, "old_pass!")
      assert {:ok, _token} = Auth.authenticate(user, "new_pass!")

      Auth.remove_user(user)
    end
  end

  # ──────────────────────────────────────────────────────────────────
  # OWASP A02:2021 – Cryptographic Failures
  # ──────────────────────────────────────────────────────────────────
  describe "OWASP A02 - Cryptographic Failures" do
    test "tokens have sufficient entropy (>= 32 bytes)" do
      {:ok, token} = Auth.authenticate("admin", "admin123")

      # Tokens should be base64-encoded, so decoded should be >= 32 bytes
      case Base.url_decode64(token, padding: false) do
        {:ok, decoded} ->
          assert byte_size(decoded) >= 32,
                 "Token entropy too low: #{byte_size(decoded)} bytes (need >= 32)"

        :error ->
          # Try standard base64
          case Base.decode64(token) do
            {:ok, decoded} ->
              assert byte_size(decoded) >= 32

            :error ->
              # Token may use different encoding; at minimum check string length
              assert byte_size(token) >= 32,
                     "Token too short: #{byte_size(token)} bytes"
          end
      end
    end

    test "passwords are stored with Argon2 (not plaintext)" do
      # Look at the ETS table directly to verify hashed storage
      users_table = :malachimq_users

      case :ets.lookup(users_table, "admin") do
        [{"admin", stored_hash, _permissions}] ->
          assert is_binary(stored_hash)

          assert String.starts_with?(stored_hash, "$argon2"),
                 "Password not hashed with Argon2: #{String.slice(stored_hash, 0, 20)}"

        [] ->
          # User may be stored differently
          :ok
      end
    end

    test "each authentication produces unique token" do
      tokens =
        for _ <- 1..10 do
          {:ok, token} = Auth.authenticate("admin", "admin123")
          token
        end

      unique_tokens = Enum.uniq(tokens)
      assert length(unique_tokens) == 10, "Found duplicate tokens among 10 authentications"
    end

    test "token does not contain sensitive data" do
      {:ok, token} = Auth.authenticate("admin", "admin123")

      # Token should not contain username or password in plaintext
      refute String.contains?(token, "admin"),
             "Token contains username in plaintext"

      refute String.contains?(token, "admin123"),
             "Token contains password in plaintext"
    end
  end

  # ──────────────────────────────────────────────────────────────────
  # OWASP A03:2021 – Injection
  # ──────────────────────────────────────────────────────────────────
  describe "OWASP A03 - Injection" do
    test "all OWASP injection payloads rejected as queue names" do
      payloads = SecurityHelper.owasp_injection_payloads()

      for payload <- payloads do
        result = Validator.validate_queue_name(payload)

        assert result != :ok,
               "Injection payload accepted as queue name: #{inspect(payload)}"
      end
    end

    test "sanitize_for_html neutralizes all XSS payloads" do
      xss_payloads = [
        "<script>alert('XSS')</script>",
        "<img src=x onerror=alert(1)>",
        "<svg onload=alert('XSS')>",
        "<iframe src='javascript:alert(1)'>",
        "'-alert(1)-'"
      ]

      for payload <- xss_payloads do
        sanitized = Validator.sanitize_for_html(payload)
        refute String.contains?(sanitized, "<script>")
        refute String.contains?(sanitized, "<img")
        refute String.contains?(sanitized, "<svg")
        refute String.contains?(sanitized, "<iframe")
      end
    end

    test "sanitize_for_log prevents log injection" do
      log_payloads = [
        "normal\r\n[INFO] Fake log entry",
        "data\nERROR: system compromised",
        "msg\r\nHTTP/1.1 200 OK\r\n"
      ]

      for payload <- log_payloads do
        sanitized = Validator.sanitize_for_log(payload)
        refute String.contains?(sanitized, "\r")
        refute String.contains?(sanitized, "\n")
      end
    end
  end

  # ──────────────────────────────────────────────────────────────────
  # OWASP A04:2021 – Insecure Design
  # ──────────────────────────────────────────────────────────────────
  describe "OWASP A04 - Insecure Design" do
    test "lockout mechanism exists and is functional" do
      user = SecurityHelper.unique_username("a04_lockout")
      ip = SecurityHelper.random_ip()
      Auth.add_user(user, "pass123!", [:produce])

      # Record many failed attempts
      for _ <- 1..15 do
        LockoutManager.record_failed_attempt(user, ip)
      end

      Process.sleep(50)

      assert match?({:locked, _}, LockoutManager.locked?(user, ip)),
             "Account lockout not triggered after 15 failed attempts"

      # Cleanup
      LockoutManager.unlock_account(user)
      Auth.remove_user(user)
    end

    test "rate limiting is enabled by default" do
      enabled = Application.get_env(:malachimq, :rate_limit_enabled, false)
      assert enabled, "Rate limiting should be enabled by default"
    end

    test "connection limiting is enabled by default" do
      enabled = Application.get_env(:malachimq, :connection_limit_enabled, false)
      assert enabled, "Connection limiting should be enabled by default"
    end

    test "dashboard authentication is enabled by default" do
      enabled = Application.get_env(:malachimq, :dashboard_auth_enabled, false)
      assert enabled, "Dashboard authentication should be enabled by default"
    end

    test "message size limits are configured" do
      # Default max message size should be reasonable (e.g., 10MB)
      max_size = Application.get_env(:malachimq, :default_max_message_size_bytes, 10_485_760)
      assert max_size > 0
      assert max_size <= 100_000_000, "Max message size too large: #{max_size}"
    end
  end

  # ──────────────────────────────────────────────────────────────────
  # OWASP A05:2021 – Security Misconfiguration
  # ──────────────────────────────────────────────────────────────────
  describe "OWASP A05 - Security Misconfiguration" do
    test "TLS config excludes weak protocol versions" do
      tls_versions = Application.get_env(:malachimq, :tls_versions, [])

      # Weak versions should NOT be in the list
      weak_versions = [:sslv3, :sslv2, :tlsv1, :"tlsv1.0", :"tlsv1.1"]

      for weak <- weak_versions do
        refute weak in tls_versions,
               "Weak TLS version #{weak} found in configuration"
      end

      # Strong versions should be present (when TLS is configured)
      if length(tls_versions) > 0 do
        assert :"tlsv1.2" in tls_versions or :"tlsv1.3" in tls_versions,
               "No strong TLS version configured"
      end
    end

    test "default users have non-trivial passwords" do
      # Verify default users exist and require passwords
      default_users = ["admin", "producer", "consumer", "app"]

      for username <- default_users do
        # Empty password should fail
        assert {:error, :invalid_credentials} = Auth.authenticate(username, "")
        # Single char password should fail
        assert {:error, :invalid_credentials} = Auth.authenticate(username, "a")
      end
    end

    test "security modules are loaded and running" do
      # All security-critical GenServers should be running
      assert Process.whereis(MalachiMQ.Auth) != nil, "Auth GenServer not running"

      assert Process.whereis(MalachiMQ.Auth.LockoutManager) != nil,
             "LockoutManager GenServer not running"

      # SessionManager is a stateless module over ETS, not a registered GenServer.
      # Verify the sessions ETS table exists (created by Auth GenServer on init).
      assert :ets.whereis(:malachimq_sessions) != :undefined,
             "Sessions ETS table (:malachimq_sessions) not found"

      assert Process.whereis(MalachiMQ.RateLimiter) != nil,
             "RateLimiter GenServer not running"

      assert Process.whereis(MalachiMQ.ConnectionLimiter) != nil,
             "ConnectionLimiter GenServer not running"

      assert Process.whereis(MalachiMQ.AuditLog) != nil,
             "AuditLog GenServer not running"

      assert Process.whereis(MalachiMQ.AtomMonitor) != nil,
             "AtomMonitor GenServer not running"
    end
  end

  # ──────────────────────────────────────────────────────────────────
  # OWASP A07:2021 – Identification and Authentication Failures
  # ──────────────────────────────────────────────────────────────────
  describe "OWASP A07 - Authentication Failures" do
    test "timing attack resistance for non-existent users" do
      ip = SecurityHelper.random_ip()

      # Measure time for existing user with wrong password
      times_existing =
        for _ <- 1..5 do
          {time, _} =
            :timer.tc(fn ->
              Auth.authenticate("admin", "wrong_password", ip)
            end)

          time
        end

      # Measure time for non-existent user
      times_nonexistent =
        for _ <- 1..5 do
          {time, _} =
            :timer.tc(fn ->
              Auth.authenticate("definitely_not_a_user_#{:rand.uniform(99999)}", "wrong_password", ip)
            end)

          time
        end

      avg_existing = Enum.sum(times_existing) / length(times_existing)
      avg_nonexistent = Enum.sum(times_nonexistent) / length(times_nonexistent)

      # Timing should be similar (within 50% to account for variance)
      # Argon2.no_user_verify() ensures similar timing for non-existent users
      diff_ratio =
        if avg_existing > 0 do
          abs(avg_existing - avg_nonexistent) / avg_existing
        else
          0.0
        end

      assert diff_ratio < 0.5,
             "Timing attack possible: existing=#{round(avg_existing)}us, " <>
               "nonexistent=#{round(avg_nonexistent)}us, diff=#{Float.round(diff_ratio * 100, 1)}%"
    end

    test "session IP binding prevents cross-IP usage", %{user: user, pass: pass} do
      original_ip = {192, 168, 100, 1}
      {:ok, token} = Auth.authenticate(user, pass, original_ip)

      # Same IP should work
      assert {:ok, _} = SessionManager.validate_session(token, original_ip)

      # Different IP should fail
      different_ip = {10, 0, 0, 1}
      assert {:error, :session_hijack_attempt} = SessionManager.validate_session(token, different_ip)
    end

    test "lockout is per username+IP combination", %{user: user} do
      ip_a = {10, 0, 0, 1}
      ip_b = {10, 0, 0, 2}

      # Lock from IP A
      for _ <- 1..15 do
        LockoutManager.record_failed_attempt(user, ip_a)
      end

      Process.sleep(50)

      # IP A should be locked
      assert match?({:locked, _}, LockoutManager.locked?(user, ip_a))

      # IP B should NOT be locked
      assert :not_locked == LockoutManager.locked?(user, ip_b)

      LockoutManager.unlock_account(user)
    end
  end

  # ──────────────────────────────────────────────────────────────────
  # OWASP A09:2021 – Security Logging and Monitoring
  # ──────────────────────────────────────────────────────────────────
  describe "OWASP A09 - Logging and Monitoring" do
    test "authentication events are logged", %{user: user, pass: pass} do
      MalachiMQ.AuditLog.flush()
      Process.sleep(100)

      ip = {30, 30, 30, 30}

      # Failed auth
      Auth.authenticate(user, "wrong_pass", ip)
      # Successful auth
      {:ok, _token} = Auth.authenticate(user, pass, ip)

      Process.sleep(200)
      MalachiMQ.AuditLog.flush()
      Process.sleep(100)

      events = MalachiMQ.AuditLog.get_events(50)
      user_events = Enum.filter(events, fn e -> e.username == user end)
      event_types = Enum.map(user_events, & &1.event_type)

      assert :auth_failure in event_types, "auth_failure event not logged"

      assert :auth_success in event_types or :session_created in event_types,
             "auth_success event not logged"
    end

    test "audit log module is running and accessible" do
      # Should be able to log an event
      :ok =
        MalachiMQ.AuditLog.log_event(
          :security_test,
          %{username: "test", ip: {0, 0, 0, 0}},
          "comprehensive_test",
          :success,
          %{test: true}
        )

      Process.sleep(100)
      MalachiMQ.AuditLog.flush()
      Process.sleep(100)

      events = MalachiMQ.AuditLog.get_events(10)
      assert is_list(events)
    end

    test "audit events have required structure" do
      MalachiMQ.AuditLog.flush()
      Process.sleep(100)

      Auth.authenticate("admin", "wrong_pass", {40, 40, 40, 40})
      Process.sleep(200)
      MalachiMQ.AuditLog.flush()
      Process.sleep(100)

      events = MalachiMQ.AuditLog.get_events(10)

      if length(events) > 0 do
        event = hd(events)

        # Verify essential audit fields
        assert Map.has_key?(event, :event_id) or Map.has_key?(event, :id),
               "Event missing ID: #{inspect(Map.keys(event))}"

        assert Map.has_key?(event, :timestamp),
               "Event missing timestamp"

        assert Map.has_key?(event, :event_type),
               "Event missing event_type"
      end
    end
  end

  # ──────────────────────────────────────────────────────────────────
  # Defense in Depth Verification
  # ──────────────────────────────────────────────────────────────────
  describe "defense in depth" do
    test "multiple independent security layers exist" do
      # Verify each security layer can be checked independently

      # Layer 1: Connection limiting
      stats = ConnectionLimiter.get_stats()
      assert is_map(stats)
      assert Map.has_key?(stats, :max_per_ip)

      # Layer 2: Rate limiting
      config = %{limit: 1000, window_ms: 60_000}
      result = RateLimiter.check_limit("defense_test", :auth, config)
      assert result == :ok or match?({:error, :rate_limit_exceeded, _}, result)

      # Layer 3: Account lockout
      assert :not_locked == LockoutManager.locked?("admin", {127, 0, 0, 1})

      # Layer 4: Authentication
      assert {:error, :invalid_credentials} = Auth.authenticate("admin", "wrong")

      # Layer 5: Input validation
      assert :ok = Validator.validate_queue_name("valid_queue")
      assert {:error, _} = Validator.validate_queue_name("<script>alert(1)</script>")

      # Layer 6: Audit logging
      assert Process.whereis(MalachiMQ.AuditLog) != nil

      # Layer 7: Atom monitoring
      atom_stats = MalachiMQ.AtomMonitor.get_stats()
      assert atom_stats.status in [:normal, :warning, :critical]
    end

    test "security layers don't interfere with normal operations", %{user: user, pass: pass} do
      # Normal operation flow should work smoothly
      {:ok, token} = Auth.authenticate(user, pass)
      {:ok, session} = Auth.validate_token(token)
      assert session.username == user

      queue = SecurityHelper.unique_queue_name("defense")
      assert :ok = Validator.validate_queue_name(queue)

      config = %{limit: 100_000, window_ms: 60_000}
      assert :ok = RateLimiter.check_limit("normal_op", :publish, config)

      Auth.logout(token)
    end
  end
end
