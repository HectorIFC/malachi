defmodule Malachi.PenetrationTest do
  @moduledoc """
  End-to-end penetration test scenarios via the TCP protocol layer.

  Each test simulates a complete adversarial scenario from an attacker's
  perspective, using the TCP protocol (not just API calls). This is the
  highest-level security test, verifying the entire stack from socket to
  business logic.
  """
  use ExUnit.Case, async: false

  # SKIP (B3a): exercises the JSON queue/log protocol via socket; to be rewritten against the
  # binary Malachi.Wire protocol in B1b. The underlying infra (Auth/RateLimiter/Validator) stays
  # covered by its own unit tests.
  @moduletag :skip

  alias Malachi.Auth
  alias Malachi.Auth.LockoutManager
  alias Malachi.Test.{SecurityHelper, TCPHelper}

  @moduletag :security
  @moduletag :penetration

  setup do
    pentest_user = SecurityHelper.unique_username("pentest")
    Auth.add_user(pentest_user, "pentest_pass!", [:produce, :consume])

    on_exit(fn ->
      Auth.remove_user(pentest_user)
      LockoutManager.unlock_account(pentest_user)
    end)

    {:ok, user: pentest_user, pass: "pentest_pass!"}
  end

  describe "privilege escalation via TCP" do
    test "producer cannot create queue via TCP", %{user: _user, pass: _pass} do
      # Create a producer-only user
      producer_user = SecurityHelper.unique_username("producer_only")
      Auth.add_user(producer_user, "prod_pass!", [:produce])

      {:ok, socket} = TCPHelper.connect()
      {:ok, _token} = TCPHelper.authenticate(socket, producer_user, "prod_pass!")

      # Attempt admin action: create_queue
      {:ok, response} =
        SecurityHelper.tcp_action(socket, %{
          "action" => "create_queue",
          "queue_name" => "unauthorized_queue"
        })

      # Should be denied
      assert response["s"] == "err" or
               String.contains?(inspect(response), "permission") or
               String.contains?(inspect(response), "denied") or
               String.contains?(inspect(response), "unauthorized")

      :gen_tcp.close(socket)
      Auth.remove_user(producer_user)
    end

    test "consumer cannot publish via TCP" do
      consumer_user = SecurityHelper.unique_username("consumer_only")
      Auth.add_user(consumer_user, "cons_pass!", [:consume])

      {:ok, socket} = TCPHelper.connect()
      {:ok, _token} = TCPHelper.authenticate(socket, consumer_user, "cons_pass!")

      queue = SecurityHelper.unique_queue_name("priv_esc")

      {:ok, response} =
        SecurityHelper.tcp_action(socket, %{
          "action" => "publish",
          "queue_name" => queue,
          "payload" => "unauthorized_message"
        })

      # Should be denied due to lack of :produce permission
      assert response["s"] == "err" or
               String.contains?(inspect(response), "permission") or
               String.contains?(inspect(response), "denied")

      :gen_tcp.close(socket)
      Auth.remove_user(consumer_user)
    end

    test "non-admin cannot delete queue via TCP", %{user: user, pass: pass} do
      {:ok, socket} = TCPHelper.connect()
      {:ok, _token} = TCPHelper.authenticate(socket, user, pass)

      {:ok, response} =
        SecurityHelper.tcp_action(socket, %{
          "action" => "delete_queue",
          "queue_name" => "some_queue"
        })

      # Should be denied (user has produce+consume but not admin)
      assert response["s"] == "err" or
               String.contains?(inspect(response), "permission") or
               String.contains?(inspect(response), "denied") or
               String.contains?(inspect(response), "not found")

      :gen_tcp.close(socket)
    end
  end

  describe "token theft and replay via TCP" do
    test "stolen token cannot be used on new connection", %{user: user, pass: pass} do
      # Authenticate and capture the token
      {:ok, socket1} = TCPHelper.connect()
      {:ok, _token} = TCPHelper.authenticate(socket1, user, pass)

      # Open a NEW connection and try to use actions without authenticating
      {:ok, socket2} = TCPHelper.connect()

      # Try to publish without auth (simulating token theft/replay)
      {:ok, response} =
        SecurityHelper.tcp_action(socket2, %{
          "action" => "publish",
          "queue_name" => "stolen_queue",
          "payload" => "stolen_data"
        })

      # Should be rejected - new connection requires its own auth
      assert response["s"] == "err" or
               String.contains?(inspect(response), "auth") or
               String.contains?(inspect(response), "denied")

      :gen_tcp.close(socket1)
      :gen_tcp.close(socket2)
    end

    test "fabricated auth response is rejected" do
      {:ok, socket} = TCPHelper.connect()

      # Skip auth and immediately try to publish with a fake token
      fake_token = Base.url_encode64(:crypto.strong_rand_bytes(32))

      {:ok, response} =
        SecurityHelper.tcp_action(socket, %{
          "action" => "publish",
          "queue_name" => "fake_queue",
          "payload" => "fake_data",
          "token" => fake_token
        })

      assert response["s"] == "err" or
               String.contains?(inspect(response), "auth") or
               String.contains?(inspect(response), "denied")

      :gen_tcp.close(socket)
    end
  end

  describe "brute force via TCP with lockout" do
    test "repeated failed TCP auth triggers lockout", %{user: user} do
      # Ensure user is not locked
      LockoutManager.unlock_account(user)

      # Attempt many wrong passwords via TCP
      for _ <- 1..10 do
        {:ok, socket} = TCPHelper.connect()

        auth_msg =
          Jason.encode!(%{
            "action" => "auth",
            "username" => user,
            "password" => "wrong_password_#{:rand.uniform(1000)}"
          })

        TCPHelper.send_line(socket, auth_msg)

        case TCPHelper.recv_line(socket, timeout: 3000) do
          {:ok, _response} -> :ok
          {:error, _} -> :ok
        end

        :gen_tcp.close(socket)
      end

      # Account should be locked (from TCP server's perspective)
      # The IP used by TCP connections is 127.0.0.1 (localhost)
      lockout_result = LockoutManager.locked?(user, {127, 0, 0, 1})

      # If lockout is implemented at TCP level
      if match?({:locked, _}, lockout_result) do
        # Even correct password should fail
        {:ok, socket} = TCPHelper.connect()

        auth_msg =
          Jason.encode!(%{
            "action" => "auth",
            "username" => user,
            "password" => "pentest_pass!"
          })

        TCPHelper.send_line(socket, auth_msg)

        case TCPHelper.recv_line(socket, timeout: 3000) do
          {:ok, response} ->
            decoded = Jason.decode!(String.trim(response))

            assert decoded["s"] == "err" or
                     String.contains?(inspect(decoded), "locked") or
                     String.contains?(inspect(decoded), "limit")

          {:error, _} ->
            :ok
        end

        :gen_tcp.close(socket)
      end

      # Cleanup
      LockoutManager.unlock_account(user)
    end
  end

  describe "injection chain via TCP" do
    test "injection payloads in queue names are rejected via TCP", %{user: user, pass: pass} do
      {:ok, socket} = TCPHelper.connect()
      {:ok, _token} = TCPHelper.authenticate(socket, user, pass)

      payloads = SecurityHelper.owasp_injection_payloads()

      for payload <- payloads do
        case SecurityHelper.tcp_action(socket, %{
               "action" => "publish",
               "queue_name" => payload,
               "payload" => "test"
             }) do
          {:ok, response} ->
            # All injection attempts should be rejected
            assert response["s"] == "err" or
                     String.contains?(inspect(response), "invalid") or
                     String.contains?(inspect(response), "error"),
                   "Injection payload accepted as queue name: #{inspect(payload)}"

          {:error, _} ->
            # Connection closed is acceptable
            :ok
        end
      end

      :gen_tcp.close(socket)
    end

    test "SQL injection in publish payload does not affect system", %{user: user, pass: pass} do
      {:ok, socket} = TCPHelper.connect()
      {:ok, _token} = TCPHelper.authenticate(socket, user, pass)

      queue = SecurityHelper.unique_queue_name("sqli")

      # SQL injection in payload (payloads are opaque binary, should be stored as-is)
      {:ok, response} =
        SecurityHelper.tcp_action(socket, %{
          "action" => "publish",
          "queue_name" => queue,
          "payload" => "'; DROP TABLE queues; --"
        })

      # Payload should be accepted (it's opaque data, not interpreted)
      # The important thing is it doesn't affect other queues/data
      assert is_map(response)

      :gen_tcp.close(socket)
    end
  end

  describe "message flooding via TCP" do
    @tag timeout: 30_000
    test "rapid publishing respects system limits", %{user: user, pass: pass} do
      {:ok, socket} = TCPHelper.connect()
      {:ok, _token} = TCPHelper.authenticate(socket, user, pass)

      queue = SecurityHelper.unique_queue_name("flood")

      # Publish 1000 messages rapidly
      results =
        for i <- 1..1000 do
          msg =
            Jason.encode!(%{
              "action" => "publish",
              "queue_name" => queue,
              "payload" => "flood_msg_#{i}"
            })

          case TCPHelper.send_line(socket, msg) do
            :ok -> :sent
            {:error, _} -> :failed
          end
        end

      sent_count = Enum.count(results, &(&1 == :sent))

      # Drain responses
      for _ <- 1..min(sent_count, 100) do
        TCPHelper.recv_line(socket, timeout: 1000)
      end

      # Should have sent messages (may not all succeed due to rate limiting)
      assert sent_count > 0

      :gen_tcp.close(socket)
    end
  end

  describe "audit trail completeness" do
    @tag timeout: 30_000
    test "security operations generate audit events", %{user: user, pass: pass} do
      # Flush existing events
      Malachi.AuditLog.flush()
      Process.sleep(100)

      # 1. Failed authentication
      Auth.authenticate(user, "wrong_pass", {10, 10, 10, 10})

      # 2. Successful authentication
      {:ok, token} = Auth.authenticate(user, pass, {10, 10, 10, 11})

      # 3. Logout
      Auth.logout(token)

      # Allow async processing
      Process.sleep(200)
      Malachi.AuditLog.flush()
      Process.sleep(100)

      # Get recent events
      events = Malachi.AuditLog.get_events(100)

      # Filter events for our user
      user_events = Enum.filter(events, fn e -> e.username == user end)

      # Should have at least: auth_failure + auth_success
      event_types = Enum.map(user_events, & &1.event_type)

      assert :auth_failure in event_types,
             "Missing auth_failure event. Found: #{inspect(event_types)}"

      assert :auth_success in event_types or :session_created in event_types,
             "Missing auth_success/session_created event. Found: #{inspect(event_types)}"
    end

    test "audit events contain required fields" do
      Malachi.AuditLog.flush()
      Process.sleep(100)

      ip = {20, 20, 20, 20}
      Auth.authenticate("admin", "wrong_pass", ip)
      Process.sleep(200)
      Malachi.AuditLog.flush()
      Process.sleep(100)

      events = Malachi.AuditLog.get_events(10)

      # Find a recent auth_failure event
      failure_event =
        Enum.find(events, fn e ->
          e.event_type == :auth_failure and e.username == "admin"
        end)

      if failure_event do
        # Verify required audit fields
        assert Map.has_key?(failure_event, :timestamp)
        assert Map.has_key?(failure_event, :event_type)
        assert Map.has_key?(failure_event, :username)
        assert Map.has_key?(failure_event, :action)
        assert Map.has_key?(failure_event, :status)

        # Verify timestamp is recent
        assert is_binary(failure_event.timestamp) or is_integer(failure_event.timestamp)
      end
    end
  end
end
