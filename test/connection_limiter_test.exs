defmodule Malachi.ConnectionLimiterTest do
  use ExUnit.Case, async: false
  alias Malachi.ConnectionLimiter

  setup do
    # Temporarily enable connection limiting for these tests
    original_value = Application.get_env(:malachi, :connection_limit_enabled)
    Application.put_env(:malachi, :connection_limit_enabled, true)

    # The tracker is process-global (shared with the live server and other tests, whose killed pids clean
    # up asynchronously via :DOWN). Reset to a clean baseline so a test's counts are its own, otherwise
    # leaked connections make the global-limit assertions flaky.
    ConnectionLimiter.reset()

    on_exit(fn ->
      Application.put_env(:malachi, :connection_limit_enabled, original_value)
    end)

    :ok
  end

  describe "per-IP connection limits" do
    test "allows connections within per-IP limit" do
      ip = "192.168.1.#{:rand.uniform(255)}"
      max_per_ip = Application.get_env(:malachi, :max_connections_per_ip, 100)

      # Create processes up to limit
      pids =
        for _ <- 1..min(max_per_ip, 10) do
          spawn(fn -> Process.sleep(1000) end)
        end

      results =
        for pid <- pids do
          ConnectionLimiter.register_connection(pid, ip)
        end

      assert Enum.all?(results, &(&1 == :ok))

      # Cleanup
      Enum.each(pids, &Process.exit(&1, :kill))
    end

    test "blocks connections exceeding per-IP limit" do
      ip = "10.0.0.#{:rand.uniform(255)}"

      # Set a low limit for testing
      original_limit = Application.get_env(:malachi, :max_connections_per_ip)
      Application.put_env(:malachi, :max_connections_per_ip, 5)

      on_exit(fn ->
        Application.put_env(:malachi, :max_connections_per_ip, original_limit)
      end)

      # Create processes up to limit
      pids =
        for _ <- 1..5 do
          spawn(fn -> Process.sleep(2000) end)
        end

      # Register up to limit
      for pid <- pids do
        assert :ok = ConnectionLimiter.register_connection(pid, ip)
      end

      # Next connection should be blocked
      overflow_pid = spawn(fn -> Process.sleep(2000) end)
      result = ConnectionLimiter.register_connection(overflow_pid, ip)
      assert {:error, :connection_limit_exceeded} = result

      # Cleanup
      Enum.each(pids ++ [overflow_pid], &Process.exit(&1, :kill))
    end

    test "different IPs have independent limits" do
      ip1 = "192.168.1.#{:rand.uniform(255)}"
      ip2 = "192.168.2.#{:rand.uniform(255)}"

      # Set a low limit
      original_limit = Application.get_env(:malachi, :max_connections_per_ip)
      Application.put_env(:malachi, :max_connections_per_ip, 3)

      on_exit(fn ->
        Application.put_env(:malachi, :max_connections_per_ip, original_limit)
      end)

      # Exhaust ip1's limit
      pids_ip1 =
        for _ <- 1..3 do
          pid = spawn(fn -> Process.sleep(2000) end)
          assert :ok = ConnectionLimiter.register_connection(pid, ip1)
          pid
        end

      # ip1 should be blocked
      overflow_pid = spawn(fn -> Process.sleep(2000) end)

      assert {:error, :connection_limit_exceeded} =
               ConnectionLimiter.register_connection(overflow_pid, ip1)

      # ip2 should still work
      pids_ip2 =
        for _ <- 1..3 do
          pid = spawn(fn -> Process.sleep(2000) end)
          assert :ok = ConnectionLimiter.register_connection(pid, ip2)
          pid
        end

      # Cleanup
      Enum.each(pids_ip1 ++ pids_ip2 ++ [overflow_pid], &Process.exit(&1, :kill))
      Application.put_env(:malachi, :max_connections_per_ip, original_limit)
    end
  end

  describe "global connection limits" do
    test "blocks connections exceeding global limit" do
      # Set very low global limit
      original_global = Application.get_env(:malachi, :max_total_connections)
      original_per_ip = Application.get_env(:malachi, :max_connections_per_ip)

      Application.put_env(:malachi, :max_total_connections, 10)
      Application.put_env(:malachi, :max_connections_per_ip, 100)

      on_exit(fn ->
        Application.put_env(:malachi, :max_total_connections, original_global)
        Application.put_env(:malachi, :max_connections_per_ip, original_per_ip)
      end)

      # Create connections from different IPs
      pids =
        for i <- 1..10 do
          ip = "192.168.#{rem(i, 10)}.#{:rand.uniform(255)}"
          pid = spawn(fn -> Process.sleep(2000) end)
          assert :ok = ConnectionLimiter.register_connection(pid, ip)
          pid
        end

      # Next connection should hit global limit
      overflow_ip = "10.0.0.#{:rand.uniform(255)}"
      overflow_pid = spawn(fn -> Process.sleep(2000) end)
      result = ConnectionLimiter.register_connection(overflow_pid, overflow_ip)
      assert {:error, :global_limit_exceeded} = result

      # Cleanup
      Enum.each(pids ++ [overflow_pid], &Process.exit(&1, :kill))
    end
  end

  describe "process monitoring and cleanup" do
    test "automatically decrements counter when process dies" do
      ip = "192.168.100.#{:rand.uniform(255)}"

      # Create and register a process
      pid = spawn(fn -> Process.sleep(100) end)
      assert :ok = ConnectionLimiter.register_connection(pid, ip)

      # Verify count increased
      count_before = ConnectionLimiter.get_connection_count(ip)
      assert count_before >= 1

      # Wait for process to die
      Process.sleep(200)

      # Count should decrease
      count_after = ConnectionLimiter.get_connection_count(ip)
      assert count_after < count_before
    end

    test "cleans up when process is killed" do
      ip = "192.168.101.#{:rand.uniform(255)}"

      # Create long-lived process
      pid = spawn(fn -> Process.sleep(5000) end)
      assert :ok = ConnectionLimiter.register_connection(pid, ip)

      initial_count = ConnectionLimiter.get_connection_count(ip)
      assert initial_count >= 1

      # Kill process
      Process.exit(pid, :kill)
      # Give monitor time to trigger
      Process.sleep(50)

      # Count should decrease
      final_count = ConnectionLimiter.get_connection_count(ip)
      assert final_count < initial_count
    end

    test "handles multiple deaths correctly" do
      ip = "192.168.102.#{:rand.uniform(255)}"

      # Create multiple processes
      _pids =
        for _ <- 1..5 do
          pid = spawn(fn -> Process.sleep(100) end)
          assert :ok = ConnectionLimiter.register_connection(pid, ip)
          pid
        end

      initial_count = ConnectionLimiter.get_connection_count(ip)
      assert initial_count >= 5

      # Wait for all to die
      Process.sleep(200)

      # All should be cleaned up
      final_count = ConnectionLimiter.get_connection_count(ip)
      assert final_count < initial_count
    end
  end

  describe "unregister_connection/1" do
    test "manually unregisters connection" do
      ip = "192.168.103.#{:rand.uniform(255)}"

      pid = spawn(fn -> Process.sleep(5000) end)
      assert :ok = ConnectionLimiter.register_connection(pid, ip)

      count_before = ConnectionLimiter.get_connection_count(ip)
      assert count_before >= 1

      # Manual unregister
      assert :ok = ConnectionLimiter.unregister_connection(pid)

      count_after = ConnectionLimiter.get_connection_count(ip)
      assert count_after < count_before

      # Cleanup
      Process.exit(pid, :kill)
    end

    test "returns error for unknown pid" do
      unknown_pid = spawn(fn -> :ok end)
      # Let it finish
      Process.sleep(10)

      result = ConnectionLimiter.unregister_connection(unknown_pid)
      assert {:error, :not_found} = result
    end
  end

  describe "get_connection_count/1" do
    test "returns 0 for IP with no connections" do
      ip = "172.16.0.#{:rand.uniform(255)}"
      assert ConnectionLimiter.get_connection_count(ip) == 0
    end

    test "returns correct count for IP" do
      ip = "172.16.1.#{:rand.uniform(255)}"

      pids =
        for _ <- 1..3 do
          pid = spawn(fn -> Process.sleep(1000) end)
          ConnectionLimiter.register_connection(pid, ip)
          pid
        end

      count = ConnectionLimiter.get_connection_count(ip)
      assert count >= 3

      # Cleanup
      Enum.each(pids, &Process.exit(&1, :kill))
    end
  end

  describe "get_total_connections/0" do
    test "returns total connection count" do
      initial_total = ConnectionLimiter.get_total_connections()

      # Add some connections
      pids =
        for i <- 1..5 do
          ip = "10.#{rem(i, 255)}.0.#{:rand.uniform(255)}"
          pid = spawn(fn -> Process.sleep(1000) end)
          ConnectionLimiter.register_connection(pid, ip)
          pid
        end

      new_total = ConnectionLimiter.get_total_connections()
      assert new_total >= initial_total + 5

      # Cleanup
      Enum.each(pids, &Process.exit(&1, :kill))
    end
  end

  describe "get_stats/0" do
    test "returns connection statistics" do
      stats = ConnectionLimiter.get_stats()

      assert is_map(stats)
      assert Map.has_key?(stats, :total_connections)
      assert Map.has_key?(stats, :unique_ips)
      assert Map.has_key?(stats, :max_per_ip)
      assert Map.has_key?(stats, :max_total)

      assert is_integer(stats.total_connections)
      assert is_integer(stats.unique_ips)
      assert is_integer(stats.max_per_ip)
      assert is_integer(stats.max_total)
    end
  end

  describe "list_connections/0" do
    test "lists connections by IP" do
      connections = ConnectionLimiter.list_connections()
      assert is_map(connections)
    end

    test "includes active connections" do
      ip = "192.168.200.#{:rand.uniform(255)}"

      pid = spawn(fn -> Process.sleep(1000) end)
      ConnectionLimiter.register_connection(pid, ip)

      connections = ConnectionLimiter.list_connections()

      # Should have entry for our IP
      ip_count = Map.get(connections, ip, 0)
      assert ip_count >= 1

      # Cleanup
      Process.exit(pid, :kill)
    end
  end

  describe "disabled connection limiting" do
    test "bypasses checks when disabled" do
      # Temporarily disable for this test
      original = Application.get_env(:malachi, :connection_limit_enabled)
      Application.put_env(:malachi, :connection_limit_enabled, false)

      on_exit(fn ->
        Application.put_env(:malachi, :connection_limit_enabled, original)
      end)

      ip = "any.ip.address.123"
      pid = spawn(fn -> Process.sleep(100) end)

      # Should always succeed regardless of limits
      assert :ok = ConnectionLimiter.register_connection(pid, ip)

      # Cleanup
      Process.exit(pid, :kill)
    end
  end

  describe "concurrent registration" do
    @tag :concurrent
    test "handles concurrent registrations correctly" do
      ip = "192.168.50.#{:rand.uniform(255)}"

      # Set specific limit
      original_limit = Application.get_env(:malachi, :max_connections_per_ip)
      Application.put_env(:malachi, :max_connections_per_ip, 20)

      on_exit(fn ->
        Application.put_env(:malachi, :max_connections_per_ip, original_limit)
      end)

      # Spawn 50 concurrent registration attempts
      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            pid = spawn(fn -> Process.sleep(2000) end)
            result = ConnectionLimiter.register_connection(pid, ip)
            {pid, result}
          end)
        end

      results = Task.await_many(tasks, 5_000)

      ok_results = Enum.filter(results, fn {_pid, res} -> res == :ok end)

      error_results =
        Enum.filter(results, fn {_pid, res} ->
          match?({:error, :connection_limit_exceeded}, res)
        end)

      # Should have exactly 20 successful
      assert length(ok_results) == 20
      assert length(error_results) == 30

      # Cleanup
      Enum.each(ok_results, fn {pid, _} -> Process.exit(pid, :kill) end)
    end
  end
end
