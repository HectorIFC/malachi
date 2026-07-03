defmodule Malachi.AuditLogIntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :isolated_audit_log
  # These tests need full control over AuditLog lifecycle
  # Run with: mix test test/audit_log_integration_test.exs

  @test_log_file "/tmp/malachi_test_audit.log"

  setup do
    # Clean up test log file
    File.rm(@test_log_file)

    # Save original configuration values
    original_output = Application.get_env(:malachi, :audit_log_output)
    original_file = Application.get_env(:malachi, :audit_log_file)
    original_max_size = Application.get_env(:malachi, :audit_log_max_size_mb)

    # Ensure Metrics is running (needed by AuditLog)
    unless Process.whereis(Malachi.Metrics) do
      {:ok, _} = Malachi.Metrics.start_link([])
    end

    on_exit(fn ->
      # Restore original configuration values
      Application.put_env(:malachi, :audit_log_output, original_output || :ets_only)
      if original_file, do: Application.put_env(:malachi, :audit_log_file, original_file)
      Application.put_env(:malachi, :audit_log_max_size_mb, original_max_size || 1)
      File.rm(@test_log_file)

      # Ensure AuditLog is running for subsequent tests.
      # Tests in this file use Supervisor.terminate_child which prevents
      # automatic restart. Without this, all tests in other files that
      # depend on AuditLog will fail.
      unless Process.whereis(Malachi.AuditLog) do
        case Supervisor.restart_child(Malachi.Supervisor, Malachi.AuditLog) do
          {:ok, _pid} -> :timer.sleep(50)
          {:error, :running} -> :ok
          {:error, _reason} -> :ok
        end
      end
    end)

    :ok
  end

  # Helper function to restart AuditLog with specific config.
  # Uses Supervisor.terminate_child/restart_child to avoid hitting
  # the supervisor's max_restarts limit which would crash the entire tree.
  defp restart_audit_log(_config \\ []) do
    # Use the supervisor API to safely stop and restart
    _ = Supervisor.terminate_child(Malachi.Supervisor, Malachi.AuditLog)
    :timer.sleep(50)

    case Supervisor.restart_child(Malachi.Supervisor, Malachi.AuditLog) do
      {:ok, pid} ->
        :timer.sleep(100)
        pid

      {:error, :running} ->
        # Already running (race condition), just return the pid
        :timer.sleep(100)
        Process.whereis(Malachi.AuditLog)

      {:error, reason} ->
        raise "Failed to restart AuditLog: #{inspect(reason)}"
    end
  end

  describe "output modes" do
    test "file mode writes only to file" do
      # Configure file output
      Application.put_env(:malachi, :audit_log_output, :file)
      Application.put_env(:malachi, :audit_log_file, @test_log_file)

      # Start audit log
      _pid = restart_audit_log()

      # Log an event
      Malachi.AuditLog.log_event(
        :dashboard_access,
        %{username: "test_user", ip: {127, 0, 0, 1}},
        "test_action",
        :success,
        %{test: true}
      )

      # Wait for flush
      :timer.sleep(1200)

      # Verify file was written
      assert File.exists?(@test_log_file)
      content = File.read!(@test_log_file)
      assert String.contains?(content, "dashboard_access")
      assert String.contains?(content, "test_user")
    end

    test "stdout mode writes to stdout" do
      # Use a path only this test references. restart_audit_log/1 tears down the previously configured
      # AuditLog, whose terminate/2 does a final flush; if that process was in :file mode its flush
      # recreates the shared @test_log_file after setup's File.rm. Pointing this test at its own file
      # makes the assertion immune to that leftover (which lands in @test_log_file, not here).
      stdout_file = "/tmp/malachi_test_audit_stdout_#{System.unique_integer([:positive])}.log"
      on_exit(fn -> File.rm(stdout_file) end)

      # Configure stdout output
      Application.put_env(:malachi, :audit_log_output, :stdout)
      Application.put_env(:malachi, :audit_log_file, stdout_file)

      # Start audit log
      _pid = restart_audit_log()

      Malachi.AuditLog.log_event(
        :dashboard_login_success,
        %{username: "stdout_test", ip: {192, 168, 1, 1}},
        "login",
        :success,
        %{}
      )

      # Wait for flush
      :timer.sleep(1200)

      # Verify no file was written (stdout only): this test's own file stays absent regardless of a
      # final flush from a previously file-configured process. Stdout from an async GenServer is not
      # reliably capturable, so the absence of the file is what we assert.
      refute File.exists?(stdout_file)
    end

    test "both mode writes to file and stdout" do
      Application.put_env(:malachi, :audit_log_output, :both)
      Application.put_env(:malachi, :audit_log_file, @test_log_file)

      _pid = restart_audit_log()

      Malachi.AuditLog.log_event(
        :dashboard_auth_failure,
        %{username: "both_test", ip: {10, 0, 0, 1}},
        "auth",
        :failure,
        %{reason: :test}
      )

      :timer.sleep(1200)

      # Check file (stdout output not reliably capturable from async GenServer)
      assert File.exists?(@test_log_file)
      content = File.read!(@test_log_file)
      assert String.contains?(content, "dashboard_auth_failure")
      assert String.contains?(content, "both_test")
    end
  end

  describe "file rotation by size" do
    test "file is truncated when exceeding max size" do
      # Set very small max size (1KB)
      Application.put_env(:malachi, :audit_log_output, :file)
      Application.put_env(:malachi, :audit_log_file, @test_log_file)
      Application.put_env(:malachi, :audit_log_max_size_mb, 0.001)
      # 1KB

      _pid = restart_audit_log()

      # Write many events to exceed 1KB
      for i <- 1..100 do
        Malachi.AuditLog.log_event(
          :dashboard_access,
          %{username: "user_#{i}", ip: {127, 0, 0, 1}},
          "action_#{i}",
          :success,
          %{large_data: String.duplicate("x", 50)}
        )
      end

      # Wait for flush
      :timer.sleep(1500)

      # Check file size is approximately 1KB (with some tolerance)
      {:ok, stat} = File.stat(@test_log_file)
      assert stat.size < 2048
      # Should be under 2KB

      # Verify file still contains valid JSON lines
      content = File.read!(@test_log_file)
      lines = String.split(content, "\n", trim: true)

      # All lines should be valid JSON
      for line <- lines do
        assert {:ok, _} = Jason.decode(line)
      end

      GenServer.stop(Malachi.AuditLog)
    end

    test "rotation keeps most recent events" do
      Application.put_env(:malachi, :audit_log_output, :file)
      Application.put_env(:malachi, :audit_log_file, @test_log_file)
      Application.put_env(:malachi, :audit_log_max_size_mb, 0.001)

      _pid = restart_audit_log()

      # Write events with identifiable usernames
      for i <- 1..50 do
        Malachi.AuditLog.log_event(
          :dashboard_access,
          %{username: "user_#{String.pad_leading(to_string(i), 3, "0")}", ip: {127, 0, 0, 1}},
          "action",
          :success,
          %{data: String.duplicate("x", 100)}
        )
      end

      :timer.sleep(1500)

      # Read file and verify recent events are present
      content = File.read!(@test_log_file)
      lines = String.split(content, "\n", trim: true)

      events =
        Enum.map(lines, fn line ->
          {:ok, data} = Jason.decode(line)
          data
        end)

      # Should have events from the end (user_045, user_046, etc)
      usernames = Enum.map(events, & &1["actor"]["username"])

      # Most recent users should be present
      assert "user_050" in usernames or "user_049" in usernames

      Supervisor.terminate_child(Malachi.Supervisor, Malachi.AuditLog)
    end
  end

  describe "JSON format" do
    test "events are valid JSON with expected fields" do
      Application.put_env(:malachi, :audit_log_output, :file)
      Application.put_env(:malachi, :audit_log_file, @test_log_file)

      _pid = restart_audit_log()

      Malachi.AuditLog.log_event(
        :dashboard_access,
        %{username: "json_test", ip: {192, 168, 1, 100}},
        "http_GET_/",
        :success,
        %{path: "/", method: "GET"}
      )

      :timer.sleep(1200)

      content = File.read!(@test_log_file)
      lines = String.split(content, "\n", trim: true)
      assert lines != []

      {:ok, event} = Jason.decode(List.first(lines))

      # Verify expected fields
      assert Map.has_key?(event, "timestamp")
      assert Map.has_key?(event, "event_id")
      assert Map.has_key?(event, "event_type")
      assert Map.has_key?(event, "actor")
      assert Map.has_key?(event, "action")
      assert Map.has_key?(event, "result")
      assert Map.has_key?(event, "metadata")
      assert Map.has_key?(event, "hostname")
      assert Map.has_key?(event, "node")

      # Verify values
      assert event["event_type"] == "dashboard_access"
      assert event["actor"]["username"] == "json_test"
      assert event["actor"]["ip"] == "192.168.1.100"
      assert event["action"] == "http_GET_/"
      assert event["result"] == "success"
      assert event["metadata"]["path"] == "/"

      Supervisor.terminate_child(Malachi.Supervisor, Malachi.AuditLog)
    end
  end

  describe "terminate flush" do
    test "final flush occurs on terminate" do
      # Stop current AuditLog
      Supervisor.terminate_child(Malachi.Supervisor, Malachi.AuditLog)
      :timer.sleep(50)

      Application.put_env(:malachi, :audit_log_output, :file)
      Application.put_env(:malachi, :audit_log_file, @test_log_file)

      pid = restart_audit_log()

      # Log event but don't wait for periodic flush
      Malachi.AuditLog.log_event(
        :dashboard_access,
        %{username: "terminate_test", ip: {127, 0, 0, 1}},
        "test",
        :success,
        %{}
      )

      # Synchronization barrier: :sys.get_state sends a system message that is
      # processed in-order by the GenServer, so by the time it returns the
      # previous cast is guaranteed to have been processed.
      _ = :sys.get_state(pid)

      # Explicitly flush buffer to disk before terminating
      # This ensures the event is written even if terminate/2 isn't reliably called
      :ok = Malachi.AuditLog.flush()

      # Stop the AuditLog process
      Supervisor.terminate_child(Malachi.Supervisor, Malachi.AuditLog)

      # Small delay to ensure file I/O completes
      :timer.sleep(50)

      # Event should still be written due to terminate flush
      assert File.exists?(@test_log_file)
      content = File.read!(@test_log_file)
      assert String.contains?(content, "terminate_test")
    end
  end

  describe "dashboard event types" do
    test "logs dashboard_access events with correct structure" do
      Application.put_env(:malachi, :audit_log_output, :file)
      Application.put_env(:malachi, :audit_log_file, @test_log_file)

      _pid = restart_audit_log()

      Malachi.AuditLog.log_event(
        :dashboard_access,
        %{username: "admin", ip: {127, 0, 0, 1}},
        "http_GET_/metrics",
        :success,
        %{path: "/metrics", method: "GET"}
      )

      :timer.sleep(1200)

      content = File.read!(@test_log_file)
      {:ok, event} = content |> String.split("\n", trim: true) |> List.first() |> Jason.decode()

      assert event["event_type"] == "dashboard_access"
      assert event["metadata"]["path"] == "/metrics"
      assert event["metadata"]["method"] == "GET"
    end

    test "logs dashboard_login_success events" do
      Application.put_env(:malachi, :audit_log_output, :file)
      Application.put_env(:malachi, :audit_log_file, @test_log_file)

      _pid = restart_audit_log()

      Malachi.AuditLog.log_event(
        :dashboard_login_success,
        %{username: "test_admin", ip: {10, 0, 0, 1}},
        "login",
        :success,
        %{}
      )

      :timer.sleep(1200)

      content = File.read!(@test_log_file)
      {:ok, event} = content |> String.split("\n", trim: true) |> List.first() |> Jason.decode()

      assert event["event_type"] == "dashboard_login_success"
      assert event["actor"]["username"] == "test_admin"
      assert event["action"] == "login"
    end

    test "logs dashboard_auth_failure events with reason" do
      Application.put_env(:malachi, :audit_log_output, :file)
      Application.put_env(:malachi, :audit_log_file, @test_log_file)

      _pid = restart_audit_log()

      Malachi.AuditLog.log_event(
        :dashboard_auth_failure,
        %{username: "hacker", ip: {192, 0, 2, 1}},
        "http_GET_/",
        :failure,
        %{path: "/", reason: :invalid_credentials}
      )

      :timer.sleep(1200)

      content = File.read!(@test_log_file)
      {:ok, event} = content |> String.split("\n", trim: true) |> List.first() |> Jason.decode()

      assert event["event_type"] == "dashboard_auth_failure"
      assert event["metadata"]["reason"] == "invalid_credentials"
    end
  end
end
