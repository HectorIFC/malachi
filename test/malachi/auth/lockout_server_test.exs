defmodule Malachi.Auth.LockoutServerTest do
  # async: false — ra is global/stateful (one data dir, on-disk Raft logs).
  use ExUnit.Case, async: false

  alias Malachi.Auth.LockoutServer

  @config %{max_attempts: 5, base_duration_ms: 300_000, progressive: true}
  @key {"alice", "10.0.0.1"}

  defp start_cluster do
    name = :"lockouts_#{System.unique_integer([:positive])}"
    {:ok, server_id} = LockoutServer.start(name)
    on_exit(fn -> LockoutServer.delete(name) end)
    server_id
  end

  defp now, do: System.system_time(:millisecond)

  test "failed attempts replicate through the log and locked?/failed_attempts read the local replica" do
    server_id = start_cluster()

    assert {:ok, %{count: 1, locked: nil}} = LockoutServer.record_failed_attempt(server_id, @key, @config)
    assert {:ok, 1} = LockoutServer.failed_attempts(server_id, @key)
    assert {:ok, :not_locked} = LockoutServer.locked?(server_id, @key, now())
  end

  test "reaching the threshold locks the account cluster-wide" do
    server_id = start_cluster()

    reply =
      Enum.reduce(1..5, nil, fn _i, _acc ->
        {:ok, r} = LockoutServer.record_failed_attempt(server_id, @key, @config)
        r
      end)

    assert %{count: 5, locked: %{duration_ms: 300_000}} = reply
    assert {:ok, {:locked, remaining}} = LockoutServer.locked?(server_id, @key, now())
    assert remaining > 0 and remaining <= 300_000
  end

  test "record_successful_auth clears attempts and any lockout" do
    server_id = start_cluster()
    for _ <- 1..5, do: LockoutServer.record_failed_attempt(server_id, @key, @config)
    assert {:ok, {:locked, _}} = LockoutServer.locked?(server_id, @key, now())

    assert {:ok, :ok} = LockoutServer.record_successful_auth(server_id, @key)
    assert {:ok, :not_locked} = LockoutServer.locked?(server_id, @key, now())
    assert {:ok, 0} = LockoutServer.failed_attempts(server_id, @key)
  end

  test "unlock_user clears every IP for the user and reports the count cleared" do
    server_id = start_cluster()
    for _ <- 1..5, do: LockoutServer.record_failed_attempt(server_id, {"alice", "1.1.1.1"}, @config)
    for _ <- 1..5, do: LockoutServer.record_failed_attempt(server_id, {"alice", "2.2.2.2"}, @config)
    for _ <- 1..5, do: LockoutServer.record_failed_attempt(server_id, {"bob", "3.3.3.3"}, @config)

    assert {:ok, {:ok, 2}} = LockoutServer.unlock_user(server_id, "alice")
    assert {:ok, :not_locked} = LockoutServer.locked?(server_id, {"alice", "1.1.1.1"}, now())
    assert {:ok, :not_locked} = LockoutServer.locked?(server_id, {"alice", "2.2.2.2"}, now())
    # bob is untouched
    assert {:ok, {:locked, _}} = LockoutServer.locked?(server_id, {"bob", "3.3.3.3"}, now())
  end

  test "unlock_key clears a single key" do
    server_id = start_cluster()
    for _ <- 1..5, do: LockoutServer.record_failed_attempt(server_id, @key, @config)
    assert {:ok, {:locked, _}} = LockoutServer.locked?(server_id, @key, now())

    assert {:ok, :ok} = LockoutServer.unlock_key(server_id, @key)
    assert {:ok, :not_locked} = LockoutServer.locked?(server_id, @key, now())
  end

  test "list_locked reports accounts locked at now, with remaining time" do
    server_id = start_cluster()
    for _ <- 1..5, do: LockoutServer.record_failed_attempt(server_id, @key, @config)

    assert {:ok, [entry]} = LockoutServer.list_locked(server_id, now())
    assert %{username: "alice", ip: "10.0.0.1", attempt_count: 5, time_remaining_ms: remaining} = entry
    assert remaining > 0 and remaining <= @config.base_duration_ms
  end

  test "cleanup physically removes an expired lockout from state, not just from time-filtered views" do
    server_id = start_cluster()

    # A 1ms lockout: genuinely expired a moment after it is applied. `t0` is captured before the attempts,
    # so it precedes locked_until (= apply_time + 1) — an at-t0 read therefore sees the entry while stored.
    t0 = now()
    short = %{@config | base_duration_ms: 1}
    for _ <- 1..5, do: LockoutServer.record_failed_attempt(server_id, @key, short)

    # While the entry is in the map, reading at t0 (before locked_until) reports it locked.
    assert {:ok, {:locked, _}} = LockoutServer.locked?(server_id, @key, t0)

    # Let the 1ms lockout lapse, then compact (cleanup uses the leader clock, now well past locked_until).
    Process.sleep(5)
    assert {:ok, :ok} = LockoutServer.cleanup(server_id, 0)

    # The same at-t0 read now reports not_locked — only possible if cleanup removed the entry, since
    # time-filtering alone cannot change the result of a read at a fixed past `now`.
    assert {:ok, :not_locked} = LockoutServer.locked?(server_id, @key, t0)
    # cleanup(ttl: 0) also drops the failed-attempt counters older than the leader clock.
    assert {:ok, 0} = LockoutServer.failed_attempts(server_id, @key)
  end
end
