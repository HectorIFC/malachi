defmodule Malachi.Auth.LockoutRegistryTest do
  use ExUnit.Case, async: true

  alias Malachi.Auth.LockoutRegistry, as: Reg

  # Base config mirroring the production defaults (5 failures, 5-minute base, progressive on).
  @min 60_000
  @config %{max_attempts: 5, base_duration_ms: 5 * @min, progressive: true}
  @key {"alice", "10.0.0.1"}

  # Records `n` failed attempts for `key`, one ms apart starting at `now`, returning {state, last_reply}.
  defp fail_n(state, key, n, now, config \\ @config) do
    Enum.reduce(1..n, {state, nil}, fn i, {st, _reply} ->
      Reg.apply(st, {:failed_attempt, key, config}, now + i)
    end)
  end

  describe "failed_attempt, counting" do
    test "increments the counter below the threshold without locking" do
      {state, reply} = Reg.apply(Reg.new(), {:failed_attempt, @key, @config}, 1000)
      assert reply == %{count: 1, locked: nil}
      assert Reg.failed_attempts(state, @key) == 1
      assert Reg.locked?(state, @key, 2000) == :not_locked
    end

    test "stamps first_at once and moves last_at forward" do
      {state, _} = Reg.apply(Reg.new(), {:failed_attempt, @key, @config}, 1000)
      {state, _} = Reg.apply(state, {:failed_attempt, @key, @config}, 5000)
      assert %{first_at: 1000, last_at: 5000, count: 2} = state.attempts[@key]
    end

    test "attempts are tracked per {username, ip} independently" do
      {state, _} = Reg.apply(Reg.new(), {:failed_attempt, {"alice", "1.1.1.1"}, @config}, 1000)
      {state, _} = Reg.apply(state, {:failed_attempt, {"alice", "2.2.2.2"}, @config}, 1000)
      assert Reg.failed_attempts(state, {"alice", "1.1.1.1"}) == 1
      assert Reg.failed_attempts(state, {"alice", "2.2.2.2"}) == 1
    end
  end

  describe "failed_attempt, progressive lockout" do
    test "locks exactly at the threshold with the base duration" do
      {state, reply} = fail_n(Reg.new(), @key, 5, 1000)
      assert %{count: 5, locked: %{duration_ms: duration, locked_until: until}} = reply
      assert duration == 5 * @min
      # locked_until is now (last attempt at 1000+5) + duration
      assert until == 1005 + 5 * @min
      assert Reg.locked?(state, @key, 1005) == {:locked, 5 * @min}
    end

    test "escalates on each multiple of max_attempts: base -> x3 -> x9 -> x24 -> x72 (capped)" do
      escalate = fn count ->
        {_state, %{locked: %{duration_ms: d}}} =
          Reg.apply(%Reg{attempts: %{@key => %{count: count - 1, first_at: 0, last_at: 0}}}, {:failed_attempt, @key, @config}, 0)

        d
      end

      base = 5 * @min
      assert escalate.(5) == base
      assert escalate.(10) == base * 3
      assert escalate.(15) == base * 9
      assert escalate.(20) == base * 24
      assert escalate.(25) == base * 72
      # capped: further multiples stay at x72
      assert escalate.(50) == base * 72
    end

    test "every attempt at or past the threshold re-applies a lockout (matches legacy behavior)" do
      {state, reply6} = fail_n(Reg.new(), @key, 6, 1000)
      assert %{count: 6, locked: %{duration_ms: duration}} = reply6
      # count 6 -> div(6,5)=1 -> still base duration
      assert duration == 5 * @min
      assert match?({:locked, _}, Reg.locked?(state, @key, 1006))
    end

    test "progressive: false uses the base duration regardless of count" do
      config = %{@config | progressive: false}
      {_state, %{locked: %{duration_ms: d1}}} = fail_n(Reg.new(), @key, 5, 0, config)
      {_state, %{locked: %{duration_ms: d2}}} = fail_n(Reg.new(), @key, 15, 0, config)
      assert d1 == 5 * @min
      assert d2 == 5 * @min
    end
  end

  describe "locked?" do
    test "reports remaining time and expires exactly at locked_until" do
      {state, _} = fail_n(Reg.new(), @key, 5, 1000)
      until = state.lockouts[@key].locked_until
      assert {:locked, remaining} = Reg.locked?(state, @key, until - 1)
      assert remaining == 1
      # at and past locked_until the lock is gone
      assert Reg.locked?(state, @key, until) == :not_locked
      assert Reg.locked?(state, @key, until + 1) == :not_locked
    end

    test "an unknown key is never locked" do
      assert Reg.locked?(Reg.new(), {"ghost", "0.0.0.0"}, 999) == :not_locked
    end
  end

  describe "successful_auth" do
    test "clears both attempts and any lockout for the key" do
      {state, _} = fail_n(Reg.new(), @key, 5, 1000)
      assert match?({:locked, _}, Reg.locked?(state, @key, 1005))

      {state, reply} = Reg.apply(state, {:successful_auth, @key}, 2000)
      assert reply == :ok
      assert Reg.failed_attempts(state, @key) == 0
      assert Reg.locked?(state, @key, 2000) == :not_locked
    end

    test "only clears the matching key, not other IPs of the same user" do
      {state, _} = Reg.apply(Reg.new(), {:failed_attempt, {"alice", "1.1.1.1"}, @config}, 1000)
      {state, _} = Reg.apply(state, {:failed_attempt, {"alice", "2.2.2.2"}, @config}, 1000)
      {state, _} = Reg.apply(state, {:successful_auth, {"alice", "1.1.1.1"}}, 2000)
      assert Reg.failed_attempts(state, {"alice", "1.1.1.1"}) == 0
      assert Reg.failed_attempts(state, {"alice", "2.2.2.2"}) == 1
    end
  end

  describe "unlock_user / unlock_key" do
    test "unlock_user clears every IP for the user and reports the lockout count cleared" do
      {state, _} = fail_n(Reg.new(), {"alice", "1.1.1.1"}, 5, 1000)
      {state, _} = fail_n(state, {"alice", "2.2.2.2"}, 5, 1000)
      {state, _} = fail_n(state, {"bob", "3.3.3.3"}, 5, 1000)

      {state, reply} = Reg.apply(state, {:unlock_user, "alice"}, 2000)
      assert reply == {:ok, 2}
      assert Reg.locked?(state, {"alice", "1.1.1.1"}, 2000) == :not_locked
      assert Reg.locked?(state, {"alice", "2.2.2.2"}, 2000) == :not_locked
      assert Reg.failed_attempts(state, {"alice", "1.1.1.1"}) == 0
      # bob is untouched
      assert match?({:locked, _}, Reg.locked?(state, {"bob", "3.3.3.3"}, 2000))
    end

    test "unlock_user on an unlocked user reports zero cleared" do
      {state, reply} = Reg.apply(Reg.new(), {:unlock_user, "nobody"}, 1)
      assert reply == {:ok, 0}
      assert state == Reg.new()
    end

    test "unlock_key clears a single key" do
      {state, _} = fail_n(Reg.new(), @key, 5, 1000)
      {state, reply} = Reg.apply(state, {:unlock_key, @key}, 2000)
      assert reply == :ok
      assert Reg.locked?(state, @key, 2000) == :not_locked
    end
  end

  describe "cleanup" do
    test "removes expired lockouts but keeps still-active ones" do
      {state, _} = fail_n(Reg.new(), {"a", "1.1.1.1"}, 5, 0, %{@config | base_duration_ms: 100})
      {state, _} = fail_n(state, {"b", "2.2.2.2"}, 5, 0, %{@config | base_duration_ms: 10_000})

      # at now=1000: a (until ~105) expired, b (until ~10005) still active
      {state, reply} = Reg.apply(state, {:cleanup, 3_600_000}, 1000)
      assert reply == :ok
      refute Map.has_key?(state.lockouts, {"a", "1.1.1.1"})
      assert Map.has_key?(state.lockouts, {"b", "2.2.2.2"})
    end

    test "removes attempts older than the ttl, keeping recent ones" do
      {state, _} = Reg.apply(Reg.new(), {:failed_attempt, {"old", "1.1.1.1"}, @config}, 0)
      {state, _} = Reg.apply(state, {:failed_attempt, {"new", "2.2.2.2"}, @config}, 5_000_000)

      # ttl 1h; at now=5_000_000 the "old" attempt (last_at 0) is older than cutoff, "new" is not
      {state, :ok} = Reg.apply(state, {:cleanup, 3_600_000}, 5_000_000)
      assert Reg.failed_attempts(state, {"old", "1.1.1.1"}) == 0
      assert Reg.failed_attempts(state, {"new", "2.2.2.2"}) == 1
    end
  end

  describe "list_locked" do
    test "lists only accounts locked at `now`, with remaining time" do
      {state, _} = fail_n(Reg.new(), {"a", "1.1.1.1"}, 5, 0, %{@config | base_duration_ms: 100})
      {state, _} = fail_n(state, {"b", "2.2.2.2"}, 5, 0, %{@config | base_duration_ms: 10_000})

      locked = Reg.list_locked(state, 1000) |> Enum.sort_by(& &1.username)
      assert [%{username: "b", ip: "2.2.2.2", attempt_count: 5, time_remaining_ms: remaining}] = locked
      assert remaining > 0
    end

    test "empty when nothing is locked" do
      assert Reg.list_locked(Reg.new(), 1000) == []
    end
  end

  describe "replication safety" do
    test "the same command log at the same `now` yields the same state (deterministic)" do
      log = [
        {{:failed_attempt, {"a", "1.1.1.1"}, @config}, 1000},
        {{:failed_attempt, {"a", "1.1.1.1"}, @config}, 1001},
        {{:failed_attempt, {"b", "2.2.2.2"}, @config}, 1002},
        {{:successful_auth, {"a", "1.1.1.1"}}, 1003},
        {{:unlock_user, "b"}, 1004},
        {{:cleanup, 3_600_000}, 1005}
      ]

      replay = fn -> Enum.reduce(log, Reg.new(), fn {cmd, now}, st -> elem(Reg.apply(st, cmd, now), 0) end) end
      assert replay.() == replay.()
    end

    test "an unknown command is a no-op error, never a crash" do
      state = Reg.new()
      assert {^state, {:error, :unknown_command}} = Reg.apply(state, {:bogus, "x"}, 1)
    end
  end
end
