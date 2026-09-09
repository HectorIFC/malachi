defmodule Malachi.RateLimiterTest do
  use ExUnit.Case, async: false
  alias Malachi.RateLimiter

  setup do
    # Temporarily enable rate limiting for these tests
    original_value = Application.get_env(:malachi, :rate_limit_enabled)
    Application.put_env(:malachi, :rate_limit_enabled, true)

    on_exit(fn ->
      Application.put_env(:malachi, :rate_limit_enabled, original_value)
    end)

    :ok
  end

  describe "token bucket algorithm" do
    test "allows requests within limit" do
      identifier = "test_user_#{:rand.uniform(1_000_000)}"
      config = %{limit: 10, window_ms: 60_000}

      # First 10 requests should succeed
      results =
        for _ <- 1..10 do
          RateLimiter.check_limit(identifier, :auth, config)
        end

      assert Enum.all?(results, &(&1 == :ok))
    end

    test "blocks requests exceeding limit" do
      identifier = "test_user_#{:rand.uniform(1_000_000)}"
      config = %{limit: 5, window_ms: 60_000}

      # Exhaust limit
      for _ <- 1..5 do
        RateLimiter.check_limit(identifier, :auth, config)
      end

      # Next request should be blocked
      result = RateLimiter.check_limit(identifier, :auth, config)
      assert {:error, :rate_limit_exceeded, retry_after_ms} = result
      assert retry_after_ms > 0
      assert retry_after_ms <= 60_000
    end

    test "different identifiers have independent buckets" do
      user1 = "user1_#{:rand.uniform(1_000_000)}"
      user2 = "user2_#{:rand.uniform(1_000_000)}"
      config = %{limit: 3, window_ms: 60_000}

      # Exhaust user1's limit
      for _ <- 1..3 do
        assert :ok = RateLimiter.check_limit(user1, :auth, config)
      end

      # user1 should be blocked
      assert {:error, :rate_limit_exceeded, _} = RateLimiter.check_limit(user1, :auth, config)

      # user2 should still have full quota
      assert :ok = RateLimiter.check_limit(user2, :auth, config)
      assert :ok = RateLimiter.check_limit(user2, :auth, config)
      assert :ok = RateLimiter.check_limit(user2, :auth, config)
    end

    test "different actions have independent buckets" do
      identifier = "test_user_#{:rand.uniform(1_000_000)}"
      auth_config = %{limit: 3, window_ms: 60_000}
      publish_config = %{limit: 3, window_ms: 1_000}

      # Exhaust auth limit
      for _ <- 1..3 do
        assert :ok = RateLimiter.check_limit(identifier, :auth, auth_config)
      end

      # Auth should be blocked
      assert {:error, :rate_limit_exceeded, _} = RateLimiter.check_limit(identifier, :auth, auth_config)

      # Publish should still work
      assert :ok = RateLimiter.check_limit(identifier, :publish, publish_config)
    end

    test "tokens refill over time" do
      identifier = "test_user_#{:rand.uniform(1_000_000)}"
      # Small window for faster test
      config = %{limit: 5, window_ms: 100}

      # Exhaust limit
      for _ <- 1..5 do
        RateLimiter.check_limit(identifier, :auth, config)
      end

      # Should be blocked
      assert {:error, :rate_limit_exceeded, _} = RateLimiter.check_limit(identifier, :auth, config)

      # Wait for window to pass
      Process.sleep(150)

      # Should be allowed again
      assert :ok = RateLimiter.check_limit(identifier, :auth, config)
    end

    test "partial refill allows partial requests" do
      identifier = "test_user_#{:rand.uniform(1_000_000)}"
      # 10 tokens per 1000ms = 1 token per 100ms
      config = %{limit: 10, window_ms: 1_000}

      # Exhaust limit
      for _ <- 1..10 do
        RateLimiter.check_limit(identifier, :auth, config)
      end

      # Should be blocked
      assert {:error, :rate_limit_exceeded, _} = RateLimiter.check_limit(identifier, :auth, config)

      # Wait for ~3 tokens to refill (300ms)
      Process.sleep(350)

      # Should get roughly 3 tokens back
      assert :ok = RateLimiter.check_limit(identifier, :auth, config)
      assert :ok = RateLimiter.check_limit(identifier, :auth, config)
      # Third might work due to timing
      _ = RateLimiter.check_limit(identifier, :auth, config)
    end
  end

  describe "reset_bucket/2" do
    test "resets token count for identifier" do
      identifier = "test_user_#{:rand.uniform(1_000_000)}"
      config = %{limit: 3, window_ms: 60_000}

      # Exhaust limit
      for _ <- 1..3 do
        RateLimiter.check_limit(identifier, :auth, config)
      end

      # Should be blocked
      assert {:error, :rate_limit_exceeded, _} = RateLimiter.check_limit(identifier, :auth, config)

      # Reset bucket
      RateLimiter.reset_bucket(identifier, :auth)

      # Should work again
      assert :ok = RateLimiter.check_limit(identifier, :auth, config)
    end

    test "resets blocked counter" do
      identifier = "test_user_#{:rand.uniform(1_000_000)}"
      config = %{limit: 2, window_ms: 60_000}

      # Exhaust and trigger blocks
      for _ <- 1..2 do
        RateLimiter.check_limit(identifier, :auth, config)
      end

      # Trigger 5 blocks
      for _ <- 1..5 do
        RateLimiter.check_limit(identifier, :auth, config)
      end

      # Reset should clear blocked counter
      RateLimiter.reset_bucket(identifier, :auth)

      # Verify bucket works again
      assert :ok = RateLimiter.check_limit(identifier, :auth, config)
    end
  end

  describe "get_top_blocked/2" do
    test "returns empty list when no blocks" do
      action = :"test_action_#{:rand.uniform(1_000_000)}"
      result = RateLimiter.get_top_blocked(action, 10)
      assert result == []
    end

    test "returns blocked identifiers sorted by count" do
      action = :"test_action_#{:rand.uniform(1_000_000)}"
      config = %{limit: 1, window_ms: 60_000}

      # Create blocks with different counts
      identifiers = [
        {"user1_#{:rand.uniform(1_000_000)}", 10},
        {"user2_#{:rand.uniform(1_000_000)}", 5},
        {"user3_#{:rand.uniform(1_000_000)}", 15}
      ]

      for {id, block_count} <- identifiers do
        # Exhaust limit
        RateLimiter.check_limit(id, action, config)
        # Trigger blocks
        for _ <- 1..block_count do
          RateLimiter.check_limit(id, action, config)
        end
      end

      results = RateLimiter.get_top_blocked(action, 10)

      # Should be sorted by count descending
      counts = Enum.map(results, fn {_id, count} -> count end)
      assert counts == Enum.sort(counts, :desc)

      # Top result should be user3 with ~15 blocks
      assert length(results) == 3
      {_top_user, top_count} = List.first(results)
      # Allow for minor timing differences
      assert top_count >= 14
    end

    test "respects limit parameter" do
      action = :"test_action_#{:rand.uniform(1_000_000)}"
      config = %{limit: 1, window_ms: 60_000}

      # Create 10 different blocked users
      for i <- 1..10 do
        id = "user#{i}_#{:rand.uniform(1_000_000)}"
        RateLimiter.check_limit(id, action, config)
        # Block once
        RateLimiter.check_limit(id, action, config)
      end

      # Request only top 5
      results = RateLimiter.get_top_blocked(action, 5)
      assert length(results) <= 5
    end
  end

  describe "get_stats/0" do
    test "returns statistics about buckets" do
      stats = RateLimiter.get_stats()

      assert is_map(stats)
      assert Map.has_key?(stats, :total_buckets)
      assert Map.has_key?(stats, :total_blocked_entries)
      assert is_integer(stats.total_buckets)
      assert is_integer(stats.total_blocked_entries)
    end

    test "bucket count increases with usage" do
      initial_stats = RateLimiter.get_stats()
      initial_count = initial_stats.total_buckets

      # Create new buckets
      for i <- 1..5 do
        identifier = "new_user_#{i}_#{:rand.uniform(1_000_000)}"
        RateLimiter.check_limit(identifier, :auth, %{limit: 10, window_ms: 60_000})
      end

      new_stats = RateLimiter.get_stats()
      assert new_stats.total_buckets >= initial_count
    end
  end

  describe "concurrent access" do
    @tag :concurrent
    test "handles concurrent requests correctly" do
      identifier = "concurrent_user_#{:rand.uniform(1_000_000)}"
      limit = 100
      config = %{limit: limit, window_ms: 60_000}

      # Spawn 200 concurrent requests (should allow first 100)
      tasks =
        for _ <- 1..200 do
          Task.async(fn ->
            RateLimiter.check_limit(identifier, :auth, config)
          end)
        end

      results = Task.await_many(tasks, 5_000)

      ok_count = Enum.count(results, &(&1 == :ok))

      error_count =
        Enum.count(results, fn
          {:error, :rate_limit_exceeded, _} -> true
          _ -> false
        end)

      # Should have approximately limit successful requests (allow timing jitter)
      # Token bucket refill happens between GenServer calls, causing slight variance
      assert ok_count >= limit - 2 and ok_count <= limit + 2
      assert error_count >= 200 - limit - 2 and error_count <= 200 - limit + 2
    end

    @tag :concurrent
    test "maintains separate buckets under concurrency" do
      config = %{limit: 50, window_ms: 60_000}

      # Create 10 users, each making 100 requests concurrently
      user_tasks =
        for user_num <- 1..10 do
          Task.async(fn ->
            identifier = "user#{user_num}_#{:rand.uniform(1_000_000)}"

            request_tasks =
              for _ <- 1..100 do
                Task.async(fn ->
                  RateLimiter.check_limit(identifier, :auth, config)
                end)
              end

            Task.await_many(request_tasks, 5_000)
          end)
        end

      all_results = Task.await_many(user_tasks, 10_000) |> List.flatten()

      # Each user should get approximately 50 successful requests
      # Total: 10 users × 50 = ~500 successful (allow timing variance)
      ok_count = Enum.count(all_results, &(&1 == :ok))
      assert ok_count >= 490 and ok_count <= 510
    end
  end

  describe "action_config/1" do
    setup do
      original =
        for key <- [:publish_rate_limit, :publish_rate_window_ms, :subscribe_rate_limit, :subscribe_rate_window_ms],
            into: %{},
            do: {key, Application.get_env(:malachi, key)}

      on_exit(fn ->
        for {key, value} <- original, do: Application.put_env(:malachi, key, value)
      end)

      :ok
    end

    test "reads back the configured limit and window" do
      Application.put_env(:malachi, :publish_rate_limit, 250)
      Application.put_env(:malachi, :publish_rate_window_ms, 500)

      assert %{limit: 250, window_ms: 500} = RateLimiter.action_config(:publish)
    end

    test "a limit of zero means no limit" do
      Application.put_env(:malachi, :publish_rate_limit, 0)
      Application.put_env(:malachi, :subscribe_rate_limit, 0)

      assert RateLimiter.action_config(:publish) == nil
      assert RateLimiter.action_config(:subscribe) == nil
    end

    test "an unset limit means no limit" do
      Application.delete_env(:malachi, :publish_rate_limit)
      Application.delete_env(:malachi, :subscribe_rate_limit)

      assert RateLimiter.action_config(:publish) == nil
      assert RateLimiter.action_config(:subscribe) == nil
    end

    test "a zero or missing window means no limit, even with a positive limit" do
      Application.put_env(:malachi, :publish_rate_limit, 100)
      Application.put_env(:malachi, :publish_rate_window_ms, 0)
      assert RateLimiter.action_config(:publish) == nil

      Application.delete_env(:malachi, :publish_rate_window_ms)
      assert RateLimiter.action_config(:publish) == nil
    end

    test "a negative or non-integer limit means no limit rather than a broken bucket" do
      Application.put_env(:malachi, :publish_rate_window_ms, 1_000)

      for bad <- [-1, "500", nil] do
        Application.put_env(:malachi, :publish_rate_limit, bad)
        assert RateLimiter.action_config(:publish) == nil, "expected #{inspect(bad)} to read as unlimited"
      end
    end

    test "publish and subscribe are read independently" do
      Application.put_env(:malachi, :publish_rate_limit, 10)
      Application.put_env(:malachi, :publish_rate_window_ms, 1_000)
      Application.put_env(:malachi, :subscribe_rate_limit, 0)

      assert %{limit: 10} = RateLimiter.action_config(:publish)
      assert RateLimiter.action_config(:subscribe) == nil
    end
  end

  describe "check_limit_in_caller/3" do
    test "admits exactly the limit and then blocks" do
      identifier = "caller_#{:rand.uniform(1_000_000)}"
      config = %{limit: 5, window_ms: 60_000}

      results = for _ <- 1..5, do: RateLimiter.check_limit_in_caller(identifier, :publish, config)
      assert Enum.all?(results, &(&1 == :ok))

      assert {:error, :rate_limit_exceeded, retry_after_ms} =
               RateLimiter.check_limit_in_caller(identifier, :publish, config)

      assert is_integer(retry_after_ms) and retry_after_ms >= 0
    end

    test "the whole quota is spendable whatever the limit does to the shard arithmetic" do
      # The quota is split across one shard per scheduler, so a limit that does not divide evenly (and one
      # smaller than the scheduler count) is where a rounding mistake would silently reject early. Nothing
      # here may admit fewer than the configured limit.
      for limit <- [1, 2, 3, 7, 8, 9, 50, 1000] do
        identifier = "exact_#{limit}_#{:rand.uniform(1_000_000)}"
        config = %{limit: limit, window_ms: 60_000}

        admitted =
          Enum.count(1..(limit * 2 + 32), fn _ ->
            RateLimiter.check_limit_in_caller(identifier, :publish, config) == :ok
          end)

        assert admitted == limit, "limit #{limit} admitted #{admitted}"
      end
    end

    test "retry_after is the time left in the current window" do
      identifier = "retry_#{:rand.uniform(1_000_000)}"
      window_ms = 30_000
      config = %{limit: 1, window_ms: window_ms}

      assert :ok = RateLimiter.check_limit_in_caller(identifier, :publish, config)

      assert {:error, :rate_limit_exceeded, retry_after_ms} =
               RateLimiter.check_limit_in_caller(identifier, :publish, config)

      assert retry_after_ms > 0 and retry_after_ms <= window_ms
    end

    test "the two doors count independently: different algorithms, different entries" do
      # check_limit/3 is a token bucket, check_limit_in_caller/3 a sharded fixed window. They deliberately
      # do not share state, so mixing them on one identifier is not a way to spend a quota twice as fast
      # in one direction or to be blocked early in the other.
      identifier = "doors_#{:rand.uniform(1_000_000)}"
      config = %{limit: 2, window_ms: 60_000}

      assert :ok = RateLimiter.check_limit(identifier, :publish, config)
      assert :ok = RateLimiter.check_limit(identifier, :publish, config)
      assert {:error, :rate_limit_exceeded, _} = RateLimiter.check_limit(identifier, :publish, config)

      # the hot-path door still has its own full quota
      assert :ok = RateLimiter.check_limit_in_caller(identifier, :publish, config)
      assert :ok = RateLimiter.check_limit_in_caller(identifier, :publish, config)
      assert {:error, :rate_limit_exceeded, _} = RateLimiter.check_limit_in_caller(identifier, :publish, config)
    end

    test "keeps separate buckets per identifier" do
      config = %{limit: 1, window_ms: 60_000}
      one = "user_one_#{:rand.uniform(1_000_000)}"
      two = "user_two_#{:rand.uniform(1_000_000)}"

      assert :ok = RateLimiter.check_limit_in_caller(one, :publish, config)
      assert {:error, :rate_limit_exceeded, _} = RateLimiter.check_limit_in_caller(one, :publish, config)

      assert :ok = RateLimiter.check_limit_in_caller(two, :publish, config)
    end

    test "feeds the blocked counter that the dashboard reads" do
      identifier = "blocked_#{:rand.uniform(1_000_000)}"
      config = %{limit: 1, window_ms: 60_000}

      RateLimiter.check_limit_in_caller(identifier, :publish, config)
      RateLimiter.check_limit_in_caller(identifier, :publish, config)

      assert {^identifier, 1} =
               RateLimiter.get_top_blocked(:publish, 100) |> Enum.find(&(elem(&1, 0) == identifier))
    end

    test "is bypassed when rate limiting is disabled" do
      Application.put_env(:malachi, :rate_limit_enabled, false)
      identifier = "disabled_#{:rand.uniform(1_000_000)}"
      config = %{limit: 0, window_ms: 1}

      assert :ok = RateLimiter.check_limit_in_caller(identifier, :publish, config)
      assert :ok = RateLimiter.check_limit_in_caller(identifier, :publish, config)
    end

    @tag :concurrent
    test "stays exact under concurrency: every token is claimed atomically" do
      # Sharding is what makes this door fast, and the reason it is still exact is that each token is
      # claimed by one atomic update_counter, on a shard whose caps sum to the configured limit. This is
      # the property that would break first if the sharding were reworked.
      limit = 50
      attempts = 400
      identifier = "race_#{:rand.uniform(1_000_000)}"
      config = %{limit: limit, window_ms: 60_000}

      admitted =
        1..attempts
        |> Enum.map(fn _ -> Task.async(fn -> RateLimiter.check_limit_in_caller(identifier, :publish, config) end) end)
        |> Task.await_many(30_000)
        |> Enum.count(&(&1 == :ok))

      assert admitted == limit, "#{attempts} concurrent callers admitted #{admitted}, expected #{limit}"
    end
  end

  describe "cleanup" do
    # The sharded window counters live under a NEW key every window, so without reaping they would grow
    # without bound: one entry per user per shard per window, forever. This is the only thing standing
    # between the hot path and an ETS table that never stops growing, and it runs on a timer nothing else
    # asserts on, so it is exercised here by hand.
    @table :malachi_rate_limits

    setup do
      keys = [:publish_rate_limit, :publish_rate_window_ms, :subscribe_rate_limit, :subscribe_rate_window_ms]
      original = for key <- keys, into: %{}, do: {key, Application.get_env(:malachi, key)}

      on_exit(fn ->
        for {key, value} <- original do
          if value == nil, do: Application.delete_env(:malachi, key), else: Application.put_env(:malachi, key, value)
        end
      end)

      :ok
    end

    defp run_cleanup do
      send(Process.whereis(RateLimiter), :cleanup)
      # the cleanup is a cast-like info message; a sync call flushes it
      _ = RateLimiter.get_stats()
      :ok
    end

    test "reaps stale sharded window counters and keeps live ones" do
      tag = :rand.uniform(1_000_000)
      hour_ms = 3_600_000
      now = System.system_time(:millisecond)

      stale = {"stale_#{tag}", :publish, now - hour_ms - 60_000, 0}
      live = {"live_#{tag}", :publish, now, 0}
      :ets.insert(@table, {stale, 5, 1_000})
      :ets.insert(@table, {live, 5, 1_000})

      run_cleanup()

      assert :ets.lookup(@table, stale) == []
      assert [{^live, 5, _window_ms}] = :ets.lookup(@table, live)
    end

    test "reaps stale token buckets and keeps live ones" do
      tag = :rand.uniform(1_000_000)
      now = System.monotonic_time(:millisecond)

      stale = {"stale_bucket_#{tag}", :auth}
      live = {"live_bucket_#{tag}", :auth}
      :ets.insert(@table, {stale, {5, now - 3_600_000 - 60_000, now}})
      :ets.insert(@table, {live, {5, now, now}})

      run_cleanup()

      assert :ets.lookup(@table, stale) == []
      assert [{^live, _}] = :ets.lookup(@table, live)
    end

    test "keeps a live counter whose window is wider than the token bucket's hour" do
      # A quota measured over more than an hour (a subscribe allowance per day, say) has windows that
      # outlive the flat hour a token bucket is reaped on. Aging those counters on that hour would delete
      # the CURRENT window's counters and hand the user its whole quota back mid-window. They must be
      # aged against the window they actually count.
      tag = :rand.uniform(1_000_000)
      window_ms = 7_200_000
      Application.put_env(:malachi, :subscribe_rate_limit, 2)
      Application.put_env(:malachi, :subscribe_rate_window_ms, window_ms)

      identifier = "live_wide_#{tag}"
      # 90 minutes into a 2-hour window: past the hour, but the window is still the current one.
      live_window_start = System.system_time(:millisecond) - 90 * 60_000
      key = {identifier, :subscribe, live_window_start, 0}
      :ets.insert(@table, {key, 9_999, window_ms})

      run_cleanup()

      assert :ets.lookup(@table, key) != [], "a live window's counter was reaped, refunding the quota"
    end

    test "reaps a rolled-over counter promptly rather than holding it for an hour" do
      # The other direction of the same rule. A 1-second publish window turns over 3600 times an hour,
      # and each turn leaves a counter per shard per user behind. Holding those for a flat hour is what
      # would make the hot path's sharding expensive in memory.
      tag = :rand.uniform(1_000_000)
      Application.put_env(:malachi, :publish_rate_limit, 100)
      Application.put_env(:malachi, :publish_rate_window_ms, 1_000)

      identifier = "rolled_#{tag}"
      # Ten seconds back: ten windows ago, long dead, but nowhere near an hour old.
      key = {identifier, :publish, System.system_time(:millisecond) - 10_000, 0}
      :ets.insert(@table, {key, 100, 1_000})

      run_cleanup()

      assert :ets.lookup(@table, key) == [], "a counter ten windows old was kept"
    end

    test "still reaps counters of an action that has since been switched off" do
      # With no config left there is no window to measure against, so these fall back to the flat hour
      # rather than leaking forever.
      tag = :rand.uniform(1_000_000)
      Application.put_env(:malachi, :publish_rate_limit, 0)

      identifier = "switched_off_#{tag}"
      key = {identifier, :publish, System.system_time(:millisecond) - 3_600_000 - 60_000, 0}
      :ets.insert(@table, {key, 5, 1_000})

      run_cleanup()

      assert :ets.lookup(@table, key) == []
    end

    test "never reaps blocked counters, whatever their age" do
      # These are the dashboard's evidence that a limit fired; losing them on a timer would turn a real
      # signal back into the silent zero this enforcement exists to fix.
      identifier = "blocked_survivor_#{:rand.uniform(1_000_000)}"
      key = {:blocked, identifier, :publish}
      :ets.insert(@table, {key, 42})

      run_cleanup()

      assert [{^key, 42}] = :ets.lookup(@table, key)
    end

    test "a live window survives cleanup, so a spent quota is not silently refunded" do
      identifier = "refund_#{:rand.uniform(1_000_000)}"
      config = %{limit: 1, window_ms: 60_000}

      assert :ok = RateLimiter.check_limit_in_caller(identifier, :publish, config)
      assert {:error, :rate_limit_exceeded, _} = RateLimiter.check_limit_in_caller(identifier, :publish, config)

      run_cleanup()

      assert {:error, :rate_limit_exceeded, _} = RateLimiter.check_limit_in_caller(identifier, :publish, config)
    end
  end

  describe "disabled rate limiting" do
    test "bypasses checks when disabled" do
      # Temporarily disable for this test
      Application.put_env(:malachi, :rate_limit_enabled, false)

      identifier = "any_user"
      # Would normally block everything
      config = %{limit: 0, window_ms: 1}

      # Should still succeed because rate limiting is disabled
      assert :ok = RateLimiter.check_limit(identifier, :auth, config)
      assert :ok = RateLimiter.check_limit(identifier, :auth, config)
      assert :ok = RateLimiter.check_limit(identifier, :auth, config)

      # Re-enable for other tests
      Application.put_env(:malachi, :rate_limit_enabled, true)
    end
  end
end
