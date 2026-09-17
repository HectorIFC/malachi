defmodule Malachi.RateLimiterTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Malachi.Test.QuotaForensics, only: [within_one_window: 2]

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

  describe "get_stats/0 counts each entry shape" do
    @table :malachi_rate_limits

    test "counts blocked entries instead of reporting a confident zero" do
      # The match spec used to wrap its object pattern in one tuple too many. A wrong spec does not
      # raise, it simply never matches, so this reported 0 however many entries the table held: the same
      # silent zero the publish and subscribe counters were fixed to stop reporting.
      identifier = "stats_blocked_#{:rand.uniform(1_000_000)}"
      before = RateLimiter.get_stats().total_blocked_entries

      config = %{limit: 1, window_ms: 60_000}
      assert :ok = RateLimiter.check_limit(identifier, :auth, config)
      assert {:error, :rate_limit_exceeded, _} = RateLimiter.check_limit(identifier, :auth, config)

      assert [{_key, _count}] = :ets.lookup(@table, {:blocked, identifier, :auth})
      assert RateLimiter.get_stats().total_blocked_entries == before + 1
    end

    test "counts sharded window counters, which are their own entry shape" do
      identifier = "stats_window_#{:rand.uniform(1_000_000)}"
      before = RateLimiter.get_stats().total_window_counters

      assert :ok = RateLimiter.check_limit_in_caller(identifier, :publish, %{limit: 8, window_ms: 60_000})

      assert RateLimiter.get_stats().total_window_counters > before
    end

    test "keeps the three shapes apart rather than double counting" do
      # A bucket must not be counted as a window counter, nor either as a blocked entry. Each head has to
      # match exactly one shape, which is the property a copy-pasted spec quietly breaks.
      tag = :rand.uniform(1_000_000)
      stats = RateLimiter.get_stats()

      assert :ok = RateLimiter.check_limit("bucket_#{tag}", :auth, %{limit: 5, window_ms: 60_000})
      after_bucket = RateLimiter.get_stats()

      assert after_bucket.total_buckets == stats.total_buckets + 1
      assert after_bucket.total_window_counters == stats.total_window_counters
      assert after_bucket.total_blocked_entries == stats.total_blocked_entries
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
      config = %{limit: 5, window_ms: 60_000}

      {results, blocked} =
        within_one_window(config.window_ms, fn ->
          identifier = "caller_#{System.unique_integer([:positive])}"
          results = for _ <- 1..5, do: RateLimiter.check_limit_in_caller(identifier, :publish, config)
          {results, RateLimiter.check_limit_in_caller(identifier, :publish, config)}
        end)

      assert Enum.all?(results, &(&1 == :ok))
      assert {:error, :rate_limit_exceeded, retry_after_ms} = blocked
      assert is_integer(retry_after_ms) and retry_after_ms >= 0
    end

    test "the whole quota is spendable whatever the limit does to the shard arithmetic" do
      # The quota is split across one shard per scheduler, so a limit that does not divide evenly (and one
      # smaller than the scheduler count) is where a rounding mistake would silently reject early. Nothing
      # here may admit fewer than the configured limit.
      for limit <- [1, 2, 3, 7, 8, 9, 50, 1000] do
        window_ms = 60_000

        admitted =
          within_one_window(window_ms, fn ->
            # a fresh identifier per attempt, so a retry never inherits a half-spent quota
            identifier = "exact_#{limit}_#{:rand.uniform(1_000_000)}"
            config = %{limit: limit, window_ms: window_ms}

            Enum.count(1..(limit * 2 + 32), fn _ ->
              RateLimiter.check_limit_in_caller(identifier, :publish, config) == :ok
            end)
          end)

        assert admitted == limit, "limit #{limit} admitted #{admitted}"
      end
    end

    test "retry_after is the time left in the current window" do
      window_ms = 30_000
      config = %{limit: 1, window_ms: window_ms}

      {first, second} =
        within_one_window(window_ms, fn ->
          identifier = "retry_#{System.unique_integer([:positive])}"

          {RateLimiter.check_limit_in_caller(identifier, :publish, config),
           RateLimiter.check_limit_in_caller(identifier, :publish, config)}
        end)

      assert :ok = first
      assert {:error, :rate_limit_exceeded, retry_after_ms} = second
      assert retry_after_ms > 0 and retry_after_ms <= window_ms
    end

    test "the two doors count independently: different algorithms, different entries" do
      # check_limit/3 is a token bucket, check_limit_in_caller/3 a sharded fixed window. They deliberately
      # do not share state, so mixing them on one identifier is not a way to spend a quota twice as fast
      # in one direction or to be blocked early in the other.
      config = %{limit: 2, window_ms: 60_000}

      {identifier, in_caller} =
        within_one_window(config.window_ms, fn ->
          identifier = "doors_#{System.unique_integer([:positive])}"
          {identifier, for(_ <- 1..3, do: RateLimiter.check_limit_in_caller(identifier, :publish, config))}
        end)

      # the hot-path door has its own full quota...
      assert [:ok, :ok, {:error, :rate_limit_exceeded, _}] = in_caller

      # ...and spending it left the token bucket untouched
      assert :ok = RateLimiter.check_limit(identifier, :publish, config)
      assert :ok = RateLimiter.check_limit(identifier, :publish, config)
      assert {:error, :rate_limit_exceeded, _} = RateLimiter.check_limit(identifier, :publish, config)
    end

    test "keeps separate buckets per identifier" do
      config = %{limit: 1, window_ms: 60_000}

      results =
        within_one_window(config.window_ms, fn ->
          one = "user_one_#{System.unique_integer([:positive])}"
          two = "user_two_#{System.unique_integer([:positive])}"

          for id <- [one, one, two], do: RateLimiter.check_limit_in_caller(id, :publish, config)
        end)

      assert [:ok, {:error, :rate_limit_exceeded, _}, :ok] = results
    end

    test "feeds the blocked counter that the dashboard reads" do
      config = %{limit: 1, window_ms: 60_000}

      # asserted outside the guard: a failed assertion inside it would not be rerun on a straddle
      {identifier, results} =
        within_one_window(config.window_ms, fn ->
          identifier = "blocked_#{System.unique_integer([:positive])}"
          {identifier, for(_ <- 1..2, do: RateLimiter.check_limit_in_caller(identifier, :publish, config))}
        end)

      assert [:ok, {:error, :rate_limit_exceeded, _}] = results

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

    test "a change in the scheduler count mid-window does not move the caps" do
      # The quota is split into one cap per shard. If the shard count were re-read on every check, a drop
      # in `schedulers_online` would give the remaining shard a bigger cap over a counter already spent.
      # With limit 16 over n >= 2 shards, shard 0 holds at most 8 and its counter pins at its cap plus one,
      # so after a drop to one shard (cap 16) the spent quota would be admitted again. Shard 0 is swept
      # first by every caller, which is what makes this hold wherever the checks are scheduled; a limit
      # of 2 does not show it, because shard 0 then pins at 2, the new cap.
      prior = :erlang.system_info(:schedulers_online)

      if prior < 2 do
        # One scheduler means one shard already; there is no count to drop to.
        IO.puts("skipped: needs at least 2 online schedulers, this VM has #{prior}")
      else
        on_exit(fn -> :erlang.system_flag(:schedulers_online, prior) end)
        config = %{limit: 16, window_ms: 60_000}

        {spent, after_drop} =
          within_one_window(config.window_ms, fn ->
            identifier = "shards_#{System.unique_integer([:positive])}"
            :erlang.system_flag(:schedulers_online, prior)
            spent = for _ <- 1..16, do: RateLimiter.check_limit_in_caller(identifier, :publish, config)

            :erlang.system_flag(:schedulers_online, 1)
            {spent, RateLimiter.check_limit_in_caller(identifier, :publish, config)}
          end)

        assert Enum.all?(spent, &(&1 == :ok))
        assert {:error, :rate_limit_exceeded, _} = after_drop, "a request over the limit was admitted after the drop"
      end
    end

    test "without the shard count the limiter fixed at start, a check fails loudly" do
      # No fallback to the live scheduler count: that fallback is the mid-window cap shift above. The
      # term only goes missing if the limiter never started, and then its table is missing too.
      key = {RateLimiter, :shard_count}
      fixed = :persistent_term.get(key)
      on_exit(fn -> :persistent_term.put(key, fixed) end)
      :persistent_term.erase(key)

      assert_raise ArgumentError, fn ->
        RateLimiter.check_limit_in_caller("no_shards_#{System.unique_integer([:positive])}", :publish, %{
          limit: 1,
          window_ms: 60_000
        })
      end
    end

    test "the shard count is the scheduler count when the limiter started" do
      assert :persistent_term.get({RateLimiter, :shard_count}) == :erlang.system_info(:schedulers_online)
      assert length(RateLimiter.shard_caps(1)) == :erlang.system_info(:schedulers_online)
    end

    @tag :concurrent
    test "stays exact under concurrency: every token is claimed atomically" do
      # Sharding is what makes this door fast, and the reason it is still exact is that each token is
      # claimed by one atomic update_counter, on a shard whose caps sum to the configured limit. This is
      # the property that would break first if the sharding were reworked.
      limit = 50
      attempts = 400
      window_ms = 60_000

      admitted =
        within_one_window(window_ms, fn ->
          identifier = "race_#{:rand.uniform(1_000_000)}"
          config = %{limit: limit, window_ms: window_ms}

          1..attempts
          |> Enum.map(fn _ ->
            Task.async(fn -> RateLimiter.check_limit_in_caller(identifier, :publish, config) end)
          end)
          |> Task.await_many(30_000)
          |> Enum.count(&(&1 == :ok))
        end)

      assert admitted == limit, "#{attempts} concurrent callers admitted #{admitted}, expected #{limit}"
    end
  end

  describe "diagnostics" do
    @table :malachi_rate_limits

    test "window_bounds/2 splits a timestamp into its window start and the time elapsed in it" do
      assert RateLimiter.window_bounds(0, 1_000) == {0, 0}
      assert RateLimiter.window_bounds(999, 1_000) == {0, 999}
      assert RateLimiter.window_bounds(1_000, 1_000) == {1_000, 0}
      assert RateLimiter.window_bounds(123_456, 60_000) == {120_000, 3_456}
    end

    property "window_bounds/2 aligns every timestamp to the floor of its window, negative ones included" do
      # The window is named by the monotonic clock, whose origin is arbitrary and often far below zero
      # (-576460751119 ms was seen on a workstation). `rem/2` truncates toward zero, so on a negative
      # timestamp it put the start AFTER the timestamp; this is the property that caught it.
      check all(
              now <- StreamData.integer(-1_000_000_000_000_000..1_000_000_000_000_000),
              window_ms <- StreamData.integer(1..86_400_000)
            ) do
        {start, elapsed} = RateLimiter.window_bounds(now, window_ms)

        assert start <= now and now < start + window_ms
        assert Integer.mod(start, window_ms) == 0
        assert elapsed == now - start
        # every timestamp of the window names the same window
        assert RateLimiter.window_bounds(start + window_ms - 1, window_ms) == {start, window_ms - 1}
      end
    end

    test "the window is named by the monotonic clock, not by wall-clock time" do
      # A clock step moves Erlang system time outright under multi_time_warp (the OTP 29 default), and a
      # window named by it would then hand out a fresh quota mid-window. The monotonic clock never steps.
      # No test can step the real clock, so this pins the property that makes the step harmless: the
      # counter's window comes from the monotonic clock. The guarantee itself is the ERTS contract for
      # `erlang:monotonic_time/1`.
      window_ms = 60_000
      floor_of = fn now -> Integer.floor_div(now, window_ms) * window_ms end

      {earliest, counters, latest} =
        within_one_window(window_ms, fn ->
          identifier = "monotonic_#{System.unique_integer([:positive])}"
          earliest = floor_of.(System.monotonic_time(:millisecond))
          :ok = RateLimiter.check_limit_in_caller(identifier, :publish, %{limit: 1, window_ms: window_ms})
          {earliest, RateLimiter.window_counters(identifier, :publish), floor_of.(System.monotonic_time(:millisecond))}
        end)

      assert counters != []

      for %{window_start: window_start} <- counters do
        assert window_start in earliest..latest//window_ms,
               "window #{window_start} is not the monotonic window #{earliest}; system time names #{floor_of.(System.system_time(:millisecond))}"
      end
    end

    test "current_window_start/1 names the window a check made now is counted in" do
      window_ms = 60_000

      {before, counters, later} =
        within_one_window(window_ms, fn ->
          identifier = "current_#{System.unique_integer([:positive])}"
          before = RateLimiter.current_window_start(window_ms)
          :ok = RateLimiter.check_limit_in_caller(identifier, :publish, %{limit: 1, window_ms: window_ms})
          {before, RateLimiter.window_counters(identifier, :publish), RateLimiter.current_window_start(window_ms)}
        end)

      assert before == later
      # a shard holding none of the quota still records the attempt, so there may be more than one entry
      assert counters != []
      assert Enum.all?(counters, &(&1.window_start == before))
      assert Integer.mod(before, window_ms) == 0
    end

    test "shard_caps/1 splits the limit over one shard per scheduler and sums to it" do
      shards = :erlang.system_info(:schedulers_online)

      for limit <- [1, 2, shards, shards + 1, 1_000] do
        caps = RateLimiter.shard_caps(limit)
        assert length(caps) == shards
        assert Enum.sum(caps) == limit, "limit #{limit} split as #{inspect(caps)}"
        # the remainder goes to the low shards, so the caps never increase with the shard index
        assert caps == Enum.sort(caps, :desc)
        assert Enum.max(caps) - Enum.min(caps) <= 1
      end
    end

    test "window_counters/2 is empty for an identifier that was never checked" do
      assert RateLimiter.window_counters("never_#{System.unique_integer([:positive])}", :publish) == []
    end

    test "window_counters/2 lists every window and shard of one identifier and action, in order" do
      identifier = "counters_#{System.unique_integer([:positive])}"
      other = "counters_other_#{System.unique_integer([:positive])}"

      for {id, action, window_start, shard, used} <- [
            {identifier, :publish, 2_000, 1, 3},
            {identifier, :publish, 1_000, 0, 1},
            {identifier, :publish, 1_000, 2, 2},
            {identifier, :subscribe, 1_000, 0, 9},
            {other, :publish, 1_000, 0, 9}
          ] do
        :ets.insert(@table, {{id, action, window_start, shard}, used, 1_000})
      end

      on_exit(fn ->
        for id <- [identifier, other], do: :ets.match_delete(@table, {{id, :_, :_, :_}, :_, :_})
      end)

      assert RateLimiter.window_counters(identifier, :publish) == [
               %{window_start: 1_000, shard: 0, used: 1, window_ms: 1_000},
               %{window_start: 1_000, shard: 2, used: 2, window_ms: 1_000},
               %{window_start: 2_000, shard: 1, used: 3, window_ms: 1_000}
             ]

      assert [%{used: 9}] = RateLimiter.window_counters(identifier, :subscribe)
    end

    test "window_counters/2 ignores token buckets and blocked counters of the same identifier" do
      identifier = "counters_mixed_#{System.unique_integer([:positive])}"
      config = %{limit: 1, window_ms: 60_000}

      assert :ok = RateLimiter.check_limit(identifier, :publish, config)
      assert {:error, _, _} = RateLimiter.check_limit(identifier, :publish, config)

      assert RateLimiter.window_counters(identifier, :publish) == []
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

    # Now, by the clock the sharded windows are named with: a 1ms window starts at the current instant.
    # Stale and live counters are built from it, as the limiter would have written them.
    defp window_now, do: RateLimiter.current_window_start(1)

    defp run_cleanup do
      send(Process.whereis(RateLimiter), :cleanup)
      # the cleanup is a cast-like info message; a sync call flushes it
      _ = RateLimiter.get_stats()
      :ok
    end

    test "a manual pass cleans without starting another timer chain" do
      # Only the limiter's own timer reschedules. If a manual pass rescheduled too, every `:cleanup` sent
      # by hand (this file sends several) would leave one more timer sweeping the table forever.
      pid = Process.whereis(RateLimiter)
      %{cleanup_timer: timer} = :sys.get_state(pid)
      assert is_integer(Process.read_timer(timer))

      run_cleanup()
      run_cleanup()

      assert %{cleanup_timer: ^timer} = :sys.get_state(pid)
      assert is_integer(Process.read_timer(timer)), "the scheduled cleanup stopped after a manual pass"
    end

    test "the scheduled pass cleans and arms exactly one new timer" do
      pid = Process.whereis(RateLimiter)
      %{cleanup_timer: old_timer} = :sys.get_state(pid)

      stale = {"scheduled_stale_#{:rand.uniform(1_000_000)}", :publish, window_now() - 3_600_000, 0}
      :ets.insert(@table, {stale, 1, 1_000})

      send(pid, :scheduled_cleanup)
      %{cleanup_timer: new_timer} = :sys.get_state(pid)

      assert :ets.lookup(@table, stale) == []
      assert new_timer != old_timer
      assert is_integer(Process.read_timer(new_timer))

      # The message this test sent stands in for the old timer's; cancel the old one so the chain count
      # stays at one, as it would in production where the timer message IS the old timer firing.
      Process.cancel_timer(old_timer)
    end

    test "reaps stale sharded window counters and keeps live ones" do
      tag = :rand.uniform(1_000_000)
      hour_ms = 3_600_000
      now = window_now()

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
      :ets.insert(@table, {stale, {5, now - 3_600_000 - 60_000, now, 60_000}})
      :ets.insert(@table, {live, {5, now, now, 60_000}})

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
      live_window_start = window_now() - 90 * 60_000
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
      key = {identifier, :publish, window_now() - 10_000, 0}
      :ets.insert(@table, {key, 100, 1_000})

      run_cleanup()

      assert :ets.lookup(@table, key) == [], "a counter ten windows old was kept"
    end

    test "ages an entry by its stored window, never by the current configuration" do
      # This is the property that makes the other two cleanup tests mean anything, and the one that was
      # got wrong once: an earlier version of the cleanup read the action's window back from config, which
      # is a second source of truth that can disagree with what the caller actually counted under.
      #
      # The setup makes the two answers point in OPPOSITE directions. The entry is ten of ITS OWN windows
      # old, so by its stored width it is long dead. The configured window is a full day and the action
      # is switched off entirely, so anything consulting configuration would either keep the entry or
      # have no window to judge it by. It must be reaped, and only the stored width says so.
      tag = :rand.uniform(1_000_000)
      Application.put_env(:malachi, :publish_rate_limit, 0)
      Application.put_env(:malachi, :publish_rate_window_ms, 86_400_000)

      identifier = "stored_window_#{tag}"
      key = {identifier, :publish, window_now() - 10_000, 0}
      :ets.insert(@table, {key, 5, 1_000})

      run_cleanup()

      assert :ets.lookup(@table, key) == [],
             "the entry was judged by configuration rather than by the window it was counted under"
    end

    test "keeps an exhausted bucket whose window is wider than the flat hour" do
      # A bucket idle for a whole window has refilled anyway, so reaping it after an hour is free for any
      # window under one. For a wider window it is a refund: the tokens are handed back before they were
      # due. An auth allowance measured over more than an hour is the case that breaks.
      identifier = "wide_bucket_#{:rand.uniform(1_000_000)}"
      window_ms = 7_200_000
      now = System.monotonic_time(:millisecond)
      key = {identifier, :auth}

      # Exhausted, last touched 61 minutes ago: past the flat hour, but only halfway through its window.
      :ets.insert(@table, {key, {0, now - 61 * 60_000, now - 61 * 60_000, window_ms}})

      run_cleanup()

      assert :ets.lookup(@table, key) != [], "a bucket mid-window was reaped, refunding its tokens"

      # A token bucket refills gradually, so being admitted here is correct: 61 minutes of a 2-hour
      # window is worth 5 of the 10 tokens back. What must NOT happen is a reset to the full allowance,
      # and the stored count tells the two apart. Kept: 5 refilled, 1 spent, 4 left. Reaped: the bucket
      # is recreated at limit - 1, or 9.
      assert :ok = RateLimiter.check_limit(identifier, :auth, %{limit: 10, window_ms: window_ms})
      assert [{^key, {4, _last_refill, _window_start, ^window_ms}}] = :ets.lookup(@table, key)
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
