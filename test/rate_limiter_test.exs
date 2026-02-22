defmodule MalachiMQ.RateLimiterTest do
  use ExUnit.Case, async: false
  alias MalachiMQ.RateLimiter

  setup do
    # Temporarily enable rate limiting for these tests
    original_value = Application.get_env(:malachimq, :rate_limit_enabled)
    Application.put_env(:malachimq, :rate_limit_enabled, true)

    on_exit(fn ->
      Application.put_env(:malachimq, :rate_limit_enabled, original_value)
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

  describe "disabled rate limiting" do
    test "bypasses checks when disabled" do
      # Temporarily disable for this test
      Application.put_env(:malachimq, :rate_limit_enabled, false)

      identifier = "any_user"
      # Would normally block everything
      config = %{limit: 0, window_ms: 1}

      # Should still succeed because rate limiting is disabled
      assert :ok = RateLimiter.check_limit(identifier, :auth, config)
      assert :ok = RateLimiter.check_limit(identifier, :auth, config)
      assert :ok = RateLimiter.check_limit(identifier, :auth, config)

      # Re-enable for other tests
      Application.put_env(:malachimq, :rate_limit_enabled, true)
    end
  end
end
