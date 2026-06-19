defmodule Malachi.OverflowIntegrationTest do
  use ExUnit.Case, async: false
  alias Malachi.{Backpressure, Queue, QueueConfig}

  setup do
    Application.ensure_all_started(:malachi)
    queue_name = "overflow_test_#{:erlang.unique_integer([:positive])}"

    on_exit(fn ->
      try do
        QueueConfig.delete_queue(queue_name, force: true)
      catch
        _, _ -> :ok
      end
    end)

    {:ok, queue: queue_name}
  end

  describe "backpressure flow integration" do
    test "producer receives backpressure signal when threshold reached", %{queue: queue} do
      # Create queue with low threshold for testing
      QueueConfig.create_queue(queue,
        max_buffer_size: 100,
        backpressure_threshold: 0.7,
        overflow_behavior: :drop_newest
      )

      # Enqueue messages below threshold (60%)
      for i <- 1..60 do
        {:ok, _} = Queue.enqueue(queue, "msg_#{i}", %{})
      end

      # Should not have backpressure yet
      {:ok, pressure_info} = Backpressure.get_queue_pressure(queue)
      refute Backpressure.should_apply_backpressure?(pressure_info)
      assert pressure_info.status == :medium_pressure

      # Enqueue more to reach threshold (80%)
      for i <- 61..80 do
        {:ok, _} = Queue.enqueue(queue, "msg_#{i}", %{})
      end

      # Now should have backpressure
      {:ok, pressure_info} = Backpressure.get_queue_pressure(queue)
      assert Backpressure.should_apply_backpressure?(pressure_info)
      assert pressure_info.status == :high_pressure

      # Verify pressure percentage
      assert pressure_info.pressure >= 0.7
      assert pressure_info.buffer_size == 80
    end

    test "backpressure signal persists until buffer drains", %{queue: queue} do
      QueueConfig.create_queue(queue,
        max_buffer_size: 50,
        backpressure_threshold: 0.8,
        overflow_behavior: :drop_newest
      )

      # Fill to trigger backpressure
      for i <- 1..45 do
        Queue.enqueue(queue, "msg_#{i}", %{})
      end

      {:ok, info} = Backpressure.get_queue_pressure(queue)
      assert Backpressure.should_apply_backpressure?(info)

      # Start consumer to drain queue
      consumer_pid =
        spawn(fn ->
          Queue.subscribe(queue, self())
          receive_messages(40)
        end)

      Process.sleep(200)

      # Backpressure should be relieved
      {:ok, info} = Backpressure.get_queue_pressure(queue)
      refute Backpressure.should_apply_backpressure?(info)

      Process.exit(consumer_pid, :kill)
    end

    defp receive_messages(0), do: :ok

    defp receive_messages(n) do
      receive do
        {:queue_message, _msg} ->
          receive_messages(n - 1)
      after
        500 -> :timeout
      end
    end
  end

  describe "FIFO unblocking behavior" do
    test "blocked producers are unblocked in FIFO order", %{queue: queue} do
      QueueConfig.create_queue(queue,
        max_buffer_size: 5,
        overflow_behavior: :block,
        # Long timeout
        block_timeout_ms: 30_000
      )

      # Fill buffer completely
      for i <- 1..5 do
        {:ok, _} = Queue.enqueue(queue, "initial_#{i}", %{})
      end

      # Track completion order
      parent = self()

      # Spawn 3 producers that will block in staggered fashion
      for i <- 1..3 do
        spawn(fn ->
          # Stagger to ensure order
          Process.sleep(i * 20)
          start = System.monotonic_time(:millisecond)
          result = Queue.enqueue(queue, "blocked_#{i}", %{})
          elapsed = System.monotonic_time(:millisecond) - start
          send(parent, {:completed, i, result, elapsed})
        end)
      end

      # Wait for all to block
      Process.sleep(200)

      # Start draining
      consumer_pid =
        spawn(fn ->
          Queue.subscribe(queue, parent)
          :ok
        end)

      # Drain initial messages
      for _i <- 1..5 do
        receive do
          {:queue_message, _msg} -> :ok
        after
          2000 -> :ok
        end
      end

      # Small delay for unblocking
      Process.sleep(100)

      # Collect completion order
      completion_order = collect_completions(3, [])

      # Verify FIFO order
      assert completion_order == [1, 2, 3],
             "Expected [1,2,3] but got #{inspect(completion_order)}"

      Process.exit(consumer_pid, :kill)
    end

    test "FIFO fairness with concurrent blocking", %{queue: queue} do
      QueueConfig.create_queue(queue,
        max_buffer_size: 5,
        overflow_behavior: :block,
        block_timeout_ms: 3000
      )

      # Fill buffer
      for i <- 1..5 do
        Queue.enqueue(queue, "initial_#{i}", %{})
      end

      parent = self()

      # Spawn producers that will block at different times
      _task1 =
        Task.async(fn ->
          # Block immediately
          Process.sleep(0)
          Queue.enqueue(queue, "first", %{})
          send(parent, {:done, 1})
        end)

      _task2 =
        Task.async(fn ->
          # Block 50ms later
          Process.sleep(50)
          Queue.enqueue(queue, "second", %{})
          send(parent, {:done, 2})
        end)

      _task3 =
        Task.async(fn ->
          # Block 100ms later
          Process.sleep(100)
          Queue.enqueue(queue, "third", %{})
          send(parent, {:done, 3})
        end)

      Process.sleep(200)

      # Start draining
      consumer_pid =
        spawn(fn ->
          Queue.subscribe(queue, self())
          drain_slowly(5, 100)
        end)

      # Collect completion order
      completion_order = collect_done(3, [])

      # Should complete in FIFO order based on block time
      assert completion_order == [1, 2, 3]

      Process.exit(consumer_pid, :kill)
    end

    defp drain_slowly(0, _delay), do: :ok

    defp drain_slowly(n, delay) do
      receive do
        {:queue_message, _msg} ->
          Process.sleep(delay)
          drain_slowly(n - 1, delay)
      after
        1000 -> :timeout
      end
    end

    defp collect_completions(0, acc), do: Enum.reverse(acc)

    defp collect_completions(n, acc) do
      receive do
        {:completed, i, {:ok, _}, _elapsed} ->
          collect_completions(n - 1, [i | acc])
      after
        5000 -> Enum.reverse(acc)
      end
    end

    defp collect_done(0, acc), do: Enum.reverse(acc)

    defp collect_done(n, acc) do
      receive do
        {:done, i} -> collect_done(n - 1, [i | acc])
      after
        5000 -> Enum.reverse(acc)
      end
    end
  end

  describe "block timeout behavior" do
    test "producer times out after configured period", %{queue: queue} do
      QueueConfig.create_queue(queue,
        max_buffer_size: 5,
        overflow_behavior: :block,
        # Short timeout for testing
        block_timeout_ms: 500
      )

      # Fill buffer
      for i <- 1..5 do
        Queue.enqueue(queue, "msg_#{i}", %{})
      end

      # Attempt to enqueue with blocking - should timeout
      start_time = System.monotonic_time(:millisecond)
      result = Queue.enqueue(queue, "should_timeout", %{})
      end_time = System.monotonic_time(:millisecond)

      # Verify timeout error
      assert {:error, :block_timeout} = result

      # Verify timing (should be close to 500ms)
      elapsed = end_time - start_time
      assert elapsed >= 450 and elapsed <= 700
    end

    test "different timeout values are respected", %{queue: queue} do
      QueueConfig.create_queue(queue,
        max_buffer_size: 3,
        overflow_behavior: :block,
        block_timeout_ms: 1000
      )

      # Fill buffer
      for i <- 1..3 do
        Queue.enqueue(queue, "msg_#{i}", %{})
      end

      # First timeout with 1000ms
      start1 = System.monotonic_time(:millisecond)
      {:error, :block_timeout} = Queue.enqueue(queue, "test1", %{})
      elapsed1 = System.monotonic_time(:millisecond) - start1

      # Update timeout to 300ms
      QueueConfig.update_queue(queue, %{block_timeout_ms: 300})
      # Let config propagate
      Process.sleep(50)

      # Second timeout should be faster
      start2 = System.monotonic_time(:millisecond)
      {:error, :block_timeout} = Queue.enqueue(queue, "test2", %{})
      elapsed2 = System.monotonic_time(:millisecond) - start2

      # Verify both timeouts
      assert elapsed1 >= 900 and elapsed1 <= 1200
      assert elapsed2 >= 250 and elapsed2 <= 450
    end

    test "timeout is cancelled when producer is unblocked", %{queue: queue} do
      QueueConfig.create_queue(queue,
        max_buffer_size: 2,
        overflow_behavior: :block,
        # Very long timeout
        block_timeout_ms: 30_000
      )

      # Fill buffer
      for i <- 1..2 do
        {:ok, _} = Queue.enqueue(queue, "msg_#{i}", %{})
      end

      parent = self()

      # Start blocked producer
      producer_pid =
        spawn(fn ->
          start = System.monotonic_time(:millisecond)
          result = Queue.enqueue(queue, "unblocked", %{})
          elapsed = System.monotonic_time(:millisecond) - start
          send(parent, {:producer_done, result, elapsed})
        end)

      # Let it block
      Process.sleep(100)

      # Start consumer to drain
      consumer_pid =
        spawn(fn ->
          Queue.subscribe(queue, parent)
          :ok
        end)

      # Receive and drain all messages
      for _i <- 1..2 do
        receive do
          {:queue_message, _msg} -> :ok
        after
          2000 -> :ok
        end
      end

      # Give time for producer to complete
      Process.sleep(200)

      # Check result
      receive do
        {:producer_done, {:ok, _msg}, elapsed} ->
          # Should complete quickly (< 2s), not wait for 30s timeout
          assert elapsed < 3000, "Producer took #{elapsed}ms, expected < 3000ms"
      after
        5000 -> flunk("Producer should have completed within 5s")
      end

      Process.exit(consumer_pid, :kill)
      Process.exit(producer_pid, :kill)
    end
  end

  describe "max_blocked_producers limit" do
    test "rejects new blocks when limit reached", %{queue: queue} do
      QueueConfig.create_queue(queue,
        max_buffer_size: 2,
        overflow_behavior: :block,
        max_blocked_producers: 3,
        block_timeout_ms: 10_000
      )

      # Fill buffer
      for i <- 1..2 do
        Queue.enqueue(queue, "msg_#{i}", %{})
      end

      # Block 3 producers (at limit)
      tasks =
        for i <- 1..3 do
          Task.async(fn ->
            Queue.enqueue(queue, "blocked_#{i}", %{})
          end)
        end

      Process.sleep(100)

      # 4th producer should be rejected immediately
      start = System.monotonic_time(:millisecond)
      result = Queue.enqueue(queue, "rejected", %{})
      elapsed = System.monotonic_time(:millisecond) - start

      assert {:error, :too_many_blocked_producers} = result
      # Should reject immediately, not block
      assert elapsed < 100

      # Cleanup
      Enum.each(tasks, &Task.shutdown(&1, :brutal_kill))
    end
  end

  describe "overflow strategy transitions" do
    test "switching from drop to block at runtime", %{queue: queue} do
      QueueConfig.create_queue(queue,
        max_buffer_size: 5,
        overflow_behavior: :drop_newest
      )

      # Fill buffer
      for i <- 1..5 do
        Queue.enqueue(queue, "msg_#{i}", %{})
      end

      # With drop_newest, overflow messages are silently dropped
      {:ok, _} = Queue.enqueue(queue, "dropped", %{})

      stats = Queue.get_stats(queue)
      # Still 5, newest was dropped
      assert stats.buffered == 5

      # Switch to block
      QueueConfig.update_queue(queue, %{overflow_behavior: :block, block_timeout_ms: 500})
      Process.sleep(50)

      # Now should block
      result = Queue.enqueue(queue, "blocked", %{})
      assert {:error, :block_timeout} = result
    end

    test "switching from reject to drop_oldest", %{queue: queue} do
      QueueConfig.create_queue(queue,
        max_buffer_size: 3,
        overflow_behavior: :reject
      )

      # Fill buffer
      for i <- 1..3 do
        Queue.enqueue(queue, "msg_#{i}", %{})
      end

      # Reject overflow
      result = Queue.enqueue(queue, "rejected", %{})
      assert {:error, :queue_full} = result

      # Switch to drop_oldest
      QueueConfig.update_queue(queue, %{overflow_behavior: :drop_oldest})
      Process.sleep(50)

      # Now should accept by dropping oldest
      {:ok, _} = Queue.enqueue(queue, "new_msg", %{})

      stats = Queue.get_stats(queue)
      # Still 3, oldest was dropped
      assert stats.buffered == 3
    end
  end
end
