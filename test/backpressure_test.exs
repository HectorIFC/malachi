defmodule MalachiMQ.BackpressureTest do
  use ExUnit.Case, async: false
  alias MalachiMQ.{Backpressure, Queue, QueueConfig}

  setup do
    # Ensure application is started
    Application.ensure_all_started(:malachimq)

    # Create unique queue for each test
    queue_name = "test_bp_#{:erlang.unique_integer([:positive])}"

    on_exit(fn ->
      # Cleanup
      try do
        QueueConfig.delete_queue(queue_name, force: true)
      catch
        _, _ -> :ok
      end
    end)

    {:ok, queue: queue_name}
  end

  describe "get_queue_pressure/1" do
    test "returns low_pressure for empty queue", %{queue: queue} do
      QueueConfig.create_queue(queue, max_buffer_size: 1000, backpressure_threshold: 0.8)

      {:ok, pressure_info} = Backpressure.get_queue_pressure(queue)

      assert pressure_info.status == :low_pressure
      assert pressure_info.pressure == 0.0
      assert pressure_info.buffer_size == 0
      assert pressure_info.max_size == 1000
      assert pressure_info.threshold == 0.8
    end

    test "returns low_pressure for buffer < 50%", %{queue: queue} do
      QueueConfig.create_queue(queue, max_buffer_size: 1000, backpressure_threshold: 0.8)

      # Enqueue 400 messages (40%)
      for i <- 1..400 do
        Queue.enqueue(queue, "payload_#{i}", %{})
      end

      {:ok, pressure_info} = Backpressure.get_queue_pressure(queue)

      assert pressure_info.status == :low_pressure
      assert pressure_info.pressure >= 0.4
      assert pressure_info.pressure < 0.5
      assert pressure_info.buffer_size >= 400
    end

    test "returns medium_pressure for buffer 50-79%", %{queue: queue} do
      QueueConfig.create_queue(queue, max_buffer_size: 1000, backpressure_threshold: 0.8)

      # Enqueue 600 messages (60%)
      for i <- 1..600 do
        Queue.enqueue(queue, "payload_#{i}", %{})
      end

      {:ok, pressure_info} = Backpressure.get_queue_pressure(queue)

      assert pressure_info.status == :medium_pressure
      assert pressure_info.pressure >= 0.5
      assert pressure_info.pressure < 0.8
      assert pressure_info.buffer_size >= 600
    end

    test "returns high_pressure for buffer >= threshold", %{queue: queue} do
      QueueConfig.create_queue(queue, max_buffer_size: 1000, backpressure_threshold: 0.8)

      # Enqueue 850 messages (85%)
      for i <- 1..850 do
        Queue.enqueue(queue, "payload_#{i}", %{})
      end

      {:ok, pressure_info} = Backpressure.get_queue_pressure(queue)

      assert pressure_info.status == :high_pressure
      assert pressure_info.pressure >= 0.8
      assert pressure_info.pressure < 1.0
      assert pressure_info.buffer_size >= 850
    end

    test "returns full for buffer at 100%", %{queue: queue} do
      QueueConfig.create_queue(queue,
        max_buffer_size: 100,
        backpressure_threshold: 0.8,
        overflow_behavior: :drop_newest
      )

      # Fill buffer to 100%
      for i <- 1..100 do
        Queue.enqueue(queue, "payload_#{i}", %{})
      end

      {:ok, pressure_info} = Backpressure.get_queue_pressure(queue)

      assert pressure_info.status == :full
      assert pressure_info.pressure == 1.0
      assert pressure_info.buffer_size == 100
    end

    test "respects custom backpressure_threshold", %{queue: queue} do
      # Custom threshold at 60%
      QueueConfig.create_queue(queue, max_buffer_size: 1000, backpressure_threshold: 0.6)

      # Enqueue 650 messages (65%)
      for i <- 1..650 do
        Queue.enqueue(queue, "payload_#{i}", %{})
      end

      {:ok, pressure_info} = Backpressure.get_queue_pressure(queue)

      # Should be high_pressure with 60% threshold
      assert pressure_info.status == :high_pressure
      assert pressure_info.pressure >= 0.6
      assert pressure_info.threshold == 0.6
    end

    test "handles pressure > 1.0 after config update", %{queue: queue} do
      QueueConfig.create_queue(queue, max_buffer_size: 1000)

      # Fill to 800 messages
      for i <- 1..800 do
        Queue.enqueue(queue, "payload_#{i}", %{})
      end

      # Update config to reduce buffer (force)
      QueueConfig.update_queue(queue, [max_buffer_size: 500], force: true)

      # Allow time for config update to propagate
      Process.sleep(100)

      {:ok, pressure_info} = Backpressure.get_queue_pressure(queue)

      # Pressure can exceed 1.0 temporarily
      assert pressure_info.status == :full
      assert pressure_info.pressure >= 1.0
      assert pressure_info.max_size == 500
    end

    test "returns error for non-existent queue" do
      assert {:error, :queue_not_found} = Backpressure.get_queue_pressure("nonexistent_queue_xyz")
    end

    test "handles zero max_buffer_size gracefully", %{queue: queue} do
      QueueConfig.create_queue(queue, max_buffer_size: 0)

      {:ok, pressure_info} = Backpressure.get_queue_pressure(queue)

      # With max_buffer_size=0, pressure should be 0.0
      assert pressure_info.pressure == 0.0
      assert pressure_info.status == :low_pressure
    end
  end

  describe "should_apply_backpressure?/1" do
    test "returns false for low_pressure" do
      pressure_info = %{status: :low_pressure, pressure: 0.3}
      refute Backpressure.should_apply_backpressure?(pressure_info)
    end

    test "returns false for medium_pressure" do
      pressure_info = %{status: :medium_pressure, pressure: 0.65}
      refute Backpressure.should_apply_backpressure?(pressure_info)
    end

    test "returns true for high_pressure" do
      pressure_info = %{status: :high_pressure, pressure: 0.85}
      assert Backpressure.should_apply_backpressure?(pressure_info)
    end

    test "returns true for full" do
      pressure_info = %{status: :full, pressure: 1.0}
      assert Backpressure.should_apply_backpressure?(pressure_info)
    end

    test "works with actual queue pressure", %{queue: queue} do
      QueueConfig.create_queue(queue, max_buffer_size: 100, backpressure_threshold: 0.8)

      # Low pressure
      for i <- 1..30 do
        Queue.enqueue(queue, "msg_#{i}", %{})
      end

      {:ok, pressure_info} = Backpressure.get_queue_pressure(queue)
      refute Backpressure.should_apply_backpressure?(pressure_info)

      # High pressure
      for i <- 31..85 do
        Queue.enqueue(queue, "msg_#{i}", %{})
      end

      {:ok, pressure_info} = Backpressure.get_queue_pressure(queue)
      assert Backpressure.should_apply_backpressure?(pressure_info)
    end
  end

  describe "pressure level transitions" do
    test "transitions through all pressure levels", %{queue: queue} do
      QueueConfig.create_queue(queue, max_buffer_size: 100, backpressure_threshold: 0.8)

      # Start: low_pressure
      {:ok, info} = Backpressure.get_queue_pressure(queue)
      assert info.status == :low_pressure

      # Add 40 messages: still low_pressure
      for i <- 1..40 do
        Queue.enqueue(queue, "msg_#{i}", %{})
      end

      {:ok, info} = Backpressure.get_queue_pressure(queue)
      assert info.status == :low_pressure

      # Add to 60 total: medium_pressure
      for i <- 41..60 do
        Queue.enqueue(queue, "msg_#{i}", %{})
      end

      {:ok, info} = Backpressure.get_queue_pressure(queue)
      assert info.status == :medium_pressure

      # Add to 85 total: high_pressure
      for i <- 61..85 do
        Queue.enqueue(queue, "msg_#{i}", %{})
      end

      {:ok, info} = Backpressure.get_queue_pressure(queue)
      assert info.status == :high_pressure

      # Fill to 100: full
      for i <- 86..100 do
        Queue.enqueue(queue, "msg_#{i}", %{})
      end

      {:ok, info} = Backpressure.get_queue_pressure(queue)
      assert info.status == :full
    end
  end

  describe "boundary conditions" do
    test "pressure at exactly 50%", %{queue: queue} do
      QueueConfig.create_queue(queue, max_buffer_size: 100, backpressure_threshold: 0.8)

      for i <- 1..50 do
        Queue.enqueue(queue, "msg_#{i}", %{})
      end

      {:ok, info} = Backpressure.get_queue_pressure(queue)
      assert info.status == :medium_pressure
      assert info.pressure == 0.5
    end

    test "pressure at exactly threshold (80%)", %{queue: queue} do
      QueueConfig.create_queue(queue, max_buffer_size: 100, backpressure_threshold: 0.8)

      for i <- 1..80 do
        Queue.enqueue(queue, "msg_#{i}", %{})
      end

      {:ok, info} = Backpressure.get_queue_pressure(queue)
      assert info.status == :high_pressure
      assert info.pressure == 0.8
    end

    test "pressure just below threshold", %{queue: queue} do
      QueueConfig.create_queue(queue, max_buffer_size: 100, backpressure_threshold: 0.8)

      for i <- 1..79 do
        Queue.enqueue(queue, "msg_#{i}", %{})
      end

      {:ok, info} = Backpressure.get_queue_pressure(queue)
      assert info.status == :medium_pressure
      assert info.pressure == 0.79
    end

    test "pressure at exactly 100%", %{queue: queue} do
      QueueConfig.create_queue(queue,
        max_buffer_size: 50,
        backpressure_threshold: 0.8,
        overflow_behavior: :drop_newest
      )

      for i <- 1..50 do
        Queue.enqueue(queue, "msg_#{i}", %{})
      end

      {:ok, info} = Backpressure.get_queue_pressure(queue)
      assert info.status == :full
      assert info.pressure == 1.0
    end
  end

  describe "concurrent pressure monitoring" do
    test "handles concurrent enqueues correctly", %{queue: queue} do
      QueueConfig.create_queue(queue, max_buffer_size: 1000, backpressure_threshold: 0.8)

      # Spawn multiple producers
      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            for j <- 1..50 do
              Queue.enqueue(queue, "msg_#{i}_#{j}", %{})
            end
          end)
        end

      Task.await_many(tasks, 5000)

      {:ok, info} = Backpressure.get_queue_pressure(queue)

      # Should have ~500 messages (50 * 10)
      assert info.buffer_size >= 450
      assert info.buffer_size <= 550
      assert info.status == :medium_pressure
    end
  end
end
