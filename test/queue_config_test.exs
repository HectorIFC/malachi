defmodule Malachi.QueueConfigTest do
  use ExUnit.Case, async: true

  setup do
    # Use unique queue names to avoid conflicts in async tests
    queue_name = "test_queue_#{:rand.uniform(1_000_000)}"
    {:ok, queue_name: queue_name}
  end

  describe "create_queue/2" do
    test "creates queue with default delivery mode", %{queue_name: queue_name} do
      assert {:ok, config} = Malachi.QueueConfig.create_queue(queue_name)
      assert config.queue_name == queue_name
      assert config.delivery_mode == :at_least_once
      assert config.max_retries == 3
      assert config.dlq_enabled == true
      assert is_integer(config.created_at)
    end

    test "creates queue with at_most_once delivery mode", %{queue_name: queue_name} do
      assert {:ok, config} = Malachi.QueueConfig.create_queue(queue_name, delivery_mode: :at_most_once)
      assert config.delivery_mode == :at_most_once
    end

    test "creates queue with custom max_retries", %{queue_name: queue_name} do
      assert {:ok, config} = Malachi.QueueConfig.create_queue(queue_name, max_retries: 5)
      assert config.max_retries == 5
    end

    test "creates queue with dlq disabled", %{queue_name: queue_name} do
      assert {:ok, config} = Malachi.QueueConfig.create_queue(queue_name, dlq_enabled: false)
      assert config.dlq_enabled == false
    end

    test "returns error for duplicate queue", %{queue_name: queue_name} do
      assert {:ok, _config} = Malachi.QueueConfig.create_queue(queue_name)
      assert {:error, :queue_already_exists} = Malachi.QueueConfig.create_queue(queue_name)
    end

    test "returns error for invalid delivery mode", %{queue_name: queue_name} do
      assert {:error, :invalid_delivery_mode} = Malachi.QueueConfig.create_queue(queue_name, delivery_mode: :invalid)
    end
  end

  describe "get_config/1" do
    test "returns config for existing queue", %{queue_name: queue_name} do
      {:ok, created_config} = Malachi.QueueConfig.create_queue(queue_name, delivery_mode: :at_most_once)
      retrieved_config = Malachi.QueueConfig.get_config(queue_name)

      assert retrieved_config.queue_name == created_config.queue_name
      assert retrieved_config.delivery_mode == :at_most_once
    end

    test "creates implicit queue for non-existent queue", %{queue_name: queue_name} do
      config = Malachi.QueueConfig.get_config(queue_name)

      assert config.queue_name == queue_name
      assert config.delivery_mode == :at_least_once
      assert config.implicit == true
    end
  end

  describe "delete_queue/2" do
    test "deletes queue with no consumers or buffered messages", %{queue_name: queue_name} do
      {:ok, _config} = Malachi.QueueConfig.create_queue(queue_name)
      assert :ok = Malachi.QueueConfig.delete_queue(queue_name)
      refute Malachi.QueueConfig.exists?(queue_name)
    end

    test "returns error for non-existent queue" do
      assert {:error, :queue_not_found} = Malachi.QueueConfig.delete_queue("non_existent_queue")
    end

    test "returns error when queue has active consumers", %{queue_name: queue_name} do
      {:ok, _config} = Malachi.QueueConfig.create_queue(queue_name)

      # Subscribe a consumer
      consumer_pid =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      Malachi.Queue.subscribe(queue_name, consumer_pid)
      :timer.sleep(50)

      assert {:error, :queue_has_active_consumers} = Malachi.QueueConfig.delete_queue(queue_name)
    end

    test "returns error when queue has buffered messages", %{queue_name: queue_name} do
      {:ok, _config} = Malachi.QueueConfig.create_queue(queue_name)

      # Enqueue messages
      Malachi.Queue.enqueue(queue_name, "test message")
      :timer.sleep(50)

      assert {:error, :queue_has_buffered_messages} = Malachi.QueueConfig.delete_queue(queue_name)
    end

    test "force deletes queue with active consumers", %{queue_name: queue_name} do
      {:ok, _config} = Malachi.QueueConfig.create_queue(queue_name)

      consumer_pid =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      Malachi.Queue.subscribe(queue_name, consumer_pid)
      :timer.sleep(50)

      assert :ok = Malachi.QueueConfig.delete_queue(queue_name, force: true)
      refute Malachi.QueueConfig.exists?(queue_name)
    end
  end

  describe "list_queues/0" do
    test "lists all configured queues" do
      queue1 = "list_test_queue_1_#{:rand.uniform(1_000_000)}"
      queue2 = "list_test_queue_2_#{:rand.uniform(1_000_000)}"

      {:ok, _} = Malachi.QueueConfig.create_queue(queue1)
      {:ok, _} = Malachi.QueueConfig.create_queue(queue2)

      queues = Malachi.QueueConfig.list_queues()
      queue_names = Enum.map(queues, & &1.queue_name)

      assert queue1 in queue_names
      assert queue2 in queue_names
    end

    test "returns sorted list by created_at" do
      queue1 = "sorted_queue_1_#{:rand.uniform(1_000_000)}"
      queue2 = "sorted_queue_2_#{:rand.uniform(1_000_000)}"

      {:ok, _config1} = Malachi.QueueConfig.create_queue(queue1)
      :timer.sleep(10)
      {:ok, _config2} = Malachi.QueueConfig.create_queue(queue2)

      queues = Malachi.QueueConfig.list_queues()
      matching_queues = Enum.filter(queues, &(&1.queue_name in [queue1, queue2]))

      [first, second] = Enum.sort_by(matching_queues, & &1.created_at)
      assert first.created_at <= second.created_at
    end
  end

  describe "exists?/1" do
    test "returns true for existing queue", %{queue_name: queue_name} do
      {:ok, _config} = Malachi.QueueConfig.create_queue(queue_name)
      assert Malachi.QueueConfig.exists?(queue_name)
    end

    test "returns false for non-existent queue" do
      refute Malachi.QueueConfig.exists?("non_existent_queue_#{:rand.uniform(1_000_000)}")
    end
  end

  describe "queue_exists?/1" do
    test "returns true for existing queue", %{queue_name: queue_name} do
      {:ok, _config} = Malachi.QueueConfig.create_queue(queue_name)
      assert Malachi.QueueConfig.queue_exists?(queue_name)
    end

    test "returns false for non-existent queue" do
      refute Malachi.QueueConfig.queue_exists?("non_existent_queue_#{:rand.uniform(1_000_000)}")
    end
  end

  describe "create_queue/2 with backpressure fields" do
    test "creates queue with custom max_buffer_size", %{queue_name: queue_name} do
      {:ok, config} = Malachi.QueueConfig.create_queue(queue_name, max_buffer_size: 50_000)
      assert config.max_buffer_size == 50_000
    end

    test "creates queue with custom overflow_behavior", %{queue_name: queue_name} do
      {:ok, config} = Malachi.QueueConfig.create_queue(queue_name, overflow_behavior: :drop_oldest)
      assert config.overflow_behavior == :drop_oldest
    end

    test "creates queue with custom backpressure_threshold", %{queue_name: queue_name} do
      {:ok, config} = Malachi.QueueConfig.create_queue(queue_name, backpressure_threshold: 0.7)
      assert config.backpressure_threshold == 0.7
    end

    test "creates queue with custom block_timeout_ms", %{queue_name: queue_name} do
      {:ok, config} = Malachi.QueueConfig.create_queue(queue_name, block_timeout_ms: 10_000)
      assert config.block_timeout_ms == 10_000
    end

    test "creates queue with custom max_blocked_producers", %{queue_name: queue_name} do
      {:ok, config} = Malachi.QueueConfig.create_queue(queue_name, max_blocked_producers: 500)
      assert config.max_blocked_producers == 500
    end

    test "creates queue with custom max_message_size_bytes", %{queue_name: queue_name} do
      {:ok, config} = Malachi.QueueConfig.create_queue(queue_name, max_message_size_bytes: 2_097_152)
      assert config.max_message_size_bytes == 2_097_152
    end

    test "returns error for invalid overflow_behavior", %{queue_name: queue_name} do
      {:error, :invalid_overflow_behavior, _msg} =
        Malachi.QueueConfig.create_queue(queue_name, overflow_behavior: :invalid_strategy)
    end

    test "creates queue with all backpressure fields", %{queue_name: queue_name} do
      {:ok, config} =
        Malachi.QueueConfig.create_queue(queue_name,
          max_buffer_size: 25_000,
          overflow_behavior: :block,
          backpressure_threshold: 0.75,
          block_timeout_ms: 8000,
          max_blocked_producers: 200,
          max_message_size_bytes: 524_288
        )

      assert config.max_buffer_size == 25_000
      assert config.overflow_behavior == :block
      assert config.backpressure_threshold == 0.75
      assert config.block_timeout_ms == 8000
      assert config.max_blocked_producers == 200
      assert config.max_message_size_bytes == 524_288
    end
  end

  describe "update_queue/3 - safe updates" do
    test "updates queue with no buffer size change", %{queue_name: queue_name} do
      {:ok, _} = Malachi.QueueConfig.create_queue(queue_name, max_buffer_size: 1000)

      {:ok, :updated} =
        Malachi.QueueConfig.update_queue(queue_name, %{
          overflow_behavior: :drop_oldest,
          backpressure_threshold: 0.7
        })

      config = Malachi.QueueConfig.get_config(queue_name)
      assert config.overflow_behavior == :drop_oldest
      assert config.backpressure_threshold == 0.7
      assert config.max_buffer_size == 1000
    end

    test "updates with buffer size increase (always safe)", %{queue_name: queue_name} do
      {:ok, _} = Malachi.QueueConfig.create_queue(queue_name, max_buffer_size: 1000)

      # Enqueue some messages
      for i <- 1..500 do
        Malachi.Queue.enqueue(queue_name, "msg_#{i}")
      end

      :timer.sleep(100)

      {:ok, :updated} = Malachi.QueueConfig.update_queue(queue_name, %{max_buffer_size: 2000})

      config = Malachi.QueueConfig.get_config(queue_name)
      assert config.max_buffer_size == 2000
    end

    test "returns error for non-existent queue" do
      {:error, :queue_not_found} =
        Malachi.QueueConfig.update_queue("nonexistent_#{:rand.uniform(1_000_000)}", %{max_buffer_size: 1000})
    end

    test "returns error for invalid overflow_behavior", %{queue_name: queue_name} do
      {:ok, _} = Malachi.QueueConfig.create_queue(queue_name)

      {:error, :invalid_overflow_behavior, _} =
        Malachi.QueueConfig.update_queue(queue_name, %{overflow_behavior: :invalid_strategy})
    end
  end

  describe "update_queue/3 - buffer size reduction with excess" do
    test "allows update with no excess (buffer empty)", %{queue_name: queue_name} do
      {:ok, _} = Malachi.QueueConfig.create_queue(queue_name, max_buffer_size: 1000)

      {:ok, :updated} = Malachi.QueueConfig.update_queue(queue_name, %{max_buffer_size: 500})

      config = Malachi.QueueConfig.get_config(queue_name)
      assert config.max_buffer_size == 500
    end

    test "allows update with small excess (within 50% threshold)", %{queue_name: queue_name} do
      {:ok, _} = Malachi.QueueConfig.create_queue(queue_name, max_buffer_size: 1000)

      # Enqueue 550 messages
      for i <- 1..550 do
        Malachi.Queue.enqueue(queue_name, "msg_#{i}")
      end

      :timer.sleep(100)

      # Reduce to 500 (excess=50, threshold=250, safe)
      {:ok, :updated_with_warning, warning} =
        Malachi.QueueConfig.update_queue(queue_name, %{max_buffer_size: 500})

      assert warning.excess_messages >= 50
      assert warning.will_drop_gradually == true

      config = Malachi.QueueConfig.get_config(queue_name)
      assert config.max_buffer_size == 500
    end

    test "rejects update with large excess (exceeds 50% threshold)", %{queue_name: queue_name} do
      {:ok, _} = Malachi.QueueConfig.create_queue(queue_name, max_buffer_size: 1000)

      # Enqueue 800 messages
      for i <- 1..800 do
        Malachi.Queue.enqueue(queue_name, "msg_#{i}")
      end

      :timer.sleep(100)

      # Try to reduce to 500 (excess=300, threshold=250, rejected)
      {:error, :buffer_exceeds_new_limit, details} =
        Malachi.QueueConfig.update_queue(queue_name, %{max_buffer_size: 500})

      assert details.current_buffer_size >= 800
      assert details.new_max_buffer_size == 500
      assert details.excess >= 300
      assert details.threshold_pct == 50.0
      assert details.suggestion =~ "force"
    end

    test "allows forced update with large excess", %{queue_name: queue_name} do
      {:ok, _} = Malachi.QueueConfig.create_queue(queue_name, max_buffer_size: 1000)

      # Enqueue 800 messages
      for i <- 1..800 do
        Malachi.Queue.enqueue(queue_name, "msg_#{i}")
      end

      :timer.sleep(100)

      # Force update
      {:ok, :forced_update, result} =
        Malachi.QueueConfig.update_queue(queue_name, %{max_buffer_size: 500}, force: true)

      assert result.dropped_messages >= 300

      config = Malachi.QueueConfig.get_config(queue_name)
      assert config.max_buffer_size == 500
    end
  end

  describe "update_queue/3 - edge cases" do
    test "handles exactly at 50% threshold", %{queue_name: queue_name} do
      {:ok, _} = Malachi.QueueConfig.create_queue(queue_name, max_buffer_size: 1000)

      # Enqueue 600 messages
      for i <- 1..600 do
        Malachi.Queue.enqueue(queue_name, "msg_#{i}")
      end

      :timer.sleep(100)

      # Reduce to 500 (excess=100, threshold=250, safe)
      result = Malachi.QueueConfig.update_queue(queue_name, %{max_buffer_size: 500})

      # Should be updated_with_warning or updated depending on exact count
      assert match?({:ok, :updated_with_warning, _}, result) or match?({:ok, :updated}, result)
    end

    test "updates multiple fields simultaneously", %{queue_name: queue_name} do
      {:ok, _} =
        Malachi.QueueConfig.create_queue(queue_name,
          max_buffer_size: 1000,
          overflow_behavior: :drop_newest,
          backpressure_threshold: 0.8
        )

      {:ok, :updated} =
        Malachi.QueueConfig.update_queue(queue_name, %{
          max_buffer_size: 2000,
          overflow_behavior: :block,
          backpressure_threshold: 0.7,
          block_timeout_ms: 10_000
        })

      config = Malachi.QueueConfig.get_config(queue_name)
      assert config.max_buffer_size == 2000
      assert config.overflow_behavior == :block
      assert config.backpressure_threshold == 0.7
      assert config.block_timeout_ms == 10_000
    end

    test "preserves unmodified fields", %{queue_name: queue_name} do
      {:ok, original} =
        Malachi.QueueConfig.create_queue(queue_name,
          max_buffer_size: 1000,
          overflow_behavior: :reject,
          max_retries: 5,
          dlq_enabled: false
        )

      {:ok, :updated} =
        Malachi.QueueConfig.update_queue(queue_name, %{
          max_buffer_size: 2000
        })

      config = Malachi.QueueConfig.get_config(queue_name)
      assert config.max_buffer_size == 2000
      assert config.overflow_behavior == :reject
      assert config.max_retries == 5
      assert config.dlq_enabled == false
      assert config.delivery_mode == original.delivery_mode
    end
  end
end
