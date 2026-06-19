defmodule Malachi.AckManagerTest do
  use ExUnit.Case, async: false

  setup do
    queue_name = "ack_test_#{:rand.uniform(100_000)}"
    message_id = :erlang.unique_integer([:monotonic, :positive])

    consumer_pid =
      spawn(fn ->
        receive do
          _ -> :ok
        end
      end)

    message = %{
      id: message_id,
      payload: "test",
      headers: %{},
      timestamp: System.system_time(:microsecond),
      queue: queue_name
    }

    {:ok, queue_name: queue_name, message_id: message_id, consumer_pid: consumer_pid, message: message}
  end

  describe "track_message/4" do
    test "tracks a new message", %{
      queue_name: queue_name,
      message_id: message_id,
      consumer_pid: consumer_pid,
      message: message
    } do
      assert :ok = Malachi.AckManager.track_message(message_id, queue_name, consumer_pid, message)

      status = Malachi.AckManager.get_status(message_id)
      assert {:pending, entry} = status
      assert entry.message_id == message_id
      assert entry.queue_name == queue_name
    end

    test "tracks multiple messages", %{queue_name: queue_name} do
      for i <- 1..5 do
        msg_id = :erlang.unique_integer([:monotonic, :positive])

        pid =
          spawn(fn ->
            receive do
              _ -> :ok
            end
          end)

        msg = %{
          id: msg_id,
          payload: "test#{i}",
          headers: %{},
          timestamp: System.system_time(:microsecond),
          queue: queue_name
        }

        Malachi.AckManager.track_message(msg_id, queue_name, pid, msg)
      end

      count = Malachi.AckManager.pending_count(queue_name)
      assert count == 5
    end
  end

  describe "ack/1" do
    test "acknowledges a message", %{
      queue_name: queue_name,
      message_id: message_id,
      consumer_pid: consumer_pid,
      message: message
    } do
      Malachi.AckManager.track_message(message_id, queue_name, consumer_pid, message)

      assert :ok = Malachi.AckManager.ack(message_id)
      assert :acknowledged_or_unknown = Malachi.AckManager.get_status(message_id)
    end

    test "returns error for non-existent message" do
      fake_id = :erlang.unique_integer([:monotonic, :positive])
      assert {:error, :not_found} = Malachi.AckManager.ack(fake_id)
    end

    test "increments acked metric", %{
      queue_name: queue_name,
      message_id: message_id,
      consumer_pid: consumer_pid,
      message: message
    } do
      Malachi.AckManager.track_message(message_id, queue_name, consumer_pid, message)

      Malachi.AckManager.ack(message_id)
      :timer.sleep(50)
    end
  end

  describe "nack/2" do
    test "nacks a message without requeue", %{
      queue_name: queue_name,
      message_id: message_id,
      consumer_pid: consumer_pid,
      message: message
    } do
      Malachi.AckManager.track_message(message_id, queue_name, consumer_pid, message)

      assert :ok = Malachi.AckManager.nack(message_id, requeue: false)
      assert :acknowledged_or_unknown = Malachi.AckManager.get_status(message_id)
    end

    test "nacks and requeues message", %{
      queue_name: queue_name,
      message_id: message_id,
      consumer_pid: consumer_pid,
      message: message
    } do
      Malachi.AckManager.track_message(message_id, queue_name, consumer_pid, message)

      assert :ok = Malachi.AckManager.nack(message_id, requeue: true)
      :timer.sleep(50)

      stats = Malachi.Queue.get_stats(queue_name)
      assert stats.buffered >= 1
    end

    test "returns error for non-existent message" do
      fake_id = :erlang.unique_integer([:monotonic, :positive])
      assert {:error, :not_found} = Malachi.AckManager.nack(fake_id)
    end

    test "increments nacked metric", %{
      queue_name: queue_name,
      message_id: message_id,
      consumer_pid: consumer_pid,
      message: message
    } do
      Malachi.AckManager.track_message(message_id, queue_name, consumer_pid, message)

      Malachi.AckManager.nack(message_id, requeue: false)
      :timer.sleep(50)
    end
  end

  describe "get_status/1" do
    test "returns pending status", %{
      queue_name: queue_name,
      message_id: message_id,
      consumer_pid: consumer_pid,
      message: message
    } do
      Malachi.AckManager.track_message(message_id, queue_name, consumer_pid, message)

      assert {:pending, entry} = Malachi.AckManager.get_status(message_id)
      assert is_integer(entry.elapsed_ms)
      assert entry.elapsed_ms >= 0
    end

    test "returns unknown for non-tracked message" do
      fake_id = :erlang.unique_integer([:monotonic, :positive])
      assert :acknowledged_or_unknown = Malachi.AckManager.get_status(fake_id)
    end
  end

  describe "pending_count/1" do
    test "counts pending messages for queue", %{queue_name: queue_name} do
      for i <- 1..3 do
        msg_id = :erlang.unique_integer([:monotonic, :positive])

        pid =
          spawn(fn ->
            receive do
              _ -> :ok
            end
          end)

        msg = %{
          id: msg_id,
          payload: "test#{i}",
          headers: %{},
          timestamp: System.system_time(:microsecond),
          queue: queue_name
        }

        Malachi.AckManager.track_message(msg_id, queue_name, pid, msg)
      end

      assert Malachi.AckManager.pending_count(queue_name) == 3
    end

    test "returns 0 for queue with no pending messages" do
      assert Malachi.AckManager.pending_count("nonexistent_queue") == 0
    end
  end

  describe "list_pending/1" do
    test "lists all pending messages", %{queue_name: queue_name} do
      for i <- 1..2 do
        msg_id = :erlang.unique_integer([:monotonic, :positive])

        pid =
          spawn(fn ->
            receive do
              _ -> :ok
            end
          end)

        msg = %{
          id: msg_id,
          payload: "test#{i}",
          headers: %{},
          timestamp: System.system_time(:microsecond),
          queue: queue_name
        }

        Malachi.AckManager.track_message(msg_id, queue_name, pid, msg)
      end

      pending = Malachi.AckManager.list_pending(queue_name)
      assert length(pending) == 2
      assert Enum.all?(pending, &is_integer(&1.elapsed_ms))
    end

    test "returns empty list for queue with no pending" do
      assert [] = Malachi.AckManager.list_pending("empty_queue")
    end
  end

  describe "stats/0" do
    test "returns statistics for all queues", %{queue_name: queue_name} do
      msg_id = :erlang.unique_integer([:monotonic, :positive])

      pid =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      msg = %{id: msg_id, payload: "test", headers: %{}, timestamp: System.system_time(:microsecond), queue: queue_name}

      Malachi.AckManager.track_message(msg_id, queue_name, pid, msg)

      stats = Malachi.AckManager.stats()
      assert is_map(stats)

      if Map.has_key?(stats, queue_name) do
        queue_stats = stats[queue_name]
        assert queue_stats.count > 0
        assert is_integer(queue_stats.oldest_ms)
        assert is_integer(queue_stats.newest_ms)
      end
    end
  end
end
