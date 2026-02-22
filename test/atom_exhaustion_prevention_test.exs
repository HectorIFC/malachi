defmodule MalachiMQ.AtomExhaustionPreventionTest do
  use ExUnit.Case, async: false

  @moduledoc """
  Integration tests for atom exhaustion prevention.
  Tests the max_dynamic_queues limit and verifies atom table
  stability under high queue creation workloads.
  """

  describe "max_dynamic_queues enforcement" do
    test "rejects queue creation when limit is reached" do
      # Set a very low limit for testing
      original = Application.get_env(:malachimq, :max_dynamic_queues)
      Application.put_env(:malachimq, :max_dynamic_queues, 5)

      try do
        # Get current queue count from ETS to understand baseline
        current_count = :ets.info(:malachimq_queue_config, :size)

        # Create queues up to the limit (accounting for existing queues)
        remaining = max(5 - current_count, 0)

        created =
          if remaining > 0 do
            for i <- 1..remaining do
              name = "limit_test_#{:rand.uniform(1_000_000)}_#{i}"

              case MalachiMQ.QueueConfig.create_queue(name) do
                {:ok, _} -> name
                {:error, :max_queues_reached} -> nil
              end
            end
            |> Enum.reject(&is_nil/1)
          else
            []
          end

        # Now try one more — should be rejected
        extra_name = "limit_test_overflow_#{:rand.uniform(1_000_000)}"
        result = MalachiMQ.QueueConfig.create_queue(extra_name)

        assert result == {:error, :max_queues_reached},
               "Expected :max_queues_reached, got #{inspect(result)}"

        # Cleanup created queues
        for name <- created do
          MalachiMQ.QueueConfig.delete_queue(name)
        end
      after
        if original do
          Application.put_env(:malachimq, :max_dynamic_queues, original)
        else
          Application.delete_env(:malachimq, :max_dynamic_queues)
        end
      end
    end
  end

  describe "atom count stability under load" do
    test "rapid queue creation/deletion does not leak atoms" do
      baseline = :erlang.system_info(:atom_count)

      # Create and interact with many queues
      for i <- 1..50 do
        name = "stability_test_#{:rand.uniform(1_000_000)}_#{i}"
        MalachiMQ.Queue.enqueue(name, "test payload")
      end

      after_creation = :erlang.system_info(:atom_count)
      atom_increase = after_creation - baseline

      # With anonymous ETS, creating 50 queues should add zero queue-name-based atoms
      # Allow tolerance for any incidental atoms (test infrastructure, etc.)
      assert atom_increase < 30,
             "Atom count increased by #{atom_increase} after creating 50 queues. " <>
               "Expected < 30 (0 from queue names). " <>
               "Baseline: #{baseline}, After: #{after_creation}"
    end

    test "publishing many messages to different queues keeps atoms stable" do
      baseline = :erlang.system_info(:atom_count)

      # Publish to 100 different queues
      for i <- 1..100 do
        name = "msg_atom_test_#{:rand.uniform(1_000_000)}_#{i}"
        MalachiMQ.Queue.enqueue(name, "payload #{i}", %{"key" => "value"})
      end

      after_publish = :erlang.system_info(:atom_count)
      atom_increase = after_publish - baseline

      # Previously this would create 300+ atoms (3 ETS tables per queue)
      assert atom_increase < 50,
             "Publishing to 100 queues created #{atom_increase} atoms. " <>
               "This suggests dynamic atom creation is still happening."
    end
  end

  describe "edge cases" do
    test "queue names that look like atoms don't cause issues" do
      # These strings look like atom literals but should be handled as strings
      tricky_names = [
        "true",
        "false",
        "nil",
        "ok",
        "error",
        "undefined"
      ]

      baseline = :erlang.system_info(:atom_count)

      for name <- tricky_names do
        MalachiMQ.Queue.enqueue(name, "test")
      end

      after_creation = :erlang.system_info(:atom_count)
      atom_increase = after_creation - baseline

      assert atom_increase < 20,
             "Atom-like queue names created #{atom_increase} new atoms"
    end

    test "concurrent queue creation doesn't cause atom leaks" do
      baseline = :erlang.system_info(:atom_count)

      tasks =
        for i <- 1..20 do
          Task.async(fn ->
            name = "concurrent_atom_test_#{:rand.uniform(1_000_000)}_#{i}"
            MalachiMQ.Queue.enqueue(name, "concurrent payload")
          end)
        end

      Task.await_many(tasks, 10_000)

      after_concurrent = :erlang.system_info(:atom_count)
      atom_increase = after_concurrent - baseline

      assert atom_increase < 30,
             "Concurrent queue creation created #{atom_increase} atoms"
    end
  end
end
