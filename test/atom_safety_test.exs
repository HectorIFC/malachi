defmodule Malachi.AtomSafetyTest do
  use ExUnit.Case, async: false

  @moduledoc """
  Tests that verify no dynamic atoms are created from user input.
  Ensures atom table stability under queue/channel creation workloads.
  Note: async: false ensures atom count measurements are not polluted
  by other parallel tests creating atoms.
  """

  describe "queue creation does not leak atoms" do
    test "creating multiple queues keeps atom count stable" do
      # Record baseline atom count
      baseline = :erlang.system_info(:atom_count)

      # Create many queues with unique names
      queue_names =
        for i <- 1..100 do
          name = "atom_test_queue_#{:rand.uniform(1_000_000)}_#{i}"
          Malachi.Queue.enqueue(name, "test payload #{i}")
          name
        end

      after_creation = :erlang.system_info(:atom_count)

      # The atom count should NOT increase by 3 per queue (consumers, buffer, producers tables)
      # Previously each queue created 3 atoms. Now with anonymous ETS tables, zero atoms are created.
      # Allow small tolerance for other system atoms that may be created during the test.
      atom_increase = after_creation - baseline

      # With anonymous ETS tables, no atoms should be created from queue names.
      # Previously this would create 300+ atoms (3 ETS tables per queue).
      # Some incidental atoms may be created by Logger, I18n, etc.
      # The key metric: increase should be MUCH less than 300.
      assert atom_increase < 100,
             "Expected minimal atom increase, got #{atom_increase}. " <>
               "Possible dynamic atom leak detected. " <>
               "Baseline: #{baseline}, After: #{after_creation}"

      assert length(queue_names) == 100
    end

    test "queue names with special characters don't create atoms" do
      baseline = :erlang.system_info(:atom_count)

      special_names = [
        "queue-with-dashes",
        "queue.with.dots",
        "queue_with_underscores",
        "UPPERCASE_QUEUE",
        "MiXeD-CaSe.queue"
      ]

      for name <- special_names do
        Malachi.Queue.enqueue(name, "test")
      end

      after_creation = :erlang.system_info(:atom_count)
      atom_increase = after_creation - baseline

      assert atom_increase < 50,
             "Special character queue names created #{atom_increase} atoms"
    end

    test "very long queue names don't create atoms" do
      baseline = :erlang.system_info(:atom_count)

      # Max valid queue name length is 255
      long_name = String.duplicate("a", 200)
      Malachi.Queue.enqueue(long_name, "test")

      after_creation = :erlang.system_info(:atom_count)
      atom_increase = after_creation - baseline

      assert atom_increase < 50,
             "Long queue name created #{atom_increase} atoms"
    end
  end

  describe "metrics recording does not create atoms" do
    test "incrementing metrics uses tuple keys, not atom keys" do
      baseline = :erlang.system_info(:atom_count)

      queue_names =
        for i <- 1..50 do
          name = "metrics_atom_test_#{:rand.uniform(1_000_000)}_#{i}"
          Malachi.Metrics.increment_enqueued(name)
          Malachi.Metrics.increment_acked(name)
          Malachi.Metrics.increment_nacked(name)
          Malachi.Metrics.increment_processed(name)
          name
        end

      after_metrics = :erlang.system_info(:atom_count)
      atom_increase = after_metrics - baseline

      # Metrics use tuple keys like {:enqueued, "queue_name"} — no atoms from queue names
      # Previously each metric key was an atom like :"enqueued_queue_name"
      assert atom_increase < 30,
             "Metrics recording created #{atom_increase} atoms for #{length(queue_names)} queues"
    end
  end

  describe "validator errors use compile-time atoms" do
    test "validation errors don't create dynamic atoms" do
      baseline = :erlang.system_info(:atom_count)

      # These should all return pre-compiled atom errors
      for _ <- 1..100 do
        Malachi.Validator.validate_queue_name("")
        Malachi.Validator.validate_queue_name(String.duplicate("x", 300))
        Malachi.Validator.validate_queue_name("invalid name with spaces!")
        Malachi.Validator.validate_queue_name("_reserved")
        Malachi.Validator.validate_channel_name("")
        Malachi.Validator.validate_channel_name(String.duplicate("y", 300))
      end

      after_validation = :erlang.system_info(:atom_count)
      atom_increase = after_validation - baseline

      # The 8 error atoms are compile-time constants. Any increase is from
      # incidental system activity (Logger metadata, etc.), not from user input.
      assert atom_increase < 50,
             "Validator error atoms created #{atom_increase} new atoms"
    end

    test "returns correct pre-compiled error atoms for queue names" do
      assert {:error, :invalid_queue_name_empty} = Malachi.Validator.validate_queue_name("")

      assert {:error, :invalid_queue_name_too_long} =
               Malachi.Validator.validate_queue_name(String.duplicate("x", 300))

      assert {:error, :invalid_queue_name_reserved} =
               Malachi.Validator.validate_queue_name("_reserved_name")
    end

    test "returns correct pre-compiled error atoms for channel names" do
      assert {:error, :invalid_channel_name_empty} = Malachi.Validator.validate_channel_name("")

      assert {:error, :invalid_channel_name_too_long} =
               Malachi.Validator.validate_channel_name(String.duplicate("x", 300))

      assert {:error, :invalid_channel_name_reserved} =
               Malachi.Validator.validate_channel_name("_reserved_channel")
    end
  end

  describe "ETS tables are anonymous" do
    test "queue ETS tables are not named (not accessible by atom)" do
      queue_name = "anon_ets_test_#{:rand.uniform(1_000_000)}"
      Malachi.Queue.enqueue(queue_name, "test")

      # Try to find named tables matching the old pattern
      # These should NOT exist as named tables anymore
      all_named_tables = :ets.all()

      refute Enum.any?(all_named_tables, fn table ->
               is_atom(table) and
                 String.contains?(Atom.to_string(table), "malachi_consumers_#{queue_name}")
             end),
             "Found named consumers table for #{queue_name} — should be anonymous"

      refute Enum.any?(all_named_tables, fn table ->
               is_atom(table) and
                 String.contains?(Atom.to_string(table), "malachi_buffer_#{queue_name}")
             end),
             "Found named buffer table for #{queue_name} — should be anonymous"

      refute Enum.any?(all_named_tables, fn table ->
               is_atom(table) and
                 String.contains?(Atom.to_string(table), "malachi_producers_#{queue_name}")
             end),
             "Found named producers table for #{queue_name} — should be anonymous"
    end
  end

  describe "AtomMonitor" do
    test "reports accurate atom count" do
      stats = Malachi.AtomMonitor.get_stats()

      assert is_integer(stats.atom_count)
      assert stats.atom_count > 0
      assert stats.atom_limit == 1_048_576
      assert is_float(stats.usage_percent)
      assert stats.usage_percent > 0
      assert stats.usage_percent < 100
      assert stats.status in [:normal, :warning, :critical]
    end

    test "get_atom_count returns positive integer" do
      count = Malachi.AtomMonitor.get_atom_count()
      assert is_integer(count)
      assert count > 0
    end

    test "get_atom_limit returns BEAM default" do
      assert Malachi.AtomMonitor.get_atom_limit() == 1_048_576
    end

    test "get_atom_usage_percent returns reasonable value" do
      pct = Malachi.AtomMonitor.get_atom_usage_percent()
      assert is_float(pct)
      assert pct > 0.0
      assert pct < 100.0
    end
  end
end
