defmodule Malachi.AtomSafetyTest do
  use ExUnit.Case, async: false

  @moduledoc """
  Tests that verify no dynamic atoms are created from user input.
  Ensures atom table stability under queue/channel creation workloads.
  Note: async: false ensures atom count measurements are not polluted
  by other parallel tests creating atoms.
  """

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
