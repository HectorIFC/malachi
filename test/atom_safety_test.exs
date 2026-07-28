defmodule Malachi.AtomSafetyTest do
  use ExUnit.Case, async: false

  @moduledoc """
  Tests for AtomMonitor, the guard against atom-table exhaustion. `async: false` keeps atom-count
  measurements from being polluted by other parallel tests creating atoms.
  """

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
