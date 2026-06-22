defmodule Malachi.KeyspaceTest do
  use ExUnit.Case, async: true

  alias Malachi.Keyspace

  describe "size_for_bits!/2" do
    test "returns 2^bits for valid bits" do
      assert Keyspace.size_for_bits!(4) == 16
      assert Keyspace.size_for_bits!(32) == 4_294_967_296
    end

    test "raises for out-of-range bits, mentioning the label" do
      assert_raise ArgumentError, ~r/keyspace_bits/, fn ->
        Keyspace.size_for_bits!(33, "keyspace_bits")
      end

      assert_raise ArgumentError, fn -> Keyspace.size_for_bits!(0) end
    end
  end

  describe "position_of/2" do
    test "hashes into [0, size)" do
      for index <- 0..100 do
        position = Keyspace.position_of("k#{index}", 16)
        assert position in 0..15
      end
    end

    test "is deterministic" do
      assert Keyspace.position_of("k", 16) == Keyspace.position_of("k", 16)
    end
  end

  describe "within?/3" do
    test "respects the half-open block [start, end)" do
      assert Keyspace.within?(4, 4, 8)
      assert Keyspace.within?(7, 4, 8)
      refute Keyspace.within?(8, 4, 8)
      refute Keyspace.within?(3, 4, 8)
    end
  end

  describe "splittable?/2 and split_point/2" do
    test "a size-1 block is not splittable" do
      refute Keyspace.splittable?(5, 6)
      assert Keyspace.splittable?(4, 8)
    end

    test "split_point halves a power-of-two block" do
      assert Keyspace.split_point(0, 16) == 8
      assert Keyspace.split_point(8, 16) == 12
      assert Keyspace.split_point(0, 2) == 1
    end
  end

  describe "buddies?/4" do
    test "the two halves of a block are buddies (either order)" do
      assert Keyspace.buddies?(0, 8, 8, 16)
      assert Keyspace.buddies?(8, 16, 0, 8)
    end

    test "mismatched-size or non-adjacent blocks are not buddies" do
      # quarter vs half
      refute Keyspace.buddies?(0, 4, 8, 16)
      # same size but not buddies (start XOR size mismatch)
      refute Keyspace.buddies?(4, 8, 8, 12)
    end
  end
end
