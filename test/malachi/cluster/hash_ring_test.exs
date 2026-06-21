defmodule Malachi.Cluster.HashRingTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.HashRing

  # Small ring (0..15) so token placement is easy to reason about.
  defp ring(tokens) do
    Enum.reduce(tokens, HashRing.new(ring_bits: 4), fn {vnode_id, token}, ring ->
      {:ok, ring} = HashRing.add_vnode(ring, vnode_id, token)
      ring
    end)
  end

  # Whether `hash` falls in the arc (start_exclusive, end_inclusive], with wraparound.
  defp in_arc?(_hash, start, end_) when start == end_, do: true
  defp in_arc?(hash, start, end_) when start < end_, do: hash > start and hash <= end_
  defp in_arc?(hash, start, end_), do: hash > start or hash <= end_

  describe "membership" do
    test "new ring is empty and routes to an error" do
      ring = HashRing.new(ring_bits: 4)
      assert HashRing.size(ring) == 0
      assert HashRing.route(ring, "anything") == {:error, :empty}
    end

    test "add and remove vnodes" do
      ring = ring(a: 4, b: 8, c: 12)
      assert Enum.sort(HashRing.vnode_ids(ring)) == [:a, :b, :c]
      assert HashRing.size(ring) == 3

      {:ok, ring} = HashRing.remove_vnode(ring, :b)
      assert Enum.sort(HashRing.vnode_ids(ring)) == [:a, :c]
      assert HashRing.remove_vnode(ring, :b) == {:error, :not_found}
    end

    test "rejects bad placements" do
      ring = ring(a: 4)
      assert HashRing.add_vnode(ring, :b, 99) == {:error, :token_out_of_range}
      assert HashRing.add_vnode(ring, :b, 4) == {:error, :token_taken}
      assert HashRing.add_vnode(ring, :a, 8) == {:error, :already_present}
    end

    test "rejects an invalid ring_bits" do
      assert_raise ArgumentError, fn -> HashRing.new(ring_bits: 33) end
      assert_raise ArgumentError, fn -> HashRing.new(ring_bits: 0) end
    end
  end

  describe "routing" do
    test "routes keys to the vnode whose arc contains the key's hash" do
      ring = ring(a: 4, b: 8, c: 12)

      for index <- 0..200 do
        key = "key-#{index}"
        hash = :erlang.phash2(key, 16)
        {:ok, owner} = HashRing.route(ring, key)
        {:ok, {start, end_}} = HashRing.boundaries(ring, owner)
        assert in_arc?(hash, start, end_), "key #{key} (hash #{hash}) routed to #{owner} #{inspect({start, end_})}"
      end
    end

    test "a single vnode owns the whole ring" do
      ring = ring(only: 7)
      assert {:ok, {7, 7}} = HashRing.boundaries(ring, :only)

      for index <- 0..50 do
        assert {:ok, :only} = HashRing.route(ring, "k#{index}")
      end
    end

    test "routing is deterministic" do
      ring = ring(a: 4, b: 8, c: 12)
      assert HashRing.route(ring, "stable") == HashRing.route(ring, "stable")
    end

    test "boundaries of an unknown vnode fails" do
      ring = ring(a: 4)
      assert HashRing.boundaries(ring, :nope) == {:error, :not_found}
    end
  end

  describe "consistent hashing" do
    test "adding a vnode only reassigns keys that fall into its new arc" do
      ring = ring(a: 4, b: 12)
      keys = for index <- 0..300, do: "key-#{index}"
      owners_before = Map.new(keys, fn key -> {key, elem(HashRing.route(ring, key), 1)} end)

      # add a vnode at token 8, inside the arc previously owned by :b (which spanned (4,12])
      {:ok, ring} = HashRing.add_vnode(ring, :c, 8)
      {:ok, {start, end_}} = HashRing.boundaries(ring, :c)

      for key <- keys do
        {:ok, owner_after} = HashRing.route(ring, key)
        owner_before = owners_before[key]
        hash = :erlang.phash2(key, 16)

        if in_arc?(hash, start, end_) do
          assert owner_after == :c, "key in :c's new arc should move to :c"
        else
          assert owner_after == owner_before, "key outside :c's arc must not move"
        end
      end
    end
  end
end
