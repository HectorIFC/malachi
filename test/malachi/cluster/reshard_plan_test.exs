defmodule Malachi.Cluster.ReshardPlanTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.ReshardPlan

  @ring_size Integer.pow(2, 32)

  # A ring of `count` evenly-spaced vnodes (the boot geometry from Application.sharded_vnodes/2).
  defp even_ring(count) do
    Enum.reduce(0..(count - 1), HashRing.new(), fn i, ring ->
      {:ok, ring} = HashRing.add_vnode(ring, :"base_vn_#{i}", div(i * @ring_size, count))
      ring
    end)
  end

  defp apply_steps(ring, steps) do
    Enum.reduce(steps, ring, fn %{new_vnode_id: id, token: token}, r ->
      {:ok, r} = HashRing.add_vnode(r, id, token)
      r
    end)
  end

  defp arcs(ring) do
    for id <- HashRing.vnode_ids(ring) do
      {:ok, {start, stop}} = HashRing.boundaries(ring, id)

      cond do
        stop > start -> stop - start
        stop < start -> @ring_size - start + stop
        true -> @ring_size
      end
    end
  end

  property "grows the ring to exactly the target count" do
    check all(current <- integer(1..8), grow <- integer(1..8)) do
      target = current + grow
      ring = even_ring(current)

      assert {:ok, steps} = ReshardPlan.plan(ring, target)
      assert length(steps) == grow
      assert HashRing.size(apply_steps(ring, steps)) == target
    end
  end

  property "is deterministic (same ring + target yields the same plan)" do
    check all(current <- integer(1..8), target <- integer(1..16)) do
      ring = even_ring(current)
      assert ReshardPlan.plan(ring, target) == ReshardPlan.plan(ring, target)
    end
  end

  property "splitting the largest arc never increases the maximum arc (stays balanced)" do
    check all(current <- integer(1..8), grow <- integer(1..8)) do
      ring = even_ring(current)
      {:ok, steps} = ReshardPlan.plan(ring, current + grow)

      assert Enum.max(arcs(apply_steps(ring, steps))) <= Enum.max(arcs(ring))
    end
  end

  test "a target equal to the current count is a no-op" do
    assert {:ok, []} = ReshardPlan.plan(even_ring(4), 4)
  end

  test "a smaller target is rejected (grow-only)" do
    assert {:error, :cannot_shrink} = ReshardPlan.plan(even_ring(4), 2)
  end

  test "an empty ring is rejected" do
    assert {:error, :empty_ring} = ReshardPlan.plan(HashRing.new(), 4)
  end

  test "splitting a single vnode adds one at the opposite side of the ring" do
    ring = even_ring(1)
    assert {:ok, [%{token: token, split_from: :base_vn_0}]} = ReshardPlan.plan(ring, 2)
    assert token == div(@ring_size, 2)
    assert HashRing.size(apply_steps(ring, [%{new_vnode_id: :"vn_#{token}", token: token}])) == 2
  end

  test "new vnode ids are unique and derived from the token" do
    {:ok, steps} = ReshardPlan.plan(even_ring(3), 8)
    ids = Enum.map(steps, & &1.new_vnode_id)

    assert length(Enum.uniq(ids)) == length(ids)
    assert Enum.all?(steps, fn %{new_vnode_id: id, token: token} -> id == :"vn_#{token}" end)
  end

  test "a custom prefix is honored" do
    {:ok, [step | _]} = ReshardPlan.plan(even_ring(2), 3, prefix: "shard")
    assert step.new_vnode_id == :"shard_#{step.token}"
  end
end
