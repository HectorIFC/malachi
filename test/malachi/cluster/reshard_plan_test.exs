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

  # A ring of `count` vnodes at arbitrary distinct tokens (the realistic post-split geometry, where arcs
  # are uneven), as opposed to `even_ring/1`. Distinct tokens give distinct vnode ids.
  defp unbalanced_ring(tokens) do
    Enum.reduce(tokens, HashRing.new(), fn token, ring ->
      {:ok, ring} = HashRing.add_vnode(ring, :"base_#{token}", token)
      ring
    end)
  end

  defp ring_tokens, do: uniq_list_of(integer(0..(@ring_size - 1)), min_length: 1, max_length: 8)

  # The set of tokens a ring owns: each vnode's arc is `(start, stop]`, so `stop` is its own token.
  defp token_set(ring) do
    for id <- HashRing.vnode_ids(ring), into: MapSet.new() do
      {:ok, {_start, stop}} = HashRing.boundaries(ring, id)
      stop
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

  property "on an arbitrary (unbalanced) ring, a plan preserves every token and never worsens balance" do
    check all(tokens <- ring_tokens(), grow <- integer(1..8)) do
      ring = unbalanced_ring(tokens)
      target = HashRing.size(ring) + grow
      {:ok, steps} = ReshardPlan.plan(ring, target)
      grown = apply_steps(ring, steps)

      assert HashRing.size(grown) == target
      assert MapSet.subset?(token_set(ring), token_set(grown)), "existing tokens must never move"
      assert Enum.max(arcs(grown)) <= Enum.max(arcs(ring)), "the max arc must not grow"
    end
  end

  property "resumability: re-planning from a partially grown ring yields exactly the remaining steps" do
    check all(tokens <- ring_tokens(), grow <- integer(1..8), taken <- integer(0..8)) do
      ring = unbalanced_ring(tokens)
      target = HashRing.size(ring) + grow
      {:ok, all_steps} = ReshardPlan.plan(ring, target)

      # apply an arbitrary prefix of the plan, then re-plan to the same target: a crashed reshard resumes
      # by re-planning from the current ring, so the remaining plan must be exactly what is left.
      k = min(taken, grow)
      partial = apply_steps(ring, Enum.take(all_steps, k))

      assert ReshardPlan.plan(partial, target) == {:ok, Enum.drop(all_steps, k)}
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
