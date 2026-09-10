defmodule Malachi.Cluster.RingTopologyPlacementTest do
  @moduledoc """
  `RingTopology.vnode_placement/1`: the bridge that lets a ring restored from the durable store drop
  into exactly the wiring an environment-derived one uses.
  """
  use ExUnit.Case, async: true

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.RingTopology

  defp ring_with(vnodes) do
    Enum.reduce(vnodes, HashRing.new(), fn {id, token}, ring ->
      {:ok, ring} = HashRing.add_vnode(ring, id, token)
      ring
    end)
  end

  test "returns {vnode_id, token, nodes} ordered by token" do
    ring = ring_with([{:vn_b, 900}, {:vn_a, 100}, {:vn_c, 500}])
    placements = %{vn_a: [:n1@h], vn_b: [:n2@h, :n3@h], vn_c: [:n3@h]}

    assert RingTopology.vnode_placement(RingTopology.new(ring, placements)) == [
             {:vn_a, 100, [:n1@h]},
             {:vn_c, 500, [:n3@h]},
             {:vn_b, 900, [:n2@h, :n3@h]}
           ]
  end

  test "skips a vnode with no placement, which has no cluster to address" do
    ring = ring_with([{:vn_a, 100}, {:vn_orphan, 200}])
    placements = %{vn_a: [:n1@h], vn_orphan: []}

    assert RingTopology.vnode_placement(RingTopology.new(ring, placements)) == [{:vn_a, 100, [:n1@h]}]
  end

  test "skips a vnode the placements do not mention at all" do
    ring = ring_with([{:vn_a, 100}, {:vn_missing, 200}])

    assert RingTopology.vnode_placement(RingTopology.new(ring, %{vn_a: [:n1@h]})) ==
             [{:vn_a, 100, [:n1@h]}]
  end

  test "a topology with no ring has no placement" do
    assert RingTopology.vnode_placement(%RingTopology{}) == []
  end
end
