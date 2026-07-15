defmodule Malachi.Cluster.RingTopologyTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.RingTopology, as: Topo

  defp ring(vnodes) do
    Enum.reduce(vnodes, HashRing.new(), fn {id, token}, ring ->
      {:ok, ring} = HashRing.add_vnode(ring, id, token)
      ring
    end)
  end

  defp topo(version, vnodes, placements) do
    %Topo{version: version, ring: ring(vnodes), placements: placements}
  end

  test "servers/1 derives {vnode => {vnode, member}} from placements, skipping an empty placement" do
    topo = %Topo{version: 0, ring: ring(v0: 0), placements: %{v0: [:a@h, :b@h], v1: []}}
    assert Topo.servers(topo) == %{v0: {:v0, :a@h}}
  end

  test "new/2 starts at version 0; advance/3 bumps the version monotonically" do
    t0 = Topo.new(ring(v0: 0), %{v0: [:a]})
    assert t0.version == 0

    t1 = Topo.advance(t0, ring(v0: 0, v1: 100), %{v0: [:a], v1: [:b]})
    assert t1.version == 1
    assert t1.ring |> HashRing.vnode_ids() |> Enum.sort() == [:v0, :v1]
    assert t1.placements == %{v0: [:a], v1: [:b]}
  end

  describe "merge/2 (CRDT join)" do
    test "keeps the higher version, regardless of argument order" do
      a = topo(1, [v0: 0], %{v0: [:a]})
      b = topo(2, [v0: 0, v1: 100], %{v0: [:a], v1: [:b]})
      assert Topo.merge(a, b) == b
      assert Topo.merge(b, a) == b
    end

    test "is idempotent" do
      a = topo(3, [v0: 0], %{v0: [:a]})
      assert Topo.merge(a, a) == a
    end

    test "is commutative, including on a same-version clash (deterministic tiebreak)" do
      a = topo(5, [v0: 0], %{v0: [:a]})
      b = topo(5, [v0: 0, v1: 100], %{v0: [:a], v1: [:b]})
      assert Topo.merge(a, b) == Topo.merge(b, a)
    end

    test "is associative" do
      a = topo(1, [v0: 0], %{v0: [:a]})
      b = topo(2, [v0: 0], %{v0: [:b]})
      c = topo(3, [v0: 0], %{v0: [:c]})
      assert Topo.merge(Topo.merge(a, b), c) == Topo.merge(a, Topo.merge(b, c))
    end

    test "converges to the latest version regardless of gossip order" do
      observations = for v <- [2, 0, 3, 1], do: topo(v, [v0: 0], %{v0: [:"n#{v}"]})
      latest = Enum.find(observations, &(&1.version == 3))

      assert Enum.reduce(observations, &Topo.merge/2) == latest
      assert Enum.reduce(Enum.reverse(observations), &Topo.merge/2) == latest
    end
  end
end
