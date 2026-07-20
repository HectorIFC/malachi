defmodule Malachi.Cluster.ReshardCoordinatorTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.ReshardCoordinator

  @ring_size Integer.pow(2, 32)

  defp even_ring(count) do
    Enum.reduce(0..(count - 1), HashRing.new(), fn i, ring ->
      {:ok, ring} = HashRing.add_vnode(ring, :"base_vn_#{i}", div(i * @ring_size, count))
      ring
    end)
  end

  # Starts a coordinator over a ring held in an Agent. The `split` seam records the call and (unless
  # `split_result` says otherwise) applies the split to the held ring, simulating what a real split does.
  defp start_coordinator(opts) do
    test = self()
    ring_agent = Keyword.get_lazy(opts, :ring_agent, fn -> start_ring_agent(even_ring(4)) end)
    split_result = Keyword.get(opts, :split_result, :ok)
    advance? = Keyword.get(opts, :advance?, true)

    split = fn vnode_id, token, nodes ->
      send(test, {:split, vnode_id, token, nodes})

      if split_result == :ok and advance? do
        Agent.update(ring_agent, fn ring ->
          {:ok, grown} = HashRing.add_vnode(ring, vnode_id, token)
          grown
        end)
      end

      split_result
    end

    {:ok, pid} =
      ReshardCoordinator.start_link(
        ring: fn -> Agent.get(ring_agent, & &1) end,
        split: split,
        placement: fn _vnode_id -> [:node_a, :node_b] end,
        leader?: Keyword.get(opts, :leader?, fn -> true end)
      )

    {pid, ring_agent}
  end

  defp start_ring_agent(ring) do
    {:ok, agent} = Agent.start_link(fn -> ring end)
    agent
  end

  test "grows the ring to the target, one split at a time" do
    {pid, ring_agent} = start_coordinator([])

    assert :ok = ReshardCoordinator.reshard(pid, 7)

    # 4 -> 7 is three splits, each with the placement's nodes
    assert_received {:split, _id1, _t1, [:node_a, :node_b]}
    assert_received {:split, _id2, _t2, [:node_a, :node_b]}
    assert_received {:split, _id3, _t3, [:node_a, :node_b]}
    refute_received {:split, _, _, _}

    assert HashRing.size(Agent.get(ring_agent, & &1)) == 7
  end

  test "a target equal to the current count is a no-op" do
    {pid, _agent} = start_coordinator([])

    assert :ok = ReshardCoordinator.reshard(pid, 4)
    refute_received {:split, _, _, _}
  end

  test "resumes an interrupted reshard by re-planning from the live ring" do
    # a ring already grown to 6 (as if a previous reshard toward 7 was interrupted after two splits)
    {pid, ring_agent} = start_coordinator(ring_agent: start_ring_agent(even_ring(6)))

    assert :ok = ReshardCoordinator.reshard(pid, 7)

    # only the one remaining split is issued
    assert_received {:split, _id, _token, _nodes}
    refute_received {:split, _, _, _}
    assert HashRing.size(Agent.get(ring_agent, & &1)) == 7
  end

  test "refuses when this node does not hold the lease" do
    {pid, _agent} = start_coordinator(leader?: fn -> false end)

    assert {:error, :not_leader} = ReshardCoordinator.reshard(pid, 8)
    refute_received {:split, _, _, _}
  end

  test "a smaller target is rejected (grow-only)" do
    {pid, _agent} = start_coordinator([])

    assert {:error, :cannot_shrink} = ReshardCoordinator.reshard(pid, 2)
    refute_received {:split, _, _, _}
  end

  test "reports :no_topology when the cluster has no ring yet" do
    {:ok, pid} =
      ReshardCoordinator.start_link(
        ring: fn -> nil end,
        split: fn _, _, _ -> :ok end,
        placement: fn _ -> [] end,
        leader?: fn -> true end
      )

    assert {:error, :no_topology} = ReshardCoordinator.reshard(pid, 8)
  end

  test "a failed split stops the reshard and reports which vnode failed" do
    {pid, ring_agent} = start_coordinator(split_result: {:error, :migrate_failed})

    assert {:error, {:split_failed, vnode_id, :migrate_failed}} = ReshardCoordinator.reshard(pid, 8)
    assert is_atom(vnode_id)

    # only the failing step was attempted; the ring is untouched
    assert_received {:split, ^vnode_id, _token, _nodes}
    refute_received {:split, _, _, _}
    assert HashRing.size(Agent.get(ring_agent, & &1)) == 4
  end

  test "refuses a step whose new vnode has nowhere to live (empty placement)" do
    test = self()

    {:ok, pid} =
      ReshardCoordinator.start_link(
        ring: fn -> even_ring(4) end,
        split: fn id, token, nodes -> send(test, {:split, id, token, nodes}) && :ok end,
        placement: fn _vnode_id -> [] end,
        leader?: fn -> true end
      )

    assert {:error, {:no_placement, vnode_id}} = ReshardCoordinator.reshard(pid, 8)
    assert is_atom(vnode_id)
    # it never attempted the split
    refute_received {:split, _, _, _}
  end

  test "a split that reports success without advancing the ring does not loop forever" do
    {pid, _agent} = start_coordinator(advance?: false)

    assert {:error, :ring_did_not_advance} = ReshardCoordinator.reshard(pid, 8)
    assert_received {:split, _, _, _}
    refute_received {:split, _, _, _}
  end
end
