defmodule Malachi.Cluster.RebalanceCoordinatorTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.RebalanceCoordinator, as: Coordinator

  @plan [%{vnode_id: :vn_0, add: [:d@h], remove: [:a@h]}]

  defp start(opts) do
    test_pid = self()

    defaults = [
      plan_fun: fn -> @plan end,
      add_member: fn vnode_id, node -> send(test_pid, {:add, vnode_id, node}) && :ok end,
      remove_member: fn vnode_id, node -> send(test_pid, {:remove, vnode_id, node}) && :ok end,
      leader?: fn -> true end
    ]

    {:ok, coordinator} = Coordinator.start_link(Keyword.merge(defaults, opts))
    coordinator
  end

  test "plan/1 returns the current plan without moving anything" do
    coordinator = start([])
    assert Coordinator.plan(coordinator) == @plan
    refute_received {:add, _, _}
    refute_received {:remove, _, _}
  end

  test "commit/1 applies the plan (add-before-remove) when this node holds the lease" do
    coordinator = start([])
    assert Coordinator.commit(coordinator) == {:ok, [:vn_0]}
    assert_received {:add, :vn_0, :d@h}
    assert_received {:remove, :vn_0, :a@h}
  end

  test "commit/1 refuses and moves nothing when this node does not hold the lease" do
    coordinator = start(leader?: fn -> false end)
    assert Coordinator.commit(coordinator) == {:error, :not_leader}
    refute_received {:add, _, _}
    refute_received {:remove, _, _}
  end

  test "commit/1 surfaces a failed change fail-fast" do
    coordinator = start(add_member: fn _vnode_id, _node -> {:error, :boom} end)
    assert Coordinator.commit(coordinator) == {:error, {[], {:add, :vn_0, :d@h, :boom}}}
  end
end
