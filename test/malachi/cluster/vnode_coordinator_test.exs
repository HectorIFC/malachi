defmodule Malachi.Cluster.VnodeCoordinatorTest do
  # async: false: ra is global/stateful (one data dir, on-disk Raft logs).
  use ExUnit.Case, async: false

  alias Malachi.Application, as: App
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.RetentionCoordinator
  alias Malachi.Cluster.VnodeCoordinatorManager, as: Manager
  alias Malachi.Metadata

  @range {"t", 0}

  setup_all do
    :ok
  end

  defp start_vnode do
    name = :"vnode_#{System.unique_integer([:positive])}"
    {:ok, server_id} = MetadataServer.start(name)
    on_exit(fn -> MetadataServer.delete(name) end)
    {name, server_id}
  end

  # Populates the vnode with topic "t" and a set of sealed segments {id, start_offset, bytes, sealed_at}.
  defp seal_segments(server_id, segments) do
    {:ok, {:ok, _root}} = MetadataServer.command(server_id, {:create_topic, "t", 4})

    Enum.each(segments, fn {id, start_offset, bytes, sealed_at} ->
      {:ok, :ok} = MetadataServer.command(server_id, {:register_segment, @range, id, [:b1], start_offset})
      {:ok, :ok} = MetadataServer.command(server_id, {:seal_segment, id, 1, bytes, sealed_at})
    end)
  end

  describe "vnode_metadata_source/1" do
    test "yields empty Metadata for an unformed/unreachable vnode (tolerant, no crash)" do
      source = App.vnode_metadata_source(:"no_such_vnode_#{System.unique_integer([:positive])}")
      assert source.() == Metadata.new()
    end

    test "reads this vnode's own shard (only its segments)" do
      {name, server_id} = start_vnode()
      seal_segments(server_id, [{"old", 0, 100, 1_000}])

      metadata = App.vnode_metadata_source(name).()
      assert Metadata.get_segment(metadata, "old").state == :sealed
    end
  end

  test "a retention coordinator bound to a vnode expires only that vnode's expired segments (1C-b-ii-a)" do
    test_pid = self()
    {name, server_id} = start_vnode()
    seal_segments(server_id, [{"old", 0, 100, 1_000}, {"new", 1, 100, 9_500}])

    {:ok, coordinator} =
      RetentionCoordinator.start_link(
        metadata_source: App.vnode_metadata_source(name),
        expire_segment: fn segment -> send(test_pid, {:expired, segment.id}) end,
        policy: %{max_age_ms: 5_000},
        clock: fn -> 10_000 end,
        interval: 60_000
      )

    assert RetentionCoordinator.run_now(coordinator) == ["old"]
    assert_receive {:expired, "old"}
    refute_receive {:expired, "new"}
  end

  test "the manager runs a real retention coordinator for each led vnode (1C-b-ii-b integration)" do
    test_pid = self()

    # two single-node vnodes → this node leads both; each has one expired sealed segment
    {name_a, server_a} = start_vnode()
    {name_b, server_b} = start_vnode()
    seal_segments(server_a, [{"a_old", 0, 100, 1_000}])
    seal_segments(server_b, [{"b_old", 0, 100, 1_000}])

    placement = [{name_a, 0, [node()]}, {name_b, 1, [node()]}]

    {:ok, supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)
    # stop the coordinators before the vnodes are deleted (on_exit runs LIFO), so no sweep queries a
    # vnode mid-teardown
    on_exit(fn -> Process.exit(supervisor, :kill) end)

    spawn_coordinator = fn vnode_id ->
      spec =
        {RetentionCoordinator,
         metadata_source: App.vnode_metadata_source(vnode_id),
         expire_segment: fn segment -> send(test_pid, {:expired, vnode_id, segment.id}) end,
         policy: %{max_age_ms: 5_000},
         clock: fn -> 10_000 end,
         interval: 20}

      {:ok, pid} = DynamicSupervisor.start_child(supervisor, spec)
      pid
    end

    {:ok, manager} =
      Manager.start_link(
        placement: fn -> {:ok, placement} end,
        leading: &App.leading_vnodes(&1, node()),
        spawn: spawn_coordinator,
        stop: fn pid -> DynamicSupervisor.terminate_child(supervisor, pid) end,
        interval: 60_000
      )

    # the manager spawned a coordinator per led vnode, and each sweeps only its own vnode's segments
    assert_receive {:expired, ^name_a, "a_old"}, 1_000
    assert_receive {:expired, ^name_b, "b_old"}, 1_000
    assert Enum.sort(Manager.reconcile_now(manager)) == Enum.sort([name_a, name_b])
  end
end

defmodule Malachi.Cluster.VnodeCoordinatorLivePlacementTest do
  @moduledoc """
  The manager against the **production** placement seams, over real `ra` groups and a real membership.

  This is the regression test for the defect the seams carried: the placement used to be resolved once
  at boot and captured, so a vnode this node gained afterwards, through a split that publishes a new
  ring or a rebalance that adds it as an `ra` member, never got coordinators. A unit test of the manager
  cannot see that: the manager already re-invoked its seams every tick, and it was the seams that were
  frozen. So this test takes `Malachi.Application.vnode_coordinator_manager_opts/0` as it ships.

  `:spawn` and `:stop` are the two seams replaced with recorders. They carried no part of the defect,
  and the real ones would start this node's whole heal, retention and consumer-group stack per vnode
  against the suite's broker.

  `async: false`: real `ra`, plus the production membership name.
  """
  use ExUnit.Case, async: false

  alias Malachi.Application, as: App
  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.MembershipServer
  alias Malachi.Cluster.MetadataMachine
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.RingTopology
  alias Malachi.Cluster.VnodeCoordinatorManager, as: Manager

  @membership Malachi.LogMembership

  # A single-node ra group for `name`: this node is its only member and therefore its leader, which is
  # exactly the state `ra` leaves a node in after a split creates the vnode here or a rebalance adds it.
  defp start_vnode do
    name = :"live_vnode_#{System.unique_integer([:positive])}"
    {:ok, _server_id} = MetadataServer.start(name)
    on_exit(fn -> MetadataServer.delete(name) end)
    name
  end

  defp topology(placements) do
    ring =
      placements
      |> Enum.with_index()
      |> Enum.reduce(HashRing.new(), fn {{id, _nodes}, index}, ring ->
        {:ok, ring} = HashRing.add_vnode(ring, id, index * 1_000)
        ring
      end)

    RingTopology.new(ring, Map.new(placements))
  end

  defp start_membership(topology) do
    {:ok, pid} =
      MembershipServer.start_link(name: @membership, self_ref: {@membership, node()}, topology: topology)

    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    pid
  end

  defp start_manager do
    test_pid = self()

    opts =
      App.vnode_coordinator_manager_opts()
      |> Keyword.drop([:name])
      |> Keyword.merge(
        spawn: fn vnode_id ->
          pid = spawn(fn -> Process.sleep(:infinity) end)
          send(test_pid, {:spawn, vnode_id, pid})
          pid
        end,
        stop: fn pid ->
          send(test_pid, {:stop, pid})
          Process.exit(pid, :kill)
        end,
        interval: 60_000
      )

    {:ok, manager} = Manager.start_link(opts)
    on_exit(fn -> if Process.alive?(manager), do: GenServer.stop(manager) end)
    manager
  end

  test "a vnode this node gains through a split gets coordinators without a restart" do
    booted = start_vnode()
    membership = start_membership(topology([{booted, [node()]}]))
    manager = start_manager()

    assert_receive {:spawn, ^booted, _pid}
    assert Manager.reconcile_now(manager) == [booted]

    # What a split publishes: the ring advances to a version carrying a vnode that did not exist at
    # boot, and its ra group is already running here.
    gained = start_vnode()
    grown = topology([{booted, [node()]}, {gained, [node()]}])
    :ok = MembershipServer.set_topology(membership, %{grown | version: 1})

    assert Enum.sort(Manager.reconcile_now(manager)) == Enum.sort([booted, gained])
    assert_receive {:spawn, ^gained, _pid}

    assert {MetadataMachine, {gained, node()}} in App.vnode_coordinator_manager_opts()[:version_servers].(
             elem(App.current_vnodes(), 1)
           )
  end

  test "a vnode this node gains through a rebalance gets coordinators, though the ring never moved" do
    booted = start_vnode()
    # A rebalance adds an ra member and publishes no ring at all, so the recorded placement keeps naming
    # the nodes it named before. Hosting has to come from ra, or this vnode stays invisible forever.
    gained = start_vnode()
    start_membership(topology([{booted, [node()]}, {gained, [:somewhere_else@nowhere]}]))
    manager = start_manager()

    assert Enum.sort(Manager.reconcile_now(manager)) == Enum.sort([booted, gained])
    assert_receive {:spawn, ^gained, _pid}
  end

  test "a vnode the ring still places here but ra no longer holds gets no coordinators" do
    booted = start_vnode()
    departed = :"live_vnode_departed_#{System.unique_integer([:positive])}"
    start_membership(topology([{booted, [node()]}, {departed, [node()]}]))
    manager = start_manager()

    assert Manager.reconcile_now(manager) == [booted]
    refute_receive {:spawn, ^departed, _pid}
  end

  test "an unreachable membership leaves the coordinators this node already runs alone" do
    booted = start_vnode()
    membership = start_membership(topology([{booted, [node()]}]))
    manager = start_manager()
    assert_receive {:spawn, ^booted, _pid}

    ExUnit.CaptureLog.capture_log(fn ->
      GenServer.stop(membership)
      assert Manager.reconcile_now(manager) == [booted]
    end)

    refute_receive {:stop, _pid}
    assert Manager.placement_status(manager) == :unreadable
  end
end
