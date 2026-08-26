defmodule Malachi.Cluster.ReplicatedDSRSMTest do
  # async: false, ra is global/stateful.
  use ExUnit.Case, async: false

  alias Malachi.Cluster.DSRSM
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.ReplicatedDSRSM
  alias Malachi.Metadata

  setup_all do
    :ok
  end

  # Two vnodes at fixed tokens, with unique names per test (ra clusters are global).
  defp start_cluster do
    suffix = System.unique_integer([:positive])
    a = :"rd_a_#{suffix}"
    b = :"rd_b_#{suffix}"
    {:ok, state} = ReplicatedDSRSM.new(ring_bits: 4) |> ReplicatedDSRSM.add_vnode(a, 4)
    {:ok, state} = ReplicatedDSRSM.add_vnode(state, b, 12)
    on_exit(fn -> ReplicatedDSRSM.delete(state) end)
    {state, a, b}
  end

  test "routes commands to the owning vnode's Raft cluster and serves consistent queries" do
    {state, _a, _b} = start_cluster()

    assert {:ok, root_id} = ReplicatedDSRSM.command(state, "events", {:create_topic, "events", 4})

    assert {:ok, %{name: "events", keyspace_size: 16, state: :active}} =
             ReplicatedDSRSM.query(state, "events", &Metadata.get_topic(&1, "events"))

    assert {:ok, left_id, right_id} = ReplicatedDSRSM.command(state, "events", {:split_range, root_id})

    assert {:ok, active} =
             ReplicatedDSRSM.query(state, "events", &Metadata.active_ranges_of_topic(&1, "events"))

    assert Enum.sort(Enum.map(active, & &1.id)) == Enum.sort([left_id, right_id])
  end

  test "shards topics across vnodes (each lives only in the cluster it routes to)" do
    {state, _a, _b} = start_cluster()

    names = for index <- 0..9, do: "topic-#{index}"
    for name <- names, do: assert({:ok, _root} = ReplicatedDSRSM.command(state, name, {:create_topic, name, 4}))

    # every topic is retrievable from the vnode it routes to, and absent from the other
    for name <- names do
      {:ok, owner} = ReplicatedDSRSM.vnode_for(state, name)
      assert {:ok, %{name: ^name}} = ReplicatedDSRSM.query(state, name, &Metadata.get_topic(&1, name))

      for other <- ReplicatedDSRSM.vnode_ids(state) -- [owner] do
        # querying the non-owning vnode directly must not find the topic
        {:ok, server_id} = Map.fetch(state.vnodes, other)
        assert {:ok, nil} = MetadataServer.query(server_id, &Metadata.get_topic(&1, name))
      end
    end

    # and at least one topic landed on each vnode (real sharding)
    owners = Enum.map(names, fn name -> elem(ReplicatedDSRSM.vnode_for(state, name), 1) end)
    assert length(Enum.uniq(owners)) >= 2
  end

  test "rejected commands return the machine error reply; empty ring reports no vnode" do
    {state, _a, _b} = start_cluster()
    {:ok, _root} = ReplicatedDSRSM.command(state, "events", {:create_topic, "events", 4})
    assert {:error, :already_exists} = ReplicatedDSRSM.command(state, "events", {:create_topic, "events", 4})

    empty = ReplicatedDSRSM.new(ring_bits: 4)
    assert {:error, :no_vnode} = ReplicatedDSRSM.command(empty, "events", {:create_topic, "events", 4})
    assert {:error, :no_vnode} = ReplicatedDSRSM.query(empty, "events", &Metadata.get_topic(&1, "events"))
  end

  test "snapshot names the vnodes it could not read instead of passing their emptiness off as metadata" do
    # route to a server that was never started; snapshot must not fail, only yield empty metadata
    {:ok, state} =
      ReplicatedDSRSM.route_vnode(ReplicatedDSRSM.new(ring_bits: 4), :rd_ghost, 0, {:rd_ghost_never_started, node()})

    assert {:ok, cache, unreachable} = ReplicatedDSRSM.snapshot(state)
    assert DSRSM.get_topic(cache, "anything") == nil

    # The empty metadata alone is indistinguishable from a vnode that genuinely holds no topics, and a
    # reader that installs it deletes live topics from its cache. The id is what makes them different.
    assert unreachable == [:rd_ghost]
  end

  test "snapshot reports a vnode it did read as reachable" do
    {state, _a, _b} = start_cluster()
    {:ok, _root} = ReplicatedDSRSM.command(state, "events", {:create_topic, "events", 4})

    assert {:ok, cache, []} = ReplicatedDSRSM.snapshot(state)
    assert DSRSM.get_topic(cache, "events").name == "events"
  end

  test "route_vnode reaches an already-started vnode without starting it" do
    # one view (the orchestrator) starts the vnode's cluster
    suffix = System.unique_integer([:positive])
    vnode = :"rd_route_#{suffix}"
    {:ok, orchestrator} = ReplicatedDSRSM.new(ring_bits: 4) |> ReplicatedDSRSM.add_vnode(vnode, 0)
    on_exit(fn -> ReplicatedDSRSM.delete(orchestrator) end)
    {:ok, _root} = ReplicatedDSRSM.command(orchestrator, "events", {:create_topic, "events", 4})

    # a second view only routes to it (no start) and still reads/commits through the same cluster
    {:ok, router} = ReplicatedDSRSM.route_vnode(ReplicatedDSRSM.new(ring_bits: 4), vnode, 0, {vnode, node()})
    assert {:ok, %{name: "events"}} = ReplicatedDSRSM.query(router, "events", &Metadata.get_topic(&1, "events"))
    assert {:ok, _root2} = ReplicatedDSRSM.command(router, "events", {:create_topic, "events2", 4})

    {:ok, cache, _unreachable} = ReplicatedDSRSM.snapshot(router)
    assert DSRSM.get_topic(cache, "events").name == "events"
    assert DSRSM.get_topic(cache, "events2").name == "events2"
  end
end
