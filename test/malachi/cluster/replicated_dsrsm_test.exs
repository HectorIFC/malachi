defmodule Malachi.Cluster.ReplicatedDSRSMTest do
  # async: false, ra is global/stateful.
  use ExUnit.Case, async: false

  alias Malachi.Cluster.DSRSM
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.RaCluster
  alias Malachi.Cluster.ReplicatedDSRSM
  alias Malachi.Metadata
  alias Malachi.Test.SilentRaMember

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

  test "snapshot reads the vnodes concurrently and bounds each read" do
    # Three vnodes that answer nothing. Read sequentially with ra's default this costs 15s, which is
    # the shape that used to sit inside the broker's loop (#178). Concurrent and bounded, it costs one
    # timeout for all three, and every one of them is reported unreachable rather than empty.
    suffix = System.unique_integer([:positive])
    names = for i <- 0..2, do: :"rd_silent_#{i}_#{suffix}"

    for name <- names do
      {:ok, _pid} = SilentRaMember.start_link(name)
      on_exit(fn -> SilentRaMember.stop(name) end)
    end

    state =
      names
      |> Enum.with_index()
      |> Enum.reduce(ReplicatedDSRSM.new(ring_bits: 4), fn {name, index}, acc ->
        {:ok, acc} = ReplicatedDSRSM.route_vnode(acc, name, index * 4, {name, node()})
        acc
      end)

    {elapsed_us, {:ok, cache, unreachable}} =
      :timer.tc(fn -> ReplicatedDSRSM.snapshot(state, timeout: 300) end)

    assert Enum.sort(unreachable) == Enum.sort(names)
    assert DSRSM.get_topic(cache, "anything") == nil

    # One timeout for the set, not one per vnode. The bound is generous: the claim is that three reads
    # overlapped, not that the scheduler hit 300ms exactly.
    assert elapsed_us < 2_000_000, "three bounded reads took #{div(elapsed_us, 1000)}ms; they serialized"
  end

  test "snapshot with no timeout keeps ra's own default" do
    # The call sites that do not pass one must not change behaviour, so the default is pinned here.
    name = :"rd_default_#{System.unique_integer([:positive])}"
    {:ok, _pid} = SilentRaMember.start_link(name)
    on_exit(fn -> SilentRaMember.stop(name) end)

    {:ok, state} = ReplicatedDSRSM.route_vnode(ReplicatedDSRSM.new(ring_bits: 4), name, 0, {name, node()})

    {elapsed_us, {:ok, _cache, unreachable}} = :timer.tc(fn -> ReplicatedDSRSM.snapshot(state) end)

    assert unreachable == [name]
    assert elapsed_us >= 4_000_000, "gave up after #{div(elapsed_us, 1000)}ms, before ra's 5s default"
  end

  test "snapshot survives members that redirect to each other instead of answering" do
    # `ra` follows a `{redirect, Leader}` reply by calling that leader with a FRESH full timeout, so two
    # members that each name the other cost unbounded time without any single call ever timing out. The
    # per-read bound cannot see that; the bound on the whole read is what ends it, and the vnode comes
    # back as what it is, one that did not answer.
    suffix = System.unique_integer([:positive])
    a = :"rd_ping_#{suffix}"
    b = :"rd_pong_#{suffix}"

    {:ok, _pid} = SilentRaMember.start_redirecting(a, {b, node()}, 20)
    on_exit(fn -> SilentRaMember.stop(a) end)
    {:ok, _pid} = SilentRaMember.start_redirecting(b, {a, node()}, 20)
    on_exit(fn -> SilentRaMember.stop(b) end)

    {:ok, state} = ReplicatedDSRSM.route_vnode(ReplicatedDSRSM.new(ring_bits: 4), a, 0, {a, node()})

    {elapsed_us, {:ok, _cache, unreachable}} =
      :timer.tc(fn -> ReplicatedDSRSM.snapshot(state, timeout: 300) end)

    assert unreachable == [a]
    assert elapsed_us < 5_000_000, "the redirect loop ran for #{div(elapsed_us, 1000)}ms before it was cut"
  end

  test "snapshot of a ring with no vnodes reads nothing" do
    assert {:ok, cache, []} = ReplicatedDSRSM.snapshot(ReplicatedDSRSM.new(ring_bits: 4), timeout: 50)
    assert DSRSM.get_topic(cache, "anything") == nil
  end

  test "snapshot reports a vnode it did read as reachable" do
    {state, _a, _b} = start_cluster()
    {:ok, _root} = ReplicatedDSRSM.command(state, "events", {:create_topic, "events", 4})

    assert {:ok, cache, []} = ReplicatedDSRSM.snapshot(state)
    assert DSRSM.get_topic(cache, "events").name == "events"
  end

  test "a split whose destination refuses the insert keeps every topic on its source" do
    {state, a, b} = start_cluster()
    names = for index <- 0..15, do: "topic-#{index}"
    for name <- names, do: {:ok, _root} = ReplicatedDSRSM.command(state, name, {:create_topic, name, 4})

    # The destination vnode's group is already up and refuses every export: ra commits the insert, and
    # the machine answers {:error, _}. Only that answer can stop the extract that would follow.
    new_vnode = :"rd_refusing_#{System.unique_integer([:positive])}"
    {:ok, new_server} = RaCluster.start(Malachi.Test.RefusingInsertMachine, new_vnode, [node()])
    on_exit(fn -> RaCluster.delete(new_server) end)

    assert {:error, {:migrate, displaced, {:refused, {:unsupported_export_format, 1, 0}}}} =
             ReplicatedDSRSM.split_vnode(state, new_vnode, 8)

    assert displaced in names

    held =
      for vnode <- [a, b], name <- names, reduce: MapSet.new() do
        acc ->
          case MetadataServer.query(Map.fetch!(state.vnodes, vnode), &Metadata.get_topic(&1, name)) do
            {:ok, %{name: ^name}} -> MapSet.put(acc, name)
            _absent -> acc
          end
      end

    assert held == MapSet.new(names)
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
