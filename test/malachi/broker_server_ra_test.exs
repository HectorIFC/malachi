defmodule Malachi.BrokerServerRaTest do
  # async: false: ra is global/stateful (one data dir, on-disk Raft logs).
  use ExUnit.Case, async: false

  alias Malachi.BrokerServer
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record
  alias Malachi.Metadata
  alias Malachi.Test.AliveMembersStub

  setup_all do
    :ok
  end

  defp start_replication do
    directory = Path.join(System.tmp_dir!(), "malachi_ra_bs_repl_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    start_supervised!({ReplicationServer, directory: directory}, id: {:repl, System.unique_integer([:positive])})
  end

  test "metadata mutations are committed to the Raft cluster, and the cache matches it" do
    cluster = :"bs_meta_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)

    brokers = for _ <- 1..3, do: start_replication()

    {:ok, control} =
      BrokerServer.start_link("unused",
        brokers: brokers,
        replication_factor: 3,
        segment_max_bytes: Record.encoded_size(Record.new("value", key: "key")),
        metadata_cluster: cluster
      )

    {:ok, root} = BrokerServer.create_topic(control, "events", 4)
    {:ok, _placements} = BrokerServer.produce(control, "events", [Record.new("v", key: "k")])

    # the topic and segment live in the replicated Raft state (queried directly), proving the
    # control-plane mutations went through the log rather than only a local map
    {:ok, replicated} = MetadataServer.query({cluster, node()}, & &1)
    assert Metadata.get_topic(replicated, "events").name == "events"
    assert Metadata.segments_of_range(replicated, root) != []

    # and the broker's local cache is exactly the replicated state (read-your-writes)
    assert BrokerServer.metadata(control) == replicated

    :ok = BrokerServer.stop(control)
  end

  test "a restarted broker serves reads of pre-restart data (range state recovery)" do
    cluster = :"bs_meta_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)
    repl = start_replication()

    {:ok, first} = BrokerServer.start_link("unused", brokers: [repl], metadata_cluster: cluster)
    {:ok, root} = BrokerServer.create_topic(first, "events", 4)
    {:ok, _} = BrokerServer.produce(first, "events", for(i <- 1..5, do: Record.new("v#{i}", key: "k#{i}")))
    :ok = BrokerServer.stop(first)

    # A fresh broker over the SAME metadata cluster and replication server, the restart shape the
    # chaos harness exercises. Before range-state recovery its empty offsets map clamped every read
    # to :eof at offset 0, so durable pre-restart data was unreadable until the next produce.
    {:ok, second} = BrokerServer.start_link("unused", brokers: [repl], metadata_cluster: cluster)

    {:ok, records} = BrokerServer.read(second, root, 0, 100)
    assert Enum.map(records, & &1.value) == for(i <- 1..5, do: "v#{i}")

    # The consume/fetch path (what the chaos verify uses) works too.
    {consumed, _next} = BrokerServer.consume(second, "events", %{}, 100, 0)
    assert length(consumed) == 5

    # And producing continues cleanly after the restart (recovered offsets + segment seq floor).
    {:ok, _} = BrokerServer.produce(second, "events", [Record.new("v6", key: "k6")])
    {:ok, all} = BrokerServer.read(second, root, 0, 100)
    assert length(all) == 6

    :ok = BrokerServer.stop(second)
  end

  test "a restarted broker AND replication server serve reads of pre-restart data" do
    # The test above restarts the broker while keeping the replication server alive, so its segment
    # logs stay open and the recovered read horizon is right by accident. A container restart takes
    # both down, and then the recovery asked a server whose logs map was empty, got told the segment
    # held nothing, and set the horizon to zero. Every read then clamped to :eof before it could reach
    # the cold-segment recovery in the read path, which is the deadlock: no read, so no open segment,
    # so no horizon, so no read. Only a produce broke it.
    cluster = :"bs_meta_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)
    directory = Path.join(System.tmp_dir!(), "malachi_cold_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)

    # A REGISTERED server, so its ref is {name, node} and survives the restart. An unregistered one is
    # referenced by pid, which a restart changes, and the recovery would then be asking a dead process
    # rather than a cold one: a different failure that would hide this one.
    name = :"cold_repl_#{System.unique_integer([:positive])}"
    {:ok, repl} = ReplicationServer.start_link(directory: directory, name: name)
    {:ok, first} = BrokerServer.start_link("unused", brokers: [{name, node()}], metadata_cluster: cluster)
    {:ok, root} = BrokerServer.create_topic(first, "events", 4)
    {:ok, _} = BrokerServer.produce(first, "events", for(i <- 1..5, do: Record.new("v#{i}", key: "k#{i}")))
    :ok = BrokerServer.stop(first)
    :ok = GenServer.stop(repl)

    # Both come back over the same directory and the same metadata cluster, holding nothing in memory.
    {:ok, cold_repl} = ReplicationServer.start_link(directory: directory, name: name)
    {:ok, second} = BrokerServer.start_link("unused", brokers: [{name, node()}], metadata_cluster: cluster)

    assert {:ok, records} = BrokerServer.read(second, root, 0, 100)
    assert Enum.map(records, & &1.value) == for(i <- 1..5, do: "v#{i}")

    {consumed, _next} = BrokerServer.consume(second, "events", %{}, 100, 0)
    assert length(consumed) == 5, "a cold replication server must not report durable records as drained"

    :ok = BrokerServer.stop(second)
    :ok = GenServer.stop(cold_repl)
  end

  test "a subscriber is pushed records produced through a different frontend" do
    # A streaming subscriber used to be pushed only on subscribe, on its own ack, and on a produce
    # through the broker it subscribed to. A produce through another frontend woke that frontend's
    # subscribers and not this one's, so a topic written on one node and streamed from another
    # delivered nothing, with no error anywhere and both nodes healthy. An ack cannot recover it,
    # having no records to acknowledge.
    cluster = :"bs_sub_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)

    name = :"sub_repl_#{System.unique_integer([:positive])}"
    directory = Path.join(System.tmp_dir!(), "malachi_sub_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    {:ok, repl} = ReplicationServer.start_link(directory: directory, name: name)
    on_exit(fn -> if Process.alive?(repl), do: GenServer.stop(repl) end)

    opts = [brokers: [{name, node()}], metadata_cluster: cluster, brokers_refresh_interval: 100]
    {:ok, writer} = BrokerServer.start_link("unused", opts)
    {:ok, streamer} = BrokerServer.start_link("unused", opts)

    {:ok, _root} = BrokerServer.create_topic(writer, "events", 4)
    :ok = BrokerServer.subscribe(streamer, "events", "g", 100, 10)

    # Produced through the OTHER broker, so nothing on this path wakes the subscription.
    {:ok, _} = BrokerServer.produce(writer, "events", [Record.new("v1", key: "k1")])

    assert_receive {:log_records, "events", records, _positions}, 3_000
    assert Enum.map(records, & &1.value) == ["v1"]

    :ok = BrokerServer.stop(writer)
    :ok = BrokerServer.stop(streamer)
  end

  test "a topic whose metadata vnode never answered is refused, not reported as drained" do
    # Two vnodes: one on this node, one routed at a node that does not exist, so its ra cluster can
    # never be read. That is the shape of a broker whose control plane is partly unreachable, which is
    # also the shape of a broker that just restarted and whose vnodes have not come up yet.
    suffix = System.unique_integer([:positive])
    reachable = {:"bs_reach_#{suffix}", 0, [node()]}
    ghost = {:"bs_ghost_#{suffix}", div(Integer.pow(2, 32), 2), [:absent@nowhere]}
    on_exit(fn -> MetadataServer.delete(elem(reachable, 0)) end)

    {:ok, control} =
      BrokerServer.start_link("unused", brokers: [start_replication()], metadata_vnodes: [reachable, ghost])

    # Names divide between the two vnodes by hash. A name on the reachable vnode reads normally; a name
    # on the ghost must NOT read as an empty topic, because this broker cannot know whether it is empty.
    results = for i <- 0..19, do: BrokerServer.consume(control, "gate_t#{i}", %{}, 100, 0)

    refused = Enum.filter(results, &match?({:error, :metadata_unavailable}, &1))
    answered = Enum.filter(results, &match?({[], _positions}, &1))

    assert refused != [], "expected topics routed to the unreachable vnode to be refused"
    assert answered != [], "expected topics routed to the reachable vnode to still be served"

    # And the node says it is not ready, so an orchestrator stops routing to it rather than letting it
    # answer half its keyspace with a successful lie.
    refute BrokerServer.metadata_ready?(control)

    :ok = BrokerServer.stop(control)
  end

  test "a rejected control-plane command surfaces the Raft machine error" do
    cluster = :"bs_meta_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)

    {:ok, control} =
      BrokerServer.start_link("unused", brokers: [start_replication()], metadata_cluster: cluster)

    {:ok, _root} = BrokerServer.create_topic(control, "events", 4)
    assert {:error, :already_exists} = BrokerServer.create_topic(control, "events", 4)

    :ok = BrokerServer.stop(control)
  end

  test "shards the control plane across vnodes: each topic's metadata lives in its own ra cluster" do
    # two vnodes at opposite ends of the ring, each its own ra cluster over the local node (this node
    # is the sole node, so it is the bootstrap orchestrator by default)
    vnodes =
      for i <- 0..1, do: {:"bs_vn_#{i}_#{System.unique_integer([:positive])}", i * div(Integer.pow(2, 32), 2), [node()]}

    on_exit(fn -> Enum.each(vnodes, fn {name, _token, _nodes} -> MetadataServer.delete(name) end) end)

    {:ok, control} =
      BrokerServer.start_link("unused", brokers: [start_replication()], metadata_vnodes: vnodes)

    names = for i <- 0..9, do: "t#{i}"
    for name <- names, do: assert({:ok, _root} = BrokerServer.create_topic(control, name, 4))

    # each topic is retrievable through the broker's cache, and its metadata was committed to exactly
    # the ra cluster its name routes to (queried directly), and to no other vnode
    home = fn name ->
      Enum.filter(vnodes, fn {vnode, _token, _nodes} ->
        match?(%{name: ^name}, elem(MetadataServer.query({vnode, node()}, &Metadata.get_topic(&1, name)), 1))
      end)
    end

    homes =
      Map.new(names, fn name ->
        assert BrokerServer.active_range_ids(control, name) == [{name, 0}]
        assert [{owner, _token, _nodes}] = home.(name), "topic #{name} must live in exactly one vnode's cluster"
        {name, owner}
      end)

    assert homes |> Map.values() |> Enum.uniq() |> length() == 2, "expected topics to shard across both vnodes"

    :ok = BrokerServer.stop(control)
  end

  test "the membership leader bootstraps the vnodes via the reconcile loop" do
    # membership where this node is the sole (thus lowest) live member → it is the bootstrap leader
    {:ok, membership} = AliveMembersStub.start_link([{Malachi.LogMembership, node()}])
    vnode = :"bs_ml_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(vnode) end)

    {:ok, control} =
      BrokerServer.start_link("unused",
        brokers: [start_replication()],
        metadata_vnodes: [{vnode, 0, [node()]}],
        bootstrap_orchestrator: Malachi.Application.membership_leader(membership)
      )

    # nothing is started at boot (build_replicated only routes); the reconcile loop on the leader
    # bootstraps the vnode's cluster, so a create_topic through the broker then commits
    assert {:ok, _root} = BrokerServer.create_topic(control, "events", 4)
    assert %{name: "events"} = Metadata.get_topic(BrokerServer.metadata(control), "events")
    assert MetadataServer.ready?({vnode, node()})

    :ok = BrokerServer.stop(control)
  end

  test "produces across a 3-broker replica set and reads the records back (replicated data plane)" do
    cluster = :"bs_meta_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)

    # The data-plane wiring D2 sets up: several ReplicationServers as the broker set + a replication
    # factor, with ra as the control plane. This is the shape Malachi.Application builds when clustered.
    brokers = for _ <- 1..3, do: start_replication()

    {:ok, control} =
      BrokerServer.start_link("unused", brokers: brokers, replication_factor: 3, metadata_cluster: cluster)

    {:ok, _root} = BrokerServer.create_topic(control, "events", 4)

    records = for index <- 0..4, do: Record.new("v#{index}", key: "k#{index}")
    {:ok, _placements} = BrokerServer.produce(control, "events", records)

    # a segment landed on a 3-broker replica set (placement across the whole broker set)
    [range_id] = BrokerServer.active_range_ids(control, "events")
    [segment] = Metadata.segments_of_range(BrokerServer.metadata(control), range_id)
    assert length(segment.replica_set) == 3

    # committed (quorum-durable) records read back through the broker's primary
    {read, _cursor} = BrokerServer.consume(control, "events", %{}, 100, 0)
    assert read |> Enum.map(& &1.value) |> Enum.sort() == Enum.map(records, & &1.value) |> Enum.sort()

    :ok = BrokerServer.stop(control)
  end
end
