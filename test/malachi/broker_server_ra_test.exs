defmodule Malachi.BrokerServerRaTest do
  # async: false: ra is global/stateful (one data dir, on-disk Raft logs).
  use ExUnit.Case, async: false

  import Malachi.Test.TeardownHelper

  alias Malachi.BrokerServer
  alias Malachi.Cluster.HealCoordinator
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record
  alias Malachi.Metadata
  alias Malachi.Storage.Layout
  alias Malachi.Test.AliveMembersStub
  alias Malachi.Test.FaultySegmentStore

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

  test "a restarted broker whose active segment's primary copy failed in storage still starts" do
    # Recovery asks the primary for the active segment's durable end. A copy latched as failed answers an
    # error, which must count as no answer (seated at zero, retried next tick), not crash the broker's init.
    cluster = :"bs_failed_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)
    directory = Path.join(System.tmp_dir!(), "malachi_failed_#{System.unique_integer([:positive])}")

    on_exit(fn ->
      FaultySegmentStore.clear(directory)
      File.rm_rf!(directory)
    end)

    name = :"failed_repl_#{System.unique_integer([:positive])}"
    {:ok, repl} = ReplicationServer.start_link(directory: directory, name: name, store: FaultySegmentStore)
    on_exit(fn -> stop_quietly(repl) end)

    {:ok, first} = BrokerServer.start_link("unused", brokers: [{name, node()}], metadata_cluster: cluster)
    {:ok, root} = BrokerServer.create_topic(first, "events", 4)
    {:ok, _} = BrokerServer.produce(first, "events", for(i <- 1..5, do: Record.new("v#{i}", key: "k#{i}")))
    :ok = BrokerServer.stop(first)

    segment_id = {root, 0}
    FaultySegmentStore.fail(Layout.segment_directory(directory, segment_id), :sync, {:error, :eio})

    ExUnit.CaptureLog.capture_log(fn ->
      late = [Record.new("late", key: "late")]
      assert {:error, {:storage, :eio}} = ReplicationServer.follow({name, node()}, segment_id, 5, late)
      assert {:error, {:storage, :eio}} = ReplicationServer.durable_end({name, node()}, segment_id, 0)

      {:ok, second} = BrokerServer.start_link("unused", brokers: [{name, node()}], metadata_cluster: cluster)
      assert Process.alive?(second)
      assert BrokerServer.active_range_ids(second, "events") == [root]
      :ok = BrokerServer.stop(second)
    end)
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
    on_exit(fn -> stop_quietly(repl) end)

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

  test "a broker that loses the control plane stays ready, and keeps serving what it already knew" do
    # Readiness answers whether this node can serve, not whether the control plane is answering. A
    # refresh that fails leaves the last view in place and reads keep working from it, so reporting
    # unready would pull a working node out of rotation. Every node sees the same outage at the same
    # moment, so that answer would empty the load balancer exactly when all of its backends still work.
    cluster = :"bs_ready_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "malachi_ready_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, repl} = ReplicationServer.start_link(directory: dir)
    on_exit(fn -> stop_quietly(repl) end)

    {:ok, server} =
      BrokerServer.start_link("unused", brokers: [repl], metadata_cluster: cluster, brokers_refresh_interval: 50)

    {:ok, root} = BrokerServer.create_topic(server, "events", 4)
    {:ok, _} = BrokerServer.produce(server, "events", [Record.new("v1", key: "k1")])
    assert BrokerServer.metadata_ready?(server)

    # The control plane goes away. Several refreshes fail before the assertions below.
    MetadataServer.delete(cluster)
    Process.sleep(300)

    assert BrokerServer.metadata_ready?(server), "a node serving from its retained view is ready"
    assert {:ok, [record]} = BrokerServer.read(server, root, 0, 100)
    assert record.value == "v1"
    assert {[_], _positions} = BrokerServer.consume(server, "events", %{}, 100, 0)

    :ok = BrokerServer.stop(server)
  end

  test "a sharded broker stays ready when a vnode it had already read stops answering" do
    # The case that distinguishes this policy from the previous one. A vnode that was never reachable
    # leaves the node unready under both, because there is no view of it to serve from. One that WAS
    # read and then went silent used to report unready, and now does not: the retained view is what it
    # serves, and it is still serving.
    #
    # `bootstrap_orchestrator` is forced off so nothing recreates the vnode's cluster underneath the
    # assertion, which is what this node would otherwise do a tick later as the sole member.
    suffix = System.unique_integer([:positive])
    vnode = :"bs_gone_#{suffix}"
    on_exit(fn -> MetadataServer.delete(vnode) end)
    {:ok, _server_id} = MetadataServer.start(vnode, [node()])

    {:ok, server} =
      BrokerServer.start_link("unused",
        brokers: [start_replication()],
        metadata_vnodes: [{vnode, 0, [node()]}],
        bootstrap_orchestrator: fn -> false end,
        brokers_refresh_interval: 50
      )

    {:ok, _root} = BrokerServer.create_topic(server, "events", 4)
    Process.sleep(200)
    assert BrokerServer.metadata_ready?(server), "the vnode answered, so the node is ready"

    MetadataServer.delete(vnode)
    Process.sleep(300)

    assert BrokerServer.metadata_ready?(server), "a vnode that went silent does not unready the node"
    assert BrokerServer.active_range_ids(server, "events") != [], "and its topics are still there"

    :ok = BrokerServer.stop(server)
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

  test "a split fences the parent even when the splitting frontend never wrote to it" do
    # ISSUE #41. A frontend produces straight into its cached active segment, and the fence a split
    # performs used to be read out of that same cache (`Broker.active_roll/2`). So a split run on a node
    # that had never produced to the parent fenced NOTHING and reported success, while the node holding
    # the segment kept appending to the now-sealed parent.
    #
    # That inverts per-key order, because a child reads its ancestors FIRST (`history_sources/2`): a
    # record written to the parent AFTER the split is served BEFORE a record the child already holds.
    # And it is not the one-refresh-interval window the issue estimated. The split seals the RANGE and
    # not its segments, and nothing else ever closes an active segment on a sealed range: failover only
    # seals segments whose primary is DEAD, retention and healing only touch sealed ones, and
    # `drop_stale_active_segments/1` matches on the SEGMENT's state. So the parent stays writable, and
    # the inversion grows for as long as that frontend keeps producing.
    cluster = :"bs_fence41_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)

    # A REGISTERED replication server, so both frontends address the same primary by `{name, node()}`
    # and place segments on a ref that compares equal on both sides.
    name = :"fence41_repl_#{System.unique_integer([:positive])}"
    directory = Path.join(System.tmp_dir!(), "malachi_fence41_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    {:ok, repl} = ReplicationServer.start_link(directory: directory, name: name)
    on_exit(fn -> stop_quietly(repl) end)

    # One shared primary and one shared control plane, two frontends. The refresh interval is long
    # enough that nothing reconciles on its own, so every convergence below is driven explicitly and the
    # window the bug lives in is deterministic rather than timed.
    opts = [
      brokers: [{name, node()}],
      metadata_cluster: cluster,
      group_commit: false,
      brokers_refresh_interval: 60_000
    ]

    {:ok, writer} = BrokerServer.start_link("unused", opts)
    on_exit(fn -> stop_quietly(writer) end)
    {:ok, splitter} = BrokerServer.start_link("unused", opts)
    on_exit(fn -> stop_quietly(splitter) end)

    {:ok, root} = BrokerServer.create_topic(writer, "events", 4)

    # ONE key throughout, so per-key order is exactly what the child's history has to preserve.
    key = "k"
    assert {:ok, _} = BrokerServer.produce(writer, "events", [Record.new("early", key: key)])

    # The asymmetry that IS issue #41: the splitter can see the parent's write head in the CONTROL
    # PLANE, and holds nothing for it in its own cache, having never produced.
    reconcile!(splitter)
    [parent_segment] = Metadata.segments_of_range(BrokerServer.metadata(splitter), root)
    assert parent_segment.state == :active

    assert :sys.get_state(splitter).broker.segments == %{},
           "the splitting frontend must not hold the parent's segment: that is the whole scenario"

    assert {:ok, left, right} = BrokerServer.split_range(splitter, root)

    # A write to the CHILD, through the frontend that performed the split.
    assert {:ok, placements} = BrokerServer.produce(splitter, "events", [Record.new("mid", key: key)])
    [child] = Map.keys(placements)
    assert child in [left, right]

    # And the write that used to invert the order. The writer has not reconciled, so it still routes
    # this key at its cached PARENT segment.
    late = BrokerServer.produce(writer, "events", [Record.new("late", key: key)])

    assert {:error, {:sealed, 1}} = late,
           "the parent's primary must refuse the write and seat the frontend at the fenced edge"

    reconcile!(splitter)
    values = splitter |> drain_history(child) |> Enum.map(& &1.value)

    assert values == ["early", "mid"],
           "per-key order inverted: a record produced to the sealed parent after the split reads " <>
             "ahead of the child's own records (issue #41); got #{inspect(values)}"

    # The STORE is fenced, not merely this frontend's bookkeeping: a direct append is refused too, so a
    # node that never learns about the split still cannot get a record into the parent.
    assert {:error, {:sealed, 1}} =
             ReplicationServer.append(
               {name, node()},
               parent_segment.id,
               parent_segment.replica_set,
               parent_segment.start_offset,
               [Record.new("direct", key: key)]
             )

    # And the refusal converges rather than wedging: once the writer sees the split it routes to the
    # child, and the retried record lands AFTER "mid", which is the order the client wrote them in.
    reconcile!(writer)
    assert {:ok, _} = BrokerServer.produce(writer, "events", [Record.new("late", key: key)])
    reconcile!(splitter)

    assert splitter |> drain_history(child) |> Enum.map(& &1.value) == ["early", "mid", "late"]

    :ok = BrokerServer.stop(writer)
    :ok = BrokerServer.stop(splitter)
  end

  test "a fence whose control-plane seal never landed stops wedging the range (issue #121)" do
    # THE REPRODUCTION. `BrokerServer.fence_and_seal/2` closes the segment on the primary and only then
    # records the seal. On a generic error from that second step (an `ra` timeout is the ordinary way to
    # get one) `Broker.record_seal/5` returns the broker UNCHANGED: no new metadata, and no roll owed,
    # so nothing retries. The store is closed, the control plane still calls the segment active, and the
    # produce path loops forever between adopting that segment and being refused by its store.
    #
    # Nothing else rescued it. `Failover` requires the primary to be DEAD and here it is alive and
    # answering; healing and retention only touch sealed segments; `drop_stale_active_segments/1` keys
    # off the segment being sealed in the metadata, which is exactly what did not happen.
    cluster = :"bs_orphan121_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)

    name = :"orphan121_repl_#{System.unique_integer([:positive])}"
    directory = Path.join(System.tmp_dir!(), "malachi_orphan121_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    {:ok, repl} = ReplicationServer.start_link(directory: directory, name: name)
    on_exit(fn -> stop_quietly(repl) end)
    primary = {name, node()}

    {:ok, broker} =
      BrokerServer.start_link("unused",
        brokers: [primary],
        metadata_cluster: cluster,
        group_commit: false,
        brokers_refresh_interval: 60_000
      )

    on_exit(fn -> stop_quietly(broker) end)

    {:ok, root} = BrokerServer.create_topic(broker, "events", 4)
    assert {:ok, _} = BrokerServer.produce(broker, "events", [Record.new("early", key: "k")])

    [segment] = Metadata.segments_of_range(BrokerServer.metadata(broker), root)

    # Put the data plane in exactly that state: fence the store, leave the metadata alone. Then drop
    # this frontend's cache, which is what a restart (or any other node) would face.
    assert {:ok, 1, sealed_bytes} = ReplicationServer.seal(primary, segment.id, segment.start_offset)
    reconcile!(broker)
    assert Metadata.get_segment(BrokerServer.metadata(broker), segment.id).state == :active

    # The loop, verbatim from the issue: every produce readopts the segment the metadata still calls
    # active, and every one of them is refused at the same offset. A successor is never opened.
    for _attempt <- 1..3 do
      assert {:error, {:sealed, 1}} = BrokerServer.produce(broker, "events", [Record.new("blocked", key: "k")])
    end

    assert [{_id, :active}] =
             BrokerServer.metadata(broker)
             |> Metadata.segments_of_range(root)
             |> Enum.map(&{&1.id, &1.state})

    # One reconciling pass, wired exactly as `Malachi.Application` wires it.
    coordinator =
      start_supervised!(
        {HealCoordinator,
         live_brokers: fn -> [primary] end,
         metadata_source: fn -> BrokerServer.metadata(broker) end,
         apply_command: fn command -> BrokerServer.apply_heal(broker, [command]) end,
         replication_factor: 1,
         interval: 60_000},
        id: {:heal121, System.unique_integer([:positive])}
      )

    assert [{:seal_segment, segment_id, 1, ^sealed_bytes, _at}] = HealCoordinator.heal_now(coordinator).applied
    assert segment_id == segment.id

    # Convergence, which is what the issue asks for in place of the three refusals: the parent is
    # sealed at the end its own store reported, and the next produce opens a SUCCESSOR there.
    parent = Metadata.get_segment(BrokerServer.metadata(broker), segment.id)
    assert parent.state == :sealed
    assert parent.length == 1

    # And in the RAFT LOG, not only in this frontend's cache. That distinction is the whole reason the
    # reconciliation is a level-triggered pass rather than a retry at the source: a retry converges only
    # while the process that owed the seal is alive, and what makes the pass hold across a restart is
    # that its seal is replicated. A consistent query reads it from the leader rather than from a copy.
    {:ok, replicated} = MetadataServer.query({cluster, node()}, &Function.identity/1)
    assert %{state: :sealed, length: 1} = Metadata.get_segment(replicated, segment.id)

    assert {:ok, placements} = BrokerServer.produce(broker, "events", [Record.new("after", key: "k")])
    assert %{^root => {1, 1}} = placements

    successor = Metadata.segments_of_range(BrokerServer.metadata(broker), root) |> Enum.find(&(&1.state == :active))
    assert successor.id != segment.id
    assert successor.start_offset == 1

    # And the range reads as one contiguous history across the fence, which is the point of sealing at
    # the store's own answer rather than at a number measured beside it.
    assert broker |> drain_history(root) |> Enum.map(& &1.value) == ["early", "after"]
  end

  # Drives one metadata reconcile and waits for it to land. `:sys.get_state/1` is a system message
  # queued behind `:reconcile` in the same mailbox, so when it answers the reconcile has been applied:
  # deterministic where a sleep would be timing-dependent.
  defp reconcile!(server) do
    send(server, :reconcile)
    _ = :sys.get_state(server)
    :ok
  end

  defp drain_history(server, range_id, cursor \\ :start, accumulated \\ []) do
    case BrokerServer.stream_history(server, range_id, cursor, 3) do
      {:ok, records, :done} -> [records | accumulated] |> Enum.reverse() |> List.flatten()
      {:ok, records, next} -> drain_history(server, range_id, next, [records | accumulated])
    end
  end
end
