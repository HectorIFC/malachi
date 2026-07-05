defmodule Malachi.BrokerServerRaTest do
  # async: false — ra is global/stateful (one data dir, on-disk Raft logs).
  use ExUnit.Case, async: false

  alias Malachi.BrokerServer
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record
  alias Malachi.Metadata

  setup_all do
    data_dir = Path.join(System.tmp_dir!(), "malachi_ra_bs_#{System.unique_integer([:positive])}")
    File.rm_rf!(data_dir)
    _ = :ra.start_in(String.to_charlist(data_dir))
    on_exit(fn -> File.rm_rf!(data_dir) end)
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
    # the ra cluster its name routes to (queried directly) — and to no other vnode
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

  test "the membership leader bootstraps the vnodes via the reconcile loop (D-c-1d)" do
    # membership where this node is the sole (thus lowest) live member → it is the bootstrap leader
    {:ok, membership} = Malachi.Test.AliveMembersStub.start_link([{Malachi.LogMembership, node()}])
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
