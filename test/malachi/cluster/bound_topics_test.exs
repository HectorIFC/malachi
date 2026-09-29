defmodule Malachi.Cluster.BoundTopicsTest do
  # async: false: ra is global and stateful.
  @moduledoc """
  What `Policies.delete/3` asks before it removes a policy some topic may still be bound to:
  which topics are bound to it, read linearizably from every vnode, never from this node's cached copy
  of the metadata.
  """
  use ExUnit.Case, async: false

  alias Malachi.Application, as: App
  alias Malachi.BrokerServer
  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.PolicyStore
  alias Malachi.Cluster.RaCluster
  alias Malachi.Cluster.ReplicatedDSRSM
  alias Malachi.Cluster.RingMachine
  alias Malachi.Cluster.RingServer
  alias Malachi.Cluster.RingTopology
  alias Malachi.DataPlaneRouter
  alias Malachi.LogApi
  alias Malachi.Policies
  alias Malachi.Test.SilentRaMember
  alias Malachi.Test.TmpDir

  @timeout 500

  defp start_vnodes do
    suffix = System.unique_integer([:positive])
    a = :"bt_a_#{suffix}"
    b = :"bt_b_#{suffix}"
    {:ok, state} = ReplicatedDSRSM.new(ring_bits: 4) |> ReplicatedDSRSM.add_vnode(a, 4)
    {:ok, state} = ReplicatedDSRSM.add_vnode(state, b, 12)
    on_exit(fn -> ReplicatedDSRSM.delete(state) end)
    {state, RingTopology.new(state.ring, %{a => [node()], b => [node()]}), a, b}
  end

  defp topic_on(ring, vnode, prefix) do
    Enum.find(Stream.map(0..500, &"#{prefix}_#{&1}"), &(HashRing.route(ring, &1) == {:ok, vnode}))
  end

  # Creates and binds the topic directly on `server_id`, the way another node's write lands: through the
  # owner's log, never through anybody's cache.
  defp bind!(server_id, topic, policy) do
    {:ok, {:ok, _root}} = MetadataServer.command(server_id, {:create_topic, topic, 4})
    {:ok, :ok} = MetadataServer.command(server_id, {:bind_topic_policy, topic, policy})
    topic
  end

  describe "MetadataServer.topics/2" do
    test "returns the vnode's topic records and nothing else" do
      {state, _topology, a, _b} = start_vnodes()
      topic = bind!(state.vnodes[a], topic_on(state.ring, a, "rec"), "keep")

      assert {:ok, %{^topic => %{policy: "keep"}} = topics} = MetadataServer.topics(state.vnodes[a])
      assert map_size(topics) == 1
    end

    test "a state without topics is an error, never an empty vnode" do
      name = :"bt_ring_#{System.unique_integer([:positive])}"
      {:ok, server_id} = RaCluster.start(RingMachine, name, [node()])
      on_exit(fn -> RaCluster.delete(name) end)

      assert {:error, {:unexpected_state, _state}} = MetadataServer.topics(server_id)
    end
  end

  describe "topics_bound_to/3" do
    test "collects the bound topics of every vnode, sorted, and no others" do
      {state, topology, a, b} = start_vnodes()
      on_a = bind!(state.vnodes[a], topic_on(state.ring, a, "x"), "keep")
      on_b = bind!(state.vnodes[b], topic_on(state.ring, b, "y"), "keep")
      _other = bind!(state.vnodes[b], topic_on(state.ring, b, "z"), "other")

      assert ReplicatedDSRSM.topics_bound_to(topology, "keep", @timeout) == {:ok, Enum.sort([on_a, on_b])}
      assert ReplicatedDSRSM.topics_bound_to(topology, "ghost", @timeout) == {:ok, []}
    end

    test "a topic bound on a vnode that does not own it by the ring still counts" do
      # A vnode only stores what was routed to it, but a broker routing by a stale ring can write a topic
      # to a vnode the ring no longer sends it to. Its binding is real.
      {state, topology, a, b} = start_vnodes()
      misplaced = bind!(state.vnodes[b], topic_on(state.ring, a, "mis"), "keep")

      assert ReplicatedDSRSM.topics_bound_to(topology, "keep", @timeout) == {:ok, [misplaced]}
    end

    test "a silent vnode fails the whole answer, because it may hold the binding" do
      {_state, topology, _a, _b} = start_vnodes()
      silent = :"bt_mute_#{System.unique_integer([:positive])}"
      {:ok, _pid} = SilentRaMember.start_link(silent)
      on_exit(fn -> SilentRaMember.stop(silent) end)

      {:ok, ring} = HashRing.add_vnode(topology.ring, silent, 8)
      topology = RingTopology.new(ring, Map.put(topology.placements, silent, [node()]))

      assert ReplicatedDSRSM.topics_bound_to(topology, "keep", 200) == {:error, {:vnodes_unreachable, [silent]}}
    end

    test "a pending split is refused: a topic between its two owners is listed by neither" do
      {_state, topology, _a, _b} = start_vnodes()
      pending = %{topology | pending: %{new_vnode: :bt_new, token: 8, nodes: [node()]}}

      assert ReplicatedDSRSM.topics_bound_to(pending, "keep", @timeout) == {:error, :split_in_progress}
    end

    test "without a ring there is nothing to ask" do
      assert ReplicatedDSRSM.topics_bound_to(nil, "keep", @timeout) == {:error, :no_topology}
    end
  end

  describe "topics_bound_to_stable/3" do
    test "answers when the topology did not move while the vnodes were asked" do
      {state, topology, a, _b} = start_vnodes()
      topic = bind!(state.vnodes[a], topic_on(state.ring, a, "st"), "keep")

      assert ReplicatedDSRSM.topics_bound_to_stable(fn -> {:ok, topology} end, "keep", @timeout) == {:ok, [topic]}
    end

    test "refuses an answer given while the topology moved, and passes a read error through" do
      {_state, topology, _a, _b} = start_vnodes()
      reads = :counters.new(1, [])

      read_topology = fn ->
        :counters.add(reads, 1, 1)
        {:ok, %{topology | version: :counters.get(reads, 1)}}
      end

      assert ReplicatedDSRSM.topics_bound_to_stable(read_topology, "keep", @timeout) ==
               {:error, {:topology_changed, 1, 2}}

      assert ReplicatedDSRSM.topics_bound_to_stable(fn -> {:error, :down} end, "keep", @timeout) == {:error, :down}
    end
  end

  describe "Malachi.Application.bound_topics/1 on a single node" do
    test "reads the brokers, whose metadata is the truth when there is no control plane" do
      suffix = System.unique_integer([:positive])
      topic = "bt-local-#{suffix}"
      :ok = LogApi.create_topic(DataPlaneRouter.shard_for(topic), topic)
      :ok = BrokerServer.bind_topic_policy(DataPlaneRouter.shard_for(topic), topic, "bt_#{suffix}")

      assert App.bound_topics("bt_#{suffix}") == {:ok, [topic]}
    end
  end

  describe "Malachi.Application.bound_topics/1 over several data-plane shards" do
    # MALACHI_DATA_SHARDS runs one broker per shard, each with its own topics: the question has to reach
    # every one of them.
    setup do
      previous = Application.fetch_env(:malachi, :data_shards)
      Application.put_env(:malachi, :data_shards, 2)

      on_exit(fn ->
        case previous do
          {:ok, value} -> Application.put_env(:malachi, :data_shards, value)
          :error -> Application.delete_env(:malachi, :data_shards)
        end
      end)
    end

    test "finds a binding held by the second shard's broker" do
      directory = TmpDir.path("bt_shard1")
      on_exit(fn -> File.rm_rf!(directory) end)
      shard = DataPlaneRouter.shard_name(1)
      start_supervised!(%{id: shard, start: {BrokerServer, :start_link, [directory, [name: shard]]}})

      suffix = System.unique_integer([:positive])
      {:ok, _root} = BrokerServer.create_topic(shard, "bt-shard1-#{suffix}", 4)
      :ok = BrokerServer.bind_topic_policy(shard, "bt-shard1-#{suffix}", "bt_#{suffix}")

      assert App.bound_topics("bt_#{suffix}") == {:ok, ["bt-shard1-#{suffix}"]}
    end

    test "a shard that does not answer refuses the answer, rather than reading as holding nothing" do
      assert App.bound_topics("keep") == {:error, {:shard_unavailable, DataPlaneRouter.shard_name(1)}}
    end

    test "a bind routed to a shard that does not answer is an error, never an exit" do
      topic =
        Enum.find(Stream.map(0..500, &"bt-routed-#{&1}"), &(DataPlaneRouter.shard_for(&1) != Malachi.LogBroker))

      policy = "bt_routed_#{System.unique_integer([:positive])}"
      :ok = Policies.define(policy, [], "tester")
      on_exit(fn -> PolicyStore.delete(policy) end)

      assert {:error, {:broker_unavailable, _reason}} = Policies.bind(topic, policy, "tester")
    end
  end

  describe "Malachi.Application.bound_topics/1 with a control plane" do
    setup do
      previous = Application.fetch_env(:malachi, :log_cluster)
      Application.put_env(:malachi, :log_cluster, :"bt_cluster_#{System.unique_integer([:positive])}")

      on_exit(fn ->
        case previous do
          {:ok, value} -> Application.put_env(:malachi, :log_cluster, value)
          :error -> Application.delete_env(:malachi, :log_cluster)
        end
      end)
    end

    test "an unsharded control plane asks its one metadata cluster" do
      # The ring store answers that no ring was ever recorded, which is what an unsharded clustered node
      # has, and the one cluster named by :log_cluster owns every topic.
      cluster = Application.fetch_env!(:malachi, :log_cluster)
      {:ok, server_id} = MetadataServer.start(cluster, [node()])
      on_exit(fn -> MetadataServer.delete(server_id) end)
      start_ring!(nil)
      topic = bind!(server_id, "single_bound", "keep")

      assert App.bound_topics("keep") == {:ok, [topic]}
    end

    test "a sharded control plane asks the vnodes by the topology of record, never the broker cache" do
      # The binding exists only on its vnode's log: a cache-backed answer would miss it.
      {state, topology, _a, b} = start_vnodes()
      topic = bind!(state.vnodes[b], topic_on(state.ring, b, "durable"), "keep")
      start_ring!(topology)

      assert App.bound_topics("keep") == {:ok, [topic]}
    end

    test "a ring store that cannot be read refuses the answer, and so the delete" do
      # This test node runs no ring store, which is what a store that lost quorum looks like to a reader.
      assert {:error, {:topology_unavailable, _reason}} = App.bound_topics("keep")

      assert {:error, {:bindings_unavailable, {:topology_unavailable, _}}} =
               Policies.delete("bt_#{System.unique_integer([:positive])}", "tester")
    end
  end

  defp start_ring!(topology) do
    {:ok, server_id} = RaCluster.start(RingMachine, Malachi.LogRing, [node()])
    on_exit(fn -> RaCluster.delete(Malachi.LogRing) end)
    if topology, do: :ok = RingServer.init(server_id, topology)
    server_id
  end
end
