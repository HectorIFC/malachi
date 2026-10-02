defmodule Malachi.Cluster.KnownSegmentsTest do
  # async: false: ra is global and stateful.
  @moduledoc """
  What `Malachi.Retention.OrphanSweeper` asks before it removes a directory, against real vnodes: which
  segment ids the vnodes that OWN them list, read linearizably from those vnodes and from no others.
  """
  use ExUnit.Case, async: false

  alias Malachi.Application, as: App
  alias Malachi.BrokerServer
  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.RaCluster
  alias Malachi.Cluster.ReplicatedDSRSM
  alias Malachi.Cluster.RingMachine
  alias Malachi.Cluster.RingServer
  alias Malachi.Cluster.RingTopology
  alias Malachi.Log.Record
  alias Malachi.Storage.Layout
  alias Malachi.Test.AppChildren
  alias Malachi.Test.SilentRaMember
  alias Malachi.Test.TmpDir

  @timeout 500

  # Two live vnodes at fixed tokens on a 16-position ring, with unique names (ra clusters are global).
  defp start_vnodes do
    suffix = System.unique_integer([:positive])
    a = :"ks_a_#{suffix}"
    b = :"ks_b_#{suffix}"
    {:ok, state} = ReplicatedDSRSM.new(ring_bits: 4) |> ReplicatedDSRSM.add_vnode(a, 4)
    {:ok, state} = ReplicatedDSRSM.add_vnode(state, b, 12)
    on_exit(fn -> ReplicatedDSRSM.delete(state) end)
    {state, RingTopology.new(state.ring, %{a => [node()], b => [node()]}), a, b}
  end

  # A topic whose name routes to `vnode` under `ring`.
  defp topic_on(ring, vnode, prefix) do
    Enum.find(Stream.map(0..500, &"#{prefix}_#{&1}"), &(HashRing.route(ring, &1) == {:ok, vnode}))
  end

  # Registers segment 1 of the topic's root range directly on `server_id`, the way another broker's
  # write lands: through the owner's log, never through anybody's cache.
  defp register_segment!(server_id, topic) do
    {:ok, {:ok, root}} = MetadataServer.command(server_id, {:create_topic, topic, 4})
    segment_id = {root, 1}
    {:ok, :ok} = MetadataServer.command(server_id, {:register_segment, root, segment_id, [:b1], 0})
    segment_id
  end

  describe "MetadataServer.segments/2" do
    test "returns the vnode's segment map and nothing else" do
      {state, _topology, a, _b} = start_vnodes()
      topic = topic_on(state.ring, a, "seg")
      segment_id = register_segment!(state.vnodes[a], topic)

      assert {:ok, segments} = MetadataServer.segments(state.vnodes[a])
      assert Map.keys(segments) == [segment_id]
    end

    test "a state without segments is an error, never an empty vnode" do
      # An empty map here would read as "this vnode lists nothing", which is what authorizes a removal.
      name = :"ks_ring_#{System.unique_integer([:positive])}"
      {:ok, server_id} = RaCluster.start(RingMachine, name, [node()])
      on_exit(fn -> RaCluster.delete(name) end)

      assert {:error, {:unexpected_state, _state}} = MetadataServer.segments(server_id)
    end

    test "a member that does not answer is an error within the bound" do
      name = :"ks_silent_#{System.unique_integer([:positive])}"
      {:ok, _pid} = SilentRaMember.start_link(name)
      on_exit(fn -> SilentRaMember.stop(name) end)

      assert {:error, _reason} = MetadataServer.segments({name, node()}, 200)
    end
  end

  describe "known_segments/3" do
    test "reports the ids their owners list, and only those" do
      {state, topology, a, b} = start_vnodes()
      on_a = register_segment!(state.vnodes[a], topic_on(state.ring, a, "live"))
      on_b = register_segment!(state.vnodes[b], topic_on(state.ring, b, "live"))
      {{topic_a, range}, _seq} = on_a
      unlisted = {{topic_a, range}, 99}

      assert {:ok, %{known: known, unroutable: []}} =
               ReplicatedDSRSM.known_segments(topology, [on_a, on_b, unlisted], @timeout)

      assert known == MapSet.new([on_a, on_b])
    end

    test "an id without a topic is unroutable, never unknown" do
      {_state, topology, _a, _b} = start_vnodes()

      assert {:ok, %{known: known, unroutable: [{:segment, 7}]}} =
               ReplicatedDSRSM.known_segments(topology, [{:segment, 7}], @timeout)

      assert known == MapSet.new()
    end

    test "asks only the owners: a silent vnode that owns none of the ids does not matter" do
      {state, topology, a, _b} = start_vnodes()
      silent = :"ks_mute_#{System.unique_integer([:positive])}"
      {:ok, _pid} = SilentRaMember.start_link(silent)
      on_exit(fn -> SilentRaMember.stop(silent) end)

      {:ok, ring} = HashRing.add_vnode(topology.ring, silent, 8)
      topology = RingTopology.new(ring, Map.put(topology.placements, silent, [node()]))
      segment_id = register_segment!(state.vnodes[a], topic_on(ring, a, "owned"))

      assert {:ok, %{known: known}} = ReplicatedDSRSM.known_segments(topology, [segment_id], @timeout)
      assert known == MapSet.new([segment_id])
    end

    test "a silent owner fails the whole answer" do
      {_state, topology, _a, _b} = start_vnodes()
      silent = :"ks_mute_#{System.unique_integer([:positive])}"
      {:ok, _pid} = SilentRaMember.start_link(silent)
      on_exit(fn -> SilentRaMember.stop(silent) end)

      {:ok, ring} = HashRing.add_vnode(topology.ring, silent, 8)
      topology = RingTopology.new(ring, Map.put(topology.placements, silent, [node()]))
      id = {{topic_on(ring, silent, "mute"), 0}, 1}

      assert ReplicatedDSRSM.known_segments(topology, [id], 200) == {:error, {:vnodes_unreachable, [silent]}}
    end

    test "an owner whose members redirect to each other is cut off by the bound, and is silent" do
      # ra follows a redirect with a fresh full timeout, so no single read ever times out here. Only the
      # bound on the whole fan-out ends it, and the owner comes back as what it is: one that did not answer.
      suffix = System.unique_integer([:positive])
      ping = :"ks_ping_#{suffix}"
      pong = :"ks_pong_#{suffix}"
      {:ok, _pid} = SilentRaMember.start_redirecting(ping, {pong, node()}, 20)
      on_exit(fn -> SilentRaMember.stop(ping) end)
      {:ok, _pid} = SilentRaMember.start_redirecting(pong, {ping, node()}, 20)
      on_exit(fn -> SilentRaMember.stop(pong) end)

      {:ok, ring} = HashRing.add_vnode(HashRing.new(ring_bits: 4), ping, 0)
      topology = RingTopology.new(ring, %{ping => [node()]})

      assert ReplicatedDSRSM.known_segments(topology, [{{"any", 0}, 1}], 200) ==
               {:error, {:vnodes_unreachable, [ping]}}
    end

    test "a segment written to a vnode that does not own it is misplaced, never unknown" do
      # A broker routing by a ring gossip had not updated yet can create a topic on the old owner of
      # its arc. Nothing moves it afterwards, but its segments are real and their replicas are on disk.
      {state, topology, a, b} = start_vnodes()
      stray = register_segment!(state.vnodes[b], topic_on(state.ring, a, "stray"))

      assert {:ok, %{known: known, misplaced: misplaced}} = ReplicatedDSRSM.known_segments(topology, [stray], @timeout)
      assert known == MapSet.new()
      assert misplaced == [stray]
    end

    test "a real orphan is asked of every vnode and comes back absent from all" do
      {state, topology, a, _b} = start_vnodes()
      gone = {{topic_on(state.ring, a, "gone"), 0}, 1}

      assert {:ok, %{known: known, unroutable: [], migrating: [], misplaced: []}} =
               ReplicatedDSRSM.known_segments(topology, [gone], @timeout)

      assert known == MapSet.new()
    end

    test "a silent vnode anywhere holds a pass that has a real orphan to decide" do
      # Every vnode must be able to say it does not have the segment before its directory can go.
      {_state, topology, a, _b} = start_vnodes()
      silent = :"ks_mute_#{System.unique_integer([:positive])}"
      {:ok, _pid} = SilentRaMember.start_link(silent)
      on_exit(fn -> SilentRaMember.stop(silent) end)

      {:ok, ring} = HashRing.add_vnode(topology.ring, silent, 8)
      topology = RingTopology.new(ring, Map.put(topology.placements, silent, [node()]))
      gone = {{topic_on(ring, a, "gone_silent"), 0}, 1}

      assert ReplicatedDSRSM.known_segments(topology, [gone], 200) == {:error, {:vnodes_unreachable, [silent]}}
    end

    test "an owner with no placement has nowhere to be asked and is as silent as one that does not answer" do
      {state, topology, a, _b} = start_vnodes()
      topology = %{topology | placements: Map.delete(topology.placements, a)}
      id = {{topic_on(state.ring, a, "nowhere"), 0}, 1}

      assert ReplicatedDSRSM.known_segments(topology, [id], 200) == {:error, {:vnodes_unreachable, [a]}}
    end

    test "tries every node of a placement, since a rebalance moves members from where they were put" do
      {state, topology, a, _b} = start_vnodes()
      segment_id = register_segment!(state.vnodes[a], topic_on(state.ring, a, "moved"))
      topology = %{topology | placements: Map.put(topology.placements, a, [:"gone@127.0.0.1", node()])}

      assert {:ok, %{known: known}} = ReplicatedDSRSM.known_segments(topology, [segment_id], @timeout)
      assert known == MapSet.new([segment_id])
    end

    test "during a pending split, a topic already moved to the new vnode is still known" do
      # The split moves a topic's metadata before the ring that routes to its new vnode is published.
      # Asked only under the current ring, the old owner no longer lists it, and its directory would
      # look orphaned for the whole window.
      {state, topology, _a, _b} = start_vnodes()
      new_vnode = :"ks_new_#{System.unique_integer([:positive])}"
      {:ok, new_server} = MetadataServer.start(new_vnode, [node()])
      on_exit(fn -> MetadataServer.delete(new_server) end)

      {:ok, advanced} = HashRing.add_vnode(state.ring, new_vnode, 8)
      moved = register_segment!(new_server, topic_on(advanced, new_vnode, "moved"))
      pending = RingTopology.begin_split(topology, new_vnode, 8, [node()])

      assert {:ok, %{known: known}} = ReplicatedDSRSM.known_segments(pending, [moved], @timeout)
      assert known == MapSet.new([moved])

      # Without the pending intent nobody routes to the new vnode, which is the window this covers.
      assert {:ok, %{known: unknown}} = ReplicatedDSRSM.known_segments(topology, [moved], @timeout)
      assert unknown == MapSet.new()
    end

    test "during a pending split, a moving topic listed by neither owner is migrating, not unknown" do
      # The two owners are read at two moments. A topic copied to the new vnode after it was read and
      # extracted from the old one before it was read is listed by neither, and has not gone anywhere.
      {state, topology, a, _b} = start_vnodes()
      new_vnode = :"ks_new_#{System.unique_integer([:positive])}"
      {:ok, new_server} = MetadataServer.start(new_vnode, [node()])
      on_exit(fn -> MetadataServer.delete(new_server) end)

      {:ok, advanced} = HashRing.add_vnode(state.ring, new_vnode, 8)
      moving = {{topic_on(advanced, new_vnode, "in_flight"), 0}, 1}
      staying = {{topic_on(advanced, a, "staying"), 0}, 1}
      pending = RingTopology.begin_split(topology, new_vnode, 8, [node()])

      assert {:ok, %{known: known, migrating: migrating}} =
               ReplicatedDSRSM.known_segments(pending, [moving, staying], @timeout)

      assert known == MapSet.new()
      assert migrating == [moving]
    end

    test "a pending split whose token is already on the ring routes by the ring alone" do
      {state, topology, a, _b} = start_vnodes()
      segment_id = register_segment!(state.vnodes[a], topic_on(state.ring, a, "done"))
      pending = RingTopology.begin_split(topology, a, 4, [node()])

      assert {:ok, %{known: known}} = ReplicatedDSRSM.known_segments(pending, [segment_id], @timeout)
      assert known == MapSet.new([segment_id])
    end

    test "without a ring there is nothing to route by" do
      assert ReplicatedDSRSM.known_segments(nil, [{{"t", 0}, 1}], @timeout) == {:error, :no_topology}

      empty = RingTopology.new(HashRing.new(ring_bits: 4), %{})
      assert ReplicatedDSRSM.known_segments(empty, [{{"t", 0}, 1}], @timeout) == {:error, :no_topology}
    end
  end

  describe "known_segments_stable/3" do
    test "answers when the topology did not move while the owners were asked" do
      {state, topology, a, _b} = start_vnodes()
      segment_id = register_segment!(state.vnodes[a], topic_on(state.ring, a, "stable"))

      assert {:ok, %{known: known}} =
               ReplicatedDSRSM.known_segments_stable(fn -> {:ok, topology} end, [segment_id], @timeout)

      assert known == MapSet.new([segment_id])
    end

    test "refuses an answer given while the topology moved" do
      {_state, topology, _a, _b} = start_vnodes()
      reads = :counters.new(1, [])

      read_topology = fn ->
        :counters.add(reads, 1, 1)
        {:ok, %{topology | version: :counters.get(reads, 1)}}
      end

      assert ReplicatedDSRSM.known_segments_stable(read_topology, [{{"t", 0}, 1}], @timeout) ==
               {:error, {:topology_changed, 1, 2}}
    end

    test "a topology that vanished while the owners were asked is a change too" do
      {_state, topology, _a, _b} = start_vnodes()
      reads = :counters.new(1, [])

      read_topology = fn ->
        :counters.add(reads, 1, 1)
        if :counters.get(reads, 1) == 1, do: {:ok, topology}, else: {:ok, nil}
      end

      assert ReplicatedDSRSM.known_segments_stable(read_topology, [{{"t", 0}, 1}], @timeout) ==
               {:error, {:topology_changed, 0, nil}}
    end

    test "a topology that cannot be read is returned as the reason" do
      assert ReplicatedDSRSM.known_segments_stable(fn -> {:error, :topology_unavailable} end, [], @timeout) ==
               {:error, :topology_unavailable}
    end

    test "no ring yet is no topology" do
      assert ReplicatedDSRSM.known_segments_stable(fn -> {:ok, nil} end, [], @timeout) == {:error, :no_topology}
    end
  end

  describe "Malachi.Application.orphan_authority/3" do
    test "an unsharded control plane asks its one cluster" do
      cluster = :"ks_single_#{System.unique_integer([:positive])}"
      {:ok, server_id} = MetadataServer.start(cluster, [node()])
      on_exit(fn -> MetadataServer.delete(server_id) end)
      live = register_segment!(server_id, "single")

      authority = App.orphan_authority(cluster, [node()], nil)
      live_name = Path.basename(Layout.segment_directory("/", live))

      assert {:ok, %{known: known, undecided: undecided}} = authority.([live_name, "single-r0-s99"])
      assert known == MapSet.new([live_name])
      assert undecided == MapSet.new()
    end

    test "the clustered sweeper asks the owners by the durable ring, never the broker cache" do
      # The wiring a clustered node runs, held to #249: the sweeper's authority has to reach the vnode
      # that owns the segment through the topology of record. Handing it a cache-backed authority (the
      # broker's metadata) or a gossip-backed topology fails here, because the segment below exists
      # only on its owner and the only topology is the one in the ring store.
      {state, topology, a, _b} = start_vnodes()
      live = register_segment!(state.vnodes[a], topic_on(state.ring, a, "wired"))
      AppChildren.borrow_ring()
      AppChildren.start_ring!(topology)

      [%{start: {Malachi.Retention.OrphanSweeper, :start_link, [opts]}}] =
        App.orphan_sweeper_children(:ks_wired, [node()], [{a, 4, [node()]}])

      live_name = Path.basename(Layout.segment_directory("/", live))
      assert {:ok, %{known: known}} = opts[:authority].([live_name])
      assert known == MapSet.new([live_name])
    end

    test "a sharded control plane without its ring store holds the pass" do
      # A store name nothing registered: the read fails at once, whichever tests ran before this one.
      missing = {:"ks_ring_missing_#{System.unique_integer([:positive])}", node()}

      assert {:error, {:topology_unavailable, _reason}} = App.durable_orphan_authority(missing).(["t-r0-s1"])
    end
  end

  describe "Malachi.Application.durable_orphan_authority/1" do
    test "routes by the topology of record" do
      {state, topology, a, _b} = start_vnodes()
      live = register_segment!(state.vnodes[a], topic_on(state.ring, a, "record"))
      ring = start_ring!(:"ks_ring_#{System.unique_integer([:positive])}", topology)

      live_name = Path.basename(Layout.segment_directory("/", live))
      assert {:ok, %{known: known}} = App.durable_orphan_authority(ring).([live_name, "record-r0-s99"])
      assert known == MapSet.new([live_name])
    end

    test "sees a pending split the moment it is recorded, whatever gossip has delivered" do
      # A split records its intent in the store before it moves any topic, and only then gossips it.
      # A node reading gossip could still route the moved topic to its old owner alone.
      {state, topology, _a, _b} = start_vnodes()
      new_vnode = :"ks_new_#{System.unique_integer([:positive])}"
      {:ok, new_server} = MetadataServer.start(new_vnode, [node()])
      on_exit(fn -> MetadataServer.delete(new_server) end)

      {:ok, advanced} = HashRing.add_vnode(state.ring, new_vnode, 8)
      moved = register_segment!(new_server, topic_on(advanced, new_vnode, "record_moved"))
      ring = start_ring!(:"ks_ring_#{System.unique_integer([:positive])}", topology)
      :ok = RingServer.advance(ring, 0, 0, RingTopology.begin_split(topology, new_vnode, 8, [node()]))

      moved_name = Path.basename(Layout.segment_directory("/", moved))
      assert {:ok, %{known: known}} = App.durable_orphan_authority(ring).([moved_name])
      assert known == MapSet.new([moved_name])
    end

    test "a store that has never had a ring is no topology" do
      ring = start_ring!(:"ks_ring_#{System.unique_integer([:positive])}", nil)

      assert App.durable_orphan_authority(ring).(["t-r0-s1"]) == {:error, :no_topology}
    end

    test "a store that does not answer holds the pass, within the sweep's short bound" do
      # ra's own default is five seconds per read; the sweep reads twice per pass. It passes a short
      # bound, and a store that does not answer costs about that, not the default.
      name = :"ks_ring_mute_#{System.unique_integer([:positive])}"
      {:ok, _pid} = SilentRaMember.start_link(name)
      on_exit(fn -> SilentRaMember.stop(name) end)

      {elapsed_us, answer} = :timer.tc(fn -> App.durable_orphan_authority({name, node()}).(["t-r0-s1"]) end)

      assert {:error, {:topology_unavailable, _reason}} = answer
      assert elapsed_us < 3_000_000, "a silent ring store held the read for #{div(elapsed_us, 1000)}ms"
    end
  end

  # A ring store holding `topology` (or none), the way boot leaves it.
  defp start_ring!(name, topology) do
    {:ok, server_id} = RaCluster.start(RingMachine, name, [node()])
    on_exit(fn -> RaCluster.delete(name) end)
    if topology, do: :ok = RingServer.init(server_id, topology)
    server_id
  end
end
