defmodule Malachi.Retention.OrphanSweeperRaTest do
  # async: false: ra is global and stateful.
  @moduledoc """
  The orphan sweep against a real sharded control plane, in the situation #249 is about: this node's
  cached copy of the metadata has not caught up with a segment registered through another path, while
  that segment's replica is already on this node's disk.
  """
  use ExUnit.Case, async: false

  import Malachi.Test.TeardownHelper

  alias Malachi.BrokerServer
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.ReplicatedDSRSM
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.RingTopology
  alias Malachi.Retention.Orphans
  alias Malachi.Retention.OrphanSweeper
  alias Malachi.Storage.Layout
  alias Malachi.Test.TmpDir

  # Past the sweep's default minimum age, so the directories created a moment ago are old enough.
  @later_ms 700_000

  test "a segment the broker's cache has not seen yet keeps its directory; a real orphan goes" do
    suffix = System.unique_integer([:positive])
    a = :"osr_a_#{suffix}"
    b = :"osr_b_#{suffix}"
    {:ok, server_a} = MetadataServer.start(a, [node()])
    {:ok, server_b} = MetadataServer.start(b, [node()])
    on_exit(fn -> Enum.each([server_a, server_b], &MetadataServer.delete/1) end)

    directory = TmpDir.path("malachi_osr")
    on_exit(fn -> File.rm_rf!(directory) end)
    {:ok, repl} = ReplicationServer.start_link(directory: directory)
    on_exit(fn -> stop_quietly(repl) end)

    # A refresh interval of an hour: after boot this broker's cache never moves again, which is the
    # stale view of #249 made by construction instead of by a slow control plane and a race.
    {:ok, broker} =
      BrokerServer.start_link("unused",
        brokers: [repl],
        metadata_vnodes: [{a, 0, [node()]}, {b, div(Integer.pow(2, 32), 2), [node()]}],
        bootstrap_orchestrator: fn -> false end,
        brokers_refresh_interval: 3_600_000
      )

    on_exit(fn -> stop_quietly(broker) end)
    ring = :sys.get_state(broker).broker.dsrsm.ring
    topology = RingTopology.new(ring, %{a => [node()], b => [node()]})

    # Registered through the owner's log, the way a write through another broker lands. The cache here
    # never hears of it.
    topic =
      Enum.find(
        Stream.map(0..500, &"live_#{&1}"),
        &(ReplicatedDSRSM.vnode_for(%ReplicatedDSRSM{ring: ring}, &1) == {:ok, a})
      )

    {:ok, {:ok, root}} = MetadataServer.command(server_a, {:create_topic, topic, 4})
    live_id = {root, 1}
    {:ok, :ok} = MetadataServer.command(server_a, {:register_segment, root, live_id, [:b1], 0})
    refute Map.has_key?(BrokerServer.metadata(broker).segments, live_id), "the cache is stale, as intended"

    live = directory!(directory, live_id)
    orphan = directory!(directory, {{topic, 0}, 42})

    sweeper =
      start_supervised!(
        {OrphanSweeper,
         authority: &Orphans.explain(&1, fn ids -> ReplicatedDSRSM.known_segments(topology, ids, 1_000) end),
         local_ref: repl,
         directory: directory,
         interval: 3_600_000,
         clock: fn -> System.system_time(:millisecond) + @later_ms end,
         on_result: fn _result -> :ok end}
      )

    assert %{removed: [], held: held} = OrphanSweeper.sweep_now(sweeper)
    assert held == [orphan]
    assert %{removed: [^orphan]} = OrphanSweeper.sweep_now(sweeper)

    assert File.exists?(Path.join(directory, live)), "the live replica's directory was removed"
    refute File.exists?(Path.join(directory, orphan))
  end

  defp directory!(directory, segment_id) do
    path = Layout.segment_directory(directory, segment_id)
    File.mkdir_p!(path)
    File.write!(Path.join(path, "00000000000000000000.log"), "")
    Path.basename(path)
  end
end
