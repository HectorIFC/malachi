defmodule Malachi.Retention.OrphanSweeperMultinodeTest do
  # The orphan sweep asking an owning vnode whose every member, leader included, is on ANOTHER node: the
  # projection the leader applies (`MetadataServer.segments/2`) runs there, and only the segment map
  # crosses the wire. Everything this node decides about its own disk comes from that answer.
  #
  # async: false and tagged, like every test that starts peer nodes.
  use ExUnit.Case, async: false

  @moduletag :multinode

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.ReplicatedDSRSM
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.RingTopology
  alias Malachi.Retention.Orphans
  alias Malachi.Retention.OrphanSweeper
  alias Malachi.Storage.Layout
  alias Malachi.Test.Distribution
  alias Malachi.Test.TmpDir

  setup_all do
    :ok = Distribution.ensure_started()
    {:ok, _} = Application.ensure_all_started(:ra)
    :ok
  end

  defp start_peer do
    {peer, node, name} = Distribution.start_peer("orphan")
    {:ok, _} = :erpc.call(node, :application, :ensure_all_started, [:ra])
    data_dir = String.to_charlist(TmpDir.path("malachi_ra_orphan_#{name}"))
    {:ok, _} = :erpc.call(node, :ra, :start_in, [data_dir])
    on_exit(fn -> Distribution.stop_peer(peer) end)
    node
  end

  test "an owner led from another node confirms the live directory, and the orphan goes" do
    peer = start_peer()
    vnode = :"orphan_mn_#{System.unique_integer([:positive])}"
    {:ok, server_id} = MetadataServer.start(vnode, [peer])
    assert server_id == {vnode, peer}

    {:ok, {:ok, root}} = MetadataServer.command(server_id, {:create_topic, "remote", 4})
    live_id = {root, 1}
    {:ok, :ok} = MetadataServer.command(server_id, {:register_segment, root, live_id, [:b1], 0})

    {:ok, ring} = HashRing.add_vnode(HashRing.new(), vnode, 0)
    topology = RingTopology.new(ring, %{vnode => [peer]})

    directory = TmpDir.path("malachi_orphan_mn")
    on_exit(fn -> File.rm_rf!(directory) end)
    {:ok, repl} = ReplicationServer.start_link(directory: directory)
    on_exit(fn -> if Process.alive?(repl), do: GenServer.stop(repl) end)

    live = directory!(directory, live_id)
    orphan = directory!(directory, {root, 2})

    sweeper =
      start_supervised!(
        {OrphanSweeper,
         authority: &Orphans.explain(&1, fn ids -> ReplicatedDSRSM.known_segments(topology, ids, 2_000) end),
         local_ref: repl,
         directory: directory,
         interval: 3_600_000,
         clock: fn -> System.system_time(:millisecond) + 700_000 end,
         on_result: fn _result -> :ok end}
      )

    assert %{removed: [], held: [^orphan], skipped: nil} = OrphanSweeper.sweep_now(sweeper)
    assert %{removed: [^orphan]} = OrphanSweeper.sweep_now(sweeper)
    assert File.exists?(Path.join(directory, live))
  end

  defp directory!(directory, segment_id) do
    path = Layout.segment_directory(directory, segment_id)
    File.mkdir_p!(path)
    Path.basename(path)
  end
end
