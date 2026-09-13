defmodule Malachi.Cluster.StorageFailureMultinodeTest do
  # A storage failure across real BEAM nodes (issue #147): the node whose copy fails answers the producer
  # and stays up, and the heal pass seals the segment on the replicas of the OTHER nodes, so writing moves
  # on to a new segment, which is NorthGuard's "seal it, make a new one, move the producers over".
  #
  # async: false and tagged, like every test that starts peer nodes.
  use ExUnit.Case, async: false

  @moduletag :multinode

  alias Malachi.Cluster.HealCoordinator
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record
  alias Malachi.Metadata
  alias Malachi.Storage.Layout
  alias Malachi.Test.FaultySegmentStore

  # Every peer registers its replication server under the same name: a broker ref is `{name, node}`, so
  # the node alone tells them apart, as in a real cluster.
  @server :storage_failure_repl

  setup_all do
    _ = System.cmd("epmd", ["-daemon"])

    case :net_kernel.start([:"malachi_primary@127.0.0.1", :longnames]) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    :ok
  end

  # A peer node running one replication server over the fault-injecting store, without the Malachi
  # application: the server needs only logging, telemetry, and the store's rules table. Returns the broker
  # ref and the server's data directory.
  defp start_peer_broker do
    name = :"malachi_storage_fail_#{System.unique_integer([:positive])}"
    {:ok, peer, node} = :peer.start_link(%{name: name, host: ~c"127.0.0.1", longnames: true})
    on_exit(fn -> try_stop(peer) end)

    :ok = :erpc.call(node, :code, :add_paths, [:code.get_path()])
    {:ok, _apps} = :erpc.call(node, :application, :ensure_all_started, [:logger])
    {:ok, _apps} = :erpc.call(node, :application, :ensure_all_started, [:telemetry])
    # The failure is expected here and asserted on the calling node; its log line would only be noise.
    :ok = :erpc.call(node, Logger, :configure, [[level: :none]])
    :ok = :erpc.call(node, FaultySegmentStore, :start, [])

    directory = Path.join(System.tmp_dir!(), "#{name}_data")
    on_exit(fn -> File.rm_rf!(directory) end)

    # Started unlinked: linked, it would die with the short-lived `:erpc` worker that started it.
    opts = [name: @server, directory: directory, store: FaultySegmentStore]
    {:ok, _pid} = :erpc.call(node, GenServer, :start, [ReplicationServer, opts, [name: @server]])

    {{@server, node}, directory}
  end

  defp try_stop(peer) do
    :peer.stop(peer)
  catch
    _kind, _reason -> :ok
  end

  defp records(values), do: for(value <- values, do: Record.new(value, key: value))

  defp eventually(check, remaining_ms \\ 5_000) do
    cond do
      check.() -> true
      remaining_ms <= 0 -> false
      true -> Process.sleep(25) && eventually(check, remaining_ms - 25)
    end
  end

  test "a node whose copy fails answers the producer, stays up, and the other nodes seal the segment" do
    {primary, primary_dir} = start_peer_broker()
    {follower, _follower_dir} = start_peer_broker()

    local_name = :"storage_failure_local_#{System.unique_integer([:positive])}"
    local_dir = Path.join(System.tmp_dir!(), "#{local_name}_data")
    on_exit(fn -> File.rm_rf!(local_dir) end)
    start_supervised!({ReplicationServer, [name: local_name, directory: local_dir]}, id: local_name)
    local = {local_name, node()}

    replica_set = [primary, follower, local]
    {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 4})
    segment_id = {root, 0}
    {metadata, :ok} = Metadata.apply(metadata, {:register_segment, root, segment_id, replica_set, 0})
    agent = start_supervised!({Agent, fn -> metadata end})
    source = fn -> Agent.get(agent, & &1) end
    apply_command = fn command -> Agent.update(agent, &elem(Metadata.apply(&1, command), 0)) end

    # Healthy first: a quorum across the three nodes, and every replica converges.
    assert {:ok, 1} = ReplicationServer.replicate(primary, segment_id, replica_set, 0, records(["x", "y"]))
    assert eventually(fn -> Enum.all?(replica_set, &(ReplicationServer.end_offset(&1, segment_id) == 2)) end)

    # The primary's node runs out of space for this segment.
    {@server, primary_node} = primary
    segment_dir = Layout.segment_directory(primary_dir, segment_id)
    :ok = :erpc.call(primary_node, FaultySegmentStore, :fail, [segment_dir, :sync, {:error, :enospc}])

    assert ReplicationServer.replicate(primary, segment_id, replica_set, 0, records(["z"])) ==
             {:error, {:storage, :enospc}}

    # Still up, and still serving the node's other work.
    assert is_pid(:erpc.call(primary_node, Process, :whereis, [@server]))
    assert ReplicationServer.failed_segments(primary, [segment_id]) == {:ok, MapSet.new([segment_id])}

    coordinator =
      start_supervised!(
        {HealCoordinator,
         live_brokers: fn -> replica_set end,
         metadata_source: source,
         apply_command: apply_command,
         replication_factor: 3,
         interval: 60_000,
         probe_timeout: 2_000}
      )

    HealCoordinator.heal_now(coordinator)

    sealed = Metadata.get_segment(source.(), segment_id)
    assert sealed.state == :sealed
    assert sealed.length == 2
    assert hd(sealed.replica_set) in [follower, local]

    # Writing moves on: the range's next segment, placed on the nodes whose copies did not fail, takes the
    # produce the failed segment could not.
    successor = {root, 1}
    apply_command.({:register_segment, root, successor, [follower, local], 2})
    assert Metadata.get_segment(source.(), successor).state == :active
    assert {:ok, 2} = ReplicationServer.replicate(follower, successor, [follower, local], 2, records(["after"]))
  end
end
