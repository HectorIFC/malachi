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
  alias Malachi.Test.Distribution
  alias Malachi.Test.FaultySegmentStore

  # Every peer registers its replication server under the same name: a broker ref is `{name, node}`, so
  # the node alone tells them apart, as in a real cluster.
  @server :storage_failure_repl

  setup_all do
    Distribution.ensure_started()
  end

  # A peer node running one replication server over the fault-injecting store, without the Malachi
  # application: the server needs only logging, telemetry, and the store's rules table. Returns the broker
  # ref and the server's data directory.
  defp start_peer_broker do
    {_peer, node, name} = Distribution.start_peer("storage_fail")
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

  # Flips one byte inside the payload of the frame holding record `index` of the only log file under
  # `segment_dir`: a frame written whole and wrong, which recovery classifies as rot and preserves.
  defp rot_record!(segment_dir, index) do
    [log_file] = Path.wildcard(Path.join(segment_dir, "*.log"))
    {frames, _valid_bytes} = Record.decode_all(File.read!(log_file))
    {_record, position} = Enum.at(frames, index)
    {:ok, fd} = :file.open(log_file, [:read, :write, :raw, :binary])
    {:ok, <<byte>>} = :file.pread(fd, position + 12, 1)
    :ok = :file.pwrite(fd, position + 12, <<Bitwise.bxor(byte, 0xFF)>>)
    :ok = :file.close(fd)
    log_file
  end

  test "a follower that restarts onto rot in its active copy is never appended over, and the segment rolls" do
    {primary, _primary_dir} = start_peer_broker()
    {follower, _follower_dir} = start_peer_broker()

    local_name = :"storage_rot_local_#{System.unique_integer([:positive])}"
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

    # One frame per record on every copy, so the damage below lands inside one record.
    for {value, offset} <- Enum.with_index(~w(x y w)) do
      assert {:ok, ^offset} = ReplicationServer.replicate(primary, segment_id, replica_set, offset, records([value]))
    end

    assert eventually(fn -> Enum.all?(replica_set, &(ReplicationServer.end_offset(&1, segment_id) == 3)) end)

    # The local follower goes down, its copy rots inside the second record, and it comes back.
    :ok = stop_supervised(local_name)
    log_file = rot_record!(Layout.segment_directory(local_dir, segment_id), 1)
    damaged = File.read!(log_file)
    start_supervised!({ReplicationServer, [name: local_name, directory: local_dir]}, id: local_name)

    # Produce keeps working on the two intact copies, and the rotted one is failed, not written over.
    ExUnit.CaptureLog.capture_log(fn ->
      assert {:ok, 3} = ReplicationServer.replicate(primary, segment_id, replica_set, 0, records(["z"]))

      assert eventually(fn ->
               ReplicationServer.failed_segments(local, [segment_id]) == {:ok, MapSet.new([segment_id])}
             end)
    end)

    assert File.read!(log_file) == damaged

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

    # Sealed at everything the intact copies hold, the record produced after the rot included.
    sealed = Metadata.get_segment(source.(), segment_id)
    assert sealed.state == :sealed
    assert sealed.length == 4
    assert hd(sealed.replica_set) in [primary, follower]

    successor = {root, 1}
    apply_command.({:register_segment, root, successor, [primary, follower], 4})
    assert {:ok, 4} = ReplicationServer.replicate(primary, successor, [primary, follower], 4, records(["after"]))
    assert File.read!(log_file) == damaged
  end
end
