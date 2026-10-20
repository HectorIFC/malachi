defmodule Malachi.Cluster.ScrubberTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.Scrubber
  alias Malachi.Log.Record
  alias Malachi.Metadata
  alias Malachi.Storage.Layout
  alias Malachi.Test.CallTrace
  alias Malachi.Test.FaultySegmentStore
  alias Malachi.Test.TmpDir
  alias Malachi.Test.UnknownMessages

  @segment {{"events", 0}, 0}

  defp start_replica(opts \\ []) do
    name = :"scrub_repl_#{System.unique_integer([:positive])}"
    directory = TmpDir.path("malachi_scrub")
    on_exit(fn -> File.rm_rf!(directory) end)
    start_supervised!({ReplicationServer, [name: name, directory: directory] ++ opts}, id: name)
    {{name, node()}, directory}
  end

  defp records(values), do: for(value <- values, do: Record.new(value, key: value))

  defp read_values(ref) do
    case ReplicationServer.read(ref, @segment, 0, 100) do
      {:ok, records} -> Enum.map(records, & &1.value)
      :eof -> []
    end
  end

  # Metadata with one SEALED segment held by every replica, each holding the real records: the shape
  # the scrub walks (sealed copies are the ones nothing ever re-reads).
  # `sealed_length` overrides what the control plane claims the segment holds, for the cases that model
  # a copy short of the seal. It is passed to the ONE seal command rather than applied as a second one:
  # re-sealing a segment at a different length is now refused, since two authorities disagreeing about
  # an edge is a conflict to converge on, not a rewrite to wave through.
  defp sealed_everywhere(replica_set, values, sealed_length \\ nil) do
    for replica <- replica_set do
      {:ok, _last} = ReplicationServer.follow(replica, @segment, 0, records(values))
    end

    {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 4})
    {metadata, :ok} = Metadata.apply(metadata, {:register_segment, root, @segment, replica_set, 0})
    bytes = ReplicationServer.stored_bytes(hd(replica_set), @segment)
    {metadata, :ok} = Metadata.apply(metadata, {:seal_segment, @segment, sealed_length || length(values), bytes, 0})
    metadata
  end

  # Rots a byte inside the second frame, keeping the file's size: the damage the byte-size probe in
  # SelfHealing cannot see and only a checksum scan catches.
  defp rot_copy(directory) do
    [log_file] = directory |> Layout.segment_directory(@segment) |> Path.join("*.log") |> Path.wildcard()
    {pairs, _valid} = Record.decode_all(File.read!(log_file))
    {_record, position} = Enum.at(pairs, 1)
    flip_at = position + 12
    <<head::binary-size(flip_at), byte, tail::binary>> = File.read!(log_file)
    File.write!(log_file, <<head::binary, Bitwise.bxor(byte, 0xFF), tail::binary>>)
    position
  end

  # Zeros everything from frame `index` to the end of the file, keeping the file's size. This is what
  # a zero-fill failure leaves when it lands on the LAST frames of a sealed copy, and it is the one
  # shape the segment cannot report on its own: the blank-tail check in `Malachi.Storage.ElixirStore`
  # sees zeros all the way to the end and correctly calls that unwritten space, so the scan comes back
  # clean and short. Only the control plane's record of what the seal held can catch it.
  defp zero_tail_from(directory, index) do
    [log_file] = directory |> Layout.segment_directory(@segment) |> Path.join("*.log") |> Path.wildcard()
    {pairs, _valid} = Record.decode_all(File.read!(log_file))
    {_record, position} = Enum.at(pairs, index)
    bytes = File.read!(log_file)
    File.write!(log_file, binary_part(bytes, 0, position) <> :binary.copy(<<0>>, byte_size(bytes) - position))
    position
  end

  # Tests assert on the returned result, so the default seams here stay quiet; pass `:default` for a
  # seam to drop it and get the module's own behaviour (used by the test that checks the log line).
  defp start_scrubber(opts) do
    defaults = [
      apply_command: fn _command -> :ok end,
      on_result: fn _result -> :ok end,
      interval: 60_000
    ]

    opts = defaults |> Keyword.merge(opts) |> Enum.reject(fn {_key, value} -> value == :default end)
    start_supervised!({Scrubber, opts}, id: {:scrubber, System.unique_integer([:positive])})
  end

  describe "a copy that failed in storage" do
    # A replica whose store fails on demand, with this segment's copy already latched as failed.
    defp start_latched_replica(values) do
      name = :"scrub_faulty_#{System.unique_integer([:positive])}"
      directory = TmpDir.path("malachi_scrub")

      on_exit(fn ->
        FaultySegmentStore.clear(directory)
        File.rm_rf!(directory)
      end)

      start_supervised!({ReplicationServer, [name: name, directory: directory, store: FaultySegmentStore]}, id: name)
      replica = {name, node()}
      metadata = sealed_everywhere([replica], values)

      FaultySegmentStore.fail(Layout.segment_directory(directory, @segment), :sync, {:error, :enospc})

      capture_log(fn ->
        next = length(values)
        assert {:error, {:storage, :enospc}} = ReplicationServer.follow(replica, @segment, next, records(["late"]))
      end)

      {replica, directory, metadata}
    end

    test "is skipped, never deleted and refetched onto the disk that failed" do
      {replica, directory, metadata} = start_latched_replica(["a", "b", "c"])
      rot_copy(directory)

      scrubber = start_scrubber(metadata_source: fn -> metadata end, local_ref: replica, directory: directory)

      assert %{skipped: [@segment], verified: [], damaged: [], repaired: [], unrepairable: []} =
               Scrubber.scrub_now(scrubber)

      # Still latched, so nothing deleted it (a delete clears the latch).
      assert ReplicationServer.failed_segments(replica, [@segment]) == {:ok, MapSet.new([@segment])}
      assert Scrubber.damaged(scrubber) == []
    end

    test "is refused as a repair source when a peer asks" do
      {replica, directory, metadata} = start_latched_replica(["a"])
      scrubber = start_scrubber(metadata_source: fn -> metadata end, local_ref: replica, directory: directory)

      assert Scrubber.verify_segment(scrubber, @segment) == {:error, :failed_copy}
    end

    # A copy on disk whose scrubber asks about latches through a reference that names no server: the latches
    # of this pass are unknown.
    defp unknown_latches(values) do
      {replica, directory} = start_replica()
      written = sealed_everywhere([replica], values)
      {metadata, :ok} = Metadata.apply(written, {:set_segment_replicas, @segment, [:no_such_server]})
      scrubber = start_scrubber(metadata_source: fn -> metadata end, local_ref: :no_such_server, directory: directory)
      {scrubber, directory}
    end

    test "an unreadable latch still verifies a clean copy, but serves no peer" do
      {scrubber, _directory} = unknown_latches(["a", "b"])

      assert %{verified: [@segment], skipped: [], unrepairable: []} = Scrubber.scrub_now(scrubber)
      assert Scrubber.verify_segment(scrubber, @segment) == {:error, :latch_unknown}
    end

    test "an unreadable latch reports damage and deletes nothing" do
      {scrubber, directory} = unknown_latches(["a", "b", "c"])
      rot_copy(directory)
      [log_file] = directory |> Layout.segment_directory(@segment) |> Path.join("*.log") |> Path.wildcard()
      rotted = File.read!(log_file)

      assert %{damaged: [{@segment, _verdict}], repaired: [], unrepairable: [{@segment, :latch_unknown}]} =
               Scrubber.scrub_now(scrubber)

      assert File.read!(log_file) == rotted
    end

    test "a peer whose latch cannot be read is not a repair source, so the damaged copy is kept" do
      # THE FINDING, reproduced before the fix: the peer's copy IS latched, but its scrubber could not ask, so
      # it answered {:ok, _} from disk; this node took it as intact, deleted its own copy, and the refetch was
      # refused by the peer's replication server, leaving this node with no copy at all.
      {local, local_dir} = start_replica()
      {peer, peer_dir, _peer_metadata} = start_latched_replica(["a", "b", "c"])
      {:ok, _last} = ReplicationServer.follow(local, @segment, 0, records(["a", "b", "c"]))

      {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 4})
      {metadata, :ok} = Metadata.apply(metadata, {:register_segment, root, @segment, [local, peer], 0})
      bytes = ReplicationServer.stored_bytes(local, @segment)
      {metadata, :ok} = Metadata.apply(metadata, {:seal_segment, @segment, 3, bytes, 0})
      rot_copy(local_dir)

      peer_scrubber =
        start_scrubber(metadata_source: fn -> metadata end, local_ref: :no_such_server, directory: peer_dir)

      local_scrubber =
        start_scrubber(
          metadata_source: fn -> metadata end,
          local_ref: local,
          directory: local_dir,
          peer_scrubber: fn _node -> peer_scrubber end
        )

      assert %{repaired: [], unrepairable: [{@segment, :no_intact_copy}]} = Scrubber.scrub_now(local_scrubber)
      assert read_values(local) != []
    end
  end

  test "verifies the node's sealed copies and reports them clean" do
    {replica, directory} = start_replica()
    metadata = sealed_everywhere([replica], ["a", "b", "c"])

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: replica,
        directory: directory
      )

    assert %{verified: [@segment], damaged: [], repaired: [], unrepairable: []} = Scrubber.scrub_now(scrubber)
    assert Scrubber.damaged(scrubber) == []
  end

  test "addresses a peer's scrubber by the name this one is registered under" do
    # Every node runs this worker under the same registered name, so a peer is reachable at that name
    # on its node. Hardcoding the module name instead is what made the first cluster run fail to
    # repair: the supervisor registers it as Malachi.LogScrubber, every peer lookup hit a dead name,
    # and each repair concluded there was no intact copy anywhere. The unit tests missed it because
    # they all injected the seam, so the default was never exercised.
    {replica, directory} = start_replica()
    metadata = sealed_everywhere([replica], ["a"])
    name = :"scrub_named_#{System.unique_integer([:positive])}"

    start_scrubber(
      name: name,
      metadata_source: fn -> metadata end,
      local_ref: replica,
      directory: directory
    )

    peer_scrubber = :sys.get_state(Process.whereis(name)).peer_scrubber
    assert peer_scrubber.(:malachi@other) == {name, :malachi@other}
  end

  test "resolves the local reference per pass, so a restarted broker's new server is still scrubbed" do
    # A single-node broker owns an UNNAMED replication server, so its reference is a pid that a
    # broker restart replaces. Capturing it once at init would leave the scrub matching nothing (the
    # metadata's replica sets name the new process), and it would fail silently: every pass would
    # verify zero segments and report a clean bill of health.
    {first_replica, directory} = start_replica()
    metadata = sealed_everywhere([first_replica], ["a", "b"])
    current = :atomics.new(1, [])
    :atomics.put(current, 1, 0)

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: fn -> if :atomics.get(current, 1) == 0, do: first_replica, else: :stale_ref end,
        directory: directory
      )

    assert %{verified: [@segment]} = Scrubber.scrub_now(scrubber)

    # the reference changed under the scrubber: the next pass must ask again, not reuse the old one
    :atomics.put(current, 1, 1)
    assert %{verified: []} = Scrubber.scrub_now(scrubber)
  end

  test "repairs a rotted copy from an intact peer, and reads return every record again" do
    # The acceptance test for the whole feature. Before it, a rotted sealed copy answered reads with
    # the records BEFORE the damage and nothing after, with no error: the consumer stalls there or
    # skips the rest of the range, silently.
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    values = ["a", "b", "c", "d"]
    metadata = sealed_everywhere([damaged_replica, peer_replica], values)

    peer_scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: peer_replica,
        directory: peer_directory
      )

    rot_copy(damaged_directory)

    # the damage is real: this copy serves only what precedes it
    assert read_values(damaged_replica) == ["a"]

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    result = Scrubber.scrub_now(scrubber)

    assert [{@segment, %{reason: :bad_crc}}] = result.damaged
    assert result.repaired == [@segment]
    assert result.unrepairable == []
    assert Scrubber.damaged(scrubber) == []

    # the whole point: the copy reads back completely again
    assert read_values(damaged_replica) == values
    assert Scrubber.verify_segment(scrubber, @segment) |> elem(0) == :ok

    # The rebuilt copy is sealed again at the recorded length, so it is fenced like the copies it was rebuilt from.
    assert {:ok, %{@segment => {4, _bytes}}} = ReplicationServer.fenced_segments(damaged_replica, [{@segment, 0}])
  end

  test "a peer that verifies but cannot be read is not a source, so the local copy is kept" do
    # The peer's verify checks checksums and counts and never opens its log, so it passes here. Its reads
    # fail, and a refetch from it could not put the damaged bytes back. Deleting the local copy first would
    # leave the segment with no readable copy at all, so the repair has to stop before the delete.
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica(store: FaultySegmentStore)
    on_exit(fn -> FaultySegmentStore.clear(peer_directory) end)
    values = ["a", "b", "c", "d"]
    metadata = sealed_everywhere([damaged_replica, peer_replica], values)

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)
    FaultySegmentStore.fail(Layout.segment_directory(peer_directory, @segment), :read, {:error, :eio})

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {result, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)

    assert [{@segment, %{reason: :bad_crc}}] = result.damaged
    assert result.repaired == []
    assert [{@segment, {:source_read, _reason}}] = result.unrepairable

    assert [_log_file] =
             damaged_directory |> Layout.segment_directory(@segment) |> Path.join("*.log") |> Path.wildcard()
  end

  test "a peer that fails on a later batch is not a source, and the damaged copy is kept" do
    # Two batches: the peer serves the first 256 records and fails from the 257th on. A confirmation that read only
    # the first batch would see a healthy peer, delete the copy, and then fail its refetch.
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica(store: FaultySegmentStore)
    on_exit(fn -> FaultySegmentStore.clear(peer_directory) end)
    values = for i <- 1..300, do: "v#{i}"
    metadata = sealed_everywhere([damaged_replica, peer_replica], values)

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)
    FaultySegmentStore.fail_reads_from(Layout.segment_directory(peer_directory, @segment), 256, {:error, :eio})

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {result, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)

    assert result.repaired == []
    assert [{@segment, {:source_read, _reason}}] = result.unrepairable

    assert [_log_file] =
             damaged_directory |> Layout.segment_directory(@segment) |> Path.join("*.log") |> Path.wildcard()
  end

  test "a peer that stops answering reads defers the repair: the copy is kept and nothing is reported unrepairable" do
    {damaged_replica, damaged_directory} = start_replica()
    {{peer_name, _node} = peer_replica, peer_directory} = start_replica()
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    # The peer's own scrubber checks latches on an idle server, so only the peer's READS are slow below.
    {idle_replica, _idle_directory} = start_replica()

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: idle_replica, directory: peer_directory)

    rot_copy(damaged_directory)
    # The peer still verifies (it reads its files, not its server), but its server answers no read in time.
    :ok = :sys.suspend(peer_name)

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {micros, {result, _log}} = :timer.tc(fn -> with_log(fn -> Scrubber.scrub_now(scrubber) end) end)

    assert [{@segment, %{reason: :bad_crc}}] = result.damaged
    # The confirmation read is bounded at one second, so the pass ends well before the five-second default would.
    assert micros < 4_000_000
    assert result.repaired == []
    assert result.unrepairable == []

    assert [_log_file] =
             damaged_directory |> Layout.segment_directory(@segment) |> Path.join("*.log") |> Path.wildcard()

    :ok = :sys.resume(peer_name)
  end

  test "a confirmation that runs out of its budget defers the repair and deletes nothing" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        confirm_deadline_ms: 0,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {result, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)

    assert result.repaired == []
    assert result.unrepairable == []

    assert [_log_file] =
             damaged_directory |> Layout.segment_directory(@segment) |> Path.join("*.log") |> Path.wildcard()
  end

  test "a demote that answers something other than :ok or an error stops the repair before the delete" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        apply_command: fn _command -> :error end,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {result, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)

    assert result.repaired == []
    assert [{@segment, {:demote, :error}}] = result.unrepairable

    assert [_log_file] =
             damaged_directory |> Layout.segment_directory(@segment) |> Path.join("*.log") |> Path.wildcard()
  end

  test "a demote whose call to the control plane exits defers the repair and keeps the copy" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        apply_command: fn _command -> exit(:timeout) end,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {result, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)

    assert result.repaired == []
    assert result.unrepairable == []

    assert [_log_file] =
             damaged_directory |> Layout.segment_directory(@segment) |> Path.join("*.log") |> Path.wildcard()
  end

  test "a demote is not written over a replica set that changed since the pass took its snapshot" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    snapshot = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    # The control plane has moved the head since the snapshot: the peer now leads the set.
    {moved, :ok} = Metadata.apply(snapshot, {:set_segment_replicas, @segment, [peer_replica, damaged_replica]})

    peer_scrubber =
      start_scrubber(metadata_source: fn -> snapshot end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)
    test_process = self()

    # The first read of the metadata is the pass's snapshot; the demote's re-read sees the moved set.
    reads = :counters.new(1, [])

    metadata_source = fn ->
      :counters.add(reads, 1, 1)
      if :counters.get(reads, 1) == 1, do: snapshot, else: moved
    end

    scrubber =
      start_scrubber(
        metadata_source: metadata_source,
        local_ref: damaged_replica,
        directory: damaged_directory,
        apply_command: fn command ->
          send(test_process, {:command, command})
          :ok
        end,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {result, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)

    assert result.repaired == []
    assert result.unrepairable == []
    refute_received {:command, {:set_segment_replicas, _id, _replicas, _expected}}

    assert [_log_file] =
             damaged_directory |> Layout.segment_directory(@segment) |> Path.join("*.log") |> Path.wildcard()
  end

  test "a demote the control plane does not answer within the scrub loop's bound defers the repair" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        apply_command: fn _command -> Process.sleep(:infinity) end,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {result, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)

    assert result.repaired == []
    assert result.unrepairable == []

    assert [_log_file] =
             damaged_directory |> Layout.segment_directory(@segment) |> Path.join("*.log") |> Path.wildcard()
  end

  test "a rebuilt copy that cannot be sealed again is latched, so it is not left in service unfenced" do
    {damaged_replica, damaged_directory} = start_replica(store: FaultySegmentStore)
    {peer_replica, peer_directory} = start_replica()
    on_exit(fn -> FaultySegmentStore.clear(damaged_directory) end)
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)
    # The rebuilt copy is written back, and then its seal fails on every attempt.
    FaultySegmentStore.fail(Layout.segment_directory(damaged_directory, @segment), :seal, {:error, :eio})

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {result, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)

    assert result.repaired == []
    assert [{@segment, {:refetch, {:error, {:seal, _reason}}}}] = result.unrepairable

    assert {:ok, failed} = ReplicationServer.failed_segments(damaged_replica, [@segment])
    assert MapSet.member?(failed, @segment)
  end

  test "a demote for a segment the control plane no longer has is dropped, not reported as damage" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        apply_command: fn _command -> {:error, :no_such_segment} end,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    result = Scrubber.scrub_now(scrubber)

    assert result.unrepairable == []
    assert result.repaired == []
    assert Scrubber.damaged(scrubber) == []
  end

  test "a deferred repair is not put back once this node has left the replica set" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    # The node has left the replica set of the segment by the time the deferred repair is requeued.
    {topic, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 4})
    {without, :ok} = Metadata.apply(topic, {:register_segment, root, @segment, [peer_replica, :elsewhere], 0})
    {without, :ok} = Metadata.apply(without, {:seal_segment, @segment, 2, 0, 0})

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)

    # Reads: the refill, the demote's re-read (both see the set as it was), then the requeue's read (the new set).
    reads = :counters.new(1, [])

    metadata_source = fn ->
      :counters.add(reads, 1, 1)
      if :counters.get(reads, 1) <= 2, do: metadata, else: without
    end

    scrubber =
      start_scrubber(
        metadata_source: metadata_source,
        local_ref: damaged_replica,
        directory: damaged_directory,
        apply_command: fn _command -> {:error, :replica_set_changed} end,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    _ = Scrubber.scrub_now(scrubber)

    assert :sys.get_state(scrubber).pending == []
  end

  test "a demote whose compare-and-swap was refused because the set moved defers the repair" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        apply_command: fn _command -> {:error, :replica_set_changed} end,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {result, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)

    assert result.repaired == []
    assert result.unrepairable == []

    assert [_log_file] =
             damaged_directory |> Layout.segment_directory(@segment) |> Path.join("*.log") |> Path.wildcard()
  end

  test "a rebuilt copy whose seal does not answer in time is reported as a timeout" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)

    # A zero bound is an answer that never comes in time, which is what a seal behind a busy server looks like.
    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        call_timeout: 0,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {result, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)

    assert result.repaired == []
    assert [{@segment, {:refetch, {:error, {:seal, :timeout}}}}] = result.unrepairable
  end

  test "a rebuilt copy whose latch did not answer is latched by a later pass" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)

    # The first pass cannot latch or delete: both calls answer unreachable, from stubs, so nothing races the zero bound.
    # The next pass runs the real latch, which answers in time and lands.
    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        call_timeout: 0,
        fail_copy: fn _ref, _segment_id, _reason -> {:error, :unreachable} end,
        delete: fn _ref, _segment_id -> {:error, :unreachable} end,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {first, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)
    assert [{@segment, {:refetch, {:error, {:seal, :timeout}}}}] = first.unrepairable

    # The next pass has time to answer: it latches the rebuilt copy before it scans, so the copy is skipped.
    :sys.replace_state(scrubber, fn state ->
      %{state | call_timeout: 1_000, fail_copy: &ReplicationServer.fail_copy/3, delete: &ReplicationServer.delete/2}
    end)

    {second, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)
    assert second.skipped == [@segment]
    assert second.repaired == []

    assert {:ok, failed} = ReplicationServer.failed_segments(damaged_replica, [@segment])
    assert MapSet.member?(failed, @segment)
  end

  test "a repair whose latch cannot be read at delete time is kept, not deleted" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)
    on_exit(fn -> resume_if_suspended(damaged_replica) end)

    # The batch-level latch check runs before this callback, and the delete's own latch check runs after it. The local
    # server stops answering in between, so the delete's check cannot read the latch and must refuse the delete.
    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        peer_scrubber: fn _node ->
          :ok = :sys.suspend(damaged_replica)
          peer_scrubber
        end
      )

    {result, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)
    :ok = :sys.resume(damaged_replica)

    assert result.repaired == []
    assert [{@segment, :latch_unknown}] = result.unrepairable

    assert [_log_file] =
             damaged_directory |> Layout.segment_directory(@segment) |> Path.join("*.log") |> Path.wildcard()
  end

  test "a confirmation resumes against the same peer, not against another one that verifies later" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_a, peer_a_directory} = start_replica(store: FaultySegmentStore)
    {peer_b, peer_b_directory} = start_replica()
    on_exit(fn -> FaultySegmentStore.clear(peer_a_directory) end)
    values = for i <- 1..300, do: "v#{i}"
    metadata = sealed_everywhere([damaged_replica, peer_a, peer_b], values)

    scrubber_a = start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_a, directory: peer_a_directory)
    scrubber_b = start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_b, directory: peer_b_directory)
    rot_copy(damaged_directory)

    # Peer A answers the first batch, and from record 256 on each read takes longer than the confirmation waits: the
    # read gives up, so the first tick defers there, with progress at 256.
    FaultySegmentStore.fail_reads_from(
      Layout.segment_directory(peer_a_directory, @segment),
      256,
      {:delay, 3_000, {:error, :unreachable}}
    )

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        peer_scrubber_for: fn
          ^peer_a -> scrubber_a
          ^peer_b -> scrubber_b
        end
      )

    first = Scrubber.scrub_now(scrubber)
    assert first.repaired == []

    # On the next tick peer A's copy is damaged, so it is no longer a source, and peer B verifies. The confirmation
    # starts over at record 0 against B: progress belongs to the peer it was made against.
    rot_copy(peer_a_directory)

    reads =
      CallTrace.calls([{ReplicationServer, :read, 5}], [pids: [scrubber]], fn ->
        send(self(), {:second, Scrubber.scrub_now(scrubber)})
      end)

    assert_received {:second, second}
    assert second.repaired == [@segment]
    assert [{ReplicationServer, :read, [^peer_b, @segment, 0 | _]} | _] = reads
  end

  test "a repair deferred on every tick does not hold up the other segments on its node" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    clean = {@segment |> elem(0), 1}

    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b"])

    {metadata, :ok} =
      Metadata.apply(metadata, {:register_segment, elem(@segment, 0), clean, [damaged_replica, peer_replica], 2})

    {:ok, _last} = ReplicationServer.follow(damaged_replica, clean, 2, records(["b"]))
    {:ok, _last} = ReplicationServer.follow(peer_replica, clean, 2, records(["b"]))
    {metadata, :ok} = Metadata.apply(metadata, {:seal_segment, clean, 1, 0, 0})

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)

    # The damaged segment's demotion is refused every time, so its repair defers on each tick it is visited.
    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        segments_per_tick: 1,
        apply_command: fn
          {:set_segment_replicas, @segment, _replicas, _expected} -> {:error, :replica_set_changed}
          _command -> :ok
        end,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    first = Scrubber.scrub_now(scrubber)
    # The deferred repair goes behind the clean segment, which is visited first.
    assert Enum.map(:sys.get_state(scrubber).pending, & &1.id) == [clean, @segment]
    second = Scrubber.scrub_now(scrubber)

    # Across two ticks the clean segment is visited, although the damaged one defers on each of them.
    assert clean in (first.verified ++ second.verified)
  end

  test "a repair that keeps deferring leaves the rotation, so the segments behind it are still visited" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    clean = {@segment |> elem(0), 1}

    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b"])

    {metadata, :ok} =
      Metadata.apply(metadata, {:register_segment, elem(@segment, 0), clean, [damaged_replica, peer_replica], 2})

    {:ok, _last} = ReplicationServer.follow(damaged_replica, clean, 2, records(["b"]))
    {:ok, _last} = ReplicationServer.follow(peer_replica, clean, 2, records(["b"]))
    {metadata, :ok} = Metadata.apply(metadata, {:seal_segment, clean, 1, 0, 0})

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)

    # The damaged segment's demotion is refused every time, so its repair defers on each tick it is visited.
    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        segments_per_tick: 1,
        apply_command: fn
          {:set_segment_replicas, @segment, _replicas, _expected} -> {:error, :replica_set_changed}
          _command -> :ok
        end,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    visits = for _tick <- 1..10, do: Scrubber.scrub_now(scrubber).verified

    # Without the cap the damaged repair holds the rotation for every tick, and the clean segment is visited once.
    assert Enum.count(List.flatten(visits), &(&1 == clean)) >= 2
  end

  test "a pending latch for a copy this node no longer holds is dropped, not applied" do
    {damaged_replica, damaged_directory} = start_replica()
    test_process = self()

    scrubber =
      start_scrubber(
        metadata_source: fn -> Metadata.new() end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        fail_copy: fn _ref, _segment_id, _reason ->
          send(test_process, :latched)
          :ok
        end
      )

    :sys.replace_state(scrubber, fn state -> %{state | unlatched: %{@segment => {:rebuilt_unsealed, :stale}}} end)

    _ = Scrubber.scrub_now(scrubber)

    refute_received :latched
    assert :sys.get_state(scrubber).unlatched == %{}
  end

  test "a repair deferred because the set moved is visited again on the next tick" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)
    attempts = :counters.new(1, [])

    # The first demote finds the set moved, and the next one takes.
    apply_command = fn _command ->
      :counters.add(attempts, 1, 1)
      if :counters.get(attempts, 1) == 1, do: {:error, :replica_set_changed}, else: :ok
    end

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        apply_command: apply_command,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {first, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)
    assert first.repaired == []
    assert first.unrepairable == []
    # The deferred repair is requeued, not dropped: it is the next to be visited.
    assert Enum.map(:sys.get_state(scrubber).pending, & &1.id) == [@segment]

    {second, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)
    assert second.repaired == [@segment]
  end

  test "a copy whose catch-up fails after the delete is latched, so healing replaces it" do
    {damaged_replica, damaged_directory} = start_replica(store: FaultySegmentStore)
    {peer_replica, peer_directory} = start_replica()
    on_exit(fn -> FaultySegmentStore.clear(damaged_directory) end)
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)
    # The delete removes the damaged copy, and the catch-up rebuilds it on a disk whose fsync fails.
    FaultySegmentStore.fail(Layout.segment_directory(damaged_directory, @segment), :sync, {:error, :eio})

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {result, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)

    assert result.repaired == []
    assert [{@segment, {:refetch, _reason}}] = result.unrepairable

    assert {:ok, failed} = ReplicationServer.failed_segments(damaged_replica, [@segment])
    assert MapSet.member?(failed, @segment)
  end

  test "a pending latch is tried on a pass whose metadata cannot be read" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    available = :counters.new(1, [])
    :counters.put(available, 1, 1)

    scrubber =
      start_scrubber(
        metadata_source: fn ->
          if :counters.get(available, 1) == 1, do: metadata, else: exit(:unavailable)
        end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    :sys.replace_state(scrubber, fn state -> %{state | unlatched: %{@segment => {:rebuilt_unsealed, :stale}}} end)

    # The next pass cannot read the metadata, so it does not scan, but the pending latch is still tried.
    :counters.put(available, 1, 0)
    result = Scrubber.scrub_now(scrubber)

    assert [{:metadata, _reason}] = result.unrepairable
    assert :sys.get_state(scrubber).unlatched == %{}
    assert {:ok, failed} = ReplicationServer.failed_segments(damaged_replica, [@segment])
    assert MapSet.member?(failed, @segment)
  end

  test "a pending latch survives a pass that cannot latch or delete the copy, and the next repair takes it away" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)

    # A zero bound is an answer that does not come in time, so the seal times out and the latch that follows does not
    # answer either. Neither does the delete that would take the copy away, so the entry stays pending.
    unanswered = fn _ref, _segment_id -> {:error, :unreachable} end

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        call_timeout: 0,
        fail_copy: fn _ref, _segment_id, _reason -> {:error, :unreachable} end,
        delete: unanswered,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    first = Scrubber.scrub_now(scrubber)
    assert [{@segment, {:refetch, {:error, {:seal, :timeout}}}}] = first.unrepairable
    assert Map.has_key?(:sys.get_state(scrubber).unlatched, @segment)

    # The calls answer in time again and the copy is damaged again: the next pass repairs it, and the repair takes the
    # pending entry away with the copy it was for.
    :sys.replace_state(scrubber, fn state -> %{state | call_timeout: 1_000} end)
    rot_copy(damaged_directory)

    second = Scrubber.scrub_now(scrubber)
    assert second.repaired == [@segment]
    assert :sys.get_state(scrubber).unlatched == %{}
  end

  describe "a rebuilt copy whose latch does not land" do
    # The rebuilt copy's seal fails, and the first latch call does not answer. The copy is kept unlatched, and the
    # later call is the real one.
    defp start_unlatched_rebuild(wrap_metadata \\ fn metadata -> fn -> metadata end end) do
      {damaged_replica, damaged_directory} = start_replica(store: FaultySegmentStore)
      {peer_replica, peer_directory} = start_replica()
      on_exit(fn -> FaultySegmentStore.clear(damaged_directory) end)
      metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

      peer_scrubber =
        start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

      rot_copy(damaged_directory)
      FaultySegmentStore.fail(Layout.segment_directory(damaged_directory, @segment), :seal, {:error, :eio})

      latches = :counters.new(1, [])
      test_process = self()

      fail_copy = fn ref, segment_id, reason ->
        send(test_process, {:latch, reason})
        :counters.add(latches, 1, 1)

        if :counters.get(latches, 1) == 1,
          do: {:error, :unreachable},
          else: ReplicationServer.fail_copy(ref, segment_id, reason)
      end

      opts = [
        metadata_source: wrap_metadata.(metadata),
        local_ref: damaged_replica,
        directory: damaged_directory,
        fail_copy: fail_copy,
        peer_scrubber: fn _node -> peer_scrubber end
      ]

      {damaged_replica, opts}
    end

    test "a latch that does not answer takes the rebuilt copy away, under the cause it was rebuilt under" do
      {_damaged_replica, opts} = start_unlatched_rebuild()

      scrubber = start_scrubber(opts)

      {first, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)
      assert [{@segment, {:refetch, {:error, {:seal, _reason}}}}] = first.unrepairable
      assert_received {:latch, first_reason}
      # The reason is the seal's own answer, the cause the copy was rebuilt under.
      assert first_reason == {:rebuilt_unsealed, {:error, {:storage, :eio}}}

      # The latch did not answer, so the copy was taken away rather than left unfenced, and nothing stays pending.
      assert :sys.get_state(scrubber).unlatched == %{}
      directory = Keyword.fetch!(opts, :directory)
      assert Path.wildcard(Path.join(Layout.segment_directory(directory, @segment), "*.log")) == []
    end

    test "does not outlive the copy it was for: a repair takes the pending latch away with the copy" do
      {_damaged_replica, opts} = start_unlatched_rebuild()

      scrubber = start_scrubber(opts)
      :sys.replace_state(scrubber, fn state -> %{state | unlatched: %{@segment => {:rebuilt_unsealed, :stale}}} end)

      _ = Scrubber.scrub_now(scrubber)
      assert :sys.get_state(scrubber).unlatched == %{}
    end
  end

  defp resume_if_suspended(name) do
    :sys.resume(name)
  catch
    :exit, _reason -> :ok
  end

  test "a confirmation that ran out of its budget resumes from where it stopped, against the same peer" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    values = for i <- 1..300, do: "v#{i}"
    metadata = sealed_everywhere([damaged_replica, peer_replica], values)

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)

    # The budget is measured by this clock. Tick one: the budget starts at 0 and ends at 10_000, the first batch is
    # read (the clock still reads 0), and the clock then reads 20_000 before the next batch, so the pass defers at
    # offset 256. Tick two: the budget starts at 20_000 and ends at 30_000 and the clock reads 20_000 before the
    # next read, so a resumed pass reads only the remaining 44 records. A pass that started over would read the first
    # batch again, and the clock then reads 40_000, which spends the budget before the second batch.
    clock_reads = :counters.new(1, [])

    confirm_clock = fn ->
      :counters.add(clock_reads, 1, 1)

      case :counters.get(clock_reads, 1) do
        n when n <= 2 -> 0
        n when n <= 5 -> 20_000
        _later -> 40_000
      end
    end

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        confirm_clock: confirm_clock,
        confirm_deadline_ms: 10_000,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {first, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)
    assert first.repaired == []
    assert first.unrepairable == []

    # The second pass's confirmation reads start where the first stopped, at offset 256, and never go back to 0.
    reads =
      CallTrace.calls([{ReplicationServer, :read, 5}], [pids: [scrubber]], fn ->
        send(self(), {:second, with_log(fn -> Scrubber.scrub_now(scrubber) end)})
      end)

    assert_received {:second, {second, _log}}
    assert second.repaired == [@segment]
    assert second.unrepairable == []
    # The refetch that follows reads the whole range from 0 too, so only the first read, the confirmation's, is pinned.
    assert [{ReplicationServer, :read, [_peer, @segment, 256 | _]} | _] = reads
  end

  test "a confirmation read is given only the time left in the budget, not a whole read bound" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)

    # The budget opens at 0 and ends at 10_000 (the default). Every check after the first reads 9_500: 500 ms are left.
    clock_reads = :counters.new(1, [])

    confirm_clock = fn ->
      :counters.add(clock_reads, 1, 1)
      if :counters.get(clock_reads, 1) == 1, do: 0, else: 9_500
    end

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        confirm_clock: confirm_clock,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    reads =
      CallTrace.calls([{ReplicationServer, :read, 5}], [pids: [scrubber]], fn ->
        send(self(), {:pass, with_log(fn -> Scrubber.scrub_now(scrubber) end)})
      end)

    assert_received {:pass, {result, _log}}
    assert result.repaired == [@segment]
    # Only the confirmation reads (one batch of 256 records) are bounded by the budget; the refetch reads are not.
    confirmations =
      Enum.filter(reads, &match?({ReplicationServer, :read, [_peer, @segment, _offset, 256, _timeout]}, &1))

    assert [_ | _] = confirmations
    assert Enum.all?(confirmations, &match?({ReplicationServer, :read, [_peer, @segment, _offset, 256, 500]}, &1))
  end

  test "a copy latched while its source is confirmed is kept, not deleted and refetched onto the disk that failed" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)

    # The copy fails in storage during the confirmation, which runs after the pass checked the latch and before the
    # delete. The first clock read happens inside that confirmation, so the failure lands in the window.
    failed_once = :counters.new(1, [])

    confirm_clock = fn ->
      if :counters.get(failed_once, 1) == 0 do
        :counters.add(failed_once, 1, 1)
        assert :ok = ReplicationServer.fail_copy(damaged_replica, @segment, :eio)
      end

      0
    end

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        confirm_clock: confirm_clock,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {result, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)

    assert result.repaired == []
    assert [{@segment, :latched_before_delete}] = result.unrepairable

    assert [_log_file] =
             damaged_directory |> Layout.segment_directory(@segment) |> Path.join("*.log") |> Path.wildcard()

    assert {:ok, failed} = ReplicationServer.failed_segments(damaged_replica, [@segment])
    assert MapSet.member?(failed, @segment)
  end

  test "a demote whose re-read of the metadata exits defers the repair and sends no command" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    snapshot = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> snapshot end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)
    test_process = self()
    reads = :counters.new(1, [])

    # The pass's snapshot comes first; the demote's re-read finds the control plane unreachable.
    metadata_source = fn ->
      :counters.add(reads, 1, 1)
      if :counters.get(reads, 1) == 1, do: snapshot, else: exit(:unreachable)
    end

    scrubber =
      start_scrubber(
        metadata_source: metadata_source,
        local_ref: damaged_replica,
        directory: damaged_directory,
        apply_command: fn command ->
          send(test_process, {:command, command})
          :ok
        end,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {result, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)

    assert result.repaired == []
    assert result.unrepairable == []
    refute_received {:command, {:set_segment_replicas, _id, _replicas, _expected}}

    assert [_log_file] =
             damaged_directory |> Layout.segment_directory(@segment) |> Path.join("*.log") |> Path.wildcard()
  end

  test "a demote whose re-read of the metadata no longer holds the segment defers the repair" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    snapshot = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> snapshot end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)
    test_process = self()
    reads = :counters.new(1, [])

    # The re-read finds metadata without the segment: nothing can be checked, so nothing is written.
    metadata_source = fn ->
      :counters.add(reads, 1, 1)
      if :counters.get(reads, 1) == 1, do: snapshot, else: Metadata.new()
    end

    scrubber =
      start_scrubber(
        metadata_source: metadata_source,
        local_ref: damaged_replica,
        directory: damaged_directory,
        apply_command: fn command ->
          send(test_process, {:command, command})
          :ok
        end,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {result, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)

    assert result.repaired == []
    assert result.unrepairable == []

    # The segment is gone from the metadata: the damage report must not keep it either.
    assert Scrubber.damaged(scrubber) == []
    refute_received {:command, {:set_segment_replicas, _id, _replicas, _expected}}
  end

  test "a repair whose demote is refused keeps the damaged copy and reports why" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        apply_command: fn _command -> {:error, :refused} end,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {result, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)

    assert result.repaired == []
    assert [{@segment, {:demote, :refused}}] = result.unrepairable

    assert [_log_file] =
             damaged_directory |> Layout.segment_directory(@segment) |> Path.join("*.log") |> Path.wildcard()
  end

  test "a damaged copy of an empty sealed segment is kept, not deleted, and reported" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()

    # Both copies hold the empty segment: a zero-byte log file each (a follow of no records writes none).
    for directory <- [damaged_directory, peer_directory] do
      File.mkdir_p!(Layout.segment_directory(directory, @segment))
      File.write!(Path.join(Layout.segment_directory(directory, @segment), "00000000000000000000.log"), <<>>)
    end

    {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 4})

    {metadata, :ok} =
      Metadata.apply(metadata, {:register_segment, root, @segment, [damaged_replica, peer_replica], 0})

    {metadata, :ok} = Metadata.apply(metadata, {:seal_segment, @segment, 0, 0, 0})

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    # a torn tail on the damaged copy: bytes that decode to no record at all
    log_file = Path.join(Layout.segment_directory(damaged_directory, @segment), "00000000000000000000.log")
    File.write!(log_file, <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17>>, [:append])

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {result, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)

    assert [{@segment, _damage}] = result.damaged
    assert result.repaired == []
    assert result.unrepairable == [{@segment, :empty_segment}]
    assert File.exists?(log_file)
  end

  test "a repair completes for a copy of 300 records" do
    # More records than one confirmation batch, so the range is read in pieces. The batch size itself is pinned by
    # the confirmation read tests, not here. The rot is on an early record, so the copy has to be rebuilt.
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    values = for i <- 1..300, do: "v#{i}"
    metadata = sealed_everywhere([damaged_replica, peer_replica], values)

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    {result, _log} = with_log(fn -> Scrubber.scrub_now(scrubber) end)

    assert result.repaired == [@segment]
    assert result.unrepairable == []
  end

  test "a sealed copy zeroed at its end scans clean and is caught by the control plane" do
    # The net under the blank-tail check, tested rather than assumed. A zeroed run at the end of a
    # sealed segment is indistinguishable from unwritten space by looking at the bytes, and #149
    # argued that the cross-check against the seal is what covers it. That argument was derived from
    # reading the code and had never been exercised.
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    values = ["a", "b", "c", "d"]
    metadata = sealed_everywhere([damaged_replica, peer_replica], values)

    peer_scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: peer_replica,
        directory: peer_directory
      )

    zero_tail_from(damaged_directory, 2)

    # The copy now serves only what precedes the zeros, silently: this is the loss being caught.
    assert read_values(damaged_replica) == ["a", "b"]

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    result = Scrubber.scrub_now(scrubber)

    # Not a checksum failure: the frames that remain are all valid, and the scan ends where the zeros
    # begin. What makes it damage is that the seal recorded four records and this copy holds two.
    assert [{@segment, %{reason: :short_copy}}] = result.damaged
    assert result.repaired == [@segment]
    assert read_values(damaged_replica) == values
  end

  test "a sealed copy zeroed at its end, with no peer, is reported rather than quietly accepted" do
    # The same shape without anywhere to repair from. The point is that it is still REPORTED: a node
    # that cannot fix a short copy must not answer that the copy is fine.
    {replica, directory} = start_replica()
    metadata = sealed_everywhere([replica], ["a", "b", "c", "d"])
    zero_tail_from(directory, 2)

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: replica,
        directory: directory
      )

    result = Scrubber.scrub_now(scrubber)

    assert [{@segment, %{reason: :short_copy}}] = result.damaged
    assert result.verified == []
    assert [{@segment, :no_intact_copy}] = result.unrepairable
    assert Scrubber.damaged(scrubber) == [@segment]
  end

  test "rebuilds a rotted index locally: no peer, no demote, no delete" do
    # The sparse index is DERIVED from the records, so nothing original is lost when it rots and no
    # replica can offer anything this node does not already hold. Treating it like a damaged segment
    # would mean deleting a perfectly good copy and refetching it over the network for nothing.
    {replica, directory} = start_replica()
    values = ["a", "b", "c", "d"]
    metadata = sealed_everywhere([replica], values)

    # force the log to roll internally, which is what writes the sidecar (seal/1 persists it)
    segment_directory = Layout.segment_directory(directory, @segment)
    {:ok, log} = Malachi.Log.recover(segment_directory, base_offset: 0)
    {:ok, _log} = Malachi.Log.roll(log)
    [index_path] = Path.wildcard(Path.join(segment_directory, "*.idx"))
    pristine = File.read!(index_path)

    <<byte, rest::binary>> = pristine
    File.write!(index_path, <<Bitwise.bxor(byte, 0xFF), rest::binary>>)

    test_process = self()

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: replica,
        directory: directory,
        peer_scrubber: fn _node -> send(test_process, :asked_a_peer) end,
        apply_command: fn command ->
          send(test_process, {:command, command})
          :ok
        end
      )

    result = Scrubber.scrub_now(scrubber)

    assert [{@segment, %{reason: :bad_index}}] = result.damaged
    assert result.repaired == [@segment]
    assert File.read!(index_path) == pristine, "the index is rebuilt to what the seal wrote"

    refute_received :asked_a_peer
    refute_received {:command, _any}
    assert read_values(replica) == values
  end

  test "demotes itself before repairing, so reads leave the damaged copy first" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    # the damaged node is the HEAD of the set, which is where reads go
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)
    test_process = self()

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        peer_scrubber: fn _node -> peer_scrubber end,
        apply_command: fn command ->
          send(test_process, {:command, command})
          :ok
        end
      )

    assert %{repaired: [@segment]} = Scrubber.scrub_now(scrubber)

    # the damaged node moved to the END of the replica set: reads follow the head, so they go to the
    # intact peer while this copy is deleted and refetched
    # The compare-and-swap names the set the demote was decided against: the snapshot's set, as it stood before.
    assert_received {:command, {:set_segment_replicas, @segment, [^peer_replica, ^damaged_replica], expected}}
    assert expected == [damaged_replica, peer_replica]
  end

  test "never deletes the local copy when no peer verifies" do
    # The safety rule: a partially readable copy is worth more than no copy. Both replicas are rotted,
    # so there is no source, and the local file must survive untouched for a manual recovery.
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c"])

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    rot_copy(damaged_directory)
    rot_copy(peer_directory)

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        peer_scrubber: fn _node -> peer_scrubber end,
        on_result: :default
      )

    log = capture_log(fn -> send(self(), {:result, Scrubber.scrub_now(scrubber)}) end)
    assert_received {:result, result}

    assert [{@segment, %{reason: :bad_crc}}] = result.damaged
    assert result.repaired == []
    assert [{@segment, :no_intact_copy}] = result.unrepairable
    assert Scrubber.damaged(scrubber) == [@segment]
    assert log =~ "NOT repaired"

    # the local bytes are still there: the valid prefix still reads, and the file was not deleted
    assert read_values(damaged_replica) == ["a"]
    assert ReplicationServer.stored_bytes(damaged_replica, @segment) > 0
  end

  test "an unreachable peer is not a repair source" do
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, _peer_directory} = start_replica()
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c"])
    rot_copy(damaged_directory)

    dead = spawn(fn -> :ok end)
    ref = Process.monitor(dead)
    assert_receive {:DOWN, ^ref, :process, ^dead, _reason}

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        peer_scrubber: fn _node -> dead end
      )

    result = Scrubber.scrub_now(scrubber)
    assert [{@segment, :no_intact_copy}] = result.unrepairable
    assert read_values(damaged_replica) == ["a"], "an unreachable peer must not cost the local copy"
  end

  test "a copy short of the sealed record count is damage even when every checksum passes" do
    # Checksums only prove that the frames PRESENT are intact. The control plane recorded what the
    # segment held when it was sealed, so a copy missing whole frames at the end is damaged too.
    {replica, directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    # claim one more record than the copies actually hold
    metadata = sealed_everywhere([replica, peer_replica], ["a", "b", "c"], 4)

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: replica,
        directory: directory,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    stored = ReplicationServer.stored_bytes(replica, @segment)
    result = Scrubber.scrub_now(scrubber)

    assert [{@segment, %{reason: :short_copy}}] = result.damaged

    # And the peer is short by exactly the same record, so it is NOT a source: a copy that verifies
    # is not the same as a copy that is complete. Deleting these three records to refetch four that
    # nobody has would take the copy out of the read path to gain nothing.
    assert result.repaired == []
    assert [{@segment, :no_intact_copy}] = result.unrepairable
    assert ReplicationServer.stored_bytes(replica, @segment) == stored
    assert read_values(replica) == ["a", "b", "c"]
  end

  test "a peer that verifies but is itself short is not a repair source" do
    # The safety rule reads "a peer confirmed its copy verifies", and a checksum-clean copy can still
    # be missing whole frames at the end. Repairing from one deletes the local copy and then catches
    # up to an offset the source cannot reach, leaving this node with LESS than the damage it started
    # with. The peer has to be complete against the sealed metadata, not merely readable.
    {damaged_replica, damaged_directory} = start_replica()
    {peer_replica, peer_directory} = start_replica()
    metadata = sealed_everywhere([damaged_replica, peer_replica], ["a", "b", "c", "d"])

    # the peer verifies cleanly but holds only the first record, so it can never serve the four the
    # seal recorded
    :ok = ReplicationServer.delete(peer_replica, @segment)
    {:ok, _last} = ReplicationServer.follow(peer_replica, @segment, 0, records(["a"]))

    rot_copy(damaged_directory)
    intact_bytes = ReplicationServer.stored_bytes(damaged_replica, @segment)

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    assert {:ok, %{records: 1}} = Scrubber.verify_segment(peer_scrubber, @segment)

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: damaged_replica,
        directory: damaged_directory,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    result = Scrubber.scrub_now(scrubber)

    assert [{@segment, :no_intact_copy}] = result.unrepairable
    assert result.repaired == []

    # The bytes of b, c and d are still on disk. They are unreadable in sequence past the rot, which
    # is what makes them salvageable by hand and what the safety rule exists to protect; refetching
    # one record over them would have destroyed three.
    assert ReplicationServer.stored_bytes(damaged_replica, @segment) == intact_bytes
    assert read_values(damaged_replica) == ["a"]
  end

  test "a segment retention deleted mid-cycle is not reported as damage" do
    {replica, directory} = start_replica()
    metadata = sealed_everywhere([replica], ["a", "b"])
    :ok = ReplicationServer.delete(replica, @segment)

    scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: replica, directory: directory)

    assert %{verified: [], damaged: [], repaired: [], unrepairable: []} = Scrubber.scrub_now(scrubber)
  end

  test "skips segments this node does not store, and active ones" do
    {replica, directory} = start_replica()
    {other, _other_directory} = start_replica()

    # sealed but held elsewhere
    metadata = sealed_everywhere([other], ["a"])

    scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: replica, directory: directory)

    assert %{verified: [], damaged: []} = Scrubber.scrub_now(scrubber)

    # active segments are excluded: they are scanned on recovery and still being written
    {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 4})
    active = {root, 7}
    {metadata, :ok} = Metadata.apply(metadata, {:register_segment, root, active, [replica], 0})
    {:ok, _last} = ReplicationServer.follow(replica, active, 0, records(["x"]))

    scrubber2 =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: replica, directory: directory)

    assert %{verified: [], damaged: []} = Scrubber.scrub_now(scrubber2)
  end

  test "walks the whole set across ticks, a batch at a time, and cycles" do
    {replica, directory} = start_replica()

    {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 4})

    metadata =
      Enum.reduce(0..2, metadata, fn i, acc ->
        # Contiguous, one record each: a range's segments tile its offsets, so spacing them out would
        # build a range with holes, which the control plane refuses and no roll can produce.
        segment = {root, i}
        {:ok, _last} = ReplicationServer.follow(replica, segment, i, records(["v#{i}"]))
        {acc, :ok} = Metadata.apply(acc, {:register_segment, root, segment, [replica], i})
        {acc, :ok} = Metadata.apply(acc, {:seal_segment, segment, 1, 0, 0})
        acc
      end)

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: replica,
        directory: directory,
        segments_per_tick: 2
      )

    first = Scrubber.scrub_now(scrubber).verified
    second = Scrubber.scrub_now(scrubber).verified

    assert length(first) == 2
    assert length(second) == 1
    assert Enum.sort(first ++ second) == Enum.sort(for i <- 0..2, do: {root, i})

    # the cycle restarts from the top, so every copy is revisited on a fixed period
    assert length(Scrubber.scrub_now(scrubber).verified) == 2
  end

  test "the scheduled tick runs a pass on its own" do
    {replica, directory} = start_replica()
    metadata = sealed_everywhere([replica], ["a"])
    test_process = self()

    start_scrubber(
      metadata_source: fn -> metadata end,
      local_ref: replica,
      directory: directory,
      interval: 20,
      on_result: fn result -> send(test_process, {:pass, result}) end
    )

    assert_receive {:pass, %{verified: [@segment]}}, 1_000
  end

  test "an unexpected message is logged, not fatal, and the cycle position survives it" do
    # This worker is meant to run for the lifetime of the node. Dying on a stray message would cost
    # the cycle position and the damaged set, so the restarted walk would begin at the first segment
    # again and never reach the tail of the rotation on a node holding many segments.
    {replica, directory} = start_replica()

    {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 4})

    metadata =
      Enum.reduce(0..1, metadata, fn i, acc ->
        # Contiguous, one record each: a range's segments tile its offsets, so spacing them out would
        # build a range with holes, which the control plane refuses and no roll can produce.
        segment = {root, i}
        {:ok, _last} = ReplicationServer.follow(replica, segment, i, records(["v#{i}"]))
        {acc, :ok} = Metadata.apply(acc, {:register_segment, root, segment, [replica], i})
        {acc, :ok} = Metadata.apply(acc, {:seal_segment, segment, 1, 0, 0})
        acc
      end)

    scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: replica, directory: directory)

    assert [{^root, 0}] = Scrubber.scrub_now(scrubber).verified

    UnknownMessages.expect_from(scrubber)

    log =
      capture_log(fn ->
        send(scrubber, {make_ref(), {:ok, %{records: 1, bytes: 1, files: 1}}})
        # a call round-trip after the send proves the message was handled and the process is alive
        assert Scrubber.damaged(scrubber) == []
      end)

    assert log =~ "unexpected message"
    assert [{^root, 1}] = Scrubber.scrub_now(scrubber).verified, "the walk resumed where it stopped"
  end

  test "the log line for an unexpected message is bounded, so a payload cannot leak into it" do
    # Whatever reaches that clause is by definition not understood, so it may carry record values. A
    # log line is the one place user data must not end up in by accident, and an unbounded inspect
    # would also turn a single message into an enormous line.
    {replica, directory} = start_replica()
    metadata = sealed_everywhere([replica], ["a"])

    scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: replica, directory: directory)

    # The assertion is on a SHORT fragment, deliberately. Asserting the absence of the whole payload
    # is the trap: any bound on the log line makes that pass while the line still prints the first
    # few hundred characters of the value, which is every bit as much of a leak.
    fragment = "s3cret-payload"
    secret = String.duplicate(fragment, 200)

    UnknownMessages.expect_from(scrubber)

    log =
      capture_log(fn ->
        send(scrubber, {make_ref(), {:ok, [%{offset: 7, key: "user-3", value: secret}]}})
        assert Scrubber.damaged(scrubber) == []
      end)

    # The absence checks stay on the WHOLE capture: the payload must not appear anywhere, whichever
    # line it might have reached.
    refute log =~ fragment
    refute log =~ "user-3"

    # The length bound, though, is about the scrubber's own line, so it is measured on that line
    # rather than on everything captured. `capture_log` collects Logger output process-wide, and the
    # application's own servers keep logging while the suite runs, so a line from another process can land in
    # `log` too: that is what failed this test in CI at 513 characters (the line itself is around 120), while
    # the file was async.
    line = log |> String.split("\n", trim: true) |> Enum.find(&(&1 =~ "unexpected message"))

    assert line, "the scrubber must log that it ignored the message"
    # the shape survives, which is the whole reason the term is printed at all
    assert line =~ "#Reference<"
    assert String.length(line) < 500
  end

  test "a control plane that cannot answer skips the pass instead of taking the scrubber down" do
    # Measured on the reshard drill: a sharded control plane comes back from a full-cluster restart
    # healthy but unable to serve metadata (#136), and this worker died once per tick for as long as
    # that lasted, losing its cycle position and its damaged set each time. The pass is worth less
    # than the state the process carries between passes.
    {replica, _directory} = start_replica()
    gone = spawn(fn -> :ok end)
    ref = Process.monitor(gone)
    assert_receive {:DOWN, ^ref, :process, ^gone, _reason}

    scrubber =
      start_scrubber(
        metadata_source: fn -> GenServer.call(gone, :metadata) end,
        local_ref: replica,
        directory: "/nonexistent"
      )

    log = capture_log(fn -> assert %{unrepairable: [{:metadata, _reason}]} = Scrubber.scrub_now(scrubber) end)

    assert log =~ "skipped the pass"
    assert Process.alive?(scrubber)
  end

  test "a non-positive interval is refused and the default used, rather than busy-looping" do
    # The interval arrives from MALACHI_SCRUB_INTERVAL_MS, which parses any integer. Zero would scan
    # the disk as fast as the loop can go; a negative value would crash the first schedule. Neither
    # is worth refusing to boot over, so the value is rejected out loud.
    {replica, directory} = start_replica()
    metadata = sealed_everywhere([replica], ["a"])

    for bad <- [0, -1] do
      log =
        capture_log(fn ->
          scrubber =
            start_scrubber(
              metadata_source: fn -> metadata end,
              local_ref: replica,
              directory: directory,
              interval: bad
            )

          assert :sys.get_state(scrubber).interval == 60_000
        end)

      assert log =~ "scrub_interval_ms does not accept the value"
    end
  end

  test "emits an integrity event for the scrub and a pass event with the counts" do
    {replica, directory} = start_replica()
    metadata = sealed_everywhere([replica], ["a", "b", "c"])
    rot_copy(directory)

    scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: replica, directory: directory)

    parent = self()
    handler_id = "scrub-telemetry-#{System.unique_integer([:positive])}"

    # Filtered by the EMITTING process, which `:telemetry.execute/3` runs the handler in. Handlers are
    # global to the VM, so any process that emits the same bad_crc/scrub event (the application's own
    # servers, or a sibling test's leftovers) would satisfy assert_receive: without this, assert_receive
    # can be satisfied by one of theirs and pass while this pass emitted nothing. Matching on the segment
    # alone would not fix the second assertion either, since a pass event carries counts and no segment
    # at all.
    :telemetry.attach_many(
      handler_id,
      [[:malachi, :storage, :integrity], [:malachi, :storage, :scrub]],
      fn name, measurements, metadata, _config ->
        if self() == scrubber, do: send(parent, {:telemetry, name, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    _result = Scrubber.scrub_now(scrubber)

    assert_receive {:telemetry, [:malachi, :storage, :integrity], _measurements,
                    %{result: :bad_crc, source: :scrub, segment: @segment}}

    assert_receive {:telemetry, [:malachi, :storage, :scrub], %{verified: 0, damaged: 1, unrepairable: 1}, _metadata}
  end

  test "an unknown cast and call are survived as well, not only an unknown info message" do
    # `use GenServer` stops the server on a cast it has no clause for, so before this server defined its
    # own catch-all a stray cast cost the cycle position and the damaged set just like an info did.
    {replica, directory} = start_replica()
    metadata = sealed_everywhere([replica], ["a"])

    scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: replica, directory: directory)

    UnknownMessages.assert_survives_unknown(scrubber, :scrubber, fn ->
      assert Scrubber.damaged(scrubber) == []
    end)
  end
end
