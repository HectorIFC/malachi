defmodule Malachi.Cluster.ScrubberTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.Scrubber
  alias Malachi.Log.Record
  alias Malachi.Metadata
  alias Malachi.Storage.Layout

  @segment {{"events", 0}, 0}

  defp start_replica do
    name = :"scrub_repl_#{System.unique_integer([:positive])}"
    directory = Path.join(System.tmp_dir!(), "malachi_scrub_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    start_supervised!({ReplicationServer, [name: name, directory: directory]}, id: name)
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
  defp sealed_everywhere(replica_set, values) do
    for replica <- replica_set do
      {:ok, _last} = ReplicationServer.follow(replica, @segment, 0, records(values))
    end

    {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 4})
    {metadata, :ok} = Metadata.apply(metadata, {:register_segment, root, @segment, replica_set, 0})
    bytes = ReplicationServer.stored_bytes(hd(replica_set), @segment)
    {metadata, :ok} = Metadata.apply(metadata, {:seal_segment, @segment, length(values), bytes, 0})
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
        apply_command: fn command -> send(test_process, {:command, command}) end
      )

    assert %{repaired: [@segment]} = Scrubber.scrub_now(scrubber)

    # the damaged node moved to the END of the replica set: reads follow the head, so they go to the
    # intact peer while this copy is deleted and refetched
    assert_received {:command, {:set_segment_replicas, @segment, [^peer_replica, ^damaged_replica]}}
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
    metadata = sealed_everywhere([replica, peer_replica], ["a", "b", "c"])

    # claim one more record than the copies actually hold
    {metadata, :ok} = Metadata.apply(metadata, {:seal_segment, @segment, 4, 0, 0})

    peer_scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: peer_replica, directory: peer_directory)

    scrubber =
      start_scrubber(
        metadata_source: fn -> metadata end,
        local_ref: replica,
        directory: directory,
        peer_scrubber: fn _node -> peer_scrubber end
      )

    assert [{@segment, %{reason: :short_copy}}] = Scrubber.scrub_now(scrubber).damaged
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
        segment = {root, i}
        {:ok, _last} = ReplicationServer.follow(replica, segment, i * 10, records(["v#{i}"]))
        {acc, :ok} = Metadata.apply(acc, {:register_segment, root, segment, [replica], i * 10})
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

  test "emits an integrity event for the scrub and a pass event with the counts" do
    {replica, directory} = start_replica()
    metadata = sealed_everywhere([replica], ["a", "b", "c"])
    rot_copy(directory)

    parent = self()
    handler_id = "scrub-telemetry-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler_id,
      [[:malachi, :storage, :integrity], [:malachi, :storage, :scrub]],
      fn name, measurements, metadata, _config -> send(parent, {:telemetry, name, measurements, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    scrubber =
      start_scrubber(metadata_source: fn -> metadata end, local_ref: replica, directory: directory)

    _result = Scrubber.scrub_now(scrubber)

    assert_receive {:telemetry, [:malachi, :storage, :integrity], _measurements,
                    %{result: :bad_crc, source: :scrub, segment: @segment}}

    assert_receive {:telemetry, [:malachi, :storage, :scrub], %{verified: 0, damaged: 1, unrepairable: 1}, _metadata}
  end
end
