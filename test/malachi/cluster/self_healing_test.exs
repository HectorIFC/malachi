defmodule Malachi.Cluster.SelfHealingTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.SelfHealing
  alias Malachi.Log.Record
  alias Malachi.Metadata

  defp start_broker do
    {ref, _directory, _id} = start_broker_with_directory()
    ref
  end

  defp start_broker_with_directory do
    directory = Path.join(System.tmp_dir!(), "malachi_heal_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    id = {:repl, System.unique_integer([:positive])}
    {start_supervised!({ReplicationServer, directory: directory}, id: id), directory, id}
  end

  defp records(values), do: for(value <- values, do: Record.new(value, key: value))

  defp read_values(ref, segment_id) do
    case ReplicationServer.read(ref, segment_id, 0, 100) do
      {:ok, records} -> Enum.map(records, & &1.value)
      :eof -> []
    end
  end

  # Builds metadata with one sealed segment over `replica_set`, and seeds `source` with its data.
  defp sealed_segment(replica_set, source, values) do
    {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 4})
    segment_id = {root, 0}
    {metadata, :ok} = Metadata.apply(metadata, {:register_segment, root, segment_id, replica_set, 0})
    {metadata, :ok} = Metadata.apply(metadata, {:seal_segment, segment_id, length(values), 0, 0})
    {:ok, _last} = ReplicationServer.replicate(source, segment_id, [source], 0, records(values))
    {metadata, segment_id}
  end

  test "backfills the new replica of an under-replicated sealed segment and returns the command" do
    [a, b, c, d] = [start_broker(), start_broker(), start_broker(), start_broker()]
    {metadata, segment_id} = sealed_segment([a, b, c], a, ["x", "y", "z"])

    # c has left; the live set is a, b, d
    result = SelfHealing.heal_sealed(metadata, [a, b, d], 3)

    assert [{:set_segment_replicas, ^segment_id, new_set}] = result.applied
    assert result.failed == []
    assert Enum.sort(new_set) == Enum.sort([a, b, d])

    # the freshly placed replica d now holds the segment's data
    assert read_values(d, segment_id) == ["x", "y", "z"]

    # applying the command makes the segment fully replicated again
    {healed, :ok} = Metadata.apply(metadata, {:set_segment_replicas, segment_id, new_set})
    assert SelfHealing.heal_sealed(healed, [a, b, d], 3).applied == []
  end

  test "re-replication is rack-aware when :spread is forwarded" do
    [a1, a2, b1, b2] = [start_broker(), start_broker(), start_broker(), start_broker()]
    dead = start_broker()
    attrs = %{a1 => %{"rack" => "a"}, a2 => %{"rack" => "a"}, b1 => %{"rack" => "b"}, b2 => %{"rack" => "b"}}

    # segment on [a1, dead]; dead has left, so it is under-replicated for rf 2
    {metadata, segment_id} = sealed_segment([a1, dead], a1, ["x", "y"])

    result = SelfHealing.heal_sealed(metadata, [a1, a2, b1, b2], 2, spread: {"rack", attrs})

    assert [{:set_segment_replicas, ^segment_id, new_set}] = result.applied
    # with spread over two racks and rf 2, the healed set must span both racks (not concentrate in one)
    assert new_set |> Enum.map(&attrs[&1]["rack"]) |> Enum.sort() == ["a", "b"]
  end

  test "reports a segment whose every replica is dead as failed" do
    [a, b, c, d] = [start_broker(), start_broker(), start_broker(), start_broker()]
    {metadata, segment_id} = sealed_segment([a, b, c], a, ["x", "y"])

    # none of a, b, c is live, only d, which has no copy
    result = SelfHealing.heal_sealed(metadata, [d], 3)

    assert result.applied == []
    assert result.failed == [{segment_id, :no_live_source}]
    assert read_values(d, segment_id) == []
  end

  test "does nothing when every sealed segment is fully replicated" do
    [a, b, c] = [start_broker(), start_broker(), start_broker()]
    {metadata, _segment_id} = sealed_segment([a, b, c], a, ["x"])

    assert SelfHealing.heal_sealed(metadata, [a, b, c], 3) == %{applied: [], failed: [], repaired: []}
  end

  # Builds metadata with one sealed segment held by ALL of `replica_set` (each replica appended
  # durably via follow/4), sealed with the REAL byte size, the shape the physical integrity pass
  # probes against. Returns {metadata, segment_id, byte_size}.
  defp sealed_segment_everywhere(replica_set, values) do
    {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 4})
    segment_id = {root, 0}
    {metadata, :ok} = Metadata.apply(metadata, {:register_segment, root, segment_id, replica_set, 0})

    for replica <- replica_set do
      {:ok, _last} = ReplicationServer.follow(replica, segment_id, 0, records(values))
    end

    byte_size = ReplicationServer.stored_bytes(hd(replica_set), segment_id)
    {metadata, :ok} = Metadata.apply(metadata, {:seal_segment, segment_id, length(values), byte_size, 0})
    {metadata, segment_id, byte_size}
  end

  # The chaos shape: the replica loses its file(s) while down and comes back over the damaged
  # directory. Restarting matters: a live server still holds the deleted file's descriptor (unix
  # keeps the data reachable through it), so loss only materializes at reopen.
  defp restart_broker_over(directory, id, damage_fun) do
    :ok = stop_supervised(id)
    damage_fun.()
    new_id = {:repl, System.unique_integer([:positive])}
    start_supervised!({ReplicationServer, directory: directory}, id: new_id)
  end

  test "re-backfills a live replica whose sealed copy was deleted (silent under-replication)" do
    {a, _dir_a, _id_a} = start_broker_with_directory()
    {b, _dir_b, _id_b} = start_broker_with_directory()
    {c, dir_c, id_c} = start_broker_with_directory()

    {metadata, segment_id, byte_size} = sealed_segment_everywhere([a, b, c], ["x", "y", "z"])

    c = restart_broker_over(dir_c, id_c, fn -> File.rm_rf!(Path.join(dir_c, "events-r0-s0")) end)
    metadata = rewrite_replicas(metadata, segment_id, [a, b, c])

    result = SelfHealing.heal_sealed(metadata, [a, b, c], 3)

    # no metadata change (the replica set is intact); the lost copy is rebuilt in place
    assert result == %{applied: [], failed: [], repaired: [{segment_id, c}]}
    assert ReplicationServer.stored_bytes(c, segment_id) == byte_size
    assert read_values(c, segment_id) == ["x", "y", "z"]

    # a second pass finds nothing to do
    assert SelfHealing.heal_sealed(metadata, [a, b, c], 3) == %{applied: [], failed: [], repaired: []}
  end

  test "repairs a truncated sealed copy from its truncation point, not by recopying" do
    {a, _dir_a, _id_a} = start_broker_with_directory()
    {b, _dir_b, _id_b} = start_broker_with_directory()
    {c, dir_c, id_c} = start_broker_with_directory()

    {metadata, segment_id, byte_size} = sealed_segment_everywhere([a, b, c], ["x", "y", "z"])

    # keep the first frame plus a partial second one: recovery must clamp to the last valid frame
    first_record = Record.new("x", key: "x")
    first_frame_bytes = byte_size(Record.encode(%Record{first_record | offset: 0}))
    [log_file] = Path.wildcard(Path.join(dir_c, "events-r0-s0/*.log"))

    c =
      restart_broker_over(dir_c, id_c, fn ->
        truncated = binary_part(File.read!(log_file), 0, first_frame_bytes + 3)
        File.write!(log_file, truncated)
      end)

    metadata = rewrite_replicas(metadata, segment_id, [a, b, c])

    result = SelfHealing.heal_sealed(metadata, [a, b, c], 3)

    assert result == %{applied: [], failed: [], repaired: [{segment_id, c}]}
    assert ReplicationServer.stored_bytes(c, segment_id) == byte_size
    assert read_values(c, segment_id) == ["x", "y", "z"]
  end

  test "an unreachable replica is skipped by the integrity probe, never repaired on unknown state" do
    [a, b] = [start_broker(), start_broker()]
    dead = spawn(fn -> :ok end)

    {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 4})
    segment_id = {root, 0}
    {metadata, :ok} = Metadata.apply(metadata, {:register_segment, root, segment_id, [a, b, dead], 0})

    for replica <- [a, b] do
      {:ok, _last} = ReplicationServer.follow(replica, segment_id, 0, records(["x"]))
    end

    byte_size = ReplicationServer.stored_bytes(a, segment_id)
    {metadata, :ok} = Metadata.apply(metadata, {:seal_segment, segment_id, 1, byte_size, 0})

    # the caller believes all three are live; the dead pid does not answer the probe and is skipped
    assert SelfHealing.heal_sealed(metadata, [a, b, dead], 3) == %{applied: [], failed: [], repaired: []}
  end

  # Replaces the sealed segment's replica set so the restarted replica's NEW ref takes the dead
  # ref's place, the same adjustment the control plane sees when a node rejoins under its name.
  defp rewrite_replicas(metadata, segment_id, replica_set) do
    {metadata, :ok} = Metadata.apply(metadata, {:set_segment_replicas, segment_id, replica_set})
    metadata
  end

  test "ignores active (unsealed) segments: those heal on the write path" do
    [a, b, c, d] = [start_broker(), start_broker(), start_broker(), start_broker()]
    {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 4})
    segment_id = {root, 0}
    # active segment (no seal), under-replicated once c leaves
    {metadata, :ok} = Metadata.apply(metadata, {:register_segment, root, segment_id, [a, b, c], 0})
    {:ok, _last} = ReplicationServer.replicate(a, segment_id, [a], 0, records(["x"]))

    assert SelfHealing.heal_sealed(metadata, [a, b, d], 3) == %{applied: [], failed: [], repaired: []}
  end
end
