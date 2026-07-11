defmodule Malachi.Cluster.SelfHealingTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.SelfHealing
  alias Malachi.Log.Record
  alias Malachi.Metadata

  defp start_broker do
    directory = Path.join(System.tmp_dir!(), "malachi_heal_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    start_supervised!({ReplicationServer, directory: directory}, id: {:repl, System.unique_integer([:positive])})
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

    # none of a, b, c is live — only d, which has no copy
    result = SelfHealing.heal_sealed(metadata, [d], 3)

    assert result.applied == []
    assert result.failed == [{segment_id, :no_live_source}]
    assert read_values(d, segment_id) == []
  end

  test "does nothing when every sealed segment is fully replicated" do
    [a, b, c] = [start_broker(), start_broker(), start_broker()]
    {metadata, _segment_id} = sealed_segment([a, b, c], a, ["x"])

    assert SelfHealing.heal_sealed(metadata, [a, b, c], 3) == %{applied: [], failed: []}
  end

  test "ignores active (unsealed) segments — those heal on the write path" do
    [a, b, c, d] = [start_broker(), start_broker(), start_broker(), start_broker()]
    {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 4})
    segment_id = {root, 0}
    # active segment (no seal), under-replicated once c leaves
    {metadata, :ok} = Metadata.apply(metadata, {:register_segment, root, segment_id, [a, b, c], 0})
    {:ok, _last} = ReplicationServer.replicate(a, segment_id, [a], 0, records(["x"]))

    assert SelfHealing.heal_sealed(metadata, [a, b, d], 3) == %{applied: [], failed: []}
  end
end
