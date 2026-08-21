defmodule Malachi.Cluster.HealCoordinatorTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.HealCoordinator
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record
  alias Malachi.Metadata

  defp start_broker do
    directory = Path.join(System.tmp_dir!(), "malachi_healco_#{System.unique_integer([:positive])}")
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

  # A metadata held in an Agent, with the source/apply seams the coordinator needs.
  defp metadata_store(metadata) do
    {:ok, agent} = start_supervised({Agent, fn -> metadata end}, id: {:meta, System.unique_integer([:positive])})
    source = fn -> Agent.get(agent, & &1) end
    apply = fn command -> Agent.update(agent, fn meta -> elem(Metadata.apply(meta, command), 0) end) end
    {source, apply}
  end

  defp start_coordinator(opts) do
    defaults = [interval: 60_000, replication_factor: 3]
    start_supervised!({HealCoordinator, Keyword.merge(defaults, opts)}, id: {:co, System.unique_integer([:positive])})
  end

  # metadata with one sealed segment over `replica_set`, with `source` seeded with its data
  defp sealed_segment(replica_set, source, values) do
    {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 4})
    segment_id = {root, 0}
    {metadata, :ok} = Metadata.apply(metadata, {:register_segment, root, segment_id, replica_set, 0})
    {metadata, :ok} = Metadata.apply(metadata, {:seal_segment, segment_id, length(values), 0, 0})
    {:ok, _last} = ReplicationServer.replicate(source, segment_id, [source], 0, records(values))
    {metadata, segment_id}
  end

  test "heals an under-replicated sealed segment against the live broker set and applies it" do
    a = start_broker()
    d = start_broker()
    {metadata, segment_id} = sealed_segment([a, :b, :c], a, ["x", "y", "z"])
    {source, apply} = metadata_store(metadata)

    # :c has left; the live set is a, :b, d
    coordinator = start_coordinator(live_brokers: fn -> [a, :b, d] end, metadata_source: source, apply_command: apply)

    result = HealCoordinator.heal_now(coordinator)

    assert [{:set_segment_replicas, ^segment_id, new_set}] = result.applied
    assert Enum.sort(new_set) == Enum.sort([a, :b, d])

    # the new replica d was backfilled, and the applied command landed in the metadata
    assert read_values(d, segment_id) == ["x", "y", "z"]
    assert Metadata.get_segment(source.(), segment_id).replica_set == new_set

    # the loop is closed: a second pass has nothing to do
    assert HealCoordinator.heal_now(coordinator) == %{applied: [], failed: [], repaired: []}
  end

  test "reports a segment with no live source as failed and applies nothing" do
    a = start_broker()
    d = start_broker()
    {metadata, segment_id} = sealed_segment([a, :b, :c], a, ["x"])
    {source, apply} = metadata_store(metadata)

    # only d is live, and it has no copy
    coordinator = start_coordinator(live_brokers: fn -> [d] end, metadata_source: source, apply_command: apply)

    result = HealCoordinator.heal_now(coordinator)
    assert result.applied == []
    assert result.failed == [{segment_id, :no_live_source}]
    assert read_values(d, segment_id) == []
  end

  test "heals automatically on its interval" do
    a = start_broker()
    d = start_broker()
    {metadata, segment_id} = sealed_segment([a, :b, :c], a, ["x", "y"])
    {source, apply} = metadata_store(metadata)

    start_coordinator(
      live_brokers: fn -> [a, :b, d] end,
      metadata_source: source,
      apply_command: apply,
      interval: 15
    )

    assert eventually(fn -> read_values(d, segment_id) == ["x", "y"] end)
  end

  test "a non-leader ticks but skips healing (only the leader acts)" do
    a = start_broker()
    d = start_broker()
    {metadata, segment_id} = sealed_segment([a, :b, :c], a, ["x", "y"])
    {source, apply} = metadata_store(metadata)

    start_coordinator(
      live_brokers: fn -> [a, :b, d] end,
      metadata_source: source,
      apply_command: apply,
      interval: 15,
      leader?: fn -> false end
    )

    # not the membership leader → no healing happens; d never receives the segment
    refute eventually(fn -> read_values(d, segment_id) == ["x", "y"] end, 300)
  end

  defp eventually(check, remaining_ms \\ 2_000) do
    cond do
      check.() -> true
      remaining_ms <= 0 -> false
      true -> Process.sleep(15) && eventually(check, remaining_ms - 15)
    end
  end
end
