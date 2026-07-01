defmodule Malachi.Cluster.FailoverTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.Failover
  alias Malachi.Metadata

  defp segment(replica_set, seal?) do
    {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 8})
    segment_id = {root, 0}
    {metadata, :ok} = Metadata.apply(metadata, {:register_segment, root, segment_id, replica_set, 0})

    metadata =
      if seal?, do: elem(Metadata.apply(metadata, {:seal_segment, segment_id, 1, 0}), 0), else: metadata

    {metadata, segment_id}
  end

  test "promotes the first live replica when an active segment's primary is dead" do
    {metadata, segment_id} = segment([:a, :b, :c], false)

    # :a (primary) is dead; :b is the first live replica in the set
    assert Failover.plan(metadata, [:b, :c, :d]) == [{:set_segment_replicas, segment_id, [:b, :a, :c]}]
  end

  test "no promotion when the active primary is alive" do
    {metadata, _segment_id} = segment([:a, :b, :c], false)
    assert Failover.plan(metadata, [:a, :b, :c]) == []
  end

  test "ignores sealed segments (those are healed, not failed over)" do
    {metadata, _segment_id} = segment([:a, :b, :c], true)
    assert Failover.plan(metadata, [:b, :c]) == []
  end

  test "skips an active segment whose every replica is dead" do
    {metadata, _segment_id} = segment([:a, :b, :c], false)
    assert Failover.plan(metadata, [:d]) == []
  end
end
