defmodule Malachi.Cluster.DSRSMTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.DSRSM

  defp with_vnodes(tokens, ring_bits \\ 4) do
    Enum.reduce(tokens, DSRSM.new(ring_bits: ring_bits), fn {vnode_id, token}, dsrsm ->
      {:ok, dsrsm} = DSRSM.add_vnode(dsrsm, vnode_id, token)
      dsrsm
    end)
  end

  describe "membership" do
    test "add vnodes and route topics to them" do
      dsrsm = with_vnodes(a: 4, b: 12)
      assert Enum.sort(DSRSM.vnode_ids(dsrsm)) == [:a, :b]
      assert {:ok, vnode} = DSRSM.vnode_for(dsrsm, "events")
      assert vnode in [:a, :b]
    end

    test "propagates ring placement errors" do
      dsrsm = with_vnodes(a: 4)
      assert DSRSM.add_vnode(dsrsm, :b, 4) == {:error, :token_taken}
    end

    test "commands on an empty ring report no vnode" do
      dsrsm = DSRSM.new(ring_bits: 4)
      assert {_dsrsm, {:error, :no_vnode}} = DSRSM.command(dsrsm, "events", {:create_topic, "events", 4})
      assert DSRSM.get_topic(dsrsm, "events") == nil
      assert DSRSM.ranges_of_topic(dsrsm, "events") == []
    end
  end

  describe "routed lifecycle" do
    test "create topic, split range, register segment, and query through the DS-RSM" do
      dsrsm = with_vnodes(a: 4, b: 12)

      {dsrsm, {:ok, root_id}} = DSRSM.command(dsrsm, "events", {:create_topic, "events", 4})
      assert %{name: "events", keyspace_size: 16} = DSRSM.get_topic(dsrsm, "events")

      {dsrsm, {:ok, left_id, right_id}} = DSRSM.command(dsrsm, "events", {:split_range, root_id})
      assert DSRSM.get_range(dsrsm, "events", root_id).state == :sealed

      active = DSRSM.active_ranges_of_topic(dsrsm, "events")
      assert Enum.sort(Enum.map(active, & &1.id)) == Enum.sort([left_id, right_id])

      {dsrsm, :ok} = DSRSM.command(dsrsm, "events", {:register_segment, left_id, "s1", [:b1, :b2], 0})
      assert DSRSM.get_segment(dsrsm, "events", "s1").state == :active
      assert DSRSM.segments_of_range(dsrsm, "events", left_id) |> length() == 1
    end

    test "routing is consistent: the same topic always resolves to the same vnode" do
      dsrsm = with_vnodes(a: 4, b: 8, c: 12)
      {dsrsm, {:ok, _root}} = DSRSM.command(dsrsm, "events", {:create_topic, "events", 4})

      {:ok, vnode} = DSRSM.vnode_for(dsrsm, "events")
      # the topic actually lives in that vnode's shard, and nowhere else
      assert topic_in_vnode?(dsrsm, vnode, "events")
      for other <- DSRSM.vnode_ids(dsrsm) -- [vnode], do: refute(topic_in_vnode?(dsrsm, other, "events"))
    end
  end

  describe "sharding" do
    test "topics are distributed across vnodes with no metadata lost" do
      dsrsm = with_vnodes([{:a, 4}, {:b, 8}, {:c, 12}, {:d, 15}])

      dsrsm =
        Enum.reduce(0..19, dsrsm, fn index, dsrsm ->
          {dsrsm, {:ok, _root}} = DSRSM.command(dsrsm, "topic-#{index}", {:create_topic, "topic-#{index}", 4})
          dsrsm
        end)

      # every topic is retrievable
      for index <- 0..19, do: assert(DSRSM.get_topic(dsrsm, "topic-#{index}"))

      topic_counts = Enum.map(dsrsm.vnodes, fn {_id, metadata} -> map_size(metadata.topics) end)
      assert Enum.sum(topic_counts) == 20
      assert Enum.count(topic_counts, &(&1 > 0)) >= 2, "expected topics to shard across vnodes"
    end
  end

  describe "vnode split (rebalancing)" do
    test "migrates displaced topics to the new vnode with no metadata lost" do
      # one vnode owns everything, then we split half the ring onto a new vnode
      dsrsm = with_vnodes(a: 0)

      dsrsm =
        Enum.reduce(0..9, dsrsm, fn index, dsrsm ->
          {dsrsm, {:ok, _root}} = DSRSM.command(dsrsm, "topic-#{index}", {:create_topic, "topic-#{index}", 4})
          dsrsm
        end)

      {:ok, dsrsm} = DSRSM.split_vnode(dsrsm, :b, 8)

      # every topic is still retrievable and lives in exactly the vnode it now routes to
      for index <- 0..9 do
        name = "topic-#{index}"
        assert DSRSM.get_topic(dsrsm, name)
        {:ok, routed} = DSRSM.vnode_for(dsrsm, name)
        assert home_of(dsrsm, name) == routed
      end

      counts = Enum.map(dsrsm.vnodes, fn {_id, metadata} -> map_size(metadata.topics) end)
      assert Enum.sum(counts) == 10
      assert map_size(Map.fetch!(dsrsm.vnodes, :b).topics) >= 1, "expected some topics to migrate to :b"
    end

    test "a migrated topic carries its ranges and segments to the new vnode" do
      dsrsm = with_vnodes(a: 0)
      {dsrsm, {:ok, root_id}} = DSRSM.command(dsrsm, "orders", {:create_topic, "orders", 4})
      {dsrsm, {:ok, left_id, _right_id}} = DSRSM.command(dsrsm, "orders", {:split_range, root_id})
      {dsrsm, :ok} = DSRSM.command(dsrsm, "orders", {:register_segment, left_id, "s1", [:b1], 0})

      # place the new vnode exactly at "orders"'s hash so the topic routes to it
      token = :erlang.phash2("orders", 16)
      assert token > 0, "test setup expects a non-zero hash"
      {:ok, dsrsm} = DSRSM.split_vnode(dsrsm, :b, token)

      assert home_of(dsrsm, "orders") == :b
      refute Malachi.Metadata.get_topic(Map.fetch!(dsrsm.vnodes, :a), "orders")

      # range and segment metadata moved and are queryable through the DS-RSM
      assert DSRSM.get_range(dsrsm, "orders", left_id).state == :active
      assert DSRSM.get_segment(dsrsm, "orders", "s1").state == :active
      assert DSRSM.segments_of_range(dsrsm, "orders", left_id) |> length() == 1
    end

    test "propagates ring placement errors" do
      dsrsm = with_vnodes(a: 4)
      assert DSRSM.split_vnode(dsrsm, :b, 4) == {:error, :token_taken}
    end
  end

  describe "determinism" do
    test "the same command sequence yields identical state" do
      build = fn ->
        dsrsm = with_vnodes(a: 4, b: 12)

        Enum.reduce(
          [
            {"events", {:create_topic, "events", 4}},
            {"logs", {:create_topic, "logs", 8}},
            {"events", {:split_range, {"events", 0}}}
          ],
          dsrsm,
          fn {topic, command}, dsrsm -> elem(DSRSM.command(dsrsm, topic, command), 0) end
        )
      end

      assert build.() == build.()
    end
  end

  # Whether `topic` exists in the given vnode's metadata shard.
  defp topic_in_vnode?(dsrsm, vnode_id, topic) do
    metadata = Map.fetch!(dsrsm.vnodes, vnode_id)
    Malachi.Metadata.get_topic(metadata, topic) != nil
  end

  # The vnode id whose shard actually holds `topic`'s metadata.
  defp home_of(dsrsm, topic) do
    Enum.find_value(dsrsm.vnodes, fn {id, metadata} ->
      if Malachi.Metadata.get_topic(metadata, topic), do: id
    end)
  end
end
