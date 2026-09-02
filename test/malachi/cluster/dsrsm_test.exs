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

    test "a migrated topic carries its consumer groups' committed offsets" do
      dsrsm = with_vnodes(a: 0)
      {dsrsm, {:ok, root_id}} = DSRSM.command(dsrsm, "orders", {:create_topic, "orders", 4})
      {dsrsm, :ok} = DSRSM.command(dsrsm, "orders", {:commit_offset, "workers", "orders", %{root_id => 500}})

      token = :erlang.phash2("orders", 16)
      assert token > 0, "test setup expects a non-zero hash"
      {:ok, dsrsm} = DSRSM.split_vnode(dsrsm, :b, token)

      # the group's committed position moved with the topic (no reprocessing after a split)
      assert home_of(dsrsm, "orders") == :b
      assert DSRSM.committed_offsets(dsrsm, "workers", "orders") == %{root_id => 500}
    end

    test "propagates ring placement errors" do
      dsrsm = with_vnodes(a: 4)
      assert DSRSM.split_vnode(dsrsm, :b, 4) == {:error, :token_taken}
    end
  end

  describe "single vnode (unsharded shape)" do
    alias Malachi.Metadata

    test "single/1 holds the seed metadata and routes every topic to it" do
      seed = elem(Metadata.apply(Metadata.new(), {:create_topic, "events", 4}), 0)
      dsrsm = DSRSM.single(seed)

      assert DSRSM.vnode_ids(dsrsm) == [:vnode_0]
      # any topic name resolves to the lone vnode, whether or not it exists yet
      assert DSRSM.vnode_for(dsrsm, "events") == {:ok, :vnode_0}
      assert DSRSM.vnode_for(dsrsm, "anything") == {:ok, :vnode_0}
      assert %{name: "events"} = DSRSM.get_topic(dsrsm, "events")
    end

    test "merged_metadata/1 of one vnode is exactly that vnode's metadata" do
      seed = elem(Metadata.apply(Metadata.new(), {:create_topic, "events", 4}), 0)
      assert DSRSM.merged_metadata(DSRSM.single(seed)) == seed
    end

    test "committed_offsets/3 and topic_policy/2 route by topic" do
      dsrsm = DSRSM.single()
      {dsrsm, {:ok, _root}} = DSRSM.command(dsrsm, "events", {:create_topic, "events", 4})
      {dsrsm, :ok} = DSRSM.command(dsrsm, "events", {:commit_offset, "g", "events", %{{"events", 0} => 7}})
      {dsrsm, :ok} = DSRSM.command(dsrsm, "events", {:define_policy, "p", %{spread_by: "rack"}})
      {dsrsm, :ok} = DSRSM.command(dsrsm, "events", {:set_topic_policy, "events", "p"})

      assert DSRSM.committed_offsets(dsrsm, "g", "events") == %{{"events", 0} => 7}
      assert DSRSM.committed_offsets(dsrsm, "g", "other") == %{}
      assert DSRSM.topic_policy(dsrsm, "events") == %{spread_by: "rack"}
      assert DSRSM.topic_policy(dsrsm, "other") == nil
    end
  end

  describe "update_vnode (mutation combinator)" do
    test "routes to the owning vnode, applies the update, and returns its reply" do
      dsrsm = with_vnodes(a: 4, b: 12)
      {:ok, owner} = DSRSM.vnode_for(dsrsm, "events")

      {dsrsm, :applied} =
        DSRSM.update_vnode(dsrsm, "events", fn metadata ->
          {elem(Malachi.Metadata.apply(metadata, {:create_topic, "events", 4}), 0), :applied}
        end)

      # the update landed in the routed vnode's shard, and command/3 is update_vnode + Metadata.apply
      assert Malachi.Metadata.get_topic(Map.fetch!(dsrsm.vnodes, owner), "events")
      assert DSRSM.get_topic(dsrsm, "events")
    end

    test "an empty ring reports no vnode and leaves the state unchanged" do
      dsrsm = DSRSM.new(ring_bits: 4)
      assert {^dsrsm, {:error, :no_vnode}} = DSRSM.update_vnode(dsrsm, "events", fn m -> {m, :ok} end)
    end
  end

  describe "merged_metadata across shards" do
    test "unions disjoint topics from every vnode" do
      dsrsm = with_vnodes([{:a, 4}, {:b, 8}, {:c, 12}, {:d, 15}])

      dsrsm =
        Enum.reduce(0..19, dsrsm, fn index, dsrsm ->
          {dsrsm, {:ok, _root}} = DSRSM.command(dsrsm, "topic-#{index}", {:create_topic, "topic-#{index}", 4})
          dsrsm
        end)

      merged = DSRSM.merged_metadata(dsrsm)
      assert map_size(merged.topics) == 20
      for index <- 0..19, do: assert(Malachi.Metadata.get_topic(merged, "topic-#{index}"))
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

  describe "retain_vnodes/3" do
    test "keeps the previous view of a named vnode and the fresh view of every other" do
      previous = with_vnodes([{:v0, 0}, {:v1, 8}])
      {previous, {:ok, _root}} = DSRSM.command(previous, "kept", {:create_topic, "kept", 4})
      {previous, {:ok, _root}} = DSRSM.command(previous, "refreshed", {:create_topic, "refreshed", 4})

      # what a snapshot that could not read `kept`'s vnode produces: an empty placeholder for it
      kept_home = home_of(previous, "kept")
      fresh = %{previous | vnodes: Map.put(previous.vnodes, kept_home, Malachi.Metadata.new())}
      assert DSRSM.get_topic(fresh, "kept") == nil

      merged = DSRSM.retain_vnodes(fresh, previous, [kept_home])

      assert DSRSM.get_topic(merged, "kept").name == "kept"
      assert DSRSM.get_topic(merged, "refreshed").name == "refreshed"
    end

    test "an empty list changes nothing, so a fully readable snapshot installs as-is" do
      previous = with_vnodes([{:v0, 0}, {:v1, 8}])
      {fresh, {:ok, _root}} = DSRSM.command(previous, "events", {:create_topic, "events", 4})

      assert DSRSM.retain_vnodes(fresh, previous, []) == fresh
    end

    test "a vnode the reader has never held keeps the placeholder rather than inventing one" do
      previous = with_vnodes([{:v0, 0}])
      fresh = with_vnodes([{:v0, 0}, {:v1, 8}])

      merged = DSRSM.retain_vnodes(fresh, previous, [:v1])

      assert Map.fetch!(merged.vnodes, :v1) == Map.fetch!(fresh.vnodes, :v1)
    end
  end

  describe "cross-topic command rejection" do
    # One vnode co-locates every topic, so "events" and "orders" share it and their range ids collide in
    # scope. This is the exact shape the guard defends: a command routed by one topic carrying the other's
    # range id.
    setup do
      dsrsm = with_vnodes(only: 8)
      {dsrsm, {:ok, events_root}} = DSRSM.command(dsrsm, "events", {:create_topic, "events", 4})
      {dsrsm, {:ok, orders_root}} = DSRSM.command(dsrsm, "orders", {:create_topic, "orders", 4})
      {:ok, dsrsm: dsrsm, events_root: events_root, orders_root: orders_root}
    end

    test "a range command routed by the wrong topic is rejected", %{dsrsm: dsrsm, orders_root: orders_root} do
      # Routed by "events" but targeting orders' range. Without the guard this would split orders' range.
      assert {^dsrsm, {:error, :range_topic_mismatch}} =
               DSRSM.command(dsrsm, "events", {:split_range, orders_root})

      # And orders' range is untouched: still the single active root, not split.
      assert [%{id: ^orders_root, state: :active}] = DSRSM.active_ranges_of_topic(dsrsm, "orders")
    end

    test "a merge routed by the wrong topic is rejected", %{
      dsrsm: dsrsm,
      events_root: events_root,
      orders_root: orders_root
    } do
      assert {^dsrsm, {:error, :range_topic_mismatch}} =
               DSRSM.command(dsrsm, "events", {:merge_ranges, orders_root, events_root})
    end

    test "a segment command routed by the wrong topic is rejected", %{dsrsm: dsrsm, orders_root: orders_root} do
      # Production segment ids are {range_id, seq}; this one belongs to orders. Seal it while routed by
      # "events" and the guard reads the topic out of the id and rejects.
      seg_id = {orders_root, 0}
      {dsrsm, :ok} = DSRSM.command(dsrsm, "orders", {:register_segment, orders_root, seg_id, [:b1], 0})

      assert {^dsrsm, {:error, :range_topic_mismatch}} =
               DSRSM.command(dsrsm, "events", {:seal_segment, seg_id, 1, 10, 0})
    end

    test "the matching pair still succeeds", %{dsrsm: dsrsm, orders_root: orders_root} do
      assert {_dsrsm, {:ok, _left, _right}} =
               DSRSM.command(dsrsm, "orders", {:split_range, orders_root})
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
