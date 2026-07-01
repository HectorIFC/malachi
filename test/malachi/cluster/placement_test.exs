defmodule Malachi.Cluster.PlacementTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Cluster.Placement
  alias Malachi.Metadata

  describe "place/3 — edge cases" do
    test "no brokers" do
      assert {:error, :no_brokers} = Placement.place("seg", [], 3)
    end

    test "replication_factor below 1 is rejected" do
      assert {:error, :invalid_replication_factor} = Placement.place("seg", [:a, :b], 0)
      assert {:error, :invalid_replication_factor} = Placement.place("seg", [:a, :b], -1)
    end

    test "fewer brokers than replication_factor degrades to all (distinct) brokers" do
      assert {:ok, replicas} = Placement.place("seg", [:a, :b], 5)
      assert Enum.sort(replicas) == [:a, :b]
    end

    test "duplicate brokers are ignored" do
      assert {:ok, replicas} = Placement.place("seg", [:a, :a, :b, :b], 5)
      assert Enum.sort(replicas) == [:a, :b]
    end

    test "placement is independent of input order (HRW)" do
      {:ok, one} = Placement.place("seg", [:a, :b, :c, :d, :e], 3)
      {:ok, two} = Placement.place("seg", [:e, :d, :c, :b, :a], 3)
      assert one == two
    end
  end

  describe "under_replicated/3 and heal/3 — edge cases" do
    test "empty broker set heals nothing (target is 0)" do
      metadata = with_segments([{"s1", [:a, :b]}], rf: 3)
      assert Placement.under_replicated(metadata, [], 3) == []
      assert Placement.heal(metadata, [], 3) == []
    end

    test "a segment with a dead replica is flagged and re-replicated" do
      metadata = with_segments([{"s1", [:a, :b, :c]}], rf: 3)
      # :c left the cluster
      assert Placement.under_replicated(metadata, [:a, :b, :d], 3) == ["s1"]

      assert [{:set_segment_replicas, "s1", replicas}] = Placement.heal(metadata, [:a, :b, :d], 3)
      assert :a in replicas and :b in replicas
      assert :c not in replicas
      assert length(replicas) == 3
    end

    test "a fully-live segment is not flagged" do
      metadata = with_segments([{"s1", [:a, :b, :c]}], rf: 3)
      assert Placement.under_replicated(metadata, [:a, :b, :c, :d], 3) == []
      assert Placement.heal(metadata, [:a, :b, :c, :d], 3) == []
    end

    test "a duplicated replica counts as one (not two) toward the target" do
      # replica_set lists :a twice → only one real copy, so this is under-replicated for rf 2
      metadata = with_segments([{"s1", [:a, :a]}], rf: 2)
      assert Placement.under_replicated(metadata, [:a, :b, :c], 2) == ["s1"]

      assert [{:set_segment_replicas, "s1", replicas}] = Placement.heal(metadata, [:a, :b, :c], 2)
      assert length(Enum.uniq(replicas)) == 2
    end

    test "sealed segments are healed too" do
      metadata =
        [{"s1", [:a, :b, :c]}]
        |> with_segments(rf: 3)
        |> apply!({:seal_segment, "s1", 100, 0})

      assert Placement.under_replicated(metadata, [:a, :b, :d], 3) == ["s1"]
    end
  end

  describe "properties" do
    property "place/3 returns min(rf, |brokers|) distinct brokers, all from the input" do
      check all(
              brokers <- uniq_list_of(broker(), min_length: 1, max_length: 8),
              rf <- integer(1..10),
              segment_id <- segment_id()
            ) do
        assert {:ok, replicas} = Placement.place(segment_id, brokers, rf)
        assert length(replicas) == min(rf, length(brokers))
        assert length(Enum.uniq(replicas)) == length(replicas)
        assert Enum.all?(replicas, &(&1 in brokers))
      end
    end

    property "placement is deterministic regardless of broker order" do
      check all(
              brokers <- uniq_list_of(broker(), min_length: 1, max_length: 8),
              rf <- integer(1..6),
              segment_id <- segment_id()
            ) do
        {:ok, expected} = Placement.place(segment_id, brokers, rf)
        {:ok, shuffled} = Placement.place(segment_id, Enum.shuffle(brokers), rf)
        assert shuffled == expected
      end
    end

    property "minimum reshuffle: removing one broker keeps every other chosen replica" do
      check all(
              brokers <- uniq_list_of(broker(), min_length: 2, max_length: 8),
              rf <- integer(1..6),
              segment_id <- segment_id()
            ) do
        {:ok, before} = Placement.place(segment_id, brokers, rf)
        removed = Enum.random(brokers)
        {:ok, after_removal} = Placement.place(segment_id, brokers -- [removed], rf)

        # every previously-chosen replica that still exists is still chosen
        for replica <- before, replica != removed do
          assert replica in after_removal
        end
      end
    end

    property "heal/3 reaches a fully-replicated fixpoint in one pass" do
      check all(
              all_brokers <- uniq_list_of(broker(), min_length: 1, max_length: 6),
              rf <- integer(1..5),
              specs <- segment_specs(all_brokers),
              live <- live_subset(all_brokers)
            ) do
        metadata = with_segments(specs, rf: rf)

        healed =
          metadata
          |> Placement.heal(live, rf)
          |> Enum.reduce(metadata, fn command, acc -> apply!(acc, command) end)

        assert Placement.under_replicated(healed, live, rf) == []
      end
    end
  end

  # --- helpers ---

  # Brokers are opaque terms; a large pool keeps uniq_list_of from exhausting the space.
  defp broker, do: integer(1..100)
  defp segment_id, do: string(:alphanumeric, min_length: 1, max_length: 6)

  # Each segment gets an arbitrary (possibly empty / possibly stale) replica set drawn from the
  # full broker pool, mirroring replicas that may since have died.
  defp segment_specs(all_brokers) do
    gen all(
          count <- integer(0..4),
          ids <- uniq_list_of(segment_id(), length: count),
          sets <- list_of(member_subset(all_brokers), length: count)
        ) do
      Enum.zip(ids, sets)
    end
  end

  defp member_subset([]), do: constant([])

  defp member_subset(brokers) do
    bind(list_of(member_of(brokers), max_length: length(brokers)), fn picks ->
      constant(Enum.uniq(picks))
    end)
  end

  defp live_subset([]), do: constant([])
  defp live_subset(all_brokers), do: member_subset(all_brokers)

  # Builds a Metadata with one topic/range, registering each {segment_id, replica_set}.
  defp with_segments(specs, rf: _rf) do
    {metadata, {:ok, root_id}} = Metadata.apply(Metadata.new(), {:create_topic, "t", 8})

    Enum.reduce(specs, metadata, fn {segment_id, replica_set}, acc ->
      apply!(acc, {:register_segment, root_id, segment_id, replica_set, 0})
    end)
  end

  defp apply!(metadata, command) do
    {next, reply} = Metadata.apply(metadata, command)
    assert reply == :ok or match?({:ok, _}, reply), "command #{inspect(command)} failed: #{inspect(reply)}"
    next
  end
end
