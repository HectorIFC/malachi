defmodule Malachi.Cluster.PlacementTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Cluster.Placement
  alias Malachi.Metadata

  describe "place/3, edge cases" do
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

  describe "under_replicated/3 and heal/3, edge cases" do
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
        |> apply!({:seal_segment, "s1", 100, 0, 0})

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

    property "minimum reshuffle: adding one broker changes at most one chosen replica (the new one)" do
      check all(
              brokers <- uniq_list_of(broker(), min_length: 1, max_length: 8),
              rf <- integer(1..6),
              segment_id <- segment_id()
            ) do
        {:ok, before} = Placement.place(segment_id, brokers, rf)

        # a broker id outside the current set: broker() draws 1..100, so one past the max is always new
        new = Enum.max(brokers) + 1
        {:ok, after_add} = Placement.place(segment_id, [new | brokers], rf)

        # HRW only lets the new broker bump an existing replica out; it never reshuffles the survivors
        assert after_add -- before == [] or after_add -- before == [new]
        assert length(before -- after_add) <= 1
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

  describe "place/4, spread (rack-aware)" do
    # rack of each broker; brokers a1/a2 share rack "a"
    @attrs %{a1: %{"rack" => "a"}, a2: %{"rack" => "a"}, b1: %{"rack" => "b"}, c1: %{"rack" => "c"}}

    defp racks(replicas), do: Enum.map(replicas, fn broker -> @attrs[broker]["rack"] end)

    test "places each replica in a distinct value when rf <= number of values" do
      {:ok, replicas} = Placement.place("seg", [:a1, :a2, :b1, :c1], 3, spread: {"rack", @attrs})

      assert length(replicas) == 3
      assert Enum.sort(racks(replicas)) == ["a", "b", "c"]
    end

    test "prioritizes diversity over pure rank: rf=2 across racks a,a,b picks a and b" do
      {:ok, replicas} = Placement.place("seg", [:a1, :a2, :b1], 2, spread: {"rack", @attrs})

      # without spread the top two could both be in rack "a"; spread guarantees two racks
      assert Enum.sort(racks(replicas)) == ["a", "b"]
    end

    test "is best-effort round-robin when rf exceeds the number of values" do
      {:ok, replicas} = Placement.place("seg", [:a1, :a2, :b1, :c1], 4, spread: {"rack", @attrs})

      assert Enum.sort(replicas) == [:a1, :a2, :b1, :c1]
      assert Enum.frequencies(racks(replicas)) == %{"a" => 2, "b" => 1, "c" => 1}
    end

    test "brokers missing the attribute form their own group and are still placed" do
      attrs = %{a1: %{"rack" => "a"}}
      {:ok, replicas} = Placement.place("seg", [:a1, :x1], 2, spread: {"rack", attrs})

      assert Enum.sort(replicas) == [:a1, :x1]
    end

    test "is deterministic" do
      opts = [spread: {"rack", @attrs}]

      assert Placement.place("seg", [:a1, :a2, :b1, :c1], 3, opts) ==
               Placement.place("seg", [:a1, :a2, :b1, :c1], 3, opts)
    end
  end

  describe "place/4, min_domains + policy (hardening)" do
    # a1/a2 in rack "a", b1 in rack "b", only two racks
    @dattrs %{a1: %{"rack" => "a"}, a2: %{"rack" => "a"}, b1: %{"rack" => "b"}}

    test "soft (default) places best-effort even when it cannot meet min_domains" do
      opts = [spread: {"rack", @dattrs}, min_domains: 3]
      assert {:ok, replicas} = Placement.place("seg", [:a1, :a2, :b1], 3, opts)
      assert Enum.sort(replicas) == [:a1, :a2, :b1]
    end

    test "hard passes when the replica set covers enough distinct domains" do
      opts = [spread: {"rack", @dattrs}, min_domains: 2, policy: :hard]
      assert {:ok, replicas} = Placement.place("seg", [:a1, :a2, :b1], 2, opts)
      assert Enum.sort(Enum.map(replicas, &@dattrs[&1]["rack"])) == ["a", "b"]
    end

    test "hard rejects when too few domains are reachable" do
      opts = [spread: {"rack", @dattrs}, min_domains: 3, policy: :hard]
      assert {:error, {:insufficient_domains, 2, 3}} = Placement.place("seg", [:a1, :a2, :b1], 3, opts)
    end

    test "hard rejects when all replicas lack the attribute (single nil domain)" do
      opts = [spread: {"rack", %{}}, min_domains: 2, policy: :hard]
      assert {:error, {:insufficient_domains, 1, 2}} = Placement.place("seg", [:a1, :a2], 2, opts)
    end

    test "without :spread, distinct brokers are the domains" do
      assert {:ok, _} = Placement.place("seg", [:a, :b, :c], 3, min_domains: 3, policy: :hard)

      assert {:error, {:insufficient_domains, 2, 3}} =
               Placement.place("seg", [:a, :b], 3, min_domains: 3, policy: :hard)
    end
  end

  describe "domain_violations/4" do
    @dattrs2 %{a1: %{"rack" => "a"}, a2: %{"rack" => "a"}, b1: %{"rack" => "b"}, c1: %{"rack" => "c"}}

    test "flags segments spanning fewer than min_domains distinct domains" do
      metadata = with_segments([{"diverse", [:a1, :b1, :c1]}, {"concentrated", [:a1, :a2]}], rf: 3)

      assert Placement.domain_violations(metadata, "rack", @dattrs2, 2) == ["concentrated"]
    end

    test "empty when every segment meets the target" do
      metadata = with_segments([{"s1", [:a1, :b1]}, {"s2", [:b1, :c1]}], rf: 2)

      assert Placement.domain_violations(metadata, "rack", @dattrs2, 2) == []
    end

    test "a higher target flags more segments and results are sorted" do
      metadata = with_segments([{"z", [:a1, :b1]}, {"a", [:a1, :b1, :c1]}], rf: 3)

      assert Placement.domain_violations(metadata, "rack", @dattrs2, 3) == ["z"]
    end
  end

  describe "place_balanced/4, global load balancing" do
    test "empty brokers give each item an empty replica set" do
      assert Placement.place_balanced([:x, :y], [], 2) == [{:x, []}, {:y, []}]
    end

    test "fewer brokers than rf degrades to all brokers per item" do
      for {_item, replicas} <- Placement.place_balanced([:x, :y], [:a, :b], 3) do
        assert Enum.sort(replicas) == [:a, :b]
      end
    end

    test "is deterministic" do
      run = fn -> Placement.place_balanced(Enum.to_list(1..12), [:a, :b, :c, :d], 2, 1) end
      assert run.() == run.()
    end

    test "with generous slack it matches plain top-rf HRW (no capacity pressure)" do
      items = Enum.to_list(1..8)
      brokers = [:a, :b, :c, :d]

      for {item, replicas} <- Placement.place_balanced(items, brokers, 2, 1_000) do
        {:ok, hrw} = Placement.place(item, brokers, 2)
        assert replicas == hrw, "with a huge max_skew, no broker is capped, so it is plain HRW"
      end
    end

    test "balances load that plain HRW would concentrate" do
      items = Enum.to_list(1..9)
      brokers = [:a, :b, :c]

      hrw_load =
        for(item <- items, {:ok, [broker]} <- [Placement.place(item, brokers, 1)], do: broker)
        |> Enum.frequencies()
        |> Map.values()

      balanced_load =
        Placement.place_balanced(items, brokers, 1, 1)
        |> Enum.flat_map(fn {_item, replicas} -> replicas end)
        |> Enum.frequencies()
        |> Map.values()

      # plain HRW piles most of the 9 items onto one broker; balancing spreads them evenly (3/3/3)
      assert Enum.max(hrw_load) > 3
      assert Enum.max(balanced_load) == 3 and Enum.min(balanced_load) == 3
    end

    property "every item gets min(rf, brokers) distinct replicas (rf is never sacrificed)" do
      check all(
              item_count <- integer(1..40),
              broker_count <- integer(1..6),
              rf <- integer(1..6),
              max_skew <- integer(1..3)
            ) do
        brokers = Enum.map(1..broker_count, &:"b#{&1}")
        target = min(rf, broker_count)

        for {_item, replicas} <-
              Placement.place_balanced(Enum.to_list(1..item_count), brokers, rf, max_skew) do
          assert length(Enum.uniq(replicas)) == target
          assert Enum.all?(replicas, &(&1 in brokers))
        end
      end
    end

    property "with rf 1 the load cap ceil(items/brokers) + max_skew - 1 is a hard bound" do
      check all(
              item_count <- integer(1..40),
              broker_count <- integer(1..6),
              max_skew <- integer(1..3)
            ) do
        brokers = Enum.map(1..broker_count, &:"b#{&1}")
        cap = div(item_count + broker_count - 1, broker_count) + max_skew - 1

        loads =
          Placement.place_balanced(Enum.to_list(1..item_count), brokers, 1, max_skew)
          |> Enum.flat_map(fn {_item, replicas} -> replicas end)
          |> Enum.frequencies()
          |> Map.values()

        assert Enum.max(loads) <= cap
      end
    end
  end

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
