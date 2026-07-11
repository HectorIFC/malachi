defmodule Malachi.BrokerTest do
  use ExUnit.Case, async: true

  alias Malachi.Broker
  alias Malachi.Cluster.DSRSM
  alias Malachi.Cluster.Placement
  alias Malachi.Log.Record
  alias Malachi.Metadata
  alias Malachi.Test.FakeSegmentStore

  setup do
    {:ok, store} = FakeSegmentStore.start_link()
    %{store: store}
  end

  defp record(value, key), do: Record.new(value, key: key)

  defp replicate_fun(store),
    do: fn ref, seg, rs, base, recs -> FakeSegmentStore.replicate(store, ref, seg, rs, base, recs) end

  defp read_fun(store), do: fn ref, seg, offset, max -> FakeSegmentStore.read(store, ref, seg, offset, max) end

  defp open_broker(opts \\ []), do: elem(Broker.open(Keyword.put_new(opts, :brokers, [:primary])), 1)

  defp broker_with_topic(name \\ "events", bits \\ 4, opts \\ []) do
    {broker, {:ok, root_id}} = Broker.create_topic(open_broker(opts), name, bits)
    {broker, root_id}
  end

  defp produce(broker, store, topic, records), do: Broker.produce(broker, topic, records, replicate_fun(store))

  defp read_all(broker, store, range_id), do: read_all(broker, store, range_id, 0, [])

  defp read_all(broker, store, range_id, offset, accumulated) do
    case Broker.read(broker, range_id, offset, 100, read_fun(store)) do
      :eof -> accumulated |> Enum.reverse() |> List.flatten()
      {:ok, records} -> read_all(broker, store, range_id, offset + length(records), [records | accumulated])
    end
  end

  # Pages `read_consume` from `cursor` until it pauses (an empty page = caught up), returning the
  # accumulated records and the paused cursor (which can be passed back later to tail new records).
  defp consume(broker, store, range_id, cursor), do: consume(broker, store, range_id, cursor, [])

  defp consume(broker, store, range_id, cursor, accumulated) do
    case Broker.read_consume(broker, range_id, cursor, 100, read_fun(store)) do
      {:ok, [], next} -> {accumulated |> Enum.reverse() |> List.flatten(), next}
      {:ok, records, next} -> consume(broker, store, range_id, next, [records | accumulated])
    end
  end

  defp segments(broker, range_id) do
    broker |> Broker.metadata() |> Metadata.segments_of_range(range_id) |> Enum.sort_by(& &1.start_offset)
  end

  describe "create_topic / produce / read" do
    test "produces records and reads them back from the owning range", %{store: store} do
      {broker, root_id} = broker_with_topic()

      records = for index <- 0..9, do: record("v#{index}", "k#{index}")
      {broker, {:ok, placements}} = produce(broker, store, "events", records)

      # single range, so all records land in the root range with contiguous offsets
      assert placements == %{root_id => {0, 9}}
      assert broker |> read_all(store, root_id) |> Enum.map(& &1.value) == Enum.map(records, & &1.value)
    end

    test "an empty produce is a no-op", %{store: store} do
      {broker, _root_id} = broker_with_topic()
      assert {_broker, {:ok, placements}} = produce(broker, store, "events", [])
      assert placements == %{}
    end

    test "producing to an unknown topic fails", %{store: store} do
      assert {_broker, {:error, :no_such_topic}} = produce(open_broker(), store, "nope", [record("a", "k")])
    end

    test "reading a range with nothing produced is eof", %{store: store} do
      {broker, root_id} = broker_with_topic()
      assert Broker.read(broker, root_id, 0, 10, read_fun(store)) == :eof
    end
  end

  describe "min_domains hard policy (failure-domain hardening)" do
    # three brokers over only two racks (a, b)
    @racks %{a1: %{"rack" => "a"}, a2: %{"rack" => "a"}, b1: %{"rack" => "b"}}

    defp hardening_opts(min_domains, policy) do
      [
        brokers: [:a1, :a2, :b1],
        replication_factor: 3,
        spread_by: "rack",
        broker_attributes: @racks,
        min_domains: min_domains,
        placement_policy: policy
      ]
    end

    test "hard policy fails the produce when the replica set cannot span min_domains", %{store: store} do
      {broker, _root} = broker_with_topic("events", 4, hardening_opts(3, :hard))

      assert {_broker, {:error, {:insufficient_domains, 2, 3}}} =
               produce(broker, store, "events", [record("v", "k")])
    end

    test "soft policy places best-effort despite too few domains", %{store: store} do
      {broker, root_id} = broker_with_topic("events", 4, hardening_opts(3, :soft))

      assert {_broker, {:ok, %{^root_id => {0, 0}}}} = produce(broker, store, "events", [record("v", "k")])
    end

    test "hard policy succeeds when enough domains are reachable", %{store: store} do
      {broker, root_id} = broker_with_topic("events", 4, hardening_opts(2, :hard))

      assert {_broker, {:ok, %{^root_id => {0, 0}}}} = produce(broker, store, "events", [record("v", "k")])
    end

    test "domain_violations reports a soft-policy segment below the target", %{store: store} do
      {broker, _root} = broker_with_topic("events", 4, hardening_opts(3, :soft))
      {broker, {:ok, _}} = produce(broker, store, "events", [record("v", "k")])

      # the segment spans only two racks (a, b) but min_domains is 3 → one violation for the topic
      assert Broker.domain_violations(broker) == %{"events" => 1}
    end

    test "domain_violations is empty when the segment meets the target", %{store: store} do
      {broker, _root} = broker_with_topic("events", 4, hardening_opts(2, :soft))
      {broker, {:ok, _}} = produce(broker, store, "events", [record("v", "k")])

      assert Broker.domain_violations(broker) == %{}
    end

    test "domain_violations is empty when spread/min_domains are unconfigured" do
      {broker, _root} = broker_with_topic()
      assert Broker.domain_violations(broker) == %{}
    end
  end

  describe "split routes records to children (control plane drives data plane)" do
    test "after a split, records route to the correct child range", %{store: store} do
      {broker, root_id} = broker_with_topic()
      {broker, {:ok, left_id, right_id}} = Broker.split_range(broker, root_id)

      records = for index <- 0..29, do: record("v#{index}", "k#{index}")
      {broker, {:ok, _placements}} = produce(broker, store, "events", records)

      left_values = read_all(broker, store, left_id) |> Enum.map(& &1.value)
      right_values = read_all(broker, store, right_id) |> Enum.map(& &1.value)

      # every record landed in exactly one child; together they reconstruct the input
      assert Enum.sort(left_values ++ right_values) == Enum.sort(Enum.map(records, & &1.value))
      refute left_values == []
      refute right_values == []
    end

    test "split is logical: the sealed parent keeps its records", %{store: store} do
      {broker, root_id} = broker_with_topic()

      records = for index <- 0..4, do: record("v#{index}", "k#{index}")
      {broker, {:ok, _placements}} = produce(broker, store, "events", records)
      {broker, {:ok, left_id, right_id}} = Broker.split_range(broker, root_id)

      assert broker |> read_all(store, root_id) |> Enum.map(& &1.value) == Enum.map(records, & &1.value)
      assert Broker.read(broker, left_id, 0, 10, read_fun(store)) == :eof
      assert Broker.read(broker, right_id, 0, 10, read_fun(store)) == :eof
    end

    test "split errors propagate from the control plane" do
      {broker, root_id} = broker_with_topic()
      {broker, {:ok, _left, _right}} = Broker.split_range(broker, root_id)
      # root is now sealed in the metadata
      assert {_broker, {:error, :sealed}} = Broker.split_range(broker, root_id)
    end
  end

  describe "cross-epoch history" do
    test "a child's history is the parent's slice then the child's own records", %{store: store} do
      {broker, root_id} = broker_with_topic()

      parent_records = for index <- 0..19, do: record("v#{index}", "k#{index}")
      {broker, {:ok, _placements}} = produce(broker, store, "events", parent_records)
      {broker, {:ok, left_id, right_id}} = Broker.split_range(broker, root_id)

      child_records = for index <- 20..39, do: record("v#{index}", "k#{index}")
      {broker, {:ok, _placements}} = produce(broker, store, "events", child_records)

      {:ok, left_history} = Broker.read_history(broker, left_id, read_fun(store))
      {:ok, right_history} = Broker.read_history(broker, right_id, read_fun(store))

      # together the children histories reconstruct every record exactly once
      all_values = Enum.map(left_history ++ right_history, & &1.value)
      assert Enum.sort(all_values) == Enum.sort(Enum.map(parent_records ++ child_records, & &1.value))

      # happens-before: in each history, parent-epoch records precede child-epoch ones
      parent_values = MapSet.new(Enum.map(parent_records, & &1.value))

      for history <- [left_history, right_history] do
        origins = Enum.map(history, &if(MapSet.member?(parent_values, &1.value), do: :parent, else: :child))
        {_parents, rest} = Enum.split_while(origins, &(&1 == :parent))
        assert Enum.all?(rest, &(&1 == :child)), "a child-epoch record appeared before a parent one"
      end
    end

    test "read_history of an unknown range fails", %{store: store} do
      {broker, _root_id} = broker_with_topic()
      assert Broker.read_history(broker, {"events", 999}, read_fun(store)) == {:error, :no_such_range}
    end
  end

  describe "cross-epoch live consume (read_consume)" do
    test "delivers pre-split records via the active children, exactly once (no loss)", %{store: store} do
      {broker, root_id} = broker_with_topic()

      # produced before the split — these live in the parent's segments, which leave active_range_ids
      parent_records = for index <- 0..19, do: record("v#{index}", "k#{index}")
      {broker, {:ok, _placements}} = produce(broker, store, "events", parent_records)
      {broker, {:ok, left_id, right_id}} = Broker.split_range(broker, root_id)

      child_records = for index <- 20..39, do: record("v#{index}", "k#{index}")
      {broker, {:ok, _placements}} = produce(broker, store, "events", child_records)

      # consuming the two active children from :start reconstructs every record (pre- and post-split)
      {left, _left_cursor} = consume(broker, store, left_id, :start)
      {right, _right_cursor} = consume(broker, store, right_id, :start)
      delivered = Enum.map(left ++ right, & &1.value)
      assert Enum.sort(delivered) == Enum.sort(Enum.map(parent_records ++ child_records, & &1.value))
    end

    test "tails the active range: records produced after catching up are delivered later", %{store: store} do
      {broker, root_id} = broker_with_topic()
      {broker, {:ok, _placements}} = produce(broker, store, "events", [record("a", "k1"), record("b", "k2")])

      {first, cursor} = consume(broker, store, root_id, :start)
      assert first |> Enum.map(& &1.value) |> Enum.sort() == ["a", "b"]

      # caught up: resuming from the paused cursor yields nothing and keeps the same cursor
      assert {:ok, [], ^cursor} = Broker.read_consume(broker, root_id, cursor, 100, read_fun(store))

      # a record produced after the pause is delivered when resuming from that same cursor
      {broker, {:ok, _placements}} = produce(broker, store, "events", [record("c", "k3")])
      {more, _cursor} = consume(broker, store, root_id, cursor)
      assert Enum.map(more, & &1.value) == ["c"]
    end

    test "read_consume of an unknown range fails", %{store: store} do
      {broker, _root_id} = broker_with_topic()
      assert Broker.read_consume(broker, {"events", 999}, :start, 100, read_fun(store)) == {:error, :no_such_range}
    end

    test "read_consume with a source_index past the end pauses instead of crashing", %{store: store} do
      {broker, root_id} = broker_with_topic()
      {broker, {:ok, _placements}} = produce(broker, store, "events", [record("a", "k1")])

      # a forged/stale cursor pointing past the range's sources has nothing to read — pause, no crash
      assert {:ok, [], {9999, 0}} = Broker.read_consume(broker, root_id, {9999, 0}, 100, read_fun(store))
    end
  end

  describe "merge" do
    test "merges buddy ranges back into one active child", %{store: store} do
      {broker, root_id} = broker_with_topic()
      {broker, {:ok, left_id, right_id}} = Broker.split_range(broker, root_id)
      {broker, {:ok, child_id}} = Broker.merge_ranges(broker, left_id, right_id)

      records = for index <- 0..9, do: record("v#{index}", "k#{index}")
      {_broker, {:ok, placements}} = produce(broker, store, "events", records)

      # the merged child covers the whole keyspace again, so all records route to it
      assert Map.keys(placements) == [child_id]
    end

    test "merge seals both parents' active segments", %{store: store} do
      {broker, root_id} = broker_with_topic()
      {broker, {:ok, left_id, right_id}} = Broker.split_range(broker, root_id)

      records = for index <- 0..29, do: record("v#{index}", "k#{index}")
      {broker, {:ok, _placements}} = produce(broker, store, "events", records)
      {broker, {:ok, _child_id}} = Broker.merge_ranges(broker, left_id, right_id)

      for parent_id <- [left_id, right_id] do
        assert Enum.all?(segments(broker, parent_id), &(&1.state == :sealed))
      end
    end
  end

  describe "segments (data-plane lifecycle)" do
    test "the first produce registers an active segment with a placed replica set", %{store: store} do
      {broker, root_id} = broker_with_topic()
      {broker, {:ok, _placements}} = produce(broker, store, "events", [record("v", "k")])

      assert [segment] = segments(broker, root_id)
      assert segment.id == {root_id, 0}
      assert segment.state == :active
      assert segment.start_offset == 0
      assert segment.replica_set == [:primary]
    end

    test "the replica set comes from Placement over the configured brokers", %{store: store} do
      brokers = [:a, :b, :c, :d]
      {broker, root_id} = broker_with_topic("events", 4, brokers: brokers, replication_factor: 3)
      {broker, {:ok, _placements}} = produce(broker, store, "events", [record("v", "k")])

      assert [segment] = segments(broker, root_id)
      assert {:ok, segment.replica_set} == Placement.place(segment.id, brokers, 3)
      assert length(segment.replica_set) == 3
    end

    test "the active segment seals and rolls once it crosses :segment_max_bytes", %{store: store} do
      one_record = Record.encoded_size(record("value", "key"))
      {broker, root_id} = broker_with_topic("events", 4, segment_max_bytes: one_record)

      broker =
        Enum.reduce(0..2, broker, fn index, broker ->
          {broker, {:ok, _placements}} = produce(broker, store, "events", [record("value", "key#{index}")])
          broker
        end)

      segs = segments(broker, root_id)
      # three rolled segments, contiguous, each holding exactly one record
      assert Enum.map(segs, & &1.id) == [{root_id, 0}, {root_id, 1}, {root_id, 2}]
      assert Enum.map(segs, & &1.start_offset) == [0, 1, 2]
      assert Enum.all?(segs, &(&1.state == :sealed and &1.length == 1))

      # and the records read back contiguously across the rolled segments
      assert read_all(broker, store, root_id) |> Enum.map(& &1.value) == ["value", "value", "value"]
    end

    test "a consumer below the earliest available offset skips retention-expired data", %{store: store} do
      one_record = Record.encoded_size(record("value", "key"))
      {broker, root_id} = broker_with_topic("events", 4, segment_max_bytes: one_record)

      # five records, each rolling into its own sealed segment (offsets 0..4)
      broker =
        Enum.reduce(0..4, broker, fn index, broker ->
          {broker, {:ok, _placements}} = produce(broker, store, "events", [record("v#{index}", "k#{index}")])
          broker
        end)

      # retention expires the two oldest segments (control plane drops them)
      [s0, s1 | _] = segments(broker, root_id) |> Enum.sort_by(& &1.start_offset)

      {broker, :ok} = Broker.delete_segment(broker, s0.id)
      {broker, :ok} = Broker.delete_segment(broker, s1.id)

      # a consumer starting at the beginning advances to the earliest data still stored
      earliest = segments(broker, root_id) |> Enum.map(& &1.start_offset) |> Enum.min()
      {records, _cursor} = consume(broker, store, root_id, :start)
      assert Enum.map(records, & &1.value) == Enum.map(earliest..4, &"v#{&1}")
    end

    test "the byte threshold is soft: a batch may overshoot before sealing", %{store: store} do
      one_record = Record.encoded_size(record("value", "key"))
      {broker, root_id} = broker_with_topic("events", 4, segment_max_bytes: one_record)

      records = for index <- 0..2, do: record("value", "key#{index}")
      {broker, {:ok, _placements}} = produce(broker, store, "events", records)

      assert [segment] = segments(broker, root_id)
      assert segment.state == :sealed
      assert segment.length == 3
    end

    test "splitting seals the parent's active segment", %{store: store} do
      {broker, root_id} = broker_with_topic()
      {broker, {:ok, _placements}} = produce(broker, store, "events", [record("v", "k")])

      assert [%{state: :active}] = segments(broker, root_id)
      {broker, {:ok, _left, _right}} = Broker.split_range(broker, root_id)
      assert [%{state: :sealed, length: 1}] = segments(broker, root_id)
    end

    test "apply_heal updates both the metadata and the active segment's cached replica set", %{store: store} do
      {broker, root_id} = broker_with_topic("events", 4, brokers: [:a, :b, :c], replication_factor: 3)
      {broker, {:ok, _placements}} = produce(broker, store, "events", [record("v", "k")])

      [segment] = segments(broker, root_id)
      new_set = [:b, :a, :c]
      broker = Broker.apply_heal(broker, [{:set_segment_replicas, segment.id, new_set}])

      assert Metadata.get_segment(Broker.metadata(broker), segment.id).replica_set == new_set
      assert broker.segments[root_id].replica_set == new_set
    end

    test "a failed register command aborts the produce instead of crashing", %{store: store} do
      # a command_fun that fails register_segment (as a Raft timeout would), passing others through
      failing = fn
        dsrsm, _topic, {:register_segment, _range, _seg, _replicas, _offset} -> {dsrsm, {:error, :ra_down}}
        dsrsm, topic, command -> DSRSM.command(dsrsm, topic, command)
      end

      {:ok, broker} = Broker.open(brokers: [:a], command_fun: failing)
      {broker, {:ok, root_id}} = Broker.create_topic(broker, "events", 4)

      assert {broker, {:error, :ra_down}} = produce(broker, store, "events", [record("v", "k")])

      # nothing was opened: no segment registered, no offset advanced
      assert segments(broker, root_id) == []
      assert broker.offsets == %{}
    end

    test "open/1 rejects an invalid placement policy" do
      assert_raise ArgumentError, fn -> Broker.open(brokers: []) end
      assert_raise ArgumentError, fn -> Broker.open(brokers: :not_a_list) end
      assert_raise ArgumentError, fn -> Broker.open(brokers: [:a], replication_factor: 0) end
      assert_raise ArgumentError, fn -> Broker.open(brokers: [:a], segment_max_bytes: 0) end
    end
  end

  describe "rack-aware placement (spread_by + broker_attributes)" do
    @attrs %{a1: %{"rack" => "a"}, a2: %{"rack" => "a"}, b1: %{"rack" => "b"}, c1: %{"rack" => "c"}}

    test "new segments spread replicas across racks", %{store: store} do
      {broker, root_id} =
        broker_with_topic("events", 4,
          brokers: [:a1, :a2, :b1, :c1],
          replication_factor: 3,
          spread_by: "rack",
          broker_attributes: @attrs
        )

      {broker, {:ok, _placements}} = produce(broker, store, "events", [record("v", "k")])

      [segment] = segments(broker, root_id)
      racks = Enum.map(segment.replica_set, fn broker -> @attrs[broker]["rack"] end)
      assert Enum.sort(racks) == ["a", "b", "c"]
    end

    test "set_broker_attributes updates the attributes used for the next placement", %{store: store} do
      # start with no attributes: placement can't spread (everything is one nil group)
      {broker, _root} =
        broker_with_topic("events", 4, brokers: [:a1, :a2, :b1, :c1], replication_factor: 3, spread_by: "rack")

      broker = Broker.set_broker_attributes(broker, @attrs)
      {broker, {:ok, _placements}} = produce(broker, store, "events", [record("v", "k")])

      [segment] = segments(broker, "events" |> then(&{&1, 0}))
      racks = Enum.map(segment.replica_set, fn broker -> @attrs[broker]["rack"] end)
      assert Enum.sort(racks) == ["a", "b", "c"]
    end
  end

  describe "per-topic placement policy (spread_by override)" do
    @brokers [:a1, :a2, :b1, :c1]

    # Seeds a broker holding topic "events" governed by a policy `policy`, opened with `opts`.
    defp broker_with_policy(policy, opts) do
      metadata =
        [{:create_topic, "events", 4}, {:define_policy, "p", policy}, {:set_topic_policy, "events", "p"}]
        |> Enum.reduce(Metadata.new(), fn command, metadata -> elem(Metadata.apply(metadata, command), 0) end)

      {open_broker(Keyword.put(opts, :dsrsm, DSRSM.single(metadata))), {"events", 0}}
    end

    test "a topic's policy spread_by turns spreading on over a global-off", %{store: store} do
      # global spread_by is nil (off); the topic's policy spreads over "rack"
      {broker, root_id} =
        broker_with_policy(%{spread_by: "rack"},
          brokers: @brokers,
          replication_factor: 3,
          broker_attributes: @attrs
        )

      {broker, {:ok, _placements}} = produce(broker, store, "events", [record("v", "k")])

      [segment] = segments(broker, root_id)
      racks = Enum.map(segment.replica_set, fn broker -> @attrs[broker]["rack"] end)
      assert Enum.sort(racks) == ["a", "b", "c"]
    end

    test "a policy spread_by: nil opts the topic out, overriding the global spread", %{store: store} do
      {broker, root_id} =
        broker_with_policy(%{spread_by: nil},
          brokers: @brokers,
          replication_factor: 3,
          spread_by: "rack",
          broker_attributes: @attrs
        )

      {broker, {:ok, _placements}} = produce(broker, store, "events", [record("v", "k")])

      # opted out => plain rendezvous ranking (no spread), ignoring the broker attributes
      [segment] = segments(broker, root_id)
      assert {:ok, segment.replica_set} == Placement.place(segment.id, @brokers, 3)
    end
  end
end
