defmodule Malachi.BrokerTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

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

  # Produce the way `Malachi.BrokerServer` does: route and replicate, then settle whatever rolls the
  # batch requested. The broker only ASKS for a roll; fencing the store and recording what it answered
  # is the caller's half, so a test that stops at `produce_only/4` sees a segment that is still active.
  defp produce(broker, store, topic, records) do
    case produce_only(broker, store, topic, records) do
      {broker, {:ok, _placements} = reply} -> {settle(broker, store), reply}
      {broker, error} -> {broker, error}
    end
  end

  defp produce_only(broker, store, topic, records),
    do: Broker.produce(broker, topic, records, replicate_fun(store))

  # Fences every owed roll through the fake store and records what it answered, which is exactly what
  # `Malachi.BrokerServer.settle_rolls/1` does with the real replication server.
  defp settle(broker, store) do
    Enum.reduce(Broker.due_rolls(broker), broker, fn roll, acc -> fence_and_record(acc, store, roll) end)
  end

  # Fences a range's active segment on demand (the split/merge path), or does nothing when the range
  # has no open segment.
  defp seal_active(broker, store, range_id) do
    case Broker.active_roll(broker, range_id) do
      :none -> broker
      roll -> fence_and_record(broker, store, roll)
    end
  end

  defp fence_and_record(broker, store, roll) do
    {:ok, end_offset, bytes} = FakeSegmentStore.seal(store, roll.primary, roll.segment_id, roll.start_offset)
    {broker, :ok} = Broker.record_seal(broker, roll, end_offset, bytes, 1_000)
    broker
  end

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

  # A range whose seq 0 is sealed at length 3 (v0..v2) with an active seq 1 at offset 3 (v3, v4), and
  # whose seq 0 STORE holds three more records (s3..s5) at offsets 3..5, above the sealed length. That
  # is the log an UNFENCED replica leaves behind: the fence never reached it, so its copy grew past the
  # offset where seq 1 was opened, and offsets 3..5 now exist in two segments. The surplus is appended
  # straight to the store, bypassing both the broker and the fence, because that is precisely the copy
  # it stands for; the metadata still says length 3.
  defp sealed_with_surplus(store) do
    one_record = Record.encoded_size(record("v0", "k0"))
    {broker, root_id} = broker_with_topic("events", 4, segment_max_bytes: 3 * one_record)

    {broker, {:ok, _placements}} = produce(broker, store, "events", for(i <- 0..2, do: record("v#{i}", "k#{i}")))
    {broker, {:ok, _placements}} = produce(broker, store, "events", for(i <- 3..4, do: record("v#{i}", "k#{i}")))

    [sealed, active] = segments(broker, root_id)
    assert %{id: {^root_id, 0}, state: :sealed, start_offset: 0, length: 3} = sealed
    assert %{id: {^root_id, 1}, state: :active, start_offset: 3, length: nil} = active

    # `force_replicate`, not `replicate`: the fence refuses seq 0 now, so the only way a store can carry
    # a surplus is a replica the fence never reached. That is the copy this fixture stands for.
    surplus = for i <- 3..5, do: record("s#{i}", "k#{i}")
    assert {:ok, 5} = FakeSegmentStore.force_replicate(store, :primary, sealed.id, 0, surplus)

    # The store really does hold the surplus at the offsets seq 1 owns; without this the tests below
    # could pass against a fixture that never reproduced the overlap.
    assert {:ok, held} = FakeSegmentStore.read(store, :primary, sealed.id, 3, 100)
    assert Enum.map(held, &{&1.offset, &1.value}) == [{3, "s3"}, {4, "s4"}, {5, "s5"}]

    {broker, root_id, sealed, active}
  end

  # A range whose single segment was ADOPTED partway through its life and then sealed: three records
  # go in, the frontend's segment cache is dropped (%{broker | segments: %{}}, the shape a restart or
  # a second frontend leaves: the segment lives in the shared metadata, this frontend has never seen
  # it), and five more records take it over the byte threshold. The seal therefore happens on a
  # segment this frontend adopted, holding eight records of which it produced only five.
  defp sealed_after_adoption(store) do
    one_record = Record.encoded_size(record("v0", "k0"))
    {broker, root_id} = broker_with_topic("events", 4, segment_max_bytes: 5 * one_record)

    {broker, {:ok, _placements}} = produce(broker, store, "events", for(i <- 0..2, do: record("v#{i}", "k#{i}")))
    assert [%{id: {^root_id, 0}, state: :active, start_offset: 0}] = segments(broker, root_id)

    broker = %{broker | segments: %{}}
    {broker, {:ok, _placements}} = produce(broker, store, "events", for(i <- 3..7, do: record("v#{i}", "k#{i}")))

    [sealed] = segments(broker, root_id)
    {broker, root_id, sealed}
  end

  # A range of four one-record segments at offsets 0..3 whose MIDDLE one has been deleted, leaving
  # starts [0, 2, 3] with a hole at 1. That is a state registration cannot produce (segments must tile
  # the offsets) but that appears afterwards: retention expires by `sealed_at`, so clocks that disagree
  # across nodes can drop a middle segment while its neighbours survive, and `delete_segment/2` is
  # exposed to operators. Shared by the read, consume and history tests rather than rebuilt in each,
  # so a change to how segments roll is edited once.
  defp range_with_hole(store) do
    # Measured from the record shape produced here, so each record fills a segment and rolls the next:
    # a threshold taken from a larger record would quietly never be crossed.
    one_record = Record.encoded_size(record("v0", "k0"))
    {broker, root_id} = broker_with_topic("events", 4, segment_max_bytes: one_record)

    broker =
      Enum.reduce(0..3, broker, fn index, broker ->
        {broker, {:ok, _placements}} = produce(broker, store, "events", [record("v#{index}", "k#{index}")])
        broker
      end)

    [_s0, s1 | _] = segments(broker, root_id)
    {broker, :ok} = Broker.delete_segment(broker, s1.id)
    assert Enum.map(segments(broker, root_id), & &1.start_offset) == [0, 2, 3]

    {broker, root_id}
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
    test "a hole is stepped over once, not re-delivered on every page", %{store: store} do
      # The symmetry `consume_page/8` got and `read_history_page/6` did not. Since `read/5` steps over a
      # hole, a read can start ABOVE the offset asked for, and a cursor advanced by "where I asked plus
      # how many came back" lands back inside the hole, so the same page is served again and again. The
      # consume path was fixed for this; the history path kept the old arithmetic and duplicated.
      {broker, root_id} = range_with_hole(store)

      assert {:ok, values} = Broker.read_history(broker, root_id, read_fun(store))
      assert Enum.map(values, & &1.value) == ["v0", "v2", "v3"]
    end

    test "an empty page ends the source instead of asking the same question forever", %{store: store} do
      # A read that answers `{:ok, []}` used to match the records clause, which returned the cursor
      # unchanged, so `read_history/3` looped on it with no way out: a hang rather than a wrong answer.
      # `Malachi.Storage.ElixirStore.read/3` makes no non-empty guarantee and `read_fun` is caller
      # supplied, so this is improbable rather than impossible, and a hang is the worst shape to debug.
      {broker, root_id} = range_with_hole(store)
      empty = fn _ref, _segment, _offset, _max -> {:ok, []} end

      assert Broker.stream_history(broker, root_id, :start, 100, empty) == {:ok, [], :done}
      assert Broker.read_history(broker, root_id, empty) == {:ok, []}
    end

    test "a read error is surfaced, never taken for the end of a source", %{store: store} do
      # The clause order this depends on: the error tuple must be matched before the catch-all that
      # treats anything else as a drained source, or a failing replica reads as an empty history.
      {broker, root_id} = range_with_hole(store)
      failing = fn _ref, _segment, _offset, _max -> {:error, :unreachable} end

      assert Broker.stream_history(broker, root_id, :start, 100, failing) == {:error, :unreachable}
    end

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

      # produced before the split: these live in the parent's segments, which leave active_range_ids
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

      # a forged/stale cursor pointing past the range's sources has nothing to read, pause, no crash
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

    test "merge is metadata-only: the caller fences both parents first", %{store: store} do
      {broker, root_id} = broker_with_topic()
      {broker, {:ok, left_id, right_id}} = Broker.split_range(broker, root_id)

      records = for index <- 0..29, do: record("v#{index}", "k#{index}")
      {broker, {:ok, _placements}} = produce(broker, store, "events", records)

      # The merge itself seals no segment: both parents are still open right up to the fence.
      {metadata_only, {:ok, _child_id}} = Broker.merge_ranges(broker, left_id, right_id)

      for parent_id <- [left_id, right_id] do
        assert Enum.any?(segments(metadata_only, parent_id), &(&1.state == :active))
      end

      broker = Enum.reduce([left_id, right_id], broker, &seal_active(&2, store, &1))
      {broker, {:ok, _child_id}} = Broker.merge_ranges(broker, left_id, right_id)

      for parent_id <- [left_id, right_id] do
        assert Enum.all?(segments(broker, parent_id), &(&1.state == :sealed))
      end
    end
  end

  describe "rolls and fenced seals" do
    # A command_fun that records every command it sees (and applies it), so a test can assert that a
    # threshold crossing emits NOTHING to the control plane.
    defp recording_command_fun(owner) do
      fn dsrsm, topic, command ->
        send(owner, {:command, command})
        DSRSM.command(dsrsm, topic, command)
      end
    end

    defp commands_seen do
      receive do
        {:command, command} -> [command | commands_seen()]
      after
        0 -> []
      end
    end

    defp one_record_topic(store, opts \\ []) do
      one_record = Record.encoded_size(record("v0", "k0"))

      broker_with_topic("events", 4, Keyword.put(opts, :segment_max_bytes, one_record))
      |> then(fn {broker, root_id} -> {broker, root_id, store} end)
    end

    test "crossing the byte threshold requests a roll and emits no metadata command", %{store: store} do
      one_record = Record.encoded_size(record("v0", "k0"))

      {broker, root_id} =
        broker_with_topic("events", 4,
          segment_max_bytes: one_record,
          command_fun: recording_command_fun(self())
        )

      {broker, {:ok, _placements}} = produce_only(broker, store, "events", [record("v0", "k0")])

      assert [%{range_id: ^root_id, segment_id: {^root_id, 0}, primary: :primary, start_offset: 0}] =
               Broker.due_rolls(broker)

      # The segment is still the write head as far as the control plane is concerned, and no seal
      # command was routed: the length is not knowable yet, so nothing is claimed.
      assert [%{state: :active, length: nil}] = segments(broker, root_id)
      refute Enum.any?(commands_seen(), &match?({:seal_segment, _, _, _, _}, &1))
    end

    test "a produce into a range that owes a roll still succeeds in the same segment", %{store: store} do
      # The roll is a REQUEST, not a barrier. A design that closed the segment at decision time would
      # stall every write until the fence answered; here whatever lands meanwhile is inside the end the
      # fence eventually reports.
      {broker, root_id, store} = one_record_topic(store)
      {broker, {:ok, _placements}} = produce_only(broker, store, "events", [record("v0", "k0")])
      assert [_roll] = Broker.due_rolls(broker)

      assert {broker, {:ok, %{^root_id => {1, 1}}}} = produce_only(broker, store, "events", [record("v1", "k1")])
      assert [%{id: {^root_id, 0}, state: :active}] = segments(broker, root_id)

      # And the fence then covers both records, not just the one that tripped the threshold.
      broker = settle(broker, store)
      assert [%{id: {^root_id, 0}, state: :sealed, length: 2}] = segments(broker, root_id)
    end

    test "a roll stays owed until record_seal/5 succeeds, and survives a dropped cache entry", %{store: store} do
      {broker, root_id, store} = one_record_topic(store)
      {broker, {:ok, _placements}} = produce_only(broker, store, "events", [record("v0", "k0")])
      [roll] = Broker.due_rolls(broker)

      # The cache entry is what a metadata refresh or a rival's seal can take away; the roll must not go
      # with it, or the fence would be owed with no segment id, primary or start offset to name it.
      broker = %{broker | segments: %{}}
      assert Broker.due_rolls(broker) == [roll]

      {broker, :ok} = Broker.record_seal(broker, roll, 1, 7, 1_000)
      assert Broker.due_rolls(broker) == []
      assert [%{id: {^root_id, 0}, state: :sealed, length: 1, byte_size: 7}] = segments(broker, root_id)
    end

    test "record_seal/5 sets the range counter to the fence's end, above the local one", %{store: store} do
      # A peer interleaved on this range and this frontend never adopted its offsets, so the fence
      # answers ABOVE the local counter. Jumping forward is what `adopt_offsets/4` would have done.
      {broker, root_id, store} = one_record_topic(store)
      {broker, {:ok, _placements}} = produce_only(broker, store, "events", [record("v0", "k0")])
      [roll] = Broker.due_rolls(broker)

      {broker, :ok} = Broker.record_seal(broker, roll, 9, 0, 1_000)
      assert broker.offsets[root_id] == 9
      assert [%{state: :sealed, length: 9}] = segments(broker, root_id)
    end

    test "record_seal/5 rewinds the range counter when the fence answers below it", %{store: store} do
      # The assertion that would have caught the wedge. Offsets reserved for a batch the fence refused,
      # or one that never reached the primary, are BURNED, so the local counter sits above the fence's
      # end. Opening the next segment there would be rejected by the tiling rule and the range would
      # stop taking writes for good; `max` would have preserved exactly that bug.
      {broker, root_id, store} = one_record_topic(store)
      {broker, {:ok, _placements}} = produce_only(broker, store, "events", [record("v0", "k0")])
      [roll] = Broker.due_rolls(broker)

      broker = %{broker | offsets: %{root_id => 5}}
      {broker, :ok} = Broker.record_seal(broker, roll, 1, 0, 1_000)

      assert broker.offsets[root_id] == 1
      assert [%{state: :sealed, length: 1}] = segments(broker, root_id)

      # And the successor registers at the fenced end with no :segment_overlap.
      assert {broker, {:ok, %{^root_id => {1, 1}}}} = produce_only(broker, store, "events", [record("v1", "k1")])
      assert Enum.map(segments(broker, root_id), & &1.start_offset) == [0, 1]
    end

    test "record_seal/5 converges on the winner's length when the segment is already sealed", %{store: store} do
      # A roll fence racing a failover seal. Overwriting would move an edge a successor may already have
      # registered at, so the loser adopts the winner's number instead of wedging.
      {broker, root_id, store} = one_record_topic(store)
      {broker, {:ok, _placements}} = produce_only(broker, store, "events", [record("v0", "k0")])
      [roll] = Broker.due_rolls(broker)

      broker = Broker.apply_heal(broker, [{:seal_segment, {root_id, 0}, 1, 3, 500}])

      # The heal already cleared this frontend's roll, so re-recording is the late fence arriving on a
      # broker that still owes it: put it back and let the conflict decide.
      broker = %{broker | rolling: %{root_id => roll}}

      assert {broker, :ok} = Broker.record_seal(broker, roll, 4, 99, 1_000)
      assert Broker.due_rolls(broker) == []
      assert [%{state: :sealed, length: 1, byte_size: 3, sealed_at: 500}] = segments(broker, root_id)
      assert broker.offsets[root_id] == 1
    end

    test "record_seal/5 leaves the broker untouched when the command fails, and a retry succeeds", %{store: store} do
      failing =
        fn dsrsm, topic, command ->
          case {command, Process.get(:fail_seal, true)} do
            {{:seal_segment, _, _, _, _}, true} ->
              Process.put(:fail_seal, false)
              {dsrsm, {:error, :ra_timeout}}

            _other ->
              DSRSM.command(dsrsm, topic, command)
          end
        end

      one_record = Record.encoded_size(record("v0", "k0"))
      {broker, root_id} = broker_with_topic("events", 4, segment_max_bytes: one_record, command_fun: failing)
      {broker, {:ok, _placements}} = produce_only(broker, store, "events", [record("v0", "k0")])
      [roll] = Broker.due_rolls(broker)

      assert {unchanged, {:error, :ra_timeout}} = Broker.record_seal(broker, roll, 1, 0, 1_000)
      assert unchanged == broker
      assert Broker.due_rolls(unchanged) == [roll]

      assert {broker, :ok} = Broker.record_seal(unchanged, roll, 1, 0, 1_000)
      assert Broker.due_rolls(broker) == []
      assert [%{state: :sealed, length: 1}] = segments(broker, root_id)
    end

    test "forget_sealed/4 seats the frontend, and ignores a refusal naming an older segment", %{store: store} do
      {broker, root_id, store} = one_record_topic(store)
      {broker, {:ok, _placements}} = produce_only(broker, store, "events", [record("v0", "k0")])
      [roll] = Broker.due_rolls(broker)

      seated = Broker.forget_sealed(broker, root_id, {root_id, 0}, 1)
      refute Map.has_key?(seated.segments, root_id)
      assert Broker.due_rolls(seated) == []
      assert seated.offsets[root_id] == 1

      # A refusal from a segment this frontend has already moved past must not evict the open one.
      stale = Broker.forget_sealed(broker, root_id, {root_id, 99}, 42)
      assert stale.segments[root_id].id == {root_id, 0}
      assert Broker.due_rolls(stale) == [roll]
      assert stale.offsets[root_id] == 1
    end

    test "adopt_offsets/4 follows the primary only while the dispatch names the range's head", %{store: store} do
      {broker, root_id} = broker_with_topic()
      {broker, {:ok, _placements}} = produce_only(broker, store, "events", [record("v0", "k0")])

      adopted = Broker.adopt_offsets(broker, root_id, {root_id, 0}, 41)
      assert adopted.offsets[root_id] == 42

      # An answer that arrives after the head was closed describes a segment that no longer owns the
      # range's end; moving the counter past the fenced edge is what wedges the next registration.
      closed = Broker.adopt_offsets(broker, root_id, {root_id, 7}, 41)
      assert closed.offsets[root_id] == 1
    end

    test "active_roll/2 names an open segment and answers :none otherwise", %{store: store} do
      {broker, root_id} = broker_with_topic()
      assert Broker.active_roll(broker, root_id) == :none

      {broker, {:ok, _placements}} = produce_only(broker, store, "events", [record("v0", "k0")])

      assert %{range_id: ^root_id, segment_id: {^root_id, 0}, primary: :primary, start_offset: 0} =
               Broker.active_roll(broker, root_id)
    end

    test "a seal command for the range's head clears the roll and seats the counter", %{store: store} do
      # How a failover seal performed on ANOTHER node unblocks a frontend caught mid-roll: without this
      # the frontend keeps re-fencing a segment somebody else already closed.
      {broker, root_id, store} = one_record_topic(store)
      {broker, {:ok, _placements}} = produce_only(broker, store, "events", [record("v0", "k0")])
      assert [_roll] = Broker.due_rolls(broker)

      broker = Broker.apply_heal(broker, [{:seal_segment, {root_id, 0}, 1, 5, 900}])

      assert Broker.due_rolls(broker) == []
      refute Map.has_key?(broker.segments, root_id)
      assert broker.offsets[root_id] == 1
    end

    test "a seal command for some other segment of the range leaves the counter alone", %{store: store} do
      {broker, root_id, store} = one_record_topic(store)
      {broker, {:ok, _placements}} = produce_only(broker, store, "events", [record("v0", "k0")])
      [roll] = Broker.due_rolls(broker)

      # A late or retried command naming a segment that is not this frontend's write head.
      broker = Broker.apply_heal(broker, [{:seal_segment, {root_id, 42}, 3, 0, 900}])

      assert Broker.due_rolls(broker) == [roll]
      assert broker.segments[root_id].id == {root_id, 0}
      assert broker.offsets[root_id] == 1
    end

    test "drop_stale_active_segments/1 evicts a segment the refreshed metadata reports sealed", %{store: store} do
      {broker, root_id} = broker_with_topic()
      {broker, {:ok, _placements}} = produce_only(broker, store, "events", [record("v0", "k0")])

      # A genuinely active segment is left alone.
      assert Broker.drop_stale_active_segments(broker).segments[root_id].id == {root_id, 0}

      # The refresh a peer's seal produces: the metadata says sealed while this frontend still routes
      # produces at the segment. Level-triggered, so every node converges within one reconcile instead
      # of waiting for its store to refuse a batch.
      {dsrsm, :ok} = DSRSM.command(broker.dsrsm, "events", {:seal_segment, {root_id, 0}, 1, 4, 900})
      refreshed = Broker.drop_stale_active_segments(%{broker | dsrsm: dsrsm})

      refute Map.has_key?(refreshed.segments, root_id)
      assert refreshed.offsets[root_id] == 1
    end

    test "a zero-length seal is stepped over rather than answered with :eof", %{store: store} do
      # Reachable on both seal paths now (a split or a failover on a segment registered and never
      # written). The zero-length segment shares its successor's start offset, so a read at that offset
      # can resolve to the one that serves nothing; before the tie-break it answered :eof and every
      # consumer of the range wedged there permanently.
      {broker, root_id} = broker_with_topic()
      {broker, {:ok, _placements}} = produce_only(broker, store, "events", [record("v0", "k0")])
      [roll] = [Broker.active_roll(broker, root_id)]

      # Fence answers 0: the segment was registered and nothing durable landed in it.
      {broker, :ok} = Broker.record_seal(broker, roll, 0, 0, 1_000)
      assert [%{state: :sealed, start_offset: 0, length: 0}] = segments(broker, root_id)

      {broker, {:ok, _placements}} = produce_only(broker, store, "events", [record("v1", "k1")])
      assert [%{length: 0, start_offset: 0}, %{state: :active, start_offset: 0}] = segments(broker, root_id)

      assert {:ok, [%{offset: 0, value: "v1"}]} = Broker.read(broker, root_id, 0, 100, read_fun(store))
    end
  end

  describe "the tiling invariant under interleaved produces and rolls" do
    # The invariant the 519-record bug violated, as a property rather than a fixture: whatever order
    # produces, primary-assigned adoptions and fenced seals arrive in, a range's segments must tile
    # `[0, next_offset)` exactly, and the counter must end on the last fence's answer.
    property "a range's segments tile its offsets with no gap and no overlap" do
      check all(steps <- list_of(step(), min_length: 1, max_length: 24), max_runs: 60) do
        {:ok, store} = FakeSegmentStore.start_link()
        one_record = Record.encoded_size(record("v0", "k0"))
        {broker, root_id} = broker_with_topic("events", 4, segment_max_bytes: 2 * one_record)

        broker = Enum.reduce(steps, broker, &apply_step(&1, &2, store, root_id))

        segs = segments(broker, root_id)
        starts = Enum.map(segs, & &1.start_offset)
        ends = Enum.map(segs, &(&1.start_offset + (&1.length || broker.offsets[root_id] - &1.start_offset)))

        assert starts == Enum.sort(starts), "segments must be registered in ascending offset order"

        # Each segment begins exactly where the previous one ended: no gap (records unreachable) and no
        # overlap (two segments handing out the same offset).
        assert Enum.drop(starts, 1) == Enum.drop(ends, -1)
        assert starts == [] or hd(starts) == 0
        assert ends == [] or List.last(ends) == Map.get(broker.offsets, root_id, 0)
      end
    end

    defp step do
      one_of([
        tuple({constant(:produce), integer(1..3)}),
        constant(:settle),
        tuple({constant(:adopt), integer(0..4)})
      ])
    end

    defp apply_step({:produce, count}, broker, store, _root_id) do
      records = for index <- 1..count, do: record("v#{index}", "k#{index}")

      case produce_only(broker, store, "events", records) do
        {broker, {:ok, _placements}} -> broker
        {broker, {:error, _reason}} -> broker
      end
    end

    defp apply_step(:settle, broker, store, _root_id), do: settle(broker, store)

    # A primary-assigned end arriving late for whatever segment the range currently heads, which is
    # what makes a frontend's counter and the store's true end diverge in the first place.
    defp apply_step({:adopt, ahead}, broker, _store, root_id) do
      case Map.get(broker.segments, root_id) do
        nil -> broker
        active -> Broker.adopt_offsets(broker, root_id, active.id, broker.offsets[root_id] + ahead)
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

    test "a hole in the middle of a range is stepped over, not stopped on", %{store: store} do
      # A range's segments are meant to tile its offsets and registration enforces it, but a hole can
      # still open afterwards: retention expires by sealed_at, so clocks that disagree across nodes can
      # drop a middle segment while its neighbours survive, and an operator can delete one outright.
      # Before this, a read landing in the hole resolved to the segment BEFORE it and got :eof, and
      # consume stops on :eof rather than advancing, so one hole wedged every consumer of the range
      # forever. The records in the hole are gone either way; the ones above it must not be.
      {broker, root_id} = range_with_hole(store)

      # The consumer crosses the hole and reaches everything above it, rather than stalling at 1.
      {records, _cursor} = consume(broker, store, root_id, :start)
      assert Enum.map(records, & &1.value) == ["v0", "v2", "v3"]

      # And a direct read of the missing offset serves the next segment instead of answering :eof.
      assert {:ok, [%{value: "v2"}]} = Broker.read(broker, root_id, 1, 1, read_fun(store))
    end

    test "a frontend whose offset lags a seal is refused rather than allowed to overlap", %{store: store} do
      # The state a failover seal leaves on a frontend that has not caught up: the range's segments are
      # all sealed, and this frontend still thinks the range ends earlier than it does. Opening a
      # segment at that stale offset would hand out offsets the sealed one already owns, which is how
      # one acknowledged record quietly replaces another, so the control plane refuses it.
      one_record = Record.encoded_size(record("value", "key"))
      {broker, root_id} = broker_with_topic("events", 4, segment_max_bytes: one_record)

      broker =
        Enum.reduce(0..2, broker, fn index, broker ->
          {broker, {:ok, _placements}} = produce(broker, store, "events", [record("value", "key#{index}")])
          broker
        end)

      before = segments(broker, root_id)
      # Rewound by hand because the situation it stands for (this node's view is behind another node's
      # seal) needs shared metadata to arise on its own, and the point under test is what the control
      # plane does with the stale offset, not how the frontend came to hold one.
      stale = %{broker | offsets: %{root_id => 1}, segments: %{}}

      assert {stale, {:error, :segment_overlap}} = produce(stale, store, "events", [record("v", "k")])

      # Refused, and nothing half-registered: the range still holds exactly the segments it did.
      assert Enum.map(segments(stale, root_id), & &1.id) == Enum.map(before, & &1.id)
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

    test "the byte threshold is soft: a batch may overshoot before the roll", %{store: store} do
      one_record = Record.encoded_size(record("value", "key"))
      {broker, root_id} = broker_with_topic("events", 4, segment_max_bytes: one_record)

      records = for index <- 0..2, do: record("value", "key#{index}")
      {broker, {:ok, _placements}} = produce(broker, store, "events", records)

      assert [segment] = segments(broker, root_id)
      assert segment.state == :sealed
      assert segment.length == 3
    end

    test "splitting is metadata-only: the parent's segment is sealed by the caller's fence", %{store: store} do
      {broker, root_id} = broker_with_topic()
      {broker, {:ok, _placements}} = produce(broker, store, "events", [record("v", "k")])

      assert [%{state: :active}] = segments(broker, root_id)

      # The split on its own leaves the parent's segment open, because its sealed length has to be what
      # the fence answered rather than a number this frontend derived from its own counter.
      {unsealed, {:ok, _left, _right}} = Broker.split_range(broker, root_id)
      assert [%{state: :active, length: nil}] = segments(unsealed, root_id)

      broker = seal_active(broker, store, root_id)
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

    test "a read of a sealed segment stops at its sealed length even when the store holds more", %{store: store} do
      # The control plane's length is the segment's truth; a store that grew past it after the seal is
      # not part of the log. Before the clamp, a read at 0 with room to spare returned seq 0's whole
      # store, offsets 0..5, so the caller got s3..s5 in place of v3..v4, which seq 1 owns.
      {broker, root_id, _sealed, _active} = sealed_with_surplus(store)

      assert {:ok, records} = Broker.read(broker, root_id, 0, 100, read_fun(store))
      assert Enum.map(records, &{&1.offset, &1.value}) == [{0, "v0"}, {1, "v1"}, {2, "v2"}]

      # Starting inside the sealed segment the cap shrinks with it: two records left, not the whole
      # remaining store.
      assert {:ok, records} = Broker.read(broker, root_id, 1, 100, read_fun(store))
      assert Enum.map(records, &{&1.offset, &1.value}) == [{1, "v1"}, {2, "v2"}]

      # And the offset at the sealed edge is answered by the next segment's store, not the sealed
      # one's tail: v3 and v4 belong to seq 1, s3 and s4 never entered the log.
      assert {:ok, records} = Broker.read(broker, root_id, 3, 100, read_fun(store))
      assert Enum.map(records, &{&1.offset, &1.value}) == [{3, "v3"}, {4, "v4"}]

      # The last offset the seal covers, asked for on its own with room for a hundred: exactly one
      # record comes back. This is the clamp at its narrowest, a remainder of one against a budget
      # that would otherwise have swept s3..s5 in behind v2.
      assert {:ok, records} = Broker.read(broker, root_id, 2, 100, read_fun(store))
      assert Enum.map(records, &{&1.offset, &1.value}) == [{2, "v2"}]
    end

    test "a scan across a sealed segment with a surplus delivers the next segment's head", %{store: store} do
      # The reproduced bug shape: the page took seq 0's surplus, the cursor moved to the last offset
      # served plus one, which is already inside seq 1, so seq 1's head was never delivered and the
      # scan looked like one unbroken run. With the read capped at the sealed edge, the page holds
      # exactly v0..v2 from seq 0 (fewer than the page size), and consume_page carries on from offset
      # 3 into seq 1 with the full page budget.
      {broker, root_id, _sealed, _active} = sealed_with_surplus(store)

      {records, cursor} = consume(broker, store, root_id, :start)
      assert Enum.map(records, &{&1.offset, &1.value}) == [{0, "v0"}, {1, "v1"}, {2, "v2"}, {3, "v3"}, {4, "v4"}]
      assert cursor == {0, 5}

      # A page boundary right at the sealed edge: page one takes v0, v1; page two starts at 2 with
      # only ONE record left in seq 0, so the cap is 1 and the rest of the page is filled from seq 1.
      # Without the cap that second page would have been v2 and s3, and the cursor would have jumped
      # to 4, skipping v3 for good.
      assert {:ok, page, {0, 2}} = Broker.read_consume(broker, root_id, :start, 2, read_fun(store))
      assert Enum.map(page, &{&1.offset, &1.value}) == [{0, "v0"}, {1, "v1"}]

      assert {:ok, page, {0, 5}} = Broker.read_consume(broker, root_id, {0, 2}, 2, read_fun(store))
      assert Enum.map(page, &{&1.offset, &1.value}) == [{2, "v2"}, {3, "v3"}, {4, "v4"}]

      assert {:ok, [], {0, 5}} = Broker.read_consume(broker, root_id, {0, 5}, 2, read_fun(store))
    end

    test "records a store holds beyond the sealed length are not part of the log", %{store: store} do
      # The documented consequence of honoring the sealed length: s3..s5 sit in seq 0's store, but the
      # control plane sealed seq 0 at 3 and opened seq 1 at 3, so those offsets are seq 1's and the
      # surplus is unreachable by any read or scan. That is the contract, not a loss: the fence on the
      # write side is what keeps a stale writer's post-seal appends from ever being acknowledged, so
      # nothing a producer was told was durable lives only in the surplus.
      {broker, root_id, sealed, _active} = sealed_with_surplus(store)

      {records, _cursor} = consume(broker, store, root_id, :start)
      delivered = Enum.map(records, & &1.value)
      refute Enum.any?(delivered, &String.starts_with?(&1, "s"))
      assert delivered == ["v0", "v1", "v2", "v3", "v4"]

      assert read_all(broker, store, root_id) |> Enum.map(& &1.value) == ["v0", "v1", "v2", "v3", "v4"]

      # Even a direct read aimed at the surplus's offsets resolves to seq 1, never to seq 0's tail.
      for offset <- 3..4 do
        assert {:ok, [%{offset: ^offset, value: "v" <> _} = record | _]} =
                 Broker.read(broker, root_id, offset, 100, read_fun(store))

        refute String.starts_with?(record.value, "s")
      end

      # Offset 5 exists only in the surplus, so to the log it is past the end. This one is decided by
      # `locate_segment/3`'s range-position gate rather than by the clamp: it is here as a contract
      # check that the surplus never extends the range, and it would hold with or without the clamp.
      assert Broker.read(broker, root_id, 5, 100, read_fun(store)) == :eof
      assert Enum.count(elem(FakeSegmentStore.read(store, :primary, sealed.id, 0, 100), 1)) == 6
    end

    test "an active segment is not clamped", %{store: store} do
      # The active segment is the write head: it has no sealed length and nothing above its start is
      # owned by another segment, so a read keeps the caller's whole budget. Ten records under the
      # default threshold stay in one active segment, and a read with room for far more returns all.
      {broker, root_id} = broker_with_topic()
      records = for i <- 0..9, do: record("v#{i}", "k#{i}")
      {broker, {:ok, _placements}} = produce(broker, store, "events", records)

      assert [%{state: :active, length: nil, start_offset: 0}] = segments(broker, root_id)

      assert {:ok, read} = Broker.read(broker, root_id, 0, 1000, read_fun(store))
      assert Enum.map(read, & &1.value) == Enum.map(0..9, &"v#{&1}")

      # And from the middle: everything from the requested offset up, not a sealed-edge remainder.
      assert {:ok, read} = Broker.read(broker, root_id, 4, 1000, read_fun(store))
      assert Enum.map(read, & &1.value) == Enum.map(4..9, &"v#{&1}")

      # The budget itself still applies: it is the sealed edge that is absent, not the page size.
      assert {:ok, read} = Broker.read(broker, root_id, 0, 3, read_fun(store))
      assert Enum.map(read, & &1.value) == ["v0", "v1", "v2"]
    end

    test "a seal after an adoption records the segment's real length, not this frontend's tally", %{store: store} do
      # The bug: the sealed length came from a per-frontend record tally that adoption reset to zero,
      # so this segment sealed at 5 (the records produced after the adoption) while holding 8. With
      # the read capped at the sealed edge and the next segment obliged to start exactly where this
      # one ends, offsets 5..7 were acknowledged and then reachable from nowhere. The length now comes
      # from the FENCE, which answers the primary's own end, so it is the segment's real end whoever
      # wrote the records.
      {_broker, root_id, sealed} = sealed_after_adoption(store)

      assert %{id: {^root_id, 0}, state: :sealed, start_offset: 0, length: 8} = sealed
    end

    test "a read at the last offset of a segment sealed after an adoption returns its record", %{store: store} do
      # 12A keeping 10A honest: the clamp trusts the sealed length, so a short length would silently
      # turn acknowledged records into :eof. Every offset the segment covers is still served, and the
      # record at the sealed edge (offset 7, the last one produced) comes back on its own.
      {broker, root_id, _sealed} = sealed_after_adoption(store)

      assert {:ok, records} = Broker.read(broker, root_id, 7, 100, read_fun(store))
      assert Enum.map(records, &{&1.offset, &1.value}) == [{7, "v7"}]

      assert broker |> read_all(store, root_id) |> Enum.map(& &1.value) == Enum.map(0..7, &"v#{&1}")

      {consumed, cursor} = consume(broker, store, root_id, :start)
      assert Enum.map(consumed, & &1.value) == Enum.map(0..7, &"v#{&1}")
      assert cursor == {0, 8}
    end

    test "a produce refused by a fence seats the frontend and its retry lands in the successor", %{store: store} do
      # The behavior change the fence buys: a batch aimed at a segment somebody else closed is REFUSED
      # rather than silently accepted into offsets the next segment owns, which is the 519-record bug.
      {broker, root_id} = broker_with_topic()
      {broker, {:ok, _placements}} = produce(broker, store, "events", [record("v0", "k0")])

      [segment] = segments(broker, root_id)
      {:ok, 1, _bytes} = FakeSegmentStore.seal(store, :primary, segment.id, 0)

      assert {broker, {:error, {:sealed, 1}}} = produce(broker, store, "events", [record("v1", "k1")])

      # Seated at the fenced end: the cache is gone and the counter names the edge, so the retry opens
      # the successor at 1 instead of racing :segment_overlap.
      refute Map.has_key?(broker.segments, root_id)
      assert broker.offsets[root_id] == 1

      # Until the control plane records the seal the range stays blocked, which is the honest state: the
      # metadata still names seq 0 the write head and refuses a second one. Once the seal lands (here
      # through a failover command, the same shape `record_seal/5` emits), the retry opens the successor
      # exactly at the fenced end.
      broker = Broker.apply_heal(broker, [{:seal_segment, {root_id, 0}, 1, 0, 1_000}])

      assert {broker, {:ok, %{^root_id => {1, 1}}}} = produce(broker, store, "events", [record("v1", "k1")])
      assert Enum.map(segments(broker, root_id), &{&1.id, &1.start_offset}) == [{{root_id, 0}, 0}, {{root_id, 1}, 1}]
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

  describe "put_cache/3" do
    test "a refresh that could not read a vnode does not erase that vnode's topics" do
      {broker, root_id} = broker_with_topic("events")
      assert Broker.active_range_ids(broker, "events") == [root_id]

      # The refresh represents an unreadable vnode with an empty Metadata. Installing it wholesale is
      # what made a read of a topic with durable records answer a successful empty page.
      {:ok, vnode} = DSRSM.vnode_for(broker.dsrsm, "events")
      blanked = %{broker.dsrsm | vnodes: Map.put(broker.dsrsm.vnodes, vnode, Metadata.new())}

      assert Broker.active_range_ids(Broker.put_cache(broker, blanked), "events") == []
      assert Broker.active_range_ids(Broker.put_cache(broker, blanked, [vnode]), "events") == [root_id]
    end

    test "a refresh that read every vnode installs as-is, so deletions still land" do
      {broker, _root_id} = broker_with_topic("events")
      emptied = %{broker.dsrsm | vnodes: Map.new(broker.dsrsm.vnodes, fn {id, _m} -> {id, Metadata.new()} end)}

      # Retention is for vnodes that did not answer. One that answered and reported nothing is
      # authoritative, or a dropped topic would live in the cache forever.
      assert Broker.active_range_ids(Broker.put_cache(broker, emptied, []), "events") == []
    end
  end
end
