defmodule Malachi.BrokerTest do
  use ExUnit.Case, async: true

  alias Malachi.Broker
  alias Malachi.Cluster.Placement
  alias Malachi.Log.Record
  alias Malachi.Metadata

  @moduletag :tmp_dir

  defp record(value, key), do: Record.new(value, key: key)

  defp segments(broker, range_id) do
    broker.metadata |> Metadata.segments_of_range(range_id) |> Enum.sort_by(& &1.start_offset)
  end

  defp broker_with_topic(directory, name \\ "events", bits \\ 4) do
    {:ok, broker} = Broker.open(directory)
    {broker, {:ok, root_id}} = Broker.create_topic(broker, name, bits)
    {broker, root_id}
  end

  defp read_all(broker, range_id) do
    read_all(broker, range_id, 0, [])
  end

  defp read_all(broker, range_id, offset, accumulated) do
    case Broker.read(broker, range_id, offset, 100) do
      :eof -> accumulated |> Enum.reverse() |> List.flatten()
      {:ok, records} -> read_all(broker, range_id, offset + length(records), [records | accumulated])
    end
  end

  describe "create_topic / produce / read" do
    test "produces records and reads them back from the owning range's log", %{tmp_dir: directory} do
      {broker, root_id} = broker_with_topic(directory)

      records = for index <- 0..9, do: record("v#{index}", "k#{index}")
      {:ok, broker, placements} = Broker.produce(broker, "events", records)
      {:ok, broker} = Broker.sync(broker)

      # single range, so all records land in the root range with contiguous offsets
      assert placements == %{root_id => {0, 9}}
      assert broker |> read_all(root_id) |> Enum.map(& &1.value) == Enum.map(records, & &1.value)

      :ok = Broker.close(broker)
    end

    test "an empty produce is a no-op", %{tmp_dir: directory} do
      {broker, _root_id} = broker_with_topic(directory)
      assert {:ok, _broker, placements} = Broker.produce(broker, "events", [])
      assert placements == %{}
    end

    test "producing to an unknown topic fails", %{tmp_dir: directory} do
      {:ok, broker} = Broker.open(directory)
      assert Broker.produce(broker, "nope", [record("a", "k")]) == {:error, :no_such_topic}
    end

    test "reading a range with no log yet is eof", %{tmp_dir: directory} do
      {broker, root_id} = broker_with_topic(directory)
      assert Broker.read(broker, root_id, 0, 10) == :eof
    end
  end

  describe "split routes records to children (control plane drives data plane)" do
    test "after a split, records route to the correct child range's log", %{tmp_dir: directory} do
      {broker, root_id} = broker_with_topic(directory)
      {broker, {:ok, left_id, right_id}} = Broker.split_range(broker, root_id)

      records = for index <- 0..29, do: record("v#{index}", "k#{index}")
      {:ok, broker, _placements} = Broker.produce(broker, "events", records)
      {:ok, broker} = Broker.sync(broker)

      left_values = read_all(broker, left_id) |> Enum.map(& &1.value)
      right_values = read_all(broker, right_id) |> Enum.map(& &1.value)

      # every record landed in exactly one child; together they reconstruct the input
      assert Enum.sort(left_values ++ right_values) == Enum.sort(Enum.map(records, & &1.value))
      refute left_values == []
      refute right_values == []
    end

    test "split is logical: the sealed parent log keeps its records", %{tmp_dir: directory} do
      {broker, root_id} = broker_with_topic(directory)

      records = for index <- 0..4, do: record("v#{index}", "k#{index}")
      {:ok, broker, _placements} = Broker.produce(broker, "events", records)
      {:ok, broker} = Broker.sync(broker)

      {broker, {:ok, left_id, right_id}} = Broker.split_range(broker, root_id)

      assert broker |> read_all(root_id) |> Enum.map(& &1.value) == Enum.map(records, & &1.value)
      assert Broker.read(broker, left_id, 0, 10) == :eof
      assert Broker.read(broker, right_id, 0, 10) == :eof
    end

    test "split errors propagate from the control plane", %{tmp_dir: directory} do
      {broker, root_id} = broker_with_topic(directory)
      {broker, {:ok, _left, _right}} = Broker.split_range(broker, root_id)
      # root is now sealed in the metadata
      assert {_broker, {:error, :sealed}} = Broker.split_range(broker, root_id)
    end
  end

  describe "cross-epoch history" do
    test "a child's history is the parent's slice then the child's own records", %{tmp_dir: directory} do
      {broker, root_id} = broker_with_topic(directory)

      parent_records = for index <- 0..19, do: record("v#{index}", "k#{index}")
      {:ok, broker, _placements} = Broker.produce(broker, "events", parent_records)
      {:ok, broker} = Broker.sync(broker)

      {broker, {:ok, left_id, right_id}} = Broker.split_range(broker, root_id)

      child_records = for index <- 20..39, do: record("v#{index}", "k#{index}")
      {:ok, broker, _placements} = Broker.produce(broker, "events", child_records)
      {:ok, broker} = Broker.sync(broker)

      {:ok, left_history} = Broker.read_history(broker, left_id)
      {:ok, right_history} = Broker.read_history(broker, right_id)

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

    test "read_history of an unknown range fails", %{tmp_dir: directory} do
      {broker, _root_id} = broker_with_topic(directory)
      assert Broker.read_history(broker, {"events", 999}) == {:error, :no_such_range}
    end

    test "pending? reflects unflushed records", %{tmp_dir: directory} do
      {broker, _root_id} = broker_with_topic(directory)
      refute Broker.pending?(broker)
      {:ok, broker, _placements} = Broker.produce(broker, "events", [record("a", "k0")])
      assert Broker.pending?(broker)
      {:ok, broker} = Broker.sync(broker)
      refute Broker.pending?(broker)
    end
  end

  describe "merge" do
    test "merges buddy ranges back into one active child", %{tmp_dir: directory} do
      {broker, root_id} = broker_with_topic(directory)
      {broker, {:ok, left_id, right_id}} = Broker.split_range(broker, root_id)
      {broker, {:ok, child_id}} = Broker.merge_ranges(broker, left_id, right_id)

      records = for index <- 0..9, do: record("v#{index}", "k#{index}")
      {:ok, broker, placements} = Broker.produce(broker, "events", records)
      {:ok, _broker} = Broker.sync(broker)

      # the merged child covers the whole keyspace again, so all records route to it
      assert Map.keys(placements) == [child_id]
    end

    test "merge seals both parents' active segments", %{tmp_dir: directory} do
      {broker, root_id} = broker_with_topic(directory)
      {broker, {:ok, left_id, right_id}} = Broker.split_range(broker, root_id)

      # land at least one record in each child so both have an open segment
      records = for index <- 0..29, do: record("v#{index}", "k#{index}")
      {:ok, broker, _placements} = Broker.produce(broker, "events", records)

      {broker, {:ok, _child_id}} = Broker.merge_ranges(broker, left_id, right_id)

      for parent_id <- [left_id, right_id] do
        assert Enum.all?(segments(broker, parent_id), &(&1.state == :sealed))
      end
    end
  end

  describe "segments (data-plane lifecycle)" do
    test "the first produce registers an active segment with a placed replica set", %{tmp_dir: directory} do
      {broker, root_id} = broker_with_topic(directory)
      {:ok, broker, _placements} = Broker.produce(broker, "events", [record("v", "k")])

      assert [segment] = segments(broker, root_id)
      # default policy: single broker (this node), replication factor 1
      assert segment.id == {root_id, 0}
      assert segment.state == :active
      assert segment.start_offset == 0
      assert segment.replica_set == [node()]
    end

    test "the replica set comes from Placement over the configured brokers", %{tmp_dir: directory} do
      brokers = [:a, :b, :c, :d]
      {:ok, broker} = Broker.open(directory, brokers: brokers, replication_factor: 3)
      {broker, {:ok, root_id}} = Broker.create_topic(broker, "events", 4)
      {:ok, broker, _placements} = Broker.produce(broker, "events", [record("v", "k")])

      assert [segment] = segments(broker, root_id)
      assert {:ok, segment.replica_set} == Placement.place(segment.id, brokers, 3)
      assert length(segment.replica_set) == 3
    end

    test "the active segment seals and rolls once it crosses :segment_max_bytes", %{tmp_dir: directory} do
      one_record = Record.encoded_size(record("value", "key"))
      # threshold = one record's bytes, so each single-record produce seals immediately
      {:ok, broker} = Broker.open(directory, segment_max_bytes: one_record)
      {broker, {:ok, root_id}} = Broker.create_topic(broker, "events", 4)

      broker =
        Enum.reduce(0..2, broker, fn index, broker ->
          {:ok, broker, _placements} = Broker.produce(broker, "events", [record("value", "key#{index}")])
          broker
        end)

      segs = segments(broker, root_id)
      # three rolled segments, contiguous, each holding exactly one record
      assert Enum.map(segs, & &1.id) == [{root_id, 0}, {root_id, 1}, {root_id, 2}]
      assert Enum.map(segs, & &1.start_offset) == [0, 1, 2]
      assert Enum.all?(segs, &(&1.state == :sealed and &1.length == 1))
    end

    test "the byte threshold is soft: a batch may overshoot before sealing", %{tmp_dir: directory} do
      one_record = Record.encoded_size(record("value", "key"))
      {:ok, broker} = Broker.open(directory, segment_max_bytes: one_record)
      {broker, {:ok, root_id}} = Broker.create_topic(broker, "events", 4)

      # one batch of three records overshoots the one-record threshold, sealing as a single segment
      records = for index <- 0..2, do: record("value", "key#{index}")
      {:ok, broker, _placements} = Broker.produce(broker, "events", records)

      assert [segment] = segments(broker, root_id)
      assert segment.state == :sealed
      assert segment.length == 3
    end

    test "splitting seals the parent's active segment", %{tmp_dir: directory} do
      {broker, root_id} = broker_with_topic(directory)
      {:ok, broker, _placements} = Broker.produce(broker, "events", [record("v", "k")])

      assert [%{state: :active}] = segments(broker, root_id)
      {broker, {:ok, _left, _right}} = Broker.split_range(broker, root_id)
      assert [%{state: :sealed, length: 1}] = segments(broker, root_id)
    end

    test "open/2 rejects an invalid placement policy", %{tmp_dir: directory} do
      assert_raise ArgumentError, fn -> Broker.open(directory, brokers: []) end
      assert_raise ArgumentError, fn -> Broker.open(directory, brokers: :not_a_list) end
      assert_raise ArgumentError, fn -> Broker.open(directory, replication_factor: 0) end
      assert_raise ArgumentError, fn -> Broker.open(directory, segment_max_bytes: 0) end
    end
  end
end
