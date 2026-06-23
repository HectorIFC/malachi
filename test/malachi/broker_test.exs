defmodule Malachi.BrokerTest do
  use ExUnit.Case, async: true

  alias Malachi.Broker
  alias Malachi.Log.Record

  @moduletag :tmp_dir

  defp record(value, key), do: Record.new(value, key: key)

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
  end
end
