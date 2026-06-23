defmodule Malachi.BrokerServerTest do
  use ExUnit.Case, async: true

  alias Malachi.BrokerServer
  alias Malachi.Log.Record

  @moduletag :tmp_dir

  defp record(value, key), do: Record.new(value, key: key)

  defp start(directory, opts \\ []) do
    {:ok, server} = BrokerServer.start_link(directory, opts)
    on_exit(fn -> if Process.alive?(server), do: BrokerServer.stop(server) end)
    server
  end

  defp with_topic(directory, opts \\ []) do
    server = start(directory, opts)
    {:ok, root_id} = BrokerServer.create_topic(server, "events", 4)
    {server, root_id}
  end

  defp read_all(server, range_id) do
    read_all(server, range_id, 0, [])
  end

  defp read_all(server, range_id, offset, accumulated) do
    case BrokerServer.read(server, range_id, offset, 100) do
      :eof -> accumulated |> Enum.reverse() |> List.flatten()
      {:ok, records} -> read_all(server, range_id, offset + length(records), [records | accumulated])
    end
  end

  describe "time-based flush" do
    test "buffered records become durable after the flush interval without explicit sync",
         %{tmp_dir: directory} do
      {server, root_id} = with_topic(directory, flush_interval_ms: 10)

      {:ok, _placements} = BrokerServer.produce(server, "events", [record("a", "k0"), record("b", "k1")])

      assert eventually(fn -> match?({:ok, [_, _]}, BrokerServer.read(server, root_id, 0, 10)) end)
      assert read_all(server, root_id) |> Enum.map(& &1.value) == ["a", "b"]
    end

    test "explicit sync commits immediately", %{tmp_dir: directory} do
      {server, root_id} = with_topic(directory, flush_interval_ms: 60_000)
      {:ok, _placements} = BrokerServer.produce(server, "events", [record("a", "k0")])
      :ok = BrokerServer.sync(server)
      assert {:ok, [record_read]} = BrokerServer.read(server, root_id, 0, 10)
      assert record_read.value == "a"
    end
  end

  describe "concurrency" do
    test "serializes concurrent produces without losing records", %{tmp_dir: directory} do
      {server, root_id} = with_topic(directory)

      1..50
      |> Enum.map(fn index ->
        Task.async(fn -> BrokerServer.produce(server, "events", [record("v#{index}", "k#{index}")]) end)
      end)
      |> Enum.each(fn task -> assert {:ok, _placements} = Task.await(task) end)

      :ok = BrokerServer.sync(server)
      assert server |> read_all(root_id) |> length() == 50
    end
  end

  describe "operations" do
    test "split, produce and read through the server", %{tmp_dir: directory} do
      {server, root_id} = with_topic(directory)
      {:ok, left_id, right_id} = BrokerServer.split_range(server, root_id)
      assert Enum.sort(BrokerServer.active_range_ids(server, "events")) == Enum.sort([left_id, right_id])

      records = for index <- 0..19, do: record("v#{index}", "k#{index}")
      {:ok, _placements} = BrokerServer.produce(server, "events", records)
      :ok = BrokerServer.sync(server)

      left = read_all(server, left_id) |> Enum.map(& &1.value)
      right = read_all(server, right_id) |> Enum.map(& &1.value)
      assert Enum.sort(left ++ right) == Enum.sort(Enum.map(records, & &1.value))
    end

    test "merge and cross-epoch history through the server", %{tmp_dir: directory} do
      {server, root_id} = with_topic(directory)
      parent_records = for index <- 0..9, do: record("v#{index}", "k#{index}")
      {:ok, _placements} = BrokerServer.produce(server, "events", parent_records)
      :ok = BrokerServer.sync(server)
      {:ok, left_id, right_id} = BrokerServer.split_range(server, root_id)

      left = drain_history(server, left_id)
      right = drain_history(server, right_id)
      assert Enum.sort(Enum.map(left ++ right, & &1.value)) == Enum.sort(Enum.map(parent_records, & &1.value))

      assert {:ok, _child_id} = BrokerServer.merge_ranges(server, left_id, right_id)
    end

    test "producing to an unknown topic fails", %{tmp_dir: directory} do
      server = start(directory)
      assert BrokerServer.produce(server, "nope", [record("a", "k")]) == {:error, :no_such_topic}
    end
  end

  defp drain_history(server, range_id, cursor \\ :start, accumulated \\ []) do
    case BrokerServer.stream_history(server, range_id, cursor, 3) do
      {:ok, records, :done} -> [records | accumulated] |> Enum.reverse() |> List.flatten()
      {:ok, records, next_cursor} -> drain_history(server, range_id, next_cursor, [records | accumulated])
    end
  end

  defp eventually(check, remaining_ms \\ 1000) do
    cond do
      check.() -> true
      remaining_ms <= 0 -> false
      true -> Process.sleep(10) && eventually(check, remaining_ms - 10)
    end
  end
end
