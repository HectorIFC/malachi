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

  # Deterministically wait until `count` long-poll waiters are parked (avoids sleep-based flakiness).
  defp wait_for_park(server, count \\ 1, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 2_000

    cond do
      length(:sys.get_state(server).waiters) >= count -> :ok
      System.monotonic_time(:millisecond) > deadline -> flunk("expected #{count} parked waiter(s)")
      true -> Process.sleep(2) && wait_for_park(server, count, deadline)
    end
  end

  describe "long-poll consume" do
    test "consume returns immediately when wait_ms is 0", %{tmp_dir: directory} do
      {server, _root} = with_topic(directory)
      assert {[], _positions} = BrokerServer.consume(server, "events", %{}, 100, 0)
    end

    test "consume with wait blocks until a produce wakes it", %{tmp_dir: directory} do
      {server, _root} = with_topic(directory)

      task = Task.async(fn -> BrokerServer.consume(server, "events", %{}, 100, 5_000) end)
      wait_for_park(server)

      {:ok, _placements} = BrokerServer.produce(server, "events", [record("a", "k0")])

      assert {records, _positions} = Task.await(task)
      assert Enum.map(records, & &1.value) == ["a"]
    end

    test "consume with wait returns empty after the timeout when nothing is produced", %{tmp_dir: directory} do
      {server, _root} = with_topic(directory)
      assert {[], _positions} = BrokerServer.consume(server, "events", %{}, 100, 50)
    end

    test "a produce wakes only waiters on the produced topic", %{tmp_dir: directory} do
      server = start(directory)
      {:ok, _} = BrokerServer.create_topic(server, "events", 4)
      {:ok, _} = BrokerServer.create_topic(server, "other", 4)

      events_task = Task.async(fn -> BrokerServer.consume(server, "events", %{}, 100, 300) end)
      other_task = Task.async(fn -> BrokerServer.consume(server, "other", %{}, 100, 300) end)
      wait_for_park(server, 2)

      {:ok, _} = BrokerServer.produce(server, "events", [record("a", "k0")])

      # the events waiter wakes with data; the other waiter is untouched and times out empty
      assert {[%{value: "a"}], _} = Task.await(events_task)
      assert {[], _} = Task.await(other_task, 1_000)
    end
  end

  describe "durability" do
    test "records are durable on return — no explicit sync needed", %{tmp_dir: directory} do
      {server, root_id} = with_topic(directory)

      {:ok, _placements} = BrokerServer.produce(server, "events", [record("a", "k0"), record("b", "k1")])

      # the replication server fsynced on a quorum before the call returned
      assert read_all(server, root_id) |> Enum.map(& &1.value) == ["a", "b"]
    end

    test "sync is a no-op and safe to call", %{tmp_dir: directory} do
      {server, root_id} = with_topic(directory)
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

      left = read_all(server, left_id) |> Enum.map(& &1.value)
      right = read_all(server, right_id) |> Enum.map(& &1.value)
      assert Enum.sort(left ++ right) == Enum.sort(Enum.map(records, & &1.value))
    end

    test "merge and cross-epoch history through the server", %{tmp_dir: directory} do
      {server, root_id} = with_topic(directory)
      parent_records = for index <- 0..9, do: record("v#{index}", "k#{index}")
      {:ok, _placements} = BrokerServer.produce(server, "events", parent_records)
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
end
