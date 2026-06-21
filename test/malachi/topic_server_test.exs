defmodule Malachi.TopicServerTest do
  use ExUnit.Case, async: true

  alias Malachi.Log.Record
  alias Malachi.TopicServer

  @moduletag :tmp_dir

  defp record(value, key), do: Record.new(value, key: key)

  defp start(directory, opts \\ []) do
    opts = Keyword.merge([keyspace_bits: 4], opts)
    {:ok, server} = TopicServer.start_link(directory, "events", opts)
    on_exit(fn -> if Process.alive?(server), do: TopicServer.stop(server) end)
    server
  end

  defp only_range(server) do
    [range_id] = TopicServer.active_range_ids(server)
    range_id
  end

  defp read_all(server, range_id) do
    read_all(server, range_id, 0, [])
  end

  defp read_all(server, range_id, offset, accumulated) do
    case TopicServer.read(server, range_id, offset, 100) do
      :eof -> accumulated |> Enum.reverse() |> List.flatten()
      {:ok, records} -> read_all(server, range_id, offset + length(records), [records | accumulated])
    end
  end

  describe "time-based flush" do
    test "buffered records become durable after the flush interval, without explicit sync",
         %{tmp_dir: directory} do
      server = start(directory, flush_interval_ms: 10)
      range_id = only_range(server)

      {:ok, _placements} = TopicServer.append(server, [record("a", "k0"), record("b", "k1")])

      # the time-based flush commits them shortly after; poll until visible
      assert eventually(fn ->
               match?({:ok, [_, _]}, TopicServer.read(server, range_id, 0, 10))
             end)

      assert read_all(server, range_id) |> Enum.map(& &1.value) == ["a", "b"]
    end

    test "explicit sync commits immediately", %{tmp_dir: directory} do
      server = start(directory, flush_interval_ms: 60_000)
      range_id = only_range(server)

      {:ok, _placements} = TopicServer.append(server, [record("a", "k0")])
      :ok = TopicServer.sync(server)

      assert {:ok, [record_read]} = TopicServer.read(server, range_id, 0, 10)
      assert record_read.value == "a"
    end
  end

  describe "concurrency" do
    test "serializes concurrent appends without losing records", %{tmp_dir: directory} do
      server = start(directory)

      1..50
      |> Enum.map(fn index ->
        Task.async(fn -> TopicServer.append(server, [record("v#{index}", "k#{index}")]) end)
      end)
      |> Enum.each(fn task -> assert {:ok, _placements} = Task.await(task) end)

      :ok = TopicServer.sync(server)
      range_id = only_range(server)
      assert server |> read_all(range_id) |> length() == 50
    end
  end

  describe "operations" do
    test "split, route, append and read through the server", %{tmp_dir: directory} do
      server = start(directory)
      root_id = only_range(server)
      {:ok, left_id, right_id} = TopicServer.split_range(server, root_id)

      records = for index <- 0..19, do: record("v#{index}", "k#{index}")
      {:ok, _placements} = TopicServer.append(server, records)
      :ok = TopicServer.sync(server)

      left_values = read_all(server, left_id) |> Enum.map(& &1.value)
      right_values = read_all(server, right_id) |> Enum.map(& &1.value)
      assert Enum.sort(left_values ++ right_values) == Enum.sort(Enum.map(records, & &1.value))

      assert {:ok, range} = TopicServer.route(server, "k0")
      assert range.id in [left_id, right_id]
      # route returns a metadata-only view (no file handle), safe outside the server
      refute Map.has_key?(range, :log)
    end

    test "merge through the server", %{tmp_dir: directory} do
      server = start(directory)
      root_id = only_range(server)
      {:ok, left_id, right_id} = TopicServer.split_range(server, root_id)
      assert {:ok, child_id} = TopicServer.merge_ranges(server, left_id, right_id)
      assert TopicServer.active_range_ids(server) == [child_id]
    end

    test "stream_history pages a range's cross-epoch history through the server",
         %{tmp_dir: directory} do
      server = start(directory)
      root_id = only_range(server)

      parent_records = for index <- 0..9, do: record("v#{index}", "k#{index}")
      {:ok, _placements} = TopicServer.append(server, parent_records)
      :ok = TopicServer.sync(server)
      {:ok, left_id, right_id} = TopicServer.split_range(server, root_id)

      # paging both children with a tiny page size reconstructs every parent record
      left = drain_history(server, left_id)
      right = drain_history(server, right_id)

      assert Enum.sort(Enum.map(left ++ right, & &1.value)) ==
               Enum.sort(Enum.map(parent_records, & &1.value))
    end

    test "sealing rejects further appends", %{tmp_dir: directory} do
      server = start(directory)
      :ok = TopicServer.seal(server)
      assert TopicServer.append(server, [record("a", "k0")]) == {:error, :sealed}
    end
  end

  # Drains a range's full cross-epoch history through the paged server API (tiny pages).
  defp drain_history(server, range_id, cursor \\ :start, accumulated \\ []) do
    case TopicServer.stream_history(server, range_id, cursor, 3) do
      {:ok, records, :done} -> [records | accumulated] |> Enum.reverse() |> List.flatten()
      {:ok, records, next_cursor} -> drain_history(server, range_id, next_cursor, [records | accumulated])
    end
  end

  # Polls `check` until it returns true or the deadline passes.
  defp eventually(check, remaining_ms \\ 1000) do
    cond do
      check.() -> true
      remaining_ms <= 0 -> false
      true -> Process.sleep(10) && eventually(check, remaining_ms - 10)
    end
  end
end
