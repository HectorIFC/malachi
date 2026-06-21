defmodule Malachi.LogTest do
  use ExUnit.Case, async: true

  alias Malachi.Log
  alias Malachi.Log.{Record, Segment}

  @moduletag :tmp_dir

  defp rec(value, opts \\ []), do: Record.new(value, opts)

  # Small segments so a handful of records forces several rolls.
  defp open(directory), do: Log.open(directory, max_bytes: 120, index_interval: 32)

  # Read the whole log by paging across segment boundaries.
  defp read_all(log) do
    read_all(log, 0, [])
  end

  defp read_all(log, offset, acc) do
    case Log.read(log, offset, 100) do
      :eof -> acc |> Enum.reverse() |> List.flatten()
      {:ok, records} -> read_all(log, offset + length(records), [records | acc])
    end
  end

  defp append_sync(log, record) do
    {:ok, log, _, _} = Log.append(log, [record])
    {:ok, log} = Log.sync(log)
    log
  end

  describe "rolling across segments" do
    test "appending past max_bytes creates multiple segments and reads span them",
         %{tmp_dir: directory} do
      {:ok, log} = open(directory)
      log = Enum.reduce(0..9, log, fn i, acc -> append_sync(acc, rec("value-#{i}")) end)

      assert log.sealed_base_offsets != [], "expected at least one roll"
      assert length(Path.wildcard(Path.join(directory, "*.log"))) >= 2

      values = log |> read_all() |> Enum.map(& &1.value)
      assert values == for(i <- 0..9, do: "value-#{i}")

      offsets = log |> read_all() |> Enum.map(& &1.offset)
      assert offsets == Enum.to_list(0..9)

      :ok = Log.close(log)
    end

    test "reads from a sealed segment use the persisted index", %{tmp_dir: directory} do
      {:ok, log} = open(directory)
      log = Enum.reduce(0..9, log, fn i, acc -> append_sync(acc, rec("v#{i}")) end)

      # offset 0 is guaranteed to live in a sealed (non-active) segment after rolls
      [first_base | _] = log.sealed_base_offsets
      assert {:ok, [record]} = Log.read(log, first_base, 1)
      assert record.offset == first_base

      :ok = Log.close(log)
    end

    test "explicit roll seals the active segment", %{tmp_dir: directory} do
      {:ok, log} = Log.open(directory)
      {:ok, log, _, _} = Log.append(log, [rec("a"), rec("b")])
      {:ok, log} = Log.sync(log)
      {:ok, log} = Log.roll(log)

      assert log.active == nil
      assert log.sealed_base_offsets == [0]

      {:ok, log, first, _} = Log.append(log, [rec("c")])
      assert first == 2
      {:ok, log} = Log.sync(log)
      assert log |> read_all() |> Enum.map(& &1.value) == ["a", "b", "c"]
    end
  end

  describe "read bounds" do
    test "eof past the end and out_of_range below the start", %{tmp_dir: directory} do
      {:ok, log} = Log.open(directory, base_offset: 50)
      log = append_sync(log, rec("only"))

      assert {:ok, [record]} = Log.read(log, 50, 10)
      assert record.offset == 50
      assert Log.read(log, 51, 10) == :eof
      assert Log.read(log, 49, 10) == {:error, :out_of_range}
    end

    test "read on an empty log is eof", %{tmp_dir: directory} do
      {:ok, log} = Log.open(directory)
      assert Log.read(log, 0, 10) == :eof
    end

    test "appending an empty list is a no-op (not a crash)", %{tmp_dir: directory} do
      {:ok, log} = Log.open(directory)
      assert {:ok, _log, 0, -1} = Log.append(log, [])
    end
  end

  describe "recovery" do
    test "recovers sealed + active segments and resumes appending", %{tmp_dir: directory} do
      {:ok, log} = open(directory)
      log = Enum.reduce(0..9, log, fn i, acc -> append_sync(acc, rec("v#{i}")) end)
      next = log.next_offset
      sealed_before = log.sealed_base_offsets
      :ok = Log.close(log)

      {:ok, log2} = Log.recover(directory, max_bytes: 120, index_interval: 32)
      assert log2.next_offset == next
      assert log2.sealed_base_offsets == sealed_before
      assert log2 |> read_all() |> Enum.map(& &1.value) == for(i <- 0..9, do: "v#{i}")

      # continue appending after recovery
      {:ok, log2, first, _} = Log.append(log2, [rec("after")])
      assert first == next
      {:ok, log2} = Log.sync(log2)
      assert {:ok, [record]} = Log.read(log2, next, 1)
      assert record.value == "after"

      :ok = Log.close(log2)
    end

    test "recovers a log whose last segment was sealed", %{tmp_dir: directory} do
      {:ok, log} = Log.open(directory)
      {:ok, log, _, _} = Log.append(log, [rec("a"), rec("b")])
      {:ok, log} = Log.sync(log)
      {:ok, log} = Log.roll(log)
      :ok = Log.close(log)

      {:ok, log2} = Log.recover(directory)
      assert log2.active == nil
      assert log2.next_offset == 2
      assert log2 |> read_all() |> Enum.map(& &1.value) == ["a", "b"]

      # next append opens a fresh active segment at the recovered offset
      {:ok, log2, first, _} = Log.append(log2, [rec("c")])
      assert first == 2
      {:ok, log2} = Log.sync(log2)
      assert log2 |> read_all() |> Enum.map(& &1.value) == ["a", "b", "c"]
    end

    test "recovers an empty directory as a fresh log", %{tmp_dir: directory} do
      {:ok, log} = Log.recover(directory, base_offset: 7)
      assert log.next_offset == 7
      assert log.sealed_base_offsets == []
      assert Log.read(log, 7, 10) == :eof
    end

    test "recovery drops a partial trailing write in the active segment", %{tmp_dir: directory} do
      {:ok, log} = open(directory)
      log = Enum.reduce(0..9, log, fn i, acc -> append_sync(acc, rec("v#{i}")) end)
      :ok = Log.close(log)

      # corrupt the tail of the active (last) segment file
      active_path =
        directory |> Path.join("*.log") |> Path.wildcard() |> Enum.sort() |> List.last()

      size = File.stat!(active_path).size
      {:ok, file_descriptor} = :file.open(active_path, [:read, :write, :raw, :binary])
      {:ok, _} = :file.position(file_descriptor, size - 3)
      :ok = :file.truncate(file_descriptor)
      :file.close(file_descriptor)

      {:ok, log2} = Log.recover(directory, max_bytes: 120, index_interval: 32)
      # exactly the last record is lost; everything before survives
      values = log2 |> read_all() |> Enum.map(& &1.value)
      assert values == for(i <- 0..8, do: "v#{i}")
      assert log2.next_offset == 9
    end
  end

  describe "segment naming" do
    test "segment files are named by zero-padded base offset", %{tmp_dir: directory} do
      {:ok, log} = Log.open(directory)
      log = append_sync(log, rec("x"))
      assert File.exists?(Path.join(directory, "00000000000000000000.log"))
      # sanity: Segment derives the same path
      segment = Segment.new("00000000000000000000", directory, base_offset: 0)
      assert File.exists?(Segment.path(segment))
      :ok = Log.close(log)
    end
  end
end
