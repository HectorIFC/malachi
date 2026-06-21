defmodule Malachi.Storage.ElixirStoreTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Log.{Record, Segment}
  alias Malachi.Storage.ElixirStore

  @moduletag :tmp_dir

  defp rec(value, opts \\ []), do: Record.new(value, opts)

  defp open(directory, opts \\ []), do: ElixirStore.open(directory, "segment-0", opts)

  describe "append / sync / read round-trip" do
    test "reads back appended records in order with sequential offsets", %{tmp_dir: directory} do
      {:ok, store} = open(directory)

      {:ok, store, first, last} =
        ElixirStore.append(store, [
          rec("a", key: "k1", headers: [{"h", "1"}]),
          rec("b"),
          rec("c", key: "")
        ])

      assert {first, last} == {0, 2}
      {:ok, store} = ElixirStore.sync(store)

      assert {:ok, records} = ElixirStore.read(store, 0, 10)
      assert Enum.map(records, & &1.value) == ["a", "b", "c"]
      assert Enum.map(records, & &1.offset) == [0, 1, 2]

      [first_record, _second_record, third_record] = records
      assert first_record.key == "k1"
      assert first_record.headers == [{"h", "1"}]
      # nil key and empty-binary key are preserved distinctly
      assert Enum.at(records, 1).key == nil
      assert third_record.key == ""

      :ok = ElixirStore.close(store)
    end

    test "buffered records are not readable before sync", %{tmp_dir: directory} do
      {:ok, store} = open(directory)
      {:ok, store, _, _} = ElixirStore.append(store, [rec("a"), rec("b")])

      # nothing committed yet
      assert ElixirStore.read(store, 0, 10) == :eof
      assert ElixirStore.next_offset(store) == 2

      {:ok, store} = ElixirStore.sync(store)
      assert {:ok, [_, _]} = ElixirStore.read(store, 0, 10)
    end

    test "respects max_records and mid-stream offset", %{tmp_dir: directory} do
      {:ok, store} = open(directory)
      records = for i <- 0..9, do: rec("v#{i}")
      {:ok, store, _, _} = ElixirStore.append(store, records)
      {:ok, store} = ElixirStore.sync(store)

      assert {:ok, got} = ElixirStore.read(store, 3, 4)
      assert Enum.map(got, & &1.offset) == [3, 4, 5, 6]
      assert Enum.map(got, & &1.value) == ["v3", "v4", "v5", "v6"]
    end

    test "eof at/after committed end, out_of_range below base", %{tmp_dir: directory} do
      {:ok, store} = open(directory, base_offset: 100)
      {:ok, store, first, _} = ElixirStore.append(store, [rec("a")])
      assert first == 100
      {:ok, store} = ElixirStore.sync(store)

      assert {:ok, [record]} = ElixirStore.read(store, 100, 10)
      assert record.offset == 100
      assert ElixirStore.read(store, 101, 10) == :eof
      assert ElixirStore.read(store, 99, 10) == {:error, :out_of_range}
    end

    test "sync with nothing pending is a durable no-op", %{tmp_dir: directory} do
      {:ok, store} = open(directory)
      assert {:ok, _store} = ElixirStore.sync(store)
    end
  end

  describe "size-based auto-flush" do
    test "commits automatically once the buffer reaches flush_bytes", %{tmp_dir: directory} do
      # tiny threshold so any append triggers an automatic flush+fsync
      {:ok, store} = ElixirStore.open(directory, "segment-0", flush_bytes: 1)
      {:ok, store, _, _} = ElixirStore.append(store, [rec("a"), rec("b")])

      # readable WITHOUT an explicit sync, and the buffer was drained
      assert {:ok, records} = ElixirStore.read(store, 0, 10)
      assert Enum.map(records, & &1.value) == ["a", "b"]
      assert store.pending_count == 0
    end

    test "does not auto-flush below the threshold", %{tmp_dir: directory} do
      # default flush_bytes (10MB) is never reached by a single small record
      {:ok, store} = open(directory)
      {:ok, store, _, _} = ElixirStore.append(store, [rec("a")])

      assert ElixirStore.read(store, 0, 10) == :eof
      assert store.pending_count == 1
    end

    test "commits automatically once the buffer reaches flush_count records",
         %{tmp_dir: directory} do
      {:ok, store} = ElixirStore.open(directory, "segment-0", flush_count: 2)
      {:ok, store, _, _} = ElixirStore.append(store, [rec("a"), rec("b")])

      assert {:ok, records} = ElixirStore.read(store, 0, 10)
      assert Enum.map(records, & &1.value) == ["a", "b"]
      assert store.pending_count == 0
    end

    test "pending? reflects whether there is unflushed data", %{tmp_dir: directory} do
      {:ok, store} = open(directory)
      refute ElixirStore.pending?(store)
      {:ok, store, _, _} = ElixirStore.append(store, [rec("a")])
      assert ElixirStore.pending?(store)
      {:ok, store} = ElixirStore.sync(store)
      refute ElixirStore.pending?(store)
    end
  end

  describe "sealing" do
    test "seal makes the segment immutable but still readable", %{tmp_dir: directory} do
      {:ok, store} = open(directory)
      {:ok, store, _, _} = ElixirStore.append(store, [rec("a"), rec("b")])
      {:ok, store} = ElixirStore.seal(store)

      assert Segment.sealed?(store.segment)
      assert ElixirStore.append(store, [rec("c")]) == {:error, :sealed}
      assert {:ok, [_, _]} = ElixirStore.read(store, 0, 10)
      assert File.exists?(Segment.index_path(store.segment))
      assert File.exists?(Segment.seal_marker_path(store.segment))
    end
  end

  describe "open" do
    test "refuses to clobber an existing segment", %{tmp_dir: directory} do
      {:ok, _store} = open(directory)
      assert open(directory) == {:error, :already_exists}
    end
  end

  describe "recovery" do
    test "recovers committed records across multiple flushes and resumes appending",
         %{tmp_dir: directory} do
      {:ok, store} = open(directory, index_interval: 64)
      {:ok, store, _, _} = ElixirStore.append(store, [rec("a"), rec("b")])
      {:ok, store} = ElixirStore.sync(store)
      {:ok, store, _, _} = ElixirStore.append(store, [rec("c")])
      {:ok, store} = ElixirStore.sync(store)
      :ok = ElixirStore.close(store)

      {:ok, recovered} = ElixirStore.recover(directory, "segment-0", index_interval: 64)
      assert recovered.segment.record_count == 3
      assert ElixirStore.next_offset(recovered) == 3
      assert {:ok, records} = ElixirStore.read(recovered, 0, 10)
      assert Enum.map(records, & &1.value) == ["a", "b", "c"]

      # can continue appending after recovery
      {:ok, recovered, first, _} = ElixirStore.append(recovered, [rec("d")])
      assert first == 3
      {:ok, recovered} = ElixirStore.sync(recovered)
      assert {:ok, [record]} = ElixirStore.read(recovered, 3, 1)
      assert record.value == "d"
    end

    test "recovers a sealed segment as sealed", %{tmp_dir: directory} do
      {:ok, store} = open(directory)
      {:ok, store, _, _} = ElixirStore.append(store, [rec("a")])
      {:ok, store} = ElixirStore.seal(store)
      :ok = ElixirStore.close(store)

      {:ok, recovered} = ElixirStore.recover(directory, "segment-0")
      assert Segment.sealed?(recovered.segment)
      assert ElixirStore.append(recovered, [rec("b")]) == {:error, :sealed}
    end

    test "drops a partial trailing write (simulated crash mid-append)", %{tmp_dir: directory} do
      {:ok, store} = open(directory)
      # one frame per flush => each record is a clean frame boundary
      store =
        Enum.reduce(0..4, store, fn i, acc ->
          {:ok, acc, _, _} = ElixirStore.append(acc, [rec("v#{i}")])
          {:ok, acc} = ElixirStore.sync(acc)
          acc
        end)

      :ok = ElixirStore.close(store)

      # truncate the last few bytes => last frame becomes incomplete
      path = Segment.path(store.segment)
      size = File.stat!(path).size
      {:ok, file_descriptor} = :file.open(path, [:read, :write, :raw, :binary])
      {:ok, _} = :file.position(file_descriptor, size - 3)
      :ok = :file.truncate(file_descriptor)
      :file.close(file_descriptor)

      {:ok, recovered} = ElixirStore.recover(directory, "segment-0")
      assert recovered.segment.record_count == 4
      assert {:ok, records} = ElixirStore.read(recovered, 0, 10)
      assert Enum.map(records, & &1.value) == ["v0", "v1", "v2", "v3"]
      # file was truncated to the last valid frame boundary
      assert File.stat!(path).size == recovered.segment.byte_size
    end

    test "stops at a corrupted frame (bit-rot), keeping the valid prefix", %{tmp_dir: directory} do
      {:ok, store} = open(directory)

      store =
        Enum.reduce(0..4, store, fn i, acc ->
          {:ok, acc, _, _} = ElixirStore.append(acc, [rec("v#{i}")])
          {:ok, acc} = ElixirStore.sync(acc)
          acc
        end)

      :ok = ElixirStore.close(store)
      path = Segment.path(store.segment)

      # find the start position of the 3rd frame (offset 2) and flip a byte in its payload
      {pairs, _} = Record.decode_all(File.read!(path))
      {_rec, frame2_pos} = Enum.at(pairs, 2)
      data = File.read!(path)
      flip_at = frame2_pos + 12
      <<head::binary-size(flip_at), byte, tail::binary>> = data
      File.write!(path, <<head::binary, Bitwise.bxor(byte, 0xFF), tail::binary>>)

      {:ok, recovered} = ElixirStore.recover(directory, "segment-0")
      assert recovered.segment.record_count == 2
      assert {:ok, records} = ElixirStore.read(recovered, 0, 10)
      assert Enum.map(records, & &1.value) == ["v0", "v1"]
    end
  end

  describe "sparse index" do
    test "seeking works with many index entries (small interval)", %{tmp_dir: directory} do
      {:ok, store} = open(directory, index_interval: 32)
      records = for i <- 0..199, do: rec(String.duplicate("x", 20), key: "k#{i}")
      {:ok, store, _, _} = ElixirStore.append(store, records)
      {:ok, store} = ElixirStore.sync(store)

      assert :array.size(store.index) > 1, "expected multiple sparse index entries"

      for target <- [0, 1, 57, 128, 199] do
        assert {:ok, [record]} = ElixirStore.read(store, target, 1)
        assert record.offset == target
      end
    end
  end

  describe "Record framing" do
    test "encode/decode round-trips key, value, headers", _ctx do
      record = %Record{Record.new("payload", key: "k", headers: [{"a", "1"}, {"b", "2"}]) | offset: 7}
      frame = Record.encode(record)
      assert {:ok, decoded, size, <<>>} = Record.decode_one(frame)
      assert size == byte_size(frame)
      assert decoded.offset == 7
      assert decoded.value == "payload"
      assert decoded.key == "k"
      assert decoded.headers == [{"a", "1"}, {"b", "2"}]
    end

    test "decode_one reports incomplete and bad framing", _ctx do
      frame = Record.encode(%Record{Record.new("v") | offset: 0})
      <<partial::binary-size(byte_size(frame) - 2), _::binary>> = frame
      assert Record.decode_one(partial) == :incomplete
      assert Record.decode_one(<<0, 0, 0>>) == :incomplete
      assert Record.decode_one(<<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11>>) == {:error, :bad_magic}
    end
  end

  # -------- property-based --------

  defp record_gen do
    gen all(
          value <- StreamData.binary(max_length: 64),
          key <- StreamData.one_of([StreamData.constant(nil), StreamData.binary(max_length: 16)]),
          headers <-
            StreamData.list_of(
              StreamData.tuple({StreamData.binary(max_length: 8), StreamData.binary(max_length: 8)}),
              max_length: 3
            )
        ) do
      Record.new(value, key: key, headers: headers)
    end
  end

  property "append + sync + read preserves the full sequence and assigns sequential offsets" do
    check all(
            records <- StreamData.list_of(record_gen(), min_length: 1, max_length: 50),
            max_runs: 50
          ) do
      directory = Path.join(System.tmp_dir!(), "ng_prop_#{System.unique_integer([:positive])}")
      File.rm_rf!(directory)
      {:ok, store} = ElixirStore.open(directory, "segment-0", index_interval: 64)
      {:ok, store, first, last} = ElixirStore.append(store, records)
      {:ok, store} = ElixirStore.sync(store)

      assert first == 0
      assert last == length(records) - 1

      {:ok, got} = ElixirStore.read(store, 0, length(records))
      assert length(got) == length(records)
      assert Enum.map(got, & &1.offset) == Enum.to_list(0..(length(records) - 1))
      assert Enum.map(got, & &1.value) == Enum.map(records, & &1.value)
      assert Enum.map(got, & &1.key) == Enum.map(records, & &1.key)
      assert Enum.map(got, & &1.headers) == Enum.map(records, & &1.headers)

      :ok = ElixirStore.close(store)
      File.rm_rf!(directory)
    end
  end

  property "reading from any committed offset returns the correct suffix" do
    check all(
            records <- StreamData.list_of(record_gen(), min_length: 1, max_length: 30),
            max_runs: 30
          ) do
      directory = Path.join(System.tmp_dir!(), "ng_prop_#{System.unique_integer([:positive])}")
      File.rm_rf!(directory)
      {:ok, store} = ElixirStore.open(directory, "segment-0", index_interval: 48)
      {:ok, store, _, _} = ElixirStore.append(store, records)
      {:ok, store} = ElixirStore.sync(store)

      n = length(records)

      for start <- 0..(n - 1) do
        {:ok, got} = ElixirStore.read(store, start, n)
        assert Enum.map(got, & &1.offset) == Enum.to_list(start..(n - 1))
      end

      :ok = ElixirStore.close(store)
      File.rm_rf!(directory)
    end
  end
end
