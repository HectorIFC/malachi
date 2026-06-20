defmodule Malachi.Storage.ElixirStoreTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Log.{Record, Segment}
  alias Malachi.Storage.ElixirStore

  @moduletag :tmp_dir

  defp rec(value, opts \\ []), do: Record.new(value, opts)

  defp open(dir, opts \\ []), do: ElixirStore.open(dir, "seg-0", opts)

  describe "append / sync / read round-trip" do
    test "reads back appended records in order with sequential offsets", %{tmp_dir: dir} do
      {:ok, h} = open(dir)

      {:ok, h, first, last} =
        ElixirStore.append(h, [
          rec("a", key: "k1", headers: [{"h", "1"}]),
          rec("b"),
          rec("c", key: "")
        ])

      assert {first, last} == {0, 2}
      {:ok, h} = ElixirStore.sync(h)

      assert {:ok, records} = ElixirStore.read(h, 0, 10)
      assert Enum.map(records, & &1.value) == ["a", "b", "c"]
      assert Enum.map(records, & &1.offset) == [0, 1, 2]

      [r1, _r2, r3] = records
      assert r1.key == "k1"
      assert r1.headers == [{"h", "1"}]
      # nil key and empty-binary key are preserved distinctly
      assert Enum.at(records, 1).key == nil
      assert r3.key == ""

      :ok = ElixirStore.close(h)
    end

    test "buffered records are not readable before sync", %{tmp_dir: dir} do
      {:ok, h} = open(dir)
      {:ok, h, _, _} = ElixirStore.append(h, [rec("a"), rec("b")])

      # nothing committed yet
      assert ElixirStore.read(h, 0, 10) == :eof
      assert ElixirStore.next_offset(h) == 2

      {:ok, h} = ElixirStore.sync(h)
      assert {:ok, [_, _]} = ElixirStore.read(h, 0, 10)
    end

    test "respects max_records and mid-stream offset", %{tmp_dir: dir} do
      {:ok, h} = open(dir)
      records = for i <- 0..9, do: rec("v#{i}")
      {:ok, h, _, _} = ElixirStore.append(h, records)
      {:ok, h} = ElixirStore.sync(h)

      assert {:ok, got} = ElixirStore.read(h, 3, 4)
      assert Enum.map(got, & &1.offset) == [3, 4, 5, 6]
      assert Enum.map(got, & &1.value) == ["v3", "v4", "v5", "v6"]
    end

    test "eof at/after committed end, out_of_range below base", %{tmp_dir: dir} do
      {:ok, h} = open(dir, base_offset: 100)
      {:ok, h, first, _} = ElixirStore.append(h, [rec("a")])
      assert first == 100
      {:ok, h} = ElixirStore.sync(h)

      assert {:ok, [r]} = ElixirStore.read(h, 100, 10)
      assert r.offset == 100
      assert ElixirStore.read(h, 101, 10) == :eof
      assert ElixirStore.read(h, 99, 10) == {:error, :out_of_range}
    end

    test "sync with nothing pending is a durable no-op", %{tmp_dir: dir} do
      {:ok, h} = open(dir)
      assert {:ok, _h} = ElixirStore.sync(h)
    end
  end

  describe "size-based auto-flush" do
    test "commits automatically once the buffer reaches flush_bytes", %{tmp_dir: dir} do
      # tiny threshold so any append triggers an automatic flush+fsync
      {:ok, h} = ElixirStore.open(dir, "seg-0", flush_bytes: 1)
      {:ok, h, _, _} = ElixirStore.append(h, [rec("a"), rec("b")])

      # readable WITHOUT an explicit sync, and the buffer was drained
      assert {:ok, recs} = ElixirStore.read(h, 0, 10)
      assert Enum.map(recs, & &1.value) == ["a", "b"]
      assert h.pending_count == 0
    end

    test "does not auto-flush below the threshold", %{tmp_dir: dir} do
      # default flush_bytes (10MB) is never reached by a single small record
      {:ok, h} = open(dir)
      {:ok, h, _, _} = ElixirStore.append(h, [rec("a")])

      assert ElixirStore.read(h, 0, 10) == :eof
      assert h.pending_count == 1
    end
  end

  describe "sealing" do
    test "seal makes the segment immutable but still readable", %{tmp_dir: dir} do
      {:ok, h} = open(dir)
      {:ok, h, _, _} = ElixirStore.append(h, [rec("a"), rec("b")])
      {:ok, h} = ElixirStore.seal(h)

      assert Segment.sealed?(h.segment)
      assert ElixirStore.append(h, [rec("c")]) == {:error, :sealed}
      assert {:ok, [_, _]} = ElixirStore.read(h, 0, 10)
      assert File.exists?(Segment.index_path(h.segment))
      assert File.exists?(Segment.seal_marker_path(h.segment))
    end
  end

  describe "open" do
    test "refuses to clobber an existing segment", %{tmp_dir: dir} do
      {:ok, _h} = open(dir)
      assert open(dir) == {:error, :already_exists}
    end
  end

  describe "recovery" do
    test "recovers committed records across multiple flushes and resumes appending",
         %{tmp_dir: dir} do
      {:ok, h} = open(dir, index_interval: 64)
      {:ok, h, _, _} = ElixirStore.append(h, [rec("a"), rec("b")])
      {:ok, h} = ElixirStore.sync(h)
      {:ok, h, _, _} = ElixirStore.append(h, [rec("c")])
      {:ok, h} = ElixirStore.sync(h)
      :ok = ElixirStore.close(h)

      {:ok, h2} = ElixirStore.recover(dir, "seg-0", index_interval: 64)
      assert h2.segment.record_count == 3
      assert ElixirStore.next_offset(h2) == 3
      assert {:ok, recs} = ElixirStore.read(h2, 0, 10)
      assert Enum.map(recs, & &1.value) == ["a", "b", "c"]

      # can continue appending after recovery
      {:ok, h2, first, _} = ElixirStore.append(h2, [rec("d")])
      assert first == 3
      {:ok, h2} = ElixirStore.sync(h2)
      assert {:ok, [d]} = ElixirStore.read(h2, 3, 1)
      assert d.value == "d"
    end

    test "recovers a sealed segment as sealed", %{tmp_dir: dir} do
      {:ok, h} = open(dir)
      {:ok, h, _, _} = ElixirStore.append(h, [rec("a")])
      {:ok, h} = ElixirStore.seal(h)
      :ok = ElixirStore.close(h)

      {:ok, h2} = ElixirStore.recover(dir, "seg-0")
      assert Segment.sealed?(h2.segment)
      assert ElixirStore.append(h2, [rec("b")]) == {:error, :sealed}
    end

    test "drops a partial trailing write (simulated crash mid-append)", %{tmp_dir: dir} do
      {:ok, h} = open(dir)
      # one frame per flush => each record is a clean frame boundary
      h =
        Enum.reduce(0..4, h, fn i, acc ->
          {:ok, acc, _, _} = ElixirStore.append(acc, [rec("v#{i}")])
          {:ok, acc} = ElixirStore.sync(acc)
          acc
        end)

      :ok = ElixirStore.close(h)

      # truncate the last few bytes => last frame becomes incomplete
      path = Segment.path(h.segment)
      size = File.stat!(path).size
      {:ok, fd} = :file.open(path, [:read, :write, :raw, :binary])
      {:ok, _} = :file.position(fd, size - 3)
      :ok = :file.truncate(fd)
      :file.close(fd)

      {:ok, h2} = ElixirStore.recover(dir, "seg-0")
      assert h2.segment.record_count == 4
      assert {:ok, recs} = ElixirStore.read(h2, 0, 10)
      assert Enum.map(recs, & &1.value) == ["v0", "v1", "v2", "v3"]
      # file was truncated to the last valid frame boundary
      assert File.stat!(path).size == h2.segment.byte_size
    end

    test "stops at a corrupted frame (bit-rot), keeping the valid prefix", %{tmp_dir: dir} do
      {:ok, h} = open(dir)

      h =
        Enum.reduce(0..4, h, fn i, acc ->
          {:ok, acc, _, _} = ElixirStore.append(acc, [rec("v#{i}")])
          {:ok, acc} = ElixirStore.sync(acc)
          acc
        end)

      :ok = ElixirStore.close(h)
      path = Segment.path(h.segment)

      # find the start position of the 3rd frame (offset 2) and flip a byte in its payload
      {pairs, _} = Record.decode_all(File.read!(path))
      {_rec, frame2_pos} = Enum.at(pairs, 2)
      data = File.read!(path)
      flip_at = frame2_pos + 12
      <<head::binary-size(flip_at), b, tail::binary>> = data
      File.write!(path, <<head::binary, Bitwise.bxor(b, 0xFF), tail::binary>>)

      {:ok, h2} = ElixirStore.recover(dir, "seg-0")
      assert h2.segment.record_count == 2
      assert {:ok, recs} = ElixirStore.read(h2, 0, 10)
      assert Enum.map(recs, & &1.value) == ["v0", "v1"]
    end
  end

  describe "sparse index" do
    test "seeking works with many index entries (small interval)", %{tmp_dir: dir} do
      {:ok, h} = open(dir, index_interval: 32)
      records = for i <- 0..199, do: rec(String.duplicate("x", 20), key: "k#{i}")
      {:ok, h, _, _} = ElixirStore.append(h, records)
      {:ok, h} = ElixirStore.sync(h)

      assert length(h.index) > 1, "expected multiple sparse index entries"

      for target <- [0, 1, 57, 128, 199] do
        assert {:ok, [r]} = ElixirStore.read(h, target, 1)
        assert r.offset == target
      end
    end
  end

  describe "Record framing" do
    test "encode/decode round-trips key, value, headers", _ctx do
      r = %Record{Record.new("payload", key: "k", headers: [{"a", "1"}, {"b", "2"}]) | offset: 7}
      frame = Record.encode(r)
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
      dir = Path.join(System.tmp_dir!(), "ng_prop_#{System.unique_integer([:positive])}")
      File.rm_rf!(dir)
      {:ok, h} = ElixirStore.open(dir, "seg-0", index_interval: 64)
      {:ok, h, first, last} = ElixirStore.append(h, records)
      {:ok, h} = ElixirStore.sync(h)

      assert first == 0
      assert last == length(records) - 1

      {:ok, got} = ElixirStore.read(h, 0, length(records))
      assert length(got) == length(records)
      assert Enum.map(got, & &1.offset) == Enum.to_list(0..(length(records) - 1))
      assert Enum.map(got, & &1.value) == Enum.map(records, & &1.value)
      assert Enum.map(got, & &1.key) == Enum.map(records, & &1.key)
      assert Enum.map(got, & &1.headers) == Enum.map(records, & &1.headers)

      :ok = ElixirStore.close(h)
      File.rm_rf!(dir)
    end
  end

  property "reading from any committed offset returns the correct suffix" do
    check all(
            records <- StreamData.list_of(record_gen(), min_length: 1, max_length: 30),
            max_runs: 30
          ) do
      dir = Path.join(System.tmp_dir!(), "ng_prop_#{System.unique_integer([:positive])}")
      File.rm_rf!(dir)
      {:ok, h} = ElixirStore.open(dir, "seg-0", index_interval: 48)
      {:ok, h, _, _} = ElixirStore.append(h, records)
      {:ok, h} = ElixirStore.sync(h)

      n = length(records)

      for start <- 0..(n - 1) do
        {:ok, got} = ElixirStore.read(h, start, n)
        assert Enum.map(got, & &1.offset) == Enum.to_list(start..(n - 1))
      end

      :ok = ElixirStore.close(h)
      File.rm_rf!(dir)
    end
  end
end
