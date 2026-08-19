defmodule Malachi.Storage.ElixirStoreTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Log.{Record, Segment}
  alias Malachi.Storage.ElixirStore

  @moduletag :tmp_dir

  defp rec(value, opts \\ []), do: Record.new(value, opts)

  defp open(directory, opts \\ []), do: ElixirStore.open(directory, "segment-0", opts)

  # One append+sync per record, so every record is its own clean frame boundary (what the damage
  # helpers below index into).
  defp seed_frames(directory, range, opts \\ []) do
    {:ok, store} = open(directory, opts)

    store =
      Enum.reduce(range, store, fn i, acc ->
        {:ok, acc, _first, _last} = ElixirStore.append(acc, [rec("v#{i}")])
        {:ok, acc} = ElixirStore.sync(acc)
        acc
      end)

    {:ok, store}
  end

  # Byte position where the frame holding record `index` starts.
  defp frame_position(path, index) do
    {pairs, _valid_bytes} = Record.decode_all(File.read!(path))
    {_record, position} = Enum.at(pairs, index)
    position
  end

  # Flips a byte inside the payload of frame `index` (past the 10-byte header), the bit-rot shape
  # the CRC exists to catch.
  defp corrupt_payload_byte(path, index), do: corrupt_byte_at(path, frame_position(path, index) + 12)

  defp corrupt_byte_at(path, position) do
    <<head::binary-size(position), byte, tail::binary>> = File.read!(path)
    File.write!(path, <<head::binary, Bitwise.bxor(byte, 0xFF), tail::binary>>)
  end

  defp truncate_to(path, bytes) do
    File.write!(path, binary_part(File.read!(path), 0, bytes))
  end

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

    test "rot in an ACTIVE segment keeps the valid frames that follow it", %{tmp_dir: directory} do
      # Sealing is a control-plane decision: a replica's file only carries a seal marker when its own
      # log rolled locally (by size or age), so a segment the cluster considers immutable usually has
      # no marker on disk. Keying the guard on the marker alone would leave the destructive path open
      # in exactly the deployment that matters, so the rule is about the DAMAGE: a checksum failure
      # means the frame was written whole and is wrong, and the frames after it may be perfectly
      # good, recoverable from a peer. Only a torn tail (the crash-mid-write shape) is dropped.
      {:ok, store} = seed_frames(directory, 0..4)
      :ok = ElixirStore.close(store)

      path = Segment.path(store.segment)
      size_before = File.stat!(path).size
      corrupt_payload_byte(path, 2)

      {:ok, recovered} = ElixirStore.recover(directory, "segment-0")

      assert File.stat!(path).size == size_before, "rot must not cost the valid frames after it"
      assert %{reason: :bad_crc, sealed?: false} = ElixirStore.integrity(recovered)
      # the copy still serves only its valid prefix; the rest waits for repair from a replica
      assert {:ok, records} = ElixirStore.read(recovered, 0, 10)
      assert Enum.map(records, & &1.value) == ["v0", "v1"]
    end

    test "recovering a corrupt SEALED segment keeps its bytes instead of truncating them",
         %{tmp_dir: directory} do
      # A sealed segment was fully durable when it was sealed, so a short scan means corruption at
      # rest, not a crash mid-write. Truncating there would destroy the valid frames AFTER the
      # damaged one, which at rf=1 is the difference between repairable and permanently lost, and
      # it happens silently on the next open. Recovery must leave the file alone and serve only the
      # valid prefix; the scrub reports it for repair from an intact replica.
      {:ok, store} = seed_frames(directory, 0..4)
      {:ok, store} = ElixirStore.seal(store)
      :ok = ElixirStore.close(store)

      path = Segment.path(store.segment)
      size_before = File.stat!(path).size
      corrupt_payload_byte(path, 2)

      {:ok, recovered} = ElixirStore.recover(directory, "segment-0")

      assert File.stat!(path).size == size_before, "a sealed segment must never be truncated by recovery"
      assert recovered.segment.record_count == 2
      assert {:ok, records} = ElixirStore.read(recovered, 0, 10)
      assert Enum.map(records, & &1.value) == ["v0", "v1"]
    end
  end

  describe "integrity/1" do
    test "a clean recovery reports :ok, and so does a freshly opened segment", %{tmp_dir: directory} do
      {:ok, store} = seed_frames(directory, 0..2)
      assert ElixirStore.integrity(store) == :ok
      :ok = ElixirStore.close(store)

      {:ok, recovered} = ElixirStore.recover(directory, "segment-0")
      assert ElixirStore.integrity(recovered) == :ok
    end

    test "a damaged SEALED segment reports the reason, position and unreadable bytes",
         %{tmp_dir: directory} do
      # The verdict exists so the caller can say something: recovery already scanned the file, and
      # without carrying its finding out the node would serve a short copy in silence.
      {:ok, store} = seed_frames(directory, 0..4)
      {:ok, store} = ElixirStore.seal(store)
      :ok = ElixirStore.close(store)

      path = Segment.path(store.segment)
      size = File.stat!(path).size
      position = frame_position(path, 2)
      corrupt_payload_byte(path, 2)

      {:ok, recovered} = ElixirStore.recover(directory, "segment-0")

      assert %{reason: :bad_crc, position: ^position, sealed?: true, unreadable_bytes: unreadable} =
               ElixirStore.integrity(recovered)

      assert unreadable == size - position
    end

    test "a torn tail on an ACTIVE segment reports what truncation dropped", %{tmp_dir: directory} do
      {:ok, store} = seed_frames(directory, 0..4)
      :ok = ElixirStore.close(store)
      path = Segment.path(store.segment)
      last_frame = frame_position(path, 4)
      truncate_to(path, last_frame + 3)

      {:ok, recovered} = ElixirStore.recover(directory, "segment-0")

      assert %{reason: :incomplete, position: ^last_frame, sealed?: false, unreadable_bytes: 3} =
               ElixirStore.integrity(recovered)

      # and the truncation still happened: an active segment's partial tail was never acked
      assert File.stat!(path).size == last_frame
    end
  end

  describe "a damaged sparse index never changes what a read returns" do
    # The sidecar has no checksum of its own and every sealed read trusts its byte positions, so a
    # rotted entry used to reproduce exactly the silent failure the record CRCs exist to prevent: the
    # read comes back empty (or short) and the broker reads that as a drained source.
    setup %{tmp_dir: directory} do
      {:ok, store} = seed_frames(directory, 0..9, index_interval: 1)
      {:ok, store} = ElixirStore.seal(store)
      :ok = ElixirStore.close(store)
      %{store: store, values: for(i <- 0..9, do: "v#{i}")}
    end

    defp open_sealed(directory, store) do
      {:ok, reader} =
        ElixirStore.open_read(directory, "segment-0", record_count: store.segment.record_count, base_offset: 0)

      reader
    end

    defp write_index(path, entries) do
      File.write!(path, for({offset, position} <- entries, into: <<>>, do: <<offset::64, position::64>>))
    end

    test "an entry pointing INSIDE a frame still reads every record", %{tmp_dir: directory} = context do
      # the shape that returned an empty page: decoding starts mid-frame and fails on the first byte
      write_index(Segment.index_path(context.store.segment), [
        {5, frame_position(Segment.path(context.store.segment), 5) + 3}
      ])

      reader = open_sealed(directory, context.store)
      assert {:ok, records} = ElixirStore.read(reader, 0, 100)
      assert Enum.map(records, & &1.value) == context.values
      assert {:ok, [record]} = ElixirStore.read(reader, 5, 1)
      assert record.value == "v5"
      :ok = ElixirStore.close(reader)
    end

    test "an entry pointing PAST the target does not skip records", %{tmp_dir: directory} = context do
      # a valid frame boundary, just the wrong one: the read would silently start after the target
      path = Segment.path(context.store.segment)
      write_index(Segment.index_path(context.store.segment), [{0, frame_position(path, 7)}])

      reader = open_sealed(directory, context.store)
      assert {:ok, records} = ElixirStore.read(reader, 0, 100)
      assert Enum.map(records, & &1.value) == context.values
      :ok = ElixirStore.close(reader)
    end

    test "an entry past the end of the file is harmless", %{tmp_dir: directory} = context do
      path = Segment.path(context.store.segment)
      write_index(Segment.index_path(context.store.segment), [{0, File.stat!(path).size * 4}])

      reader = open_sealed(directory, context.store)
      assert {:ok, records} = ElixirStore.read(reader, 0, 100)
      assert Enum.map(records, & &1.value) == context.values
      :ok = ElixirStore.close(reader)
    end

    test "no index at all still reads correctly (it is only a hint)", %{tmp_dir: directory} = context do
      File.rm!(Segment.index_path(context.store.segment))

      reader = open_sealed(directory, context.store)
      assert {:ok, records} = ElixirStore.read(reader, 3, 100)
      assert Enum.map(records, & &1.value) == Enum.drop(context.values, 3)
      :ok = ElixirStore.close(reader)
    end

    property "any single-byte corruption of the index leaves reads identical", context do
      %{tmp_dir: directory, store: store, values: values} = context
      index_path = Segment.index_path(store.segment)
      pristine = File.read!(index_path)

      check all(
              position <- StreamData.integer(0..(byte_size(pristine) - 1)),
              target <- StreamData.integer(0..9),
              max_runs: 50
            ) do
        corrupt_byte_at(index_path, position)

        reader = open_sealed(directory, store)
        result = ElixirStore.read(reader, target, 100)
        :ok = ElixirStore.close(reader)

        assert {:ok, records} = result
        assert Enum.map(records, & &1.value) == Enum.drop(values, target)

        File.write!(index_path, pristine)
      end
    end
  end

  describe "verify/3" do
    test "reports an intact segment with its record and byte counts", %{tmp_dir: directory} do
      {:ok, store} = seed_frames(directory, 0..4)
      :ok = ElixirStore.close(store)

      bytes = File.stat!(Segment.path(store.segment)).size
      assert ElixirStore.verify(directory, "segment-0") == {:ok, %{records: 5, bytes: bytes}}
    end

    test "reports a bad checksum with the position of the damaged frame", %{tmp_dir: directory} do
      {:ok, store} = seed_frames(directory, 0..4)
      :ok = ElixirStore.close(store)
      path = Segment.path(store.segment)
      frame_position = frame_position(path, 2)

      corrupt_payload_byte(path, 2)

      assert {:error, details} = ElixirStore.verify(directory, "segment-0")
      assert details.reason == :bad_crc
      assert details.position == frame_position
      assert details.file == path
    end

    test "reports a torn tail as :incomplete, not as a clean end of file", %{tmp_dir: directory} do
      {:ok, store} = seed_frames(directory, 0..4)
      :ok = ElixirStore.close(store)
      path = Segment.path(store.segment)
      last_frame = frame_position(path, 4)

      # cut the last frame in half: the scan runs out of bytes mid-frame
      truncate_to(path, last_frame + 3)

      assert {:error, %{reason: :incomplete, position: ^last_frame}} = ElixirStore.verify(directory, "segment-0")
    end

    test "reports a mangled frame header as :bad_magic", %{tmp_dir: directory} do
      {:ok, store} = seed_frames(directory, 0..2)
      :ok = ElixirStore.close(store)
      path = Segment.path(store.segment)
      frame_position = frame_position(path, 1)

      # the magic is the first byte of the frame, outside the CRC's coverage
      corrupt_byte_at(path, frame_position)

      assert {:error, %{reason: :bad_magic, position: ^frame_position}} = ElixirStore.verify(directory, "segment-0")
    end

    test "a corrupted length field is still caught, as an incomplete frame", %{tmp_dir: directory} do
      {:ok, store} = seed_frames(directory, 0..2)
      :ok = ElixirStore.close(store)
      path = Segment.path(store.segment)
      frame_position = frame_position(path, 1)

      # byte 2 of the header is the top byte of payload_length: inflating it makes the frame claim
      # far more bytes than the file holds. The CRC never covers this field, so the framing does.
      corrupt_byte_at(path, frame_position + 2)

      assert {:error, %{reason: reason, position: ^frame_position}} = ElixirStore.verify(directory, "segment-0")
      assert reason in [:incomplete, :bad_crc]
    end

    test "verifying a segment that is not stored here is :enoent, not a failure", %{tmp_dir: directory} do
      assert ElixirStore.verify(directory, "segment-0") == {:error, :enoent}
    end

    test "an absent index is not damage: it is only a hint", %{tmp_dir: directory} do
      {:ok, store} = seed_frames(directory, 0..3)
      :ok = ElixirStore.close(store)
      refute File.exists?(Segment.index_path(store.segment))

      assert {:ok, %{records: 4}} = ElixirStore.verify(directory, "segment-0")
    end

    test "a rotted index entry is reported as :bad_index", %{tmp_dir: directory} do
      {:ok, store} = seed_frames(directory, 0..9, index_interval: 1)
      {:ok, store} = ElixirStore.seal(store)
      :ok = ElixirStore.close(store)

      index_path = Segment.index_path(store.segment)
      corrupt_byte_at(index_path, byte_size(File.read!(index_path)) - 1)

      assert {:error, %{reason: :bad_index, file: ^index_path}} =
               ElixirStore.verify(directory, "segment-0", index_interval: 1)
    end

    test "damage to the records is reported before the index is even considered", %{tmp_dir: directory} do
      # An index rebuilt over damaged records would faithfully describe the damage, so the segment's
      # own verdict has to come first.
      {:ok, store} = seed_frames(directory, 0..9, index_interval: 1)
      {:ok, store} = ElixirStore.seal(store)
      :ok = ElixirStore.close(store)

      corrupt_payload_byte(Segment.path(store.segment), 4)
      corrupt_byte_at(Segment.index_path(store.segment), 0)

      assert {:error, %{reason: :bad_crc}} = ElixirStore.verify(directory, "segment-0", index_interval: 1)
    end

    test "rebuild_index writes an index the reads can trust again", %{tmp_dir: directory} do
      {:ok, store} = seed_frames(directory, 0..9, index_interval: 1)
      {:ok, store} = ElixirStore.seal(store)
      :ok = ElixirStore.close(store)

      index_path = Segment.index_path(store.segment)
      pristine = File.read!(index_path)
      corrupt_byte_at(index_path, 8)
      assert {:error, %{reason: :bad_index}} = ElixirStore.verify(directory, "segment-0", index_interval: 1)

      assert :ok = ElixirStore.rebuild_index(directory, "segment-0", index_interval: 1)

      assert {:ok, %{records: 10}} = ElixirStore.verify(directory, "segment-0", index_interval: 1)
      assert File.read!(index_path) == pristine, "a rebuild reproduces the index the seal wrote"
    end

    property "no single-byte corruption anywhere in a segment goes undetected" do
      check all(
              values <-
                StreamData.list_of(StreamData.string(:alphanumeric, min_length: 1), min_length: 1, max_length: 8),
              position_seed <- StreamData.positive_integer(),
              max_runs: 60
            ) do
        directory = Path.join(System.tmp_dir!(), "malachi_verify_prop_#{System.unique_integer([:positive])}")
        on_exit(fn -> File.rm_rf!(directory) end)

        {:ok, store} = ElixirStore.open(directory, "segment-0")
        {:ok, store, _first, _last} = ElixirStore.append(store, Enum.map(values, &rec/1))
        {:ok, store} = ElixirStore.sync(store)
        :ok = ElixirStore.close(store)

        path = Segment.path(store.segment)
        assert {:ok, _intact} = ElixirStore.verify(directory, "segment-0")

        # every byte is fair game: header (magic, length, checksum) and payload alike
        corrupt_byte_at(path, rem(position_seed, File.stat!(path).size))

        assert {:error, %{reason: _reason}} = ElixirStore.verify(directory, "segment-0"),
               "a corrupted segment must never verify as intact"

        File.rm_rf!(directory)
      end
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
      {:ok, store} = seed_frames(directory, 0..4)
      :ok = ElixirStore.close(store)

      # flip a byte in the payload of the 3rd frame (offset 2)
      corrupt_payload_byte(Segment.path(store.segment), 2)

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
