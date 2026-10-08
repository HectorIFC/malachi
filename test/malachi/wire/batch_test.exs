defmodule Malachi.Wire.BatchTest do
  # A batch comes from an untrusted client and can inflate to far more than it weighs on the wire, so every
  # bound is checked here, including the one that matters most: inflating a bomb costs a bounded amount of
  # memory, asserted by decoding inside a process the VM kills past a heap limit.
  use ExUnit.Case, async: true

  alias Malachi.Log.Record
  alias Malachi.Wire.Batch

  @mib 1_048_576

  defp records(n), do: for(i <- 1..n, do: {Record.new(String.duplicate("v", 100) <> "#{i}", key: "k#{i}"), false})

  # A plain entry's bytes, as a payload is built of them
  defp plain(record), do: <<0::8, Malachi.Wire.encode_record(record)::binary>>

  # A zstd frame that states its content size, with its blocks overwritten: the header reads, the body does
  # not inflate.
  defp corrupt_body(size) do
    frame = IO.iodata_to_binary(:zstd.compress(:binary.copy("abcdefgh", div(size, 8))))
    {:ok, %{headerSize: header, frameContentSize: ^size}} = :zstd.get_frame_header(frame)
    <<head::binary-size(header + 3), body::binary>> = frame
    head <> :binary.copy(<<0xFF>>, byte_size(body))
  end

  # <<codec, count, inflated_size, size, payload>> with any header, to build what no honest encoder would
  defp raw(codec, count, inflated_size, payload),
    do: <<Batch.codec_code(codec)::8, count::32, inflated_size::32, byte_size(payload)::32, payload::binary>>

  # A zstd frame of `total` zero bytes compressed in 1 MiB steps, so the test never holds `total` in memory.
  # A streamed frame declares no content size.
  defp streamed_zeros(total) do
    {:ok, context} = :zstd.context(:compress)
    chunk = :binary.copy(<<0>>, @mib)

    body =
      for _ <- 1..div(total, @mib), into: <<>> do
        {:continue, out} = :zstd.stream(context, chunk)
        IO.iodata_to_binary(out)
      end

    {:done, tail} = :zstd.finish(context, "")
    body <> IO.iodata_to_binary(tail)
  end

  # Runs `fun` in a process the VM kills once its memory passes `max_bytes`, and returns its result, or
  # :killed when the limit was hit. Shared binaries count: what an inflate produces are large binaries,
  # which live off the process heap, and a heap limit alone would never see them.
  defp within_heap(max_bytes, fun) do
    test = self()
    words = div(max_bytes, :erlang.system_info(:wordsize))

    {pid, ref} =
      :erlang.spawn_opt(fn -> send(test, {:result, fun.()}) end, [
        :monitor,
        {:max_heap_size, %{size: words, kill: true, error_logger: false, include_shared_binaries: true}}
      ])

    receive do
      {:result, result} ->
        receive do: ({:DOWN, ^ref, :process, ^pid, _} -> :ok)
        result

      {:DOWN, ^ref, :process, ^pid, :killed} ->
        :killed
    after
      30_000 -> flunk("the decoding process neither answered nor died")
    end
  end

  describe "round trips" do
    for codec <- [:none, :zstd] do
      test "plain records, #{codec}" do
        records = records(500)
        assert {:ok, decoded} = Batch.decode(Batch.encode(records, unquote(codec)), max_inflated_bytes: @mib)
        assert decoded == Enum.map(records, fn {record, false} -> {%{record | offset: nil}, false} end)
      end

      test "positioned records with a tombstone, #{codec}" do
        entries = [{{0, 5}, Record.new("a", key: "k"), false}, {{1, 0}, Record.new("", key: "k"), true}]
        batch = Batch.encode(entries, unquote(codec), :positioned)

        assert {:ok, [{{0, 5}, %Record{value: "a"}, false}, {{1, 0}, %Record{value: ""}, true}]} =
                 Batch.decode(batch, max_inflated_bytes: @mib, layout: :positioned)
      end
    end

    test "a tombstone in a produce batch" do
      entries = [{Record.new("a", key: "k"), false}, {Record.new("", key: "k"), true}]

      assert {:ok, [{%Record{value: "a"}, false}, {%Record{key: "k", value: ""}, true}]} =
               Batch.decode(Batch.encode(entries, :zstd), max_inflated_bytes: @mib)
    end

    test "an empty batch" do
      assert {:ok, []} = Batch.decode(Batch.encode([], :zstd), max_inflated_bytes: 0)
    end

    test "zstd makes a batch of similar records much smaller" do
      records = records(1000)
      assert byte_size(Batch.encode(records, :zstd)) * 10 < byte_size(Batch.encode(records, :none))
    end
  end

  describe "bounds, checked before any work" do
    test "a batch declaring more than the cap is refused" do
      batch = Batch.encode(records(10), :zstd)
      {%{inflated_size: size}, <<>>} = Batch.split(batch)
      assert {:error, :batch_too_large} = Batch.decode(batch, max_inflated_bytes: size - 1)
      assert {:ok, _} = Batch.decode(batch, max_inflated_bytes: size)
    end

    test "a zstd frame stating a content size other than the batch's is refused before it is inflated" do
      # Its body would not inflate at all, so only the header can tell the sizes apart.
      payload = corrupt_body(800_000)
      assert {:error, :batch_size_mismatch} = Batch.decode(raw(:zstd, 1, 100, payload), max_inflated_bytes: @mib)
    end

    test "an unknown codec is refused" do
      assert {:error, {:unknown_codec, 7}} = Batch.decode(<<7::8, 0::32, 0::32, 0::32>>, max_inflated_bytes: @mib)
    end

    test "a header or payload cut short is malformed" do
      assert {:error, :malformed_batch} = Batch.decode(<<0::8, 1::32>>, max_inflated_bytes: @mib)
      assert {:error, :malformed_batch} = Batch.decode(<<0::8, 0::32, 0::32, 5::32, "abc">>, max_inflated_bytes: @mib)
    end
  end

  describe "an inflate bomb" do
    test "a frame with no content size inflating past its declaration is refused, in bounded memory" do
      bomb = streamed_zeros(1024 * @mib)
      assert byte_size(bomb) < @mib
      batch = raw(:zstd, 1, @mib, bomb)

      # The batch says 1 MiB and inflates to 1 GiB. Inflating it whole would need a gigabyte; stopping at
      # the declaration needs about one block past it.
      assert within_heap(64 * @mib, fn -> Batch.decode(batch, max_inflated_bytes: 16 * @mib) end) ==
               {:error, :batch_size_mismatch}
    end

    test "a frame inflating to less than its declaration is refused" do
      payload = streamed_zeros(@mib)

      assert {:error, :batch_size_mismatch} =
               Batch.decode(raw(:zstd, 1, 2 * @mib, payload), max_inflated_bytes: 4 * @mib)
    end

    test "frames that each inflate to nothing still end the inflation, one per call" do
      # OTP answers each empty frame with three elements and no output, so only taking input ends the loop.
      empty = IO.iodata_to_binary(:zstd.compress(""))
      payload = :binary.copy(empty, 10_000)

      task = Task.async(fn -> Batch.decode(raw(:zstd, 0, 0, payload), max_inflated_bytes: @mib) end)
      assert {:ok, {:ok, []}} = Task.yield(task, 10_000)
    end

    test "bytes that are not a zstd frame are malformed, not a crash" do
      assert {:error, :malformed_batch} = Batch.decode(raw(:zstd, 1, 10, "garbage!garbage!"), max_inflated_bytes: @mib)
    end

    test "a frame whose header reads but whose body is corrupt is malformed, not a crash" do
      assert {:error, :malformed_batch} =
               Batch.decode(raw(:zstd, 1, 800_000, corrupt_body(800_000)), max_inflated_bytes: @mib)
    end

    test "a zstd payload shorter than any whole frame is malformed, whatever its header claims" do
      # OTP reads a header out of these bytes from memory it never wrote, so the answer would otherwise
      # change from one call to the next.
      empty = IO.iodata_to_binary(:zstd.compress(""))
      assert byte_size(empty) == 9
      assert {:ok, []} = Batch.decode(raw(:zstd, 0, 0, empty), max_inflated_bytes: @mib)

      for size <- 0..8 do
        short = binary_part(empty, 0, size)
        assert {:error, :malformed_batch} = Batch.decode(raw(:zstd, 0, 0, short), max_inflated_bytes: @mib)
      end
    end

    test "a plain payload whose size is not the declared one is refused" do
      payload = plain(Record.new("v"))

      assert {:error, :batch_size_mismatch} =
               Batch.decode(raw(:none, 1, byte_size(payload) + 1, payload), max_inflated_bytes: @mib)
    end
  end

  describe "the records inside" do
    test "more bytes than the count accounts for" do
      payload = IO.iodata_to_binary(for {r, false} <- records(2), do: plain(r))

      assert {:error, :batch_count_mismatch} =
               Batch.decode(raw(:none, 1, byte_size(payload), payload), max_inflated_bytes: @mib)
    end

    test "a count past the records, or a record cut short" do
      payload = plain(Record.new("v"))

      assert {:error, :malformed_batch} =
               Batch.decode(raw(:none, 2, byte_size(payload), payload), max_inflated_bytes: @mib)

      short = binary_part(payload, 0, byte_size(payload) - 1)
      assert {:error, :malformed_batch} = Batch.decode(raw(:none, 1, byte_size(short), short), max_inflated_bytes: @mib)
    end

    test "a reserved flag bit is refused, in either layout" do
      record = Malachi.Wire.encode_record(Record.new("v"))

      for flags <- [2, 3, 0x80, 0xFF] do
        plain = <<flags::8, record::binary>>

        assert {:error, :malformed_batch} =
                 Batch.decode(raw(:none, 1, byte_size(plain), plain), max_inflated_bytes: @mib)

        positioned = <<0::16, 7::64, flags::8, record::binary>>

        assert {:error, :malformed_batch} =
                 Batch.decode(raw(:none, 1, byte_size(positioned), positioned),
                   max_inflated_bytes: @mib,
                   layout: :positioned
                 )
      end
    end

    test "a positioned entry cut short in its position, or in its record" do
      entry = Batch.encode([{{0, 5}, Record.new("v"), false}], :none, :positioned)
      {_header, <<>>} = Batch.split(entry)
      <<_::binary-size(13), payload::binary>> = entry

      for size <- [5, byte_size(payload) - 1] do
        short = binary_part(payload, 0, size)

        assert {:error, :malformed_batch} =
                 Batch.decode(raw(:none, 1, size, short), max_inflated_bytes: @mib, layout: :positioned)
      end
    end
  end

  test "split takes one batch off the front and raises on one cut short" do
    batch = Batch.encode(records(2), :none)
    assert {%{codec: :none, count: 2}, "rest"} = Batch.split(batch <> "rest")
    assert_raise FunctionClauseError, fn -> Batch.split(binary_part(batch, 0, byte_size(batch) - 1)) end
  end
end
