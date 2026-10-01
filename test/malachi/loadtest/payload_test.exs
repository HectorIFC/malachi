defmodule Malachi.Loadtest.PayloadTest do
  # The values both load generators send. The golden vectors are shared with scripts/lib/payload.selftest.js
  # (test/scripts/payload_js_test.exs runs it), so a change here that the Node side does not make fails one
  # suite or the other; the compression band is what fails when constant bytes come back under the json name.
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Loadtest.Payload

  @fixture Path.expand("../../support/fixtures/loadtest/payload_vectors.json", __DIR__)

  setup_all do
    %{vectors: @fixture |> File.read!() |> Jason.decode!()}
  end

  defp sha256(values), do: :sha256 |> :crypto.hash(values) |> Base.encode16(case: :lower)

  describe "the generator" do
    test "is xoshiro128** seeded with SplitMix64, output for output", %{vectors: vectors} do
      for {seed, expected} <- vectors["prng"] do
        {outputs, _state} =
          Enum.map_reduce(expected, Payload.seed(String.to_integer(seed)), fn _, s -> Payload.next(s) end)

        assert outputs == expected, "seed #{seed}"
      end
    end

    test "refuses a seed outside 32 bits" do
      assert_raise FunctionClauseError, fn -> Payload.seed(-1) end
      assert_raise FunctionClauseError, fn -> Payload.seed(0x1_0000_0000) end
    end

    property "every output is a 32-bit word" do
      check all(seed <- integer(0..0xFFFF_FFFF)) do
        {outputs, _state} = Enum.map_reduce(1..8, Payload.seed(seed), fn _, s -> Payload.next(s) end)
        assert Enum.all?(outputs, &(&1 in 0..0xFFFF_FFFF))
      end
    end
  end

  describe "values" do
    test "match the golden digests", %{vectors: vectors} do
      for %{"mode" => mode, "seed" => seed, "record_size" => size, "count" => count, "sha256" => expected} <-
            vectors["digests"] do
        assert sha256(Payload.values(String.to_existing_atom(mode), seed, size, count)) == expected,
               "#{mode} seed #{seed} size #{size}"
      end
    end

    test "constant is the bytes the generators always sent, whatever the seed" do
      assert Payload.values(:constant, 99, 5, 2) == ["xxxxx", "xxxxx"]
    end

    test "the same seed gives the same values and another seed other values" do
      assert Payload.values(:json, 3, 256, 20) == Payload.values(:json, 3, 256, 20)
      refute Payload.values(:json, 3, 256, 20) == Payload.values(:json, 4, 256, 20)
      refute Payload.values(:random, 3, 64, 20) == Payload.values(:random, 4, 64, 20)
    end

    property "a json value is exactly the record size, ASCII, and parses with the documented fields" do
      check all(size <- integer(Payload.min_json_size()..4096), seed <- integer(0..0xFFFF_FFFF)) do
        for value <- Payload.values(:json, seed, size, 3) do
          assert byte_size(value) == size
          assert value =~ ~r/\A[\x20-\x7e]+\z/

          assert %{"id" => id, "ts" => ts, "user" => "u" <> user, "amount" => amount, "msg" => msg} =
                   Jason.decode!(value)

          assert id =~ ~r/\A[0-9a-f]{16}\z/
          assert ts >= 1_700_000_000_000
          assert user =~ ~r/\A\d{4}\z/
          assert amount in 100..99_999
          assert is_binary(msg)
        end
      end
    end

    property "a random value is exactly the record size" do
      check all(size <- integer(1..2048), seed <- integer(0..0xFFFF_FFFF)) do
        assert Enum.all?(Payload.values(:random, seed, size, 2), &(byte_size(&1) == size))
      end
    end

    test "the smallest json size is the widest skeleton, and one byte less is refused" do
      min = Payload.min_json_size()
      # Both load test guides quote it.
      assert min == 149
      assert Enum.all?(Payload.values(:json, 1, min, 200), &(byte_size(&1) == min))

      assert_raise ArgumentError, "json payload needs --record-size >= #{min}, got #{min - 1}", fn ->
        Payload.values(:json, 1, min - 1, 1)
      end
    end

    test "the json timestamp grows with the position in the pool, not the clock" do
      [first, second] = Payload.values(:json, 1, 256, 2) |> Enum.map(&Jason.decode!(&1)["ts"])
      assert first in 1_700_000_000_000..1_700_000_000_006
      assert second in 1_700_000_000_007..1_700_000_000_013
    end
  end

  describe "the compression band" do
    # What #186 measured on JSON of about 200 bytes with zstd level 1, which the json shape is held to. A
    # json mode that sends constant bytes again lands far above the band; one that sends noise lands at 1.
    defp ratio(values, per_block, level) do
      blocks = values |> Enum.chunk_every(per_block, per_block, :discard)
      raw = blocks |> Enum.map(&IO.iodata_length/1) |> Enum.sum()

      packed =
        blocks
        |> Enum.map(&(&1 |> IO.iodata_to_binary() |> :zstd.compress(%{compressionLevel: level}) |> IO.iodata_length()))
        |> Enum.sum()

      raw / packed
    end

    test "json lands in the reference band and compresses better with every larger block", %{vectors: v} do
      band = v["ratio_band"]
      blocks = band["json_reference"] |> Map.keys() |> Enum.map(&String.to_integer/1) |> Enum.sort()
      values = Payload.values(:json, band["seed"], band["record_size"], List.last(blocks) * 2)

      ratios =
        for per_block <- blocks do
          reference = band["json_reference"][Integer.to_string(per_block)]
          got = ratio(values, per_block, band["level"])

          assert abs(got - reference) <= reference * band["json_tolerance"],
                 "json at #{per_block} per block compresses #{Float.round(got, 2)}x, outside #{reference}x"

          got
        end

      assert ratios == Enum.sort(ratios) and Enum.uniq(ratios) == ratios
    end

    test "random is incompressible and constant is trivially compressible", %{vectors: v} do
      band = v["ratio_band"]

      for per_block <- [1, 10, 100, 1000] do
        random = ratio(Payload.values(:random, 1, band["record_size"], 2000), per_block, band["level"])
        constant = ratio(Payload.values(:constant, 1, band["record_size"], 2000), per_block, band["level"])

        assert random <= band["random_max"]
        if per_block >= 10, do: assert(random >= band["random_min_from_10"])
        assert constant >= band["constant_min"], "constant at #{per_block} per block is only #{constant}x"
      end
    end
  end

  describe "the pool" do
    test "holds about 8MiB of values in whole batches, at least one batch, at most 65536 values below 128B" do
      assert Payload.pool_size(256, 10, 1, 1) == 32_770
      assert Payload.pool_size(256, 4096, 1, 1) == 32_768
      assert Payload.pool_size(1024, 100, 1, 1) == 8_200
      assert Payload.pool_size(16, 10, 1, 1) == 65_540
      assert Payload.pool_size(1, 1, 1, 1) == 65_536
      assert Payload.pool_size(1_048_576, 3, 1, 1) == 9
      assert Payload.pool_size(149, 100_000, 1, 1) == 100_000
    end

    test "holds at least one batch per connection, so connections never start on the same batch" do
      # Batch 4096 at 256B is 8 batches of pool; the ceiling ladder runs it at 64 connections.
      assert Payload.pool_size(256, 4096, 64, 1) == 64 * 4096
      assert Payload.pool_size(256, 1024, 128, 1) == 128 * 1024
      assert Payload.pool_size(256, 10, 512, 1) == 32_770
    end

    test "holds a pipeline of batches per connection, so a connection's first burst stops short of the next start" do
      # The ceiling ladder's batch 4096 at 64 connections, pipelined 32 deep.
      assert Payload.pool_size(256, 4096, 64, 32) == 64 * 32 * 4096
      assert Payload.pool_size(256, 10, 128, 64) == 128 * 64 * 10
      assert Payload.pool_size(256, 10, 1, 64) == 32_770
    end

    property "is a whole number of batches, a pipeline per connection, and every first burst stays its own" do
      check all(
              size <- integer(1..2_000_000),
              batch <- integer(1..5000),
              connections <- integer(1..300),
              pipeline <- integer(1..64)
            ) do
        pool = Payload.pool_size(size, batch, connections, pipeline)
        batches = div(pool, batch)
        assert rem(pool, batch) == 0
        assert batches >= connections * pipeline

        # Connection i sends batches start_i .. start_i + pipeline - 1 before its first answer; no two of
        # those bursts share a batch.
        bursts =
          for i <- 0..(connections - 1),
              k <- 0..(pipeline - 1),
              do: rem(Payload.start_batch(i, batches, connections) + k, batches)

        assert length(Enum.uniq(bursts)) == length(bursts)
      end
    end

    test "never repeats a value inside one batch, nor within 8MiB of the stream" do
      values = Payload.values(:json, 1, 256, Payload.pool_size(256, 10, 1, 1))
      assert length(Enum.uniq(values)) == length(values)
      assert length(values) * 256 >= 8 * 1024 * 1024
    end

    test "spreads the connections over the batches without a shared counter" do
      assert Enum.map(0..3, &Payload.start_batch(&1, 100, 4)) == [0, 25, 50, 75]
      assert Payload.start_batch(5, 3, 10) == 2
      assert Payload.start_batch(9, 1, 4) == 0
    end
  end
end
