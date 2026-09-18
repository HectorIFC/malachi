defmodule Malachi.HistogramTest do
  # Pure: an :atomics array and arithmetic, no processes shared between cases.
  use ExUnit.Case, async: true

  alias Malachi.Histogram

  describe "percentile/2 and record/2" do
    test "records samples and reports monotonic percentiles" do
      h = Histogram.new()
      assert Histogram.count(h) == 0
      assert Histogram.percentile(h, 50) == 0.0

      # 1000 samples at ~1000us and 10 at ~100_000us: p50 near 1ms, p99.99 out in the tail
      for _ <- 1..1000, do: Histogram.record(h, 1000)
      for _ <- 1..10, do: Histogram.record(h, 100_000)

      assert Histogram.count(h) == 1010
      p50 = Histogram.percentile(h, 50)
      p99 = Histogram.percentile(h, 99)
      p100 = Histogram.percentile(h, 100)

      assert p50 >= 900 and p50 <= 1100, "p50 #{p50} should be ~1000us"
      assert p99 >= p50, "percentiles must be monotonic"
      assert p100 >= 90_000, "the tail must reflect the 100ms samples"
    end

    test "clamps non-positive and huge latencies without crashing" do
      h = Histogram.new()
      Histogram.record(h, 0)
      Histogram.record(h, -5)
      Histogram.record(h, 1_000_000_000)
      assert Histogram.count(h) == 3
    end

    test "a sub-microsecond sample lands in the first bucket instead of off the array" do
      h = Histogram.new()
      Histogram.record(h, 0.5)

      assert Histogram.count(h) == 1
      assert Histogram.percentile(h, 50) == 1.0
    end

    test "an empty histogram reports 0.0 at every percentile rather than crashing" do
      h = Histogram.new()
      for p <- [0, 50, 99, 99.9, 100], do: assert(Histogram.percentile(h, p) == 0.0)
    end

    test "p0 lands in the lowest populated bucket, not below it" do
      h = Histogram.new()
      Histogram.record(h, 5_000)
      Histogram.record(h, 50_000)

      p0 = Histogram.percentile(h, 0)
      assert p0 >= 4_500 and p0 <= 5_500, "p0 #{p0} should be the ~5ms sample"
    end

    test "a percentile past 100 saturates at the top bucket instead of running off the array" do
      h = Histogram.new()
      Histogram.record(h, 1000)

      assert Histogram.percentile(h, 150) == :math.pow(2, 1023 / 16) |> Float.round(1)
    end

    test "resolution is within one bucket width across the range" do
      # The reported value is the bucket's lower edge, so it never overstates a sample and never
      # understates it by more than the 4.4% bucket width.
      for us <- [1, 17, 300, 12_345, 987_654] do
        h = Histogram.new()
        Histogram.record(h, us)
        reported = Histogram.percentile(h, 50)

        assert reported <= us, "bucket lower edge #{reported} must not exceed the sample #{us}"
        assert reported >= us * 0.95, "bucket lower edge #{reported} is too far below #{us}"
      end
    end

    test "a latency past the last bucket saturates instead of running off the array" do
      h = Histogram.new()
      # 2^(1024/16) = 2^64 us is past the top bucket; the index must clamp, not overflow.
      Histogram.record(h, :math.pow(2, 70))
      assert Histogram.count(h) == 1
      assert Histogram.percentile(h, 100) > 0.0
    end

    test "concurrent writers all land, with no lost updates" do
      h = Histogram.new()
      writers = 16
      per_writer = 500

      1..writers
      |> Enum.map(fn _ -> Task.async(fn -> for _ <- 1..per_writer, do: Histogram.record(h, 1234) end) end)
      |> Task.await_many(10_000)

      assert Histogram.count(h) == writers * per_writer
      assert Histogram.sum(h) == writers * per_writer * 1234
    end
  end

  describe "sum/1" do
    test "adds every sample, truncating fractions" do
      h = Histogram.new()
      assert Histogram.sum(h) == 0

      Histogram.record(h, 100)
      Histogram.record(h, 250.9)

      assert Histogram.sum(h) == 350
    end

    test "non-positive samples add nothing to the sum but still count" do
      h = Histogram.new()
      Histogram.record(h, 0)
      Histogram.record(h, -5)

      assert Histogram.sum(h) == 0
      assert Histogram.count(h) == 2
    end

    test "a sample wider than the atomics word is clamped rather than crashing" do
      h = Histogram.new()
      Histogram.record(h, :math.pow(2, 70))

      assert Histogram.sum(h) == Bitwise.bsl(1, 40)
    end

    test "the sum slot is not counted as a bucket" do
      h = Histogram.new()
      Histogram.record(h, 1_000_000)

      assert Histogram.count(h) == 1
    end
  end

  describe "edges/0 and cumulative/1" do
    test "edges are four per octave from 8us to 2^24us, ascending" do
      edges = Histogram.edges()

      assert length(edges) == 85
      assert hd(edges) == 8.0
      assert List.last(edges) == 16_777_216.0
      assert edges == Enum.sort(edges)

      for [a, b] <- Enum.chunk_every(edges, 2, 1, :discard) do
        assert_in_delta b / a, :math.pow(2, 0.25), 1.0e-9
      end
    end

    test "an empty histogram reports zero at every edge" do
      h = Histogram.new()
      {cumulative, total} = Histogram.cumulative(h)

      assert Enum.map(cumulative, &elem(&1, 0)) == Histogram.edges()
      assert Enum.all?(cumulative, fn {_edge, count} -> count == 0 end)
      assert total == 0
    end

    test "counts samples at or below each edge" do
      h = Histogram.new()
      # 10us sits between the 9.51us and 11.31us edges.
      Histogram.record(h, 10)

      counts = Map.new(elem(Histogram.cumulative(h), 0))
      assert counts[:math.pow(2, 13 / 4)] == 0
      assert counts[:math.pow(2, 14 / 4)] == 1
    end

    test "a sample equal to an edge counts at that edge, as Prometheus le is inclusive" do
      h = Histogram.new()
      Histogram.record(h, 1024)
      Histogram.record(h, 1025)

      counts = Map.new(elem(Histogram.cumulative(h), 0))
      assert counts[:math.pow(2, 39 / 4)] == 0
      assert counts[1024.0] == 1
      assert counts[:math.pow(2, 41 / 4)] == 2
    end

    test "every power-of-two edge includes a sample equal to it" do
      for k <- 3..24 do
        h = Histogram.new()
        edge = Bitwise.bsl(1, k)
        Histogram.record(h, edge)

        counts = Map.new(elem(Histogram.cumulative(h), 0))
        assert counts[edge * 1.0] == 1, "a #{edge}us sample is missing from le=#{edge}"
      end
    end

    test "samples below the first edge count at every edge" do
      h = Histogram.new()
      Histogram.record(h, 0)
      Histogram.record(h, 3)

      {cumulative, total} = Histogram.cumulative(h)
      assert Enum.all?(cumulative, fn {_edge, count} -> count == 2 end)
      assert total == 2
    end

    test "samples past the last edge appear at no edge but are in the total" do
      h = Histogram.new()
      Histogram.record(h, 20_000_000)

      {cumulative, total} = Histogram.cumulative(h)
      assert Enum.all?(cumulative, fn {_edge, count} -> count == 0 end)
      assert total == 1
    end

    test "counts are monotonic and match a direct count of the samples" do
      h = Histogram.new()
      samples = [9, 16, 40, 40, 512, 700, 5_000, 5_001, 90_000, 3_000_000]
      Enum.each(samples, &Histogram.record(h, &1))

      {cumulative, total} = Histogram.cumulative(h)
      counts = Enum.map(cumulative, &elem(&1, 1))
      assert counts == Enum.sort(counts)
      assert total == length(samples)

      for {edge, count} <- cumulative do
        assert count == Enum.count(samples, &(&1 <= edge)), "edge #{edge}"
      end
    end
  end
end
