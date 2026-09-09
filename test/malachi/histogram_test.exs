defmodule Malachi.HistogramTest do
  # Pure: an :atomics array and arithmetic, no processes and no shared state between cases.
  use ExUnit.Case, async: true

  alias Malachi.Histogram

  describe "percentile/2" do
    test "records samples and reports monotonic percentiles" do
      h = Histogram.new()
      assert Histogram.count(h) == 0
      assert Histogram.percentile(h, 50) == 0.0

      # 1000 samples at ~1000us and 10 at ~100_000us: p50 near 1ms, p100 out in the tail
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
      # The defensive clause in find_bucket/4: a rank beyond the sample count can never be reached by
      # the cumulative walk, and the array must not be indexed past its end when that happens.
      h = Histogram.new()
      Histogram.record(h, 1000)

      assert Histogram.percentile(h, 150) > 0.0
    end

    test "resolution is within one bucket width across the range" do
      # Every sample is its own bucket's lower edge at worst, so the reported value must never
      # overstate the sample and never understate it by more than the 4.4% bucket width.
      for us <- [1, 17, 300, 12_345, 987_654] do
        h = Histogram.new()
        Histogram.record(h, us)
        reported = Histogram.percentile(h, 50)

        assert reported <= us, "bucket lower edge #{reported} must not exceed the sample #{us}"
        assert reported >= us * 0.95, "bucket lower edge #{reported} is too far below #{us}"
      end
    end
  end

  describe "record/2" do
    test "clamps non-positive and huge latencies without crashing" do
      h = Histogram.new()
      Histogram.record(h, 0)
      Histogram.record(h, -5)
      Histogram.record(h, 1_000_000_000)
      assert Histogram.count(h) == 3
    end

    test "a latency past the last bucket saturates instead of running off the array" do
      h = Histogram.new()
      # 2^(1024/16) = 2^64 us is well past the top bucket; the index must clamp, not overflow.
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
    end
  end
end
