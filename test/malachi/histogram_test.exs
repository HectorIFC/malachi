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
  end
end
