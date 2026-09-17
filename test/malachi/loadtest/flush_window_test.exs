defmodule Malachi.Loadtest.FlushWindowTest do
  # Pure: exposition text in, maps out. The scrapes are rendered by the real exporter from real
  # histograms, so a change on either side of the text format breaks these tests.
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Histogram
  alias Malachi.Loadtest.FlushWindow
  alias Malachi.Test.MetricsFixtures

  # Four buckets per octave: an interpolated quantile is within this factor of the flush it stands for.
  @bucket_ratio :math.pow(2, 0.25)

  defp record_all(histogram, samples), do: Enum.each(samples, &Histogram.record(histogram, &1))

  defp parse!(text) do
    {:ok, snapshot} = FlushWindow.parse(text)
    snapshot
  end

  # The window between an empty scrape and one after `samples`.
  defp window_of(samples, bytes \\ 0, records \\ 0) do
    histogram = Histogram.new()
    before = parse!(MetricsFixtures.flush_exposition(histogram))
    record_all(histogram, samples)
    {:ok, window} = FlushWindow.diff(before, parse!(MetricsFixtures.flush_exposition(histogram, bytes, records)))
    window
  end

  describe "parse/1" do
    test "reads every flush series of a scrape" do
      histogram = Histogram.new()
      record_all(histogram, [100, 2_000, 30_000_000])

      snapshot = parse!(MetricsFixtures.flush_exposition(histogram, 4096, 12))

      assert snapshot.count == 3
      assert snapshot.sum == 30.0021
      assert snapshot.bytes == 4096
      assert snapshot.records == 12
      assert length(snapshot.buckets) == length(Histogram.edges()) + 1
      assert List.last(snapshot.buckets) == {"+Inf", 3}
      assert {"8.0e-6", 0} = hd(snapshot.buckets)
    end

    test "ignores carriage returns and unrelated series" do
      text = MetricsFixtures.flush_exposition(Histogram.new()) |> String.replace("\n", "\r\n")
      assert {:ok, %{count: 0}} = FlushWindow.parse("# junk\r\nmalachi_up 1\r\n" <> text)
    end

    test "a scrape without the flush series is series_missing" do
      text = MetricsFixtures.flush_exposition(Histogram.new())

      for series <- [
            ~s(malachi_storage_flush_duration_seconds_bucket{le="+Inf"}),
            "malachi_storage_flush_duration_seconds_sum",
            "malachi_storage_flush_duration_seconds_count",
            "malachi_storage_flushed_bytes_total",
            "malachi_storage_flushed_records_total"
          ] do
        without = text |> String.split("\n") |> Enum.reject(&String.starts_with?(&1, series <> " ")) |> Enum.join("\n")
        assert FlushWindow.parse(without) == {:error, :series_missing}, "without #{series}"
      end

      assert FlushWindow.parse("") == {:error, :series_missing}
      assert FlushWindow.parse("<html>Login</html>") == {:error, :series_missing}
    end

    test "a scrape with no finite bucket is series_missing" do
      text =
        MetricsFixtures.flush_exposition(Histogram.new())
        |> String.split("\n")
        |> Enum.reject(&(&1 =~ ~r/_bucket\{le="\d/))
        |> Enum.join("\n")

      assert FlushWindow.parse(text) == {:error, :series_missing}
    end

    test "malformed or out-of-order samples are series_missing" do
      base = MetricsFixtures.flush_exposition(Histogram.new())

      broken = [
        # a count that is not a number
        String.replace(
          base,
          "malachi_storage_flush_duration_seconds_count 0",
          "malachi_storage_flush_duration_seconds_count x"
        ),
        # a count that is not an integer
        String.replace(
          base,
          "malachi_storage_flush_duration_seconds_count 0",
          "malachi_storage_flush_duration_seconds_count 0.5"
        ),
        # a series given twice
        base <> "malachi_storage_flushed_bytes_total 3\n",
        # +Inf not last
        base <> ~s(malachi_storage_flush_duration_seconds_bucket{le="1.0"} 0\n),
        # an edge that is not a number
        String.replace(base, ~s(le="8.0e-6"), ~s(le="eight")),
        # a bucket line with trailing junk after the labels
        String.replace(base, "le=\"8.0e-6\"}", "le=\"8.0e-6\",x=\"y\"}"),
        # a bucket whose count is not a number
        String.replace(base, "le=\"8.0e-6\"} 0", "le=\"8.0e-6\"} zero"),
        # a flush series line with no value
        base <> "malachi_storage_flushed_bytes_total\n",
        # a flush series nobody exports
        base <> "malachi_storage_flush_duration_seconds_created 1\n",
        # a bucket line with something glued after the closing brace
        String.replace(base, "le=\"8.0e-6\"}", "le=\"8.0e-6\"}x")
      ]

      for text <- broken, do: assert(FlushWindow.parse(text) == {:error, :series_missing})
    end

    test "edges that do not ascend are series_missing" do
      text =
        String.replace(
          MetricsFixtures.flush_exposition(Histogram.new()),
          ~s(le="9.513656920021768e-6"),
          ~s(le="7.0e-6")
        )

      assert FlushWindow.parse(text) == {:error, :series_missing}
    end
  end

  describe "diff/2" do
    test "keeps only what happened between the scrapes" do
      histogram = Histogram.new()
      # Setup and warmup: slow flushes that must not reach the window.
      record_all(histogram, List.duplicate(50_000, 100))
      before = parse!(MetricsFixtures.flush_exposition(histogram, 1000, 100))

      record_all(histogram, List.duplicate(200, 1000))
      {:ok, window} = FlushWindow.diff(before, parse!(MetricsFixtures.flush_exposition(histogram, 5000, 2100)))

      summary = FlushWindow.summarize(window)
      assert summary.flushes == 1000
      assert summary.bytes == 4000
      assert summary.records == 2000
      assert_in_delta summary.mean, 0.0002, 1.0e-12
      assert summary.p99 < 0.0002 * @bucket_ratio
    end

    test "a counter that went backwards is a counter_reset" do
      histogram = Histogram.new()
      record_all(histogram, [100, 100])
      before = parse!(MetricsFixtures.flush_exposition(histogram, 10, 2))
      restarted = parse!(MetricsFixtures.flush_exposition(Histogram.new(), 10, 2))

      assert FlushWindow.diff(before, restarted) == {:error, :counter_reset}

      # Every series is checked, not only the count.
      same = parse!(MetricsFixtures.flush_exposition(histogram, 10, 2))
      assert FlushWindow.diff(same, %{same | bytes: 9}) == {:error, :counter_reset}
      assert FlushWindow.diff(same, %{same | records: 1}) == {:error, :counter_reset}
      assert FlushWindow.diff(same, %{same | sum: 0.0}) == {:error, :counter_reset}
      [{le, n} | rest] = same.buckets
      assert FlushWindow.diff(%{same | buckets: [{le, n + 1} | rest]}, same) == {:error, :counter_reset}
    end

    test "scrapes with different edges are a bucket_mismatch" do
      snapshot = parse!(MetricsFixtures.flush_exposition(Histogram.new()))
      assert FlushWindow.diff(snapshot, %{snapshot | buckets: tl(snapshot.buckets)}) == {:error, :bucket_mismatch}
    end
  end

  describe "merge/1" do
    test "adds the nodes' windows into one distribution" do
      fast = window_of(List.duplicate(100, 990), 990, 990)
      slow = window_of(List.duplicate(40_000, 10), 10, 10)

      {:ok, merged} = FlushWindow.merge([fast, slow, window_of([])])
      summary = FlushWindow.summarize(merged)

      assert summary.flushes == 1000
      assert summary.bytes == 1000
      assert summary.records == 1000
      # 1% of the cluster's flushes were slow: the p50 is a fast one, the p999 a slow one.
      assert summary.p50 < 0.0001 * @bucket_ratio
      assert summary.p999 > 0.04 / @bucket_ratio
    end

    test "a single window merges to itself" do
      window = window_of([100])
      assert FlushWindow.merge([window]) == {:ok, window}
    end

    test "windows with different edges are a bucket_mismatch" do
      window = window_of([100])
      assert FlushWindow.merge([window, %{window | buckets: tl(window.buckets)}]) == {:error, :bucket_mismatch}
    end
  end

  describe "summarize/1" do
    test "a window without flushes has no latency, rather than a latency of zero" do
      assert FlushWindow.summarize(window_of([], 0, 0)) ==
               %{p50: nil, p99: nil, p999: nil, mean: nil, flushes: 0, bytes: 0, records: 0}
    end

    test "flushes past the last edge report the last edge, the most the histogram can say" do
      summary = FlushWindow.summarize(window_of([30_000_000]))

      assert summary.p50 == 16.777216
      assert summary.mean == 30.0
    end

    test "flushes below the first edge interpolate from zero" do
      summary = FlushWindow.summarize(window_of([3, 3]))

      assert summary.p50 == 0.000004
      assert summary.p50 > 0
    end

    test "interpolates inside the bucket the rank falls in, as histogram_quantile does" do
      # Four flushes in the (1024us, 1218us] bucket: the median rank 2 is halfway through it.
      summary = FlushWindow.summarize(window_of([1100, 1100, 1100, 1100]))
      lower = 1024 / 1_000_000
      upper = :math.pow(2, 41 / 4) / 1_000_000

      assert_in_delta summary.p50, lower + (upper - lower) * 0.5, 1.0e-15
    end
  end

  describe "describe/1" do
    test "every error has words" do
      for error <- [:series_missing, :counter_reset, :bucket_mismatch] do
        assert FlushWindow.describe(error) =~ ~r/\w+ \w+/
      end
    end
  end

  # Latencies spread log-uniformly over the histogram's finite range, 8us to 2^23us.
  defp latency, do: map(integer(30..230), fn tenth_power -> round(:math.pow(2, tenth_power / 10)) end)

  property "the window's quantiles are within one bucket of the exact quantiles of the flushes in it" do
    check all(
            setup <- list_of(latency()),
            measured <- list_of(latency(), min_length: 1)
          ) do
      histogram = Histogram.new()
      record_all(histogram, setup)
      before = parse!(MetricsFixtures.flush_exposition(histogram))
      record_all(histogram, measured)
      {:ok, window} = FlushWindow.diff(before, parse!(MetricsFixtures.flush_exposition(histogram)))

      summary = FlushWindow.summarize(window)
      sorted = Enum.sort(measured)

      assert summary.flushes == length(measured)
      assert_in_delta summary.mean * 1_000_000, Enum.sum(measured) / length(measured), 1.0e-6

      for {key, q} <- [p50: 0.5, p99: 0.99, p999: 0.999] do
        # The flush the rank stands for: the ceil(q * n)-th smallest, the first whose count reaches q * n.
        exact = Enum.at(sorted, ceil(q * length(sorted)) - 1) / 1_000_000
        estimate = Map.fetch!(summary, key)

        assert estimate >= exact / @bucket_ratio * 0.999_999, "#{key} #{estimate} is too far below #{exact}"
        assert estimate <= exact * @bucket_ratio * 1.000_001, "#{key} #{estimate} is too far above #{exact}"
      end
    end
  end
end
