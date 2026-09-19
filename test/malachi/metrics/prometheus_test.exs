defmodule Malachi.Metrics.PrometheusTest do
  use ExUnit.Case, async: true

  alias Malachi.Histogram
  alias Malachi.Metrics.Prometheus
  alias Malachi.Test.MetricsFixtures

  # 1000 flushes below 4ms, 900 of them below 1ms, and 2 past the last edge, which only `+Inf` counts.
  defp flush do
    buckets =
      for edge <- Histogram.edges() do
        cond do
          edge < 1000 -> {edge, 0}
          edge < 4000 -> {edge, 900}
          true -> {edge, 1000}
        end
      end

    %{buckets: buckets, count: 1002, sum_us: 1_500_000, bytes: 2_048_000, records: 10_000, created: 1_789_000_000.25}
  end

  # A node that has not flushed yet, which is what `Malachi.Metrics.storage_flush_histogram/0` returns
  # before the first flush.
  defp no_flush do
    %{buckets: Enum.map(Histogram.edges(), &{&1, 0}), count: 0, sum_us: 0, bytes: 0, records: 0, created: 0.0}
  end

  defp render(topics, flush \\ flush()),
    do: Prometheus.export(MetricsFixtures.system(), topics, flush) |> IO.iodata_to_binary()

  defp bucket_lines(out) do
    for line <- String.split(out, "\n"),
        [_, le, count] <- [Regex.run(~r/^malachi_storage_flush_duration_seconds_bucket\{le="([^"]+)"\} (\d+)$/, line)] do
      {le, String.to_integer(count)}
    end
  end

  test "emits HELP/TYPE and a value line per series" do
    out = render([])

    assert out =~ "# HELP malachi_up 1 while the metrics endpoint is serving"
    assert out =~ "# TYPE malachi_up gauge"
    assert out =~ "\nmalachi_up 1\n"
    assert out =~ "# TYPE malachi_process_count gauge\nmalachi_process_count 120\n"
    assert out =~ "# TYPE malachi_uptime_seconds gauge\nmalachi_uptime_seconds 3600\n"
  end

  test "labelled series carry their labels and integer values" do
    out = render([])

    assert out =~ ~s(malachi_memory_bytes{kind="total"} #{round(40.0 * 1_048_576)})
    assert out =~ ~s(malachi_rate_limit_blocked_total{action="auth"} 5)
    assert out =~ ~s(malachi_rate_limit_blocked_total{action="subscribe"} 1)
    assert out =~ ~s(malachi_dashboard_auth_total{outcome="success"} 10)
    assert out =~ ~s(malachi_io_bytes_total{direction="output"} 2000)
    # booleans render as 0/1
    assert out =~ "malachi_tls_enabled 1\n"
  end

  test "operation counters are emitted" do
    out = render([])

    assert out =~ "# TYPE malachi_records_produced_total counter\nmalachi_records_produced_total 100\n"
    assert out =~ "malachi_bytes_produced_total 4096\n"
    assert out =~ "malachi_records_consumed_total 80\n"
    assert out =~ ~s(malachi_auth_attempts_total{result="ok"} 12)
    assert out =~ ~s(malachi_auth_attempts_total{result="error"} 3)
    assert out =~ ~s(malachi_replication_commits_total{result="no_quorum"} 1)
    assert out =~ ~s(malachi_storage_integrity_failures_total{reason="bad_crc"} 2)
    assert out =~ ~s(malachi_storage_integrity_failures_total{reason="incomplete"} 1)
    assert out =~ ~s(malachi_storage_integrity_failures_total{reason="bad_index"} 4)
    assert out =~ "# TYPE malachi_storage_failures_total counter\n"
    assert out =~ ~s(malachi_storage_failures_total{reason="enospc"} 6)
    assert out =~ ~s(malachi_storage_failures_total{reason="eio"} 1)
    assert out =~ ~s(malachi_storage_failures_total{reason="eacces"} 0)
    assert out =~ ~s(malachi_storage_failures_total{reason="other"} 2)
    assert out =~ ~s(malachi_storage_scrub_segments_total{result="verified"} 4200)
    assert out =~ ~s(malachi_storage_scrub_segments_total{result="repaired"} 3)
    assert out =~ ~s(malachi_storage_scrub_segments_total{result="unrepairable"} 2)

    # The pair that says whether an orphaned fence healed: detections rising while reconciliations stay
    # flat is a range that has stopped accepting writes, which neither series alone can express.
    assert out =~ ~s(malachi_cluster_orphaned_fences_total{result="detected"} 3)
    assert out =~ ~s(malachi_cluster_orphaned_fences_total{result="reconciled"} 2)

    # One sample per server and kind, zero included: a series that appears only after the first drop can
    # be neither alerted on with increase() nor asserted to be zero.
    assert out =~ "# TYPE malachi_unexpected_messages_total counter\n"
    assert out =~ ~s(malachi_unexpected_messages_total{server="replication",kind="cast"} 5)
    assert out =~ ~s(malachi_unexpected_messages_total{server="membership",kind="info"} 0)
    assert out =~ ~s(malachi_unexpected_messages_total{server="other",kind="call"} 1)
  end

  test "per-topic series get one HELP/TYPE and a sample per topic" do
    topics = [
      %{
        name: "events",
        range_count: 3,
        active_range_count: 2,
        segment_count: 5,
        total_bytes: 4096,
        groups: ["g1", "g2"]
      },
      %{name: "orders", range_count: 1, active_range_count: 1, segment_count: 0, total_bytes: 0, groups: []}
    ]

    out = render(topics)

    # a single TYPE line, then one sample per topic
    assert out =~ "# TYPE malachi_topic_ranges gauge\n"
    assert out =~ ~s(malachi_topic_ranges{topic="events"} 3)
    assert out =~ ~s(malachi_topic_ranges{topic="orders"} 1)
    assert out =~ ~s(malachi_topic_segments{topic="events"} 5)
    assert out =~ ~s(malachi_topic_bytes{topic="events"} 4096)
    assert out =~ ~s(malachi_topic_consumer_groups{topic="events"} 2)
    assert out =~ ~s(malachi_topic_consumer_groups{topic="orders"} 0)
  end

  test "domain_violations gauge is emitted per topic, defaulting to 0 when absent" do
    topics = [
      %{
        name: "events",
        range_count: 1,
        active_range_count: 1,
        segment_count: 2,
        total_bytes: 0,
        groups: [],
        domain_violations: 3
      },
      %{name: "orders", range_count: 1, active_range_count: 1, segment_count: 0, total_bytes: 0, groups: []}
    ]

    out = render(topics)

    assert out =~ "# TYPE malachi_domain_violations gauge\n"
    assert out =~ ~s(malachi_domain_violations{topic="events"} 3)
    assert out =~ ~s(malachi_domain_violations{topic="orders"} 0)
  end

  test "no topic series are emitted when there are no topics" do
    refute render([]) =~ "malachi_topic_ranges"
  end

  test "label values are escaped (defensive, topic names are normally restricted)" do
    out =
      render([%{name: ~s(a"b\\c), range_count: 1, active_range_count: 1, segment_count: 0, total_bytes: 0, groups: []}])

    assert out =~ ~S(malachi_topic_ranges{topic="a\"b\\c"} 1)
  end

  describe "storage flush histogram" do
    test "renders a histogram block in seconds: every edge, +Inf, sum and count" do
      out = render([])

      assert out =~
               "# HELP malachi_storage_flush_duration_seconds Group-commit flush latency: the write plus sync " <>
                 "every acknowledged produce waits behind\n" <>
                 "# TYPE malachi_storage_flush_duration_seconds histogram\n" <>
                 ~s(malachi_storage_flush_duration_seconds_bucket{le="8.0e-6"} 0\n)

      # Microseconds in, seconds out; 1024us and 2048us are exact edges.
      assert out =~ ~s(\nmalachi_storage_flush_duration_seconds_bucket{le="0.001024"} 900\n)
      assert out =~ ~s(\nmalachi_storage_flush_duration_seconds_bucket{le="0.004096"} 1000\n)
      assert out =~ ~s(\nmalachi_storage_flush_duration_seconds_bucket{le="16.777216"} 1000\n)

      assert out =~
               ~s(\nmalachi_storage_flush_duration_seconds_bucket{le="+Inf"} 1002\n) <>
                 "malachi_storage_flush_duration_seconds_sum 1.5\n" <>
                 "malachi_storage_flush_duration_seconds_count 1002\n"
    end

    test "the buckets are the exported edges, ascending, with +Inf last" do
      lines = bucket_lines(render([]))
      {finite, [{"+Inf", 1002}]} = Enum.split(lines, -1)

      assert Enum.map(finite, fn {le, _count} -> String.to_float(le) end) ==
               Enum.map(Histogram.edges(), &(&1 / 1_000_000))
    end

    test "renders when the histogram began as a gauge of its own" do
      out = render([])

      assert out =~
               "# TYPE malachi_storage_flush_duration_seconds_created gauge\n" <>
                 "malachi_storage_flush_duration_seconds_created 1789000000.25\n"
    end

    test "renders the durability totals as counters" do
      out = render([])

      assert out =~ "# TYPE malachi_storage_flushed_bytes_total counter\nmalachi_storage_flushed_bytes_total 2048000\n"

      assert out =~
               "# TYPE malachi_storage_flushed_records_total counter\nmalachi_storage_flushed_records_total 10000\n"
    end

    # A freshly booted node is scraped before its first flush. It must render zeros rather than crash or
    # omit the series: a scraper that only sees the series under load cannot alert on its absence.
    test "a node that has never flushed renders every series at zero" do
      out = render([], no_flush())

      assert length(bucket_lines(out)) == length(Histogram.edges()) + 1
      assert Enum.all?(bucket_lines(out), fn {_le, count} -> count == 0 end)
      assert out =~ "\nmalachi_storage_flush_duration_seconds_sum 0.0\n"
      assert out =~ "\nmalachi_storage_flush_duration_seconds_count 0\n"
      assert out =~ "\nmalachi_storage_flushed_bytes_total 0\n"
      assert out =~ "\nmalachi_storage_flushed_records_total 0\n"
    end
  end
end
