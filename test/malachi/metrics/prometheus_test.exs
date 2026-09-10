defmodule Malachi.Metrics.PrometheusTest do
  use ExUnit.Case, async: true

  alias Malachi.Metrics.Prometheus

  # A minimal system snapshot with the shape Prometheus.export/2 reads.
  defp system do
    %{
      process_count: 120,
      process_limit: 262_144,
      run_queue: 0,
      schedulers_online: 8,
      ets_tables: 45,
      uptime_seconds: 3600,
      memory: %{total_mb: 40.0, processes_mb: 10.0, ets_mb: 2.0, atom_mb: 1.0, binary_mb: 0.5},
      io: %{input_bytes: 1000, output_bytes: 2000},
      atom_table: %{atom_count: 20_000, atom_limit: 1_048_576},
      rate_limiting: %{auth_blocked: 5, publish_blocked: 0, subscribe_blocked: 1, connection_blocks: 3},
      security: %{
        failed_auth_attempts: 7,
        account_lockouts: 2,
        active_sessions: 4,
        active_lockouts: 1,
        dashboard: %{auth_success: 10, auth_failed: 3, auth_blocked: 0}
      },
      tls: %{enabled: true, handshakes_success: 9, handshakes_failed: 1},
      operations: %{
        records_produced: 100,
        bytes_produced: 4096,
        records_consumed: 80,
        auth_ok: 12,
        auth_error: 3,
        replication_ok: 50,
        replication_no_quorum: 1,
        integrity_bad_crc: 2,
        integrity_bad_magic: 0,
        integrity_incomplete: 1,
        integrity_short_copy: 0,
        integrity_bad_index: 4,
        scrub_segments_verified: 4200,
        scrub_segments_repaired: 3,
        scrub_segments_unrepairable: 2,
        orphaned_fences: 3,
        fences_reconciled: 2,
        storage_flushes: 1000,
        storage_flushed_bytes: 2_048_000,
        storage_flushed_records: 10_000,
        storage_flush_duration_us: 1_500_000
      },
      storage_flush_latency_us: %{p50: 900.0, p99: 8700.0, p999: 41_000.0, count: 1000}
    }
  end

  defp render(topics), do: Prometheus.export(system(), topics) |> IO.iodata_to_binary()

  # A node that has not flushed yet: the histogram is empty and every counter is zero.
  defp system_without_flushes do
    system = system()

    %{
      system
      | operations: %{
          system.operations
          | storage_flushes: 0,
            storage_flushed_bytes: 0,
            storage_flushed_records: 0,
            storage_flush_duration_us: 0
        },
        storage_flush_latency_us: %{p50: 0.0, p99: 0.0, p999: 0.0, count: 0}
    }
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
    assert out =~ ~s(malachi_storage_scrub_segments_total{result="verified"} 4200)
    assert out =~ ~s(malachi_storage_scrub_segments_total{result="repaired"} 3)
    assert out =~ ~s(malachi_storage_scrub_segments_total{result="unrepairable"} 2)

    # The pair that says whether an orphaned fence healed: detections rising while reconciliations stay
    # flat is a range that has stopped accepting writes, which neither series alone can express.
    assert out =~ ~s(malachi_cluster_orphaned_fences_total{result="detected"} 3)
    assert out =~ ~s(malachi_cluster_orphaned_fences_total{result="reconciled"} 2)
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

  describe "storage flush summary" do
    test "renders the quantiles, sum and count as one summary in seconds" do
      out = render([])

      assert out =~ "# TYPE malachi_storage_flush_duration_seconds summary"
      # Microseconds in, seconds out: Prometheus convention is base units.
      assert out =~ "\nmalachi_storage_flush_duration_seconds{quantile=\"0.5\"} 0.0009\n"
      assert out =~ "\nmalachi_storage_flush_duration_seconds{quantile=\"0.99\"} 0.0087\n"
      assert out =~ "\nmalachi_storage_flush_duration_seconds{quantile=\"0.999\"} 0.041\n"
      assert out =~ "\nmalachi_storage_flush_duration_seconds_sum 1.5\n"
      assert out =~ "\nmalachi_storage_flush_duration_seconds_count 1000\n"
    end

    test "renders the durability totals alongside it" do
      out = render([])

      assert out =~ "# TYPE malachi_storage_flushed_bytes_total counter\nmalachi_storage_flushed_bytes_total 2048000\n"
      assert out =~ "\nmalachi_storage_flushed_records_total 10000\n"
    end

    # A freshly booted node scrapes before its first flush. That must render zeros, not crash and not
    # omit the series: a scraper that sees the series appear only under load cannot alert on its absence.
    test "a node that has never flushed renders zeros rather than failing" do
      out = Prometheus.export(system_without_flushes(), []) |> IO.iodata_to_binary()

      assert out =~ "\nmalachi_storage_flush_duration_seconds{quantile=\"0.5\"} 0.0\n"
      assert out =~ "\nmalachi_storage_flush_duration_seconds_sum 0.0\n"
      assert out =~ "\nmalachi_storage_flush_duration_seconds_count 0\n"
    end
  end
end
