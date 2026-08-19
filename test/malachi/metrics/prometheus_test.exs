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
        scrub_segments_verified: 4200,
        scrub_segments_repaired: 3
      }
    }
  end

  defp render(topics), do: Prometheus.export(system(), topics) |> IO.iodata_to_binary()

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

  test "operation counters (O4) are emitted" do
    out = render([])

    assert out =~ "# TYPE malachi_records_produced_total counter\nmalachi_records_produced_total 100\n"
    assert out =~ "malachi_bytes_produced_total 4096\n"
    assert out =~ "malachi_records_consumed_total 80\n"
    assert out =~ ~s(malachi_auth_attempts_total{result="ok"} 12)
    assert out =~ ~s(malachi_auth_attempts_total{result="error"} 3)
    assert out =~ ~s(malachi_replication_commits_total{result="no_quorum"} 1)
    assert out =~ ~s(malachi_storage_integrity_failures_total{reason="bad_crc"} 2)
    assert out =~ ~s(malachi_storage_integrity_failures_total{reason="incomplete"} 1)
    assert out =~ ~s(malachi_storage_scrub_segments_total{result="verified"} 4200)
    assert out =~ ~s(malachi_storage_scrub_segments_total{result="repaired"} 3)
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
end
