defmodule Malachi.Test.MetricsFixtures do
  @moduledoc """
  Hand-built `Malachi.Metrics` snapshots for tests that render the Prometheus exposition without a
  running node.
  """

  alias Malachi.Histogram
  alias Malachi.Metrics.Prometheus

  # When the fixture node's flush histogram began.
  @created 1_789_000_000.5

  @doc "A minimal system snapshot with the shape `Malachi.Metrics.Prometheus.export/3` reads."
  def system do
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
        storage_failure_enospc: 6,
        storage_failure_eio: 1,
        storage_failure_eacces: 0,
        storage_failure_other: 2,
        scrub_segments_verified: 4200,
        scrub_segments_repaired: 3,
        scrub_segments_unrepairable: 2,
        orphaned_fences: 3,
        fences_reconciled: 2,
        sealed_copies_fenced: 9,
        sealed_copies_trimmed: 1,
        sealed_copies_unsettled: 0,
        sealed_records_dropped: 1,
        unexpected_messages: [
          %{server: :replication, kind: :cast, count: 5},
          %{server: :membership, kind: :info, count: 0},
          %{server: :other, kind: :call, count: 1}
        ]
      }
    }
  end

  @doc """
  The `/metrics` text a node whose flush histogram is `histogram` would serve, rendered by the real
  exporter, with `bytes` and `records` as the durability totals and `created` as the time the histogram
  began (a node that restarted has another one).
  """
  def flush_exposition(histogram, bytes \\ 0, records \\ 0, created \\ @created) do
    {buckets, count} = Histogram.cumulative(histogram)

    flush = %{
      buckets: buckets,
      count: count,
      sum_us: Histogram.sum(histogram),
      bytes: bytes,
      records: records,
      created: created
    }

    system()
    |> Prometheus.export([], flush)
    |> IO.iodata_to_binary()
  end

  @doc """
  The scrapes the benchmark script stubs serve, written into `dir`: `before.prom` holds 100 slow (50ms)
  setup flushes, and `after.prom` adds 1000 fast (200us) ones, 64 bytes and 10 records each. A window
  between them is exactly the 1000 fast flushes. `restarted.prom` is a node that booted again and has
  already flushed past the old totals, so only its `created` gives it away, and `noseries.prom` a page
  without the flush series.
  """
  def write_flush_scrapes!(dir) do
    histogram = Histogram.new()
    for _ <- 1..100, do: Histogram.record(histogram, 50_000)
    File.write!(Path.join(dir, "before.prom"), flush_exposition(histogram, 1000, 100))
    for _ <- 1..1000, do: Histogram.record(histogram, 200)
    File.write!(Path.join(dir, "after.prom"), flush_exposition(histogram, 65_000, 10_100))
    File.write!(Path.join(dir, "restarted.prom"), flush_exposition(histogram, 65_000, 10_100, @created + 60))
    File.write!(Path.join(dir, "noseries.prom"), "# HELP malachi_up up\nmalachi_up 1\n")
  end
end
