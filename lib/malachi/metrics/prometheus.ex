defmodule Malachi.Metrics.Prometheus do
  @moduledoc """
  Renders the system snapshot (`Malachi.Metrics.get_system_metrics/0`) and the topic overview
  (`Malachi.Metadata.overview/1`) as the Prometheus **text exposition format** (version 0.0.4). Pure:
  takes the two maps, returns `iodata`. Every series is namespaced `malachi_`. Served from the dashboard's
  `/metrics` when the client asks for `text/plain` (content negotiation with the JSON dashboard payload).
  """

  @content_type "text/plain; version=0.0.4; charset=utf-8"

  @doc "The `Content-Type` a scraper expects for the exposition format."
  @spec content_type() :: String.t()
  def content_type, do: @content_type

  @doc """
  Builds the exposition text (as iodata) from a system snapshot, the topic overview and the storage flush
  histogram (`Malachi.Metrics.storage_flush_histogram/0`).
  """
  @spec export(map(), [map()], map()) :: iodata()
  def export(system, topics, flush) do
    mem = system.memory
    ops = system.operations

    [
      metric("malachi_up", :gauge, "1 while the metrics endpoint is serving", [{[], 1}]),
      metric("malachi_process_count", :gauge, "BEAM process count", [{[], system.process_count}]),
      metric("malachi_process_limit", :gauge, "BEAM process limit", [{[], system.process_limit}]),
      metric("malachi_run_queue", :gauge, "BEAM run-queue length", [{[], system.run_queue}]),
      metric("malachi_schedulers_online", :gauge, "Online schedulers", [{[], system.schedulers_online}]),
      metric("malachi_ets_tables", :gauge, "ETS table count", [{[], system.ets_tables}]),
      metric("malachi_uptime_seconds", :gauge, "Node uptime in seconds", [{[], system.uptime_seconds}]),
      metric("malachi_memory_bytes", :gauge, "BEAM memory by kind (bytes)", [
        {[kind: "total"], mb_to_bytes(mem.total_mb)},
        {[kind: "processes"], mb_to_bytes(mem.processes_mb)},
        {[kind: "ets"], mb_to_bytes(mem.ets_mb)},
        {[kind: "atom"], mb_to_bytes(mem.atom_mb)},
        {[kind: "binary"], mb_to_bytes(mem.binary_mb)}
      ]),
      metric("malachi_io_bytes_total", :counter, "Bytes transferred over all ports", [
        {[direction: "input"], system.io.input_bytes},
        {[direction: "output"], system.io.output_bytes}
      ]),
      metric("malachi_atom_count", :gauge, "Atoms in the atom table", [{[], system.atom_table.atom_count}]),
      metric("malachi_atom_limit", :gauge, "Atom-table limit", [{[], system.atom_table.atom_limit}]),
      metric("malachi_rate_limit_blocked_total", :counter, "Requests blocked by rate limiting", [
        {[action: "auth"], system.rate_limiting.auth_blocked},
        {[action: "publish"], system.rate_limiting.publish_blocked},
        {[action: "subscribe"], system.rate_limiting.subscribe_blocked}
      ]),
      metric("malachi_connection_limit_blocked_total", :counter, "Connections blocked by limits", [
        {[], system.rate_limiting.connection_blocks}
      ]),
      metric("malachi_failed_auth_total", :counter, "Failed authentication attempts", [
        {[], system.security.failed_auth_attempts}
      ]),
      metric("malachi_account_lockouts_total", :counter, "Account lockouts triggered", [
        {[], system.security.account_lockouts}
      ]),
      metric("malachi_active_sessions", :gauge, "Active authenticated sessions", [{[], system.security.active_sessions}]),
      metric("malachi_active_lockouts", :gauge, "Currently locked-out accounts", [{[], system.security.active_lockouts}]),
      metric("malachi_dashboard_auth_total", :counter, "Dashboard authentication outcomes", [
        {[outcome: "success"], system.security.dashboard.auth_success},
        {[outcome: "failed"], system.security.dashboard.auth_failed},
        {[outcome: "blocked"], system.security.dashboard.auth_blocked}
      ]),
      metric("malachi_tls_handshakes_total", :counter, "TLS handshake outcomes", [
        {[outcome: "success"], system.tls.handshakes_success},
        {[outcome: "failed"], system.tls.handshakes_failed}
      ]),
      metric("malachi_tls_enabled", :gauge, "1 if TLS is enabled", [{[], bool(system.tls.enabled)}]),
      metric("malachi_records_produced_total", :counter, "Records appended across all topics", [
        {[], ops.records_produced}
      ]),
      metric("malachi_bytes_produced_total", :counter, "Value bytes appended across all topics", [
        {[], ops.bytes_produced}
      ]),
      metric("malachi_records_consumed_total", :counter, "Records read across all topics", [{[], ops.records_consumed}]),
      metric("malachi_auth_attempts_total", :counter, "Authentication attempts by result", [
        {[result: "ok"], ops.auth_ok},
        {[result: "error"], ops.auth_error}
      ]),
      metric("malachi_replication_commits_total", :counter, "Quorum replications by result", [
        {[result: "ok"], ops.replication_ok},
        {[result: "no_quorum"], ops.replication_no_quorum}
      ]),
      metric(
        "malachi_storage_integrity_failures_total",
        :counter,
        "Segments that failed checksum verification, by reason (non-zero means data at rest is damaged)",
        [
          {[reason: "bad_crc"], ops.integrity_bad_crc},
          {[reason: "bad_magic"], ops.integrity_bad_magic},
          {[reason: "incomplete"], ops.integrity_incomplete},
          {[reason: "short_copy"], ops.integrity_short_copy},
          {[reason: "bad_index"], ops.integrity_bad_index}
        ]
      ),
      metric(
        "malachi_storage_failures_total",
        :counter,
        "Storage operations that failed on a segment copy, which was then taken out of service, by reason " <>
          "(enospc is a full volume, eio a failing device)",
        [
          {[reason: "enospc"], ops.storage_failure_enospc},
          {[reason: "eio"], ops.storage_failure_eio},
          {[reason: "eacces"], ops.storage_failure_eacces},
          {[reason: "other"], ops.storage_failure_other}
        ]
      ),
      metric(
        "malachi_storage_scrub_segments_total",
        :counter,
        "Segments the integrity scrub has processed (a total that stops advancing means it stopped)",
        [
          {[result: "verified"], ops.scrub_segments_verified},
          {[result: "repaired"], ops.scrub_segments_repaired},
          {[result: "unrepairable"], ops.scrub_segments_unrepairable}
        ]
      ),
      metric(
        "malachi_cluster_orphaned_fences_total",
        :counter,
        "Segments fenced whose control-plane seal failed, and the ones a heal pass has since reconciled " <>
          "(detected rising while reconciled stays flat means a range is stuck for writes)",
        [
          {[result: "detected"], ops.orphaned_fences},
          {[result: "reconciled"], ops.fences_reconciled}
        ]
      ),
      metric(
        "malachi_unexpected_messages_total",
        :counter,
        "Messages a long-lived server had no clause for and dropped (or answered unknown_call) instead of " <>
          "crashing (non-zero outside a rolling upgrade means a bug)",
        unexpected_message_samples(ops.unexpected_messages)
      ),
      histogram(
        "malachi_storage_flush_duration_seconds",
        "Group-commit flush latency: the write plus sync every acknowledged produce waits behind",
        flush
      ),
      metric("malachi_storage_flushed_bytes_total", :counter, "Encoded bytes made durable by group-commit flushes", [
        {[], flush.bytes}
      ]),
      metric(
        "malachi_storage_flushed_records_total",
        :counter,
        "Records made durable by group-commit flushes (divided by the flush count: records per sync)",
        [{[], flush.records}]
      ),
      topic_metrics(topics)
    ]
  end

  # A Prometheus histogram in seconds: one cumulative `_bucket` per edge, the `+Inf` bucket (every flush),
  # then `_sum` and `_count`. Buckets rather than a summary's quantiles because buckets subtract: two
  # scrapes give the distribution of the flushes between them, and nodes add up, neither of which a
  # quantile allows. `_created` (when the histogram began, which is how a reader tells a restarted node
  # from one whose counters kept growing) follows as a gauge of its own: this endpoint serves the 0.0.4
  # text format, where a histogram family has no `_created` sample.
  defp histogram(name, help, %{buckets: buckets, count: count, sum_us: sum_us, created: created}) do
    bucket_name = name <> "_bucket"

    [
      header(name, :histogram, help),
      Enum.map(buckets, fn {edge_us, below} -> sample(bucket_name, [le: us_to_seconds(edge_us)], below) end),
      sample(bucket_name, [le: "+Inf"], count),
      sample(name <> "_sum", [], us_to_seconds(sum_us)),
      sample(name <> "_count", [], count),
      metric(
        name <> "_created",
        :gauge,
        "Unix time the flush latency histogram began (changes when the node restarts)",
        [
          {[], created}
        ]
      )
    ]
  end

  defp unexpected_message_samples(counts),
    do: for(%{server: server, kind: kind, count: count} <- counts, do: {[server: server, kind: kind], count})

  # Prometheus convention is base units, so latencies are exposed in seconds, not microseconds.
  defp us_to_seconds(us), do: us / 1_000_000

  # One block per per-topic series: a single HELP/TYPE then a sample per topic (labelled by name).
  defp topic_metrics([]), do: []

  defp topic_metrics(topics) do
    [
      metric("malachi_topic_ranges", :gauge, "Ranges per topic", samples(topics, & &1.range_count)),
      metric(
        "malachi_topic_ranges_active",
        :gauge,
        "Active ranges per topic",
        samples(topics, & &1.active_range_count)
      ),
      metric("malachi_topic_segments", :gauge, "Segments per topic", samples(topics, & &1.segment_count)),
      metric("malachi_topic_bytes", :gauge, "Stored bytes per topic", samples(topics, & &1.total_bytes)),
      metric("malachi_topic_consumer_groups", :gauge, "Consumer groups per topic", samples(topics, &length(&1.groups))),
      metric(
        "malachi_domain_violations",
        :gauge,
        "Segments spanning fewer than min_domains failure domains, per topic (0 unless configured)",
        samples(topics, &Map.get(&1, :domain_violations, 0))
      )
    ]
  end

  defp samples(topics, value_fun), do: Enum.map(topics, fn t -> {[topic: t.name], value_fun.(t)} end)

  # --- exposition format ---

  defp metric(name, type, help, samples) do
    [header(name, type, help), Enum.map(samples, fn {labels, value} -> sample(name, labels, value) end)]
  end

  defp header(name, type, help), do: ["# HELP ", name, " ", help, "\n# TYPE ", name, " ", Atom.to_string(type), "\n"]

  defp sample(name, labels, value), do: [name, labels(labels), " ", value(value), "\n"]

  defp labels([]), do: ""

  defp labels(labels) do
    inner =
      labels
      |> Enum.map(fn {key, value} -> [Atom.to_string(key), "=\"", escape(to_string(value)), "\""] end)
      |> Enum.intersperse(",")

    ["{", inner, "}"]
  end

  defp value(v) when is_integer(v), do: Integer.to_string(v)
  defp value(v) when is_float(v), do: Float.to_string(v)

  # Label values must escape backslash, double-quote, and newline (Prometheus text format).
  defp escape(string) do
    string
    |> String.replace("\\", "\\\\")
    |> String.replace("\"", "\\\"")
    |> String.replace("\n", "\\n")
  end

  defp mb_to_bytes(mb), do: round(mb * 1_048_576)
  defp bool(true), do: 1
  defp bool(false), do: 0
end
