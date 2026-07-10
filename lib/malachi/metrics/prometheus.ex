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

  @doc "Builds the exposition text (as iodata) from a system snapshot and the topic overview."
  @spec export(map(), [map()]) :: iodata()
  def export(system, topics) do
    mem = system.memory

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
      topic_metrics(topics)
    ]
  end

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
      metric("malachi_topic_consumer_groups", :gauge, "Consumer groups per topic", samples(topics, &length(&1.groups)))
    ]
  end

  defp samples(topics, value_fun), do: Enum.map(topics, fn t -> {[topic: t.name], value_fun.(t)} end)

  # --- exposition format ---

  defp metric(name, type, help, samples) do
    [
      "# HELP ",
      name,
      " ",
      help,
      "\n# TYPE ",
      name,
      " ",
      Atom.to_string(type),
      "\n",
      Enum.map(samples, fn {labels, value} -> [name, labels(labels), " ", value(value), "\n"] end)
    ]
  end

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
