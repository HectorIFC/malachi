defmodule Malachi.Metrics do
  @moduledoc """
  Real-time operational and security metrics kept in ETS (atomic `update_counter`), plus a
  periodically-sampled system snapshot and its recent history.

  The `increment_*`/`record_*` functions bump counters on the hot path (fast, lock-free), rate-limit and
  connection-limit blocks, auth failures and account lockouts, audit events, dashboard-auth outcomes, and
  TLS handshakes; `get_system_metrics/0` reads the live BEAM snapshot
  (memory, processes, io) folded together with those counters, and `get_history/1` returns the recent
  snapshots. A counter is created on first touch, so callers never need to initialize one.
  """
  use GenServer
  require Logger
  alias Malachi.Auth.{LockoutManager, SessionManager}
  alias Malachi.Histogram
  alias Malachi.I18n
  alias Malachi.Telemetry.MetricsReporter
  alias Malachi.UnexpectedMessage

  @metrics_table :malachi_metrics

  # The closed label set of `record_reconcile_degraded/2`; see `Malachi.Telemetry.reconcile_degraded/1`.
  @reconcile_degraded_reasons [:skipped, :down, :timeout]
  # The storage flush numbers live in :atomics reached through :persistent_term, not in ETS. Every
  # segment's owner flushes on its own, so tens of thousands of writers a second hit the same few
  # counters, and an ETS row serializes concurrent writers to one key while an :atomics add takes no lock
  # at all. persistent_term is read-mostly by design (a put triggers a global scan), so the term is put
  # once, at the first init, and reused by every restart after it.
  @storage_flush_key {__MODULE__, :storage_flush}
  @flushed_bytes_slot 1
  @flushed_records_slot 2

  # Retention. The skip and expiry counters live in the ETS table under tuple keys, off the hot path: a
  # skip is counted only when a reader was actually moved past missing data, and an expiry once per
  # segment a sweep tried. The sweep duration histogram is shaped like the flush one.
  @retention_sweep_key {__MODULE__, :retention_sweep}
  # The {topic, group} pairs admitted as their own label, so the exported series stay bounded however many
  # groups clients invent (`:retention_metrics_max_groups`).
  @retention_groups_table :malachi_retention_groups
  @default_retention_max_groups 1000
  # Every refusal a delete can get, so each has a series at zero from boot (`Retention.reply_label/1`).
  @retention_failure_replies [:migrating, :segment_active, :other]

  @doc "Starts the metrics server (owns the ETS counter table)."
  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc """
  Increment rate limit blocked counter for specific action.
  """
  def increment_rate_limit_blocked(action) do
    key = {:rate_limit_blocked, action}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment connection limit blocked counter.
  """
  def increment_connection_limit_blocked do
    :ets.update_counter(@metrics_table, :connection_limit_blocked, {2, 1}, {:connection_limit_blocked, 0})
    :ok
  end

  # --- operation counters (fed by the telemetry -> metrics reporter, O4) ---

  @doc "Records `count` records and `bytes` value bytes produced (from the produce telemetry event)."
  def record_produce(count, bytes) do
    :ets.update_counter(@metrics_table, :records_produced, {2, count}, {:records_produced, 0})
    :ets.update_counter(@metrics_table, :bytes_produced, {2, bytes}, {:bytes_produced, 0})
    :ok
  end

  @doc "Records `count` records consumed (from the consume telemetry event)."
  def record_consume(count) do
    :ets.update_counter(@metrics_table, :records_consumed, {2, count}, {:records_consumed, 0})
    :ok
  end

  @doc """
  Records one group-commit flush (from the storage flush telemetry event): `duration_us` into the latency
  histogram, plus the bytes and records it made durable.

  Four lock-free adds, not one atomic write: a scrape that lands between them can see a flush in the
  count before its duration is in the sum, which skews a windowed mean by at most one flush. A no-op when
  `Malachi.Metrics` never started, so a store used on its own does not need it.
  """
  @spec record_flush(non_neg_integer(), non_neg_integer(), non_neg_integer()) :: :ok
  def record_flush(duration_us, bytes, records) do
    case :persistent_term.get(@storage_flush_key, nil) do
      nil ->
        :ok

      %{histogram: histogram, totals: totals} ->
        :ok = Histogram.record(histogram, duration_us)
        :ok = :atomics.add(totals, @flushed_bytes_slot, bytes)
        :atomics.add(totals, @flushed_records_slot, records)
    end
  end

  @doc """
  The storage flush histogram as the Prometheus exporter needs it: the count of flushes at or below each
  of `Malachi.Histogram.edges/0`, the total count and the sum in microseconds, the durability totals, and
  `created`, the Unix time in seconds when this node's histogram began.

  `created` is what tells two scrapes of one histogram from scrapes on either side of a node restart: it
  changes only when the node boots again, never on a `Malachi.Metrics` restart, which keeps the samples.
  Counters alone cannot tell, since a restarted node can flush past its old totals before the next scrape.
  Kept out of `get_system_metrics/0`, which is snapshotted every second and sent to the dashboard, neither
  of which has any use for the buckets. All zeros when `Malachi.Metrics` never started.
  """
  @spec storage_flush_histogram() :: %{
          buckets: [{float(), non_neg_integer()}],
          count: non_neg_integer(),
          sum_us: non_neg_integer(),
          bytes: non_neg_integer(),
          records: non_neg_integer(),
          created: float()
        }
  def storage_flush_histogram do
    case :persistent_term.get(@storage_flush_key, nil) do
      nil ->
        Map.merge(histogram_snapshot(nil), %{bytes: 0, records: 0})

      state ->
        Map.merge(storage_flush_totals(state), histogram_snapshot(state))
    end
  end

  # A histogram kept in :persistent_term as the Prometheus exporter renders it: the cumulative buckets,
  # the count and the sum in microseconds, and when it began. All zeros when it was never created.
  defp histogram_snapshot(nil), do: %{buckets: Enum.map(Histogram.edges(), &{&1, 0}), count: 0, sum_us: 0, created: 0.0}

  defp histogram_snapshot(%{histogram: histogram, created: created}) do
    # The count comes from the same pass as the buckets, so `+Inf` is never below the last edge.
    {buckets, count} = Histogram.cumulative(histogram)
    %{buckets: buckets, count: count, sum_us: Histogram.sum(histogram), created: created}
  end

  # The flush summary the dashboard shows: totals plus percentiles cumulative since the node booted (the
  # histogram never decays). A windowed view comes from subtracting two scrapes of the exported buckets.
  defp storage_flush_summary do
    case :persistent_term.get(@storage_flush_key, nil) do
      nil ->
        %{count: 0, sum_us: 0, bytes: 0, records: 0, p50_us: 0.0, p99_us: 0.0, p999_us: 0.0}

      %{histogram: histogram} = state ->
        Map.merge(storage_flush_totals(state), %{
          p50_us: Histogram.percentile(histogram, 50),
          p99_us: Histogram.percentile(histogram, 99),
          p999_us: Histogram.percentile(histogram, 99.9)
        })
    end
  end

  defp storage_flush_totals(%{histogram: histogram, totals: totals}) do
    %{
      count: Histogram.count(histogram),
      sum_us: Histogram.sum(histogram),
      bytes: :atomics.get(totals, @flushed_bytes_slot),
      records: :atomics.get(totals, @flushed_records_slot)
    }
  end

  @doc """
  Records one skip (from the retention skip telemetry event): a reader of `group` on `topic` was moved
  past `offsets` offsets no longer stored. Counted as one event and as the offsets, under the reader's
  `origin` and the skip's `span`.

  The series carry WHICH KIND of reader they count (`reader`) beside the name (`group`): `:group` with
  the name as it is, `:none` with an empty name for a fetch outside a group (`group` is `nil`), and
  `:other` with an empty name for a group folded by the cap. Nothing reserves a group name, so the kind
  has to be its own label: with the name alone, a group a client called `"__other__"` would share a
  series with the folded ones, and one called `""` with a fetch outside a group.

  A `{topic, group}` pair keeps its own name while fewer than `:retention_metrics_max_groups` (default
  1000) pairs have been admitted on this node, and is folded into `:other` after that, so a client
  inventing group names cannot grow the scrape without bound. The per-topic sum stays exact. Admission
  is check-then-insert across the few processes that report (one skip reporter per data-plane shard), so
  concurrent first skips can admit a pair or two past the cap: a bound, not an exact limit.
  """
  @spec record_retention_skip(String.t(), String.t() | nil, atom(), atom(), non_neg_integer()) :: :ok
  def record_retention_skip(topic, group, origin, span, offsets) do
    {reader, label} = retention_reader(topic, group)
    key = {:retention_skip, topic, reader, label, origin, span}
    :ets.update_counter(@metrics_table, key, [{2, 1}, {3, offsets}], {key, 0, 0})
    :ok
  end

  @doc "How many `{topic, group}` pairs have their own retention skip label on this node."
  @spec retention_group_count() :: non_neg_integer()
  def retention_group_count, do: :ets.info(@retention_groups_table, :size)

  # A fetch outside a group has no name to fold and takes no room in the cap.
  defp retention_reader(_topic, nil), do: {:none, ""}

  defp retention_reader(topic, group) do
    max = Application.get_env(:malachi, :retention_metrics_max_groups, @default_retention_max_groups)

    cond do
      :ets.member(@retention_groups_table, {topic, group}) ->
        {:group, group}

      retention_group_count() < max ->
        # `insert_new` losing a race only means another reporter admitted the same pair first.
        _ = :ets.insert_new(@retention_groups_table, {{topic, group}})
        {:group, group}

      # The name is dropped rather than exported: keeping it is what the cap exists to prevent.
      true ->
        {:other, ""}
    end
  end

  @doc """
  Records what one expire answered (from the retention expire telemetry event): an expired segment
  (`:ok`) counts itself and its `bytes` under `topic`; a segment already gone (`:no_such_segment`)
  counts nowhere, since an earlier sweep expired it; anything else is a refusal counted under its reply.
  """
  @spec record_retention_expire(String.t(), non_neg_integer(), atom()) :: :ok
  def record_retention_expire(topic, bytes, :ok) do
    key = {:retention_expired, topic}
    :ets.update_counter(@metrics_table, key, [{2, 1}, {3, bytes}], {key, 0, 0})
    :ok
  end

  def record_retention_expire(_topic, _bytes, :no_such_segment), do: :ok

  def record_retention_expire(_topic, _bytes, reply) do
    key = {:retention_expire_failure, if(reply in @retention_failure_replies, do: reply, else: :other)}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Records `count` replica directories an expire of `topic` left behind, because the replica holding them
  did not answer its delete (from the retention orphan telemetry event). The counterpart of what the
  orphan sweeper reclaims: the two together say whether the sweeper is keeping up.
  """
  @spec record_retention_orphan_left(String.t(), non_neg_integer()) :: :ok
  def record_retention_orphan_left(topic, count) do
    key = {:retention_orphan_left, topic}
    :ets.update_counter(@metrics_table, key, {2, count}, {key, 0})
    :ok
  end

  @doc """
  Records that a sweep found `topic` bound to a policy name it could not resolve, and expired nothing
  of it. Counted per sweep, so a name that stays unresolved keeps the series moving: the disk it holds
  is invisible otherwise, and the fix is a binding an operator has to make.
  """
  @spec record_retention_unresolved_policy(String.t(), non_neg_integer()) :: :ok
  def record_retention_unresolved_policy(topic, count) do
    key = {:retention_unresolved_policy, topic}
    :ets.update_counter(@metrics_table, key, {2, count}, {key, 0})
    :ok
  end

  @doc """
  Records `count` replica directories the orphan sweeper reclaimed (from the retention orphan
  telemetry event). Unlabeled: a reclaimed directory is named by the sweeper's log line, and the
  segment it belonged to is exactly what the control plane no longer knows.
  """
  @spec record_retention_orphan_removed(non_neg_integer()) :: :ok
  def record_retention_orphan_removed(count) do
    :ets.update_counter(@metrics_table, :retention_orphan_removed, {2, count}, {:retention_orphan_removed, 0})
    :ok
  end

  @doc """
  Records one retention sweep's duration (from the retention sweep telemetry event). The histogram exists
  from this server's first start, before the reporter that calls this is attached.
  """
  @spec record_retention_sweep(non_neg_integer()) :: :ok
  def record_retention_sweep(duration_us) do
    %{histogram: histogram} = :persistent_term.get(@retention_sweep_key)
    Histogram.record(histogram, duration_us)
  end

  @doc """
  The retention counters as the Prometheus exporter needs them: every skip series (`topic`, `reader`,
  `group`, `origin`, `span`, with its `events` and `offsets`), the expired `segments` and `bytes` per topic, the
  refusals per reply (every known reply, zero included), the replica directories expiries left behind per
  topic, the ones the sweeper reclaimed, and the sweep duration histogram in the shape
  of `storage_flush_histogram/0` (its `count` is the number of sweeps). Read only at scrape time.
  """
  @spec retention_snapshot() :: %{
          skips: [map()],
          expired: [map()],
          orphans_left: [map()],
          orphans_removed: non_neg_integer(),
          unresolved_policies: [map()],
          failures: %{atom() => non_neg_integer()},
          sweeps: map()
        }
  def retention_snapshot do
    skips =
      for [topic, reader, group, origin, span, events, offsets] <-
            :ets.match(@metrics_table, {{:retention_skip, :"$1", :"$2", :"$3", :"$4", :"$5"}, :"$6", :"$7"}) do
        %{topic: topic, reader: reader, group: group, origin: origin, span: span, events: events, offsets: offsets}
      end

    expired =
      for [topic, segments, bytes] <- :ets.match(@metrics_table, {{:retention_expired, :"$1"}, :"$2", :"$3"}) do
        %{topic: topic, segments: segments, bytes: bytes}
      end

    orphans_left =
      for [topic, directories] <- :ets.match(@metrics_table, {{:retention_orphan_left, :"$1"}, :"$2"}) do
        %{topic: topic, directories: directories}
      end

    unresolved_policies =
      for [topic, sweeps] <- :ets.match(@metrics_table, {{:retention_unresolved_policy, :"$1"}, :"$2"}) do
        %{topic: topic, sweeps: sweeps}
      end

    %{
      skips: Enum.sort(skips),
      expired: Enum.sort(expired),
      orphans_left: Enum.sort(orphans_left),
      orphans_removed: get_counter(:retention_orphan_removed),
      unresolved_policies: Enum.sort(unresolved_policies),
      failures: Map.new(@retention_failure_replies, &{&1, get_counter({:retention_expire_failure, &1})}),
      sweeps: histogram_snapshot(:persistent_term.get(@retention_sweep_key, nil))
    }
  end

  @doc "Records an authentication attempt with `result` (`:ok` / `:error`)."
  def record_auth(result) do
    key = {:auth_result, result}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc "Records a quorum replication with `result` (`:ok` / `:no_quorum`)."
  def record_replication(result) do
    key = {:replication_result, result}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Records a segment that failed checksum verification, by `reason` (`:bad_crc`, `:bad_magic`,
  `:incomplete`). Non-zero means data at rest is damaged somewhere on this node, which is otherwise
  a silent condition: a damaged copy serves short reads without any error.
  """
  def record_integrity_failure(reason) do
    key = {:integrity_failure, reason}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Records a storage operation that failed on a segment's copy (`Malachi.Cluster.ReplicationServer` took
  the copy out of service), bucketed by `reason`: `:enospc` (a full volume), `:eio` (a failing device),
  `:eacces` (a permission problem), and `:other` for anything else. Buckets rather than the raw reason
  because a Prometheus label must come from a closed set, and these three are the ones that call for
  different operator actions.
  """
  def record_storage_failure(reason) do
    key = {:storage_failure, storage_failure_bucket(reason)}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  defp storage_failure_bucket(reason) when reason in [:enospc, :eio, :eacces], do: reason
  defp storage_failure_bucket(_reason), do: :other

  @doc """
  Records one integrity scrub pass: how many segments it verified, repaired, and could not repair. A
  verified total that stops advancing is how an operator sees that the scrub itself has stopped,
  which the failure counters alone cannot show (they stay at zero both when all is well and when
  nothing is checking). The unrepairable total is the one that calls for a human: it counts damage
  the cluster could not heal by itself, which on a single node is every finding there is.
  """
  def record_scrub_pass(verified, repaired, unrepairable) do
    :ets.update_counter(@metrics_table, :scrub_segments_verified, {2, verified}, {:scrub_segments_verified, 0})
    :ets.update_counter(@metrics_table, :scrub_segments_repaired, {2, repaired}, {:scrub_segments_repaired, 0})

    :ets.update_counter(
      @metrics_table,
      :scrub_segments_unrepairable,
      {2, unrepairable},
      {:scrub_segments_unrepairable, 0}
    )

    :ok
  end

  @doc """
  Records one segment whose store was fenced while recording its seal in the control plane failed, so
  it is closed to writes while the metadata still calls it active and its range accepts nothing.

  Counted separately from `record_fences_reconciled/1` because it is the RATIO that carries the
  meaning: a detection that a later pass reconciles is a split that recovered, and a detection nothing
  reconciles is a range that is stuck. One counter could not tell those apart.
  """
  def record_orphaned_fence do
    :ets.update_counter(@metrics_table, :orphaned_fences, {2, 1}, {:orphaned_fences, 0})
    :ok
  end

  @doc """
  Records that a heal pass finished the seal for `count` segments whose store was already fenced,
  which is what lets their ranges take writes again. See `record_orphaned_fence/0` for why both halves
  are counted.
  """
  def record_fences_reconciled(count) do
    :ets.update_counter(@metrics_table, :fences_reconciled, {2, count}, {:fences_reconciled, 0})
    :ok
  end

  @doc """
  Records `count` ticks on which the broker's control plane reconcile did not complete, by `reason`
  (`:skipped`, `:down` or `:timeout`; anything else is counted as `:other`, so the exported label set
  stays closed). See `Malachi.Telemetry.reconcile_degraded/1`.
  """
  def record_reconcile_degraded(reason, count \\ 1) do
    key = {:reconcile_degraded, reconcile_degraded_bucket(reason)}
    :ets.update_counter(@metrics_table, key, {2, count}, {key, 0})
    :ok
  end

  defp reconcile_degraded_bucket(reason) when reason in @reconcile_degraded_reasons, do: reason
  defp reconcile_degraded_bucket(_reason), do: :other

  # Every reason, zero included: a series that appears only after the first degraded tick cannot be
  # alerted on with `increase()` nor asserted to be zero.
  defp reconcile_degraded_counts do
    for reason <- @reconcile_degraded_reasons ++ [:other],
        do: %{reason: reason, count: get_counter({:reconcile_degraded, reason})}
  end

  @doc """
  Records one message a long-lived server had no clause for (see `Malachi.UnexpectedMessage`), by the
  server's label and the `kind` it arrived as. A label outside `Malachi.UnexpectedMessage.servers/0` is
  counted as `:other`, so the exported label set stays closed whatever a caller passes.
  """
  def record_unexpected_message(server, kind) do
    key = {:unexpected_message, unexpected_message_bucket(server), kind}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  defp unexpected_message_bucket(server) do
    if server in UnexpectedMessage.servers(), do: server, else: :other
  end

  # Every server and kind, zero included: a series that exists only after the first drop cannot be
  # alerted on with `increase()` nor asserted to be zero. Maps rather than tuples: the snapshot is also
  # served to the dashboard as JSON.
  defp unexpected_message_counts do
    for server <- UnexpectedMessage.servers() ++ [:other], kind <- UnexpectedMessage.kinds() do
      %{server: server, kind: kind, count: get_counter({:unexpected_message, server, kind})}
    end
  end

  @doc """
  Increment failed authentication attempt counter.
  """
  def increment_failed_auth_attempt do
    key = :failed_auth_attempts
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment account lockout counter.
  """
  def increment_account_lockout do
    key = :account_lockouts
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment account lockout blocked attempt counter.
  """
  def increment_account_lockout_blocked do
    key = :account_lockout_blocked
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment audit event counter by event type.
  """
  def increment_audit_event(event_type) do
    key = {:audit_event, event_type}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment dashboard authentication success counter.
  """
  def increment_dashboard_auth_success do
    key = :dashboard_auth_success
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment dashboard authentication failure counter.
  """
  def increment_dashboard_auth_failed do
    key = :dashboard_auth_failed
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment dashboard authentication blocked (rate limited) counter.
  """
  def increment_dashboard_auth_blocked do
    key = :dashboard_auth_blocked
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment TLS handshake success counter.
  """
  def increment_tls_handshake_success do
    key = :tls_handshake_success
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Increment TLS handshake failure counter.
  """
  def increment_tls_handshake_failed do
    key = :tls_handshake_failed
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc """
  Record the negotiated TLS version for a connection.
  """
  def record_tls_version(version) do
    key = {:tls_version, version}
    :ets.update_counter(@metrics_table, key, {2, 1}, {key, 0})
    :ok
  end

  @doc "A snapshot of system-wide metrics (memory, processes, connections, auth) for the dashboard."
  def get_system_metrics do
    memory = :erlang.memory()
    {{:input, input_bytes}, {:output, output_bytes}} = :erlang.statistics(:io)

    %{
      timestamp: System.system_time(:second),
      schedulers_online: :erlang.system_info(:schedulers_online),
      process_count: :erlang.system_info(:process_count),
      process_limit: :erlang.system_info(:process_limit),
      run_queue: :erlang.statistics(:run_queue),
      memory: %{
        total_mb: memory[:total] / 1_048_576,
        processes_mb: memory[:processes] / 1_048_576,
        ets_mb: memory[:ets] / 1_048_576,
        atom_mb: memory[:atom] / 1_048_576,
        binary_mb: memory[:binary] / 1_048_576
      },
      ets_tables: length(:ets.all()),
      io: %{
        input_bytes: input_bytes,
        output_bytes: output_bytes
      },
      uptime_seconds: :erlang.statistics(:wall_clock) |> elem(0) |> div(1000),
      rate_limiting: %{
        auth_blocked: get_counter({:rate_limit_blocked, :auth}),
        publish_blocked: get_counter({:rate_limit_blocked, :publish}),
        subscribe_blocked: get_counter({:rate_limit_blocked, :subscribe}),
        connection_blocks: get_counter(:connection_limit_blocked)
      },
      security: %{
        failed_auth_attempts: get_counter(:failed_auth_attempts),
        account_lockouts: get_counter(:account_lockouts),
        lockout_blocks: get_counter(:account_lockout_blocked),
        active_lockouts: get_active_lockout_count(),
        active_sessions: get_active_session_count(),
        dashboard: %{
          auth_success: get_counter(:dashboard_auth_success),
          auth_failed: get_counter(:dashboard_auth_failed),
          auth_blocked: get_counter(:dashboard_auth_blocked)
        },
        audit_events: %{
          auth_success: get_counter({:audit_event, :auth_success}),
          auth_failure: get_counter({:audit_event, :auth_failure}),
          auth_lockout: get_counter({:audit_event, :auth_lockout}),
          session_created: get_counter({:audit_event, :session_created}),
          session_revoked: get_counter({:audit_event, :session_revoked}),
          session_expired: get_counter({:audit_event, :session_expired}),
          session_hijack_attempt: get_counter({:audit_event, :session_hijack_attempt}),
          account_unlocked: get_counter({:audit_event, :account_unlocked}),
          config_validation_failed: get_counter({:audit_event, :config_validation_failed}),
          dashboard_access: get_counter({:audit_event, :dashboard_access}),
          dashboard_login_success: get_counter({:audit_event, :dashboard_login_success}),
          dashboard_auth_failure: get_counter({:audit_event, :dashboard_auth_failure})
        }
      },
      tls: %{
        enabled: Application.get_env(:malachi, :enable_tls, false),
        required: Application.get_env(:malachi, :require_tls, false),
        handshakes_success: get_counter(:tls_handshake_success),
        handshakes_failed: get_counter(:tls_handshake_failed),
        versions: %{
          "tlsv1.3": get_counter({:tls_version, :"tlsv1.3"}),
          "tlsv1.2": get_counter({:tls_version, :"tlsv1.2"})
        }
      },
      operations: %{
        records_produced: get_counter(:records_produced),
        bytes_produced: get_counter(:bytes_produced),
        records_consumed: get_counter(:records_consumed),
        auth_ok: get_counter({:auth_result, :ok}),
        auth_error: get_counter({:auth_result, :error}),
        replication_ok: get_counter({:replication_result, :ok}),
        replication_no_quorum: get_counter({:replication_result, :no_quorum}),
        integrity_bad_crc: get_counter({:integrity_failure, :bad_crc}),
        integrity_bad_magic: get_counter({:integrity_failure, :bad_magic}),
        integrity_incomplete: get_counter({:integrity_failure, :incomplete}),
        integrity_short_copy: get_counter({:integrity_failure, :short_copy}),
        integrity_bad_index: get_counter({:integrity_failure, :bad_index}),
        storage_failure_enospc: get_counter({:storage_failure, :enospc}),
        storage_failure_eio: get_counter({:storage_failure, :eio}),
        storage_failure_eacces: get_counter({:storage_failure, :eacces}),
        storage_failure_other: get_counter({:storage_failure, :other}),
        scrub_segments_verified: get_counter(:scrub_segments_verified),
        scrub_segments_repaired: get_counter(:scrub_segments_repaired),
        scrub_segments_unrepairable: get_counter(:scrub_segments_unrepairable),
        orphaned_fences: get_counter(:orphaned_fences),
        fences_reconciled: get_counter(:fences_reconciled),
        reconcile_degraded: reconcile_degraded_counts(),
        unexpected_messages: unexpected_message_counts()
      },
      storage_flush: storage_flush_summary(),
      atom_table: get_atom_monitor_stats(),
      memory_details: get_memory_monitor_stats()
    }
  end

  defp get_atom_monitor_stats do
    if Code.ensure_loaded?(Malachi.AtomMonitor) and Process.whereis(Malachi.AtomMonitor) do
      try do
        Malachi.AtomMonitor.get_stats()
      rescue
        _ -> %{atom_count: 0, atom_limit: 0, usage_percent: 0.0, status: :unknown}
      end
    else
      %{atom_count: :erlang.system_info(:atom_count), atom_limit: 1_048_576, usage_percent: 0.0, status: :unavailable}
    end
  end

  defp get_memory_monitor_stats do
    if Code.ensure_loaded?(Malachi.MemoryMonitor) and Process.whereis(Malachi.MemoryMonitor) do
      try do
        Malachi.MemoryMonitor.get_memory_stats()
      rescue
        _ -> %{}
      end
    else
      %{}
    end
  end

  @impl true
  def init(:ok) do
    :ets.new(@metrics_table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(:malachi_metrics_history, [
      :ordered_set,
      :public,
      :named_table,
      read_concurrency: true
    ])

    :ets.new(@retention_groups_table, [:set, :public, :named_table, read_concurrency: true])

    # Before the reporter is attached, so no flush event can arrive to a missing histogram. Reused across
    # restarts: the samples are the node's history, and dropping them on a Metrics crash would silently
    # reset a percentile an operator is watching.
    put_new_histogram(@storage_flush_key, %{totals: :atomics.new(2, signed: false)})
    put_new_histogram(@retention_sweep_key, %{})

    # Fold the telemetry hot-path events into these counters. Attached here so the ETS table exists
    # first; idempotent, so a Metrics restart re-attaches cleanly.
    MetricsReporter.attach()

    schedule_snapshot()
    schedule_cleanup()

    Logger.info(I18n.t(:metrics_started))
    {:ok, %{}}
  end

  @impl true
  def handle_info(:snapshot, state) do
    take_snapshot()
    schedule_snapshot()
    {:noreply, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    cleanup_old_snapshots()
    schedule_cleanup()
    {:noreply, state}
  end

  # Creates the :persistent_term histogram under `key` (with `extra` fields) unless an earlier start of
  # this server already did, keeping the node's samples across a Metrics restart.
  defp put_new_histogram(key, extra) do
    if :persistent_term.get(key, nil) == nil do
      :persistent_term.put(
        key,
        Map.merge(extra, %{histogram: Histogram.new(), created: System.system_time(:millisecond) / 1000})
      )
    end
  end

  defp get_counter(key) do
    case :ets.lookup(@metrics_table, key) do
      [{^key, value}] -> value
      [] -> 0
    end
  end

  defp get_active_lockout_count do
    if Code.ensure_loaded?(LockoutManager) and Process.whereis(LockoutManager) do
      try do
        LockoutManager.list_locked_accounts() |> length()
      rescue
        ArgumentError -> 0
      end
    else
      0
    end
  end

  defp get_active_session_count do
    if Code.ensure_loaded?(SessionManager) do
      try do
        SessionManager.list_sessions() |> length()
      rescue
        ArgumentError -> 0
      end
    else
      0
    end
  end

  defp schedule_snapshot do
    interval = Application.get_env(:malachi, :metrics_snapshot_interval_ms, 1_000)
    Process.send_after(self(), :snapshot, interval)
  end

  defp schedule_cleanup do
    interval = Application.get_env(:malachi, :metrics_cleanup_interval_ms, 60_000)
    Process.send_after(self(), :cleanup, interval)
  end

  defp take_snapshot do
    timestamp = System.system_time(:second)

    snapshot = %{
      timestamp: timestamp,
      system: get_system_metrics()
    }

    :ets.insert(:malachi_metrics_history, {timestamp, snapshot})
  end

  defp cleanup_old_snapshots do
    history_seconds = Application.get_env(:malachi, :metrics_history_seconds, 300)
    cutoff = System.system_time(:second) - history_seconds

    :ets.select_delete(:malachi_metrics_history, [
      {{:"$1", :_}, [{:<, :"$1", cutoff}], [true]}
    ])
  end

  @doc "The recent per-second history samples for the last `seconds` (default 60)."
  def get_history(seconds \\ 60) do
    cutoff = System.system_time(:second) - seconds

    :ets.select(:malachi_metrics_history, [
      {{:"$1", :"$2"}, [{:>=, :"$1", cutoff}], [:"$2"]}
    ])
  end
end
