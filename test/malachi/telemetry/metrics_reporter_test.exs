defmodule Malachi.Telemetry.MetricsReporterTest do
  # The default reporter is attached at boot (Metrics.init), so emitting a telemetry event folds into the
  # ETS Metrics counters that get_system_metrics/0 (and the Prometheus endpoint) expose. async: false:
  # it reads the shared, process-global metrics counters.
  use ExUnit.Case, async: false

  alias Malachi.Metrics
  alias Malachi.Telemetry

  test "hot-path telemetry events increment the operation counters" do
    before = Metrics.get_system_metrics().operations

    Telemetry.produce("t", 5, 100)
    Telemetry.consume("t", 3)
    Telemetry.auth(:ok)
    Telemetry.auth(:error)
    Telemetry.replication_commit(5, :ok)
    Telemetry.replication_commit(2, :no_quorum)

    ops = Metrics.get_system_metrics().operations

    # records/bytes counters advance by the measurement; auth/replication by one per event
    assert ops.records_produced == before.records_produced + 5
    assert ops.bytes_produced == before.bytes_produced + 100
    assert ops.records_consumed == before.records_consumed + 3
    assert ops.auth_ok == before.auth_ok + 1
    assert ops.auth_error == before.auth_error + 1
    assert ops.replication_ok == before.replication_ok + 1
    assert ops.replication_no_quorum == before.replication_no_quorum + 1
  end

  test "an integrity failure increments the counter for its reason" do
    before = Metrics.get_system_metrics().operations
    verdict = %{reason: :bad_crc, position: 128, unreadable_bytes: 64, sealed?: true}

    Telemetry.storage_integrity(verdict, {{"events", 0}, 0}, :recover)

    ops = Metrics.get_system_metrics().operations
    assert ops.integrity_bad_crc == before.integrity_bad_crc + 1
    assert ops.integrity_bad_magic == before.integrity_bad_magic
    assert ops.integrity_incomplete == before.integrity_incomplete
  end

  test "a torn tail on an ACTIVE segment is not counted as damage, but on a sealed one it is" do
    # Recovering past unacknowledged bytes after a crash is routine, and ReplicationServer logs it as
    # the cost of the crash rather than as an alarm. This counter is the series an operator alerts on,
    # so counting a restart there would make an ordinary reboot look like corruption at rest.
    before = Metrics.get_system_metrics().operations
    torn_tail = %{reason: :incomplete, position: 128, unreadable_bytes: 64, sealed?: false}

    Telemetry.storage_integrity(torn_tail, {{"events", 0}, 0}, :recover)

    assert Metrics.get_system_metrics().operations.integrity_incomplete == before.integrity_incomplete

    # the same reason on an immutable copy is corruption at rest, and stays counted
    Telemetry.storage_integrity(%{torn_tail | sealed?: true}, {{"events", 0}, 0}, :scrub)

    assert Metrics.get_system_metrics().operations.integrity_incomplete == before.integrity_incomplete + 1

    # so is rot on an active segment: only the torn TAIL is expected after a crash
    Telemetry.storage_integrity(%{torn_tail | reason: :bad_crc}, {{"events", 0}, 0}, :recover)

    assert Metrics.get_system_metrics().operations.integrity_bad_crc == before.integrity_bad_crc + 1
  end

  test "a scrub pass exports what it could not repair, not only what it fixed" do
    # A non-zero failure counter says something is damaged; only this says whether the cluster healed
    # it. On a single node, with no replica to repair from, it is every finding there is.
    before = Metrics.get_system_metrics().operations

    Telemetry.scrub_pass(10, 2, 1, 1)

    ops = Metrics.get_system_metrics().operations
    assert ops.scrub_segments_verified == before.scrub_segments_verified + 10
    assert ops.scrub_segments_repaired == before.scrub_segments_repaired + 1
    assert ops.scrub_segments_unrepairable == before.scrub_segments_unrepairable + 1
  end
end
