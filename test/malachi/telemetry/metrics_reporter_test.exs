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
end
