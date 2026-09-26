defmodule Malachi.Telemetry.MetricsReporterTest do
  # The default reporter is attached at boot (Metrics.init), so emitting a telemetry event folds into the
  # ETS Metrics counters that get_system_metrics/0 (and the Prometheus endpoint) expose. async: false:
  # it reads the shared, process-global metrics counters.
  use ExUnit.Case, async: false

  alias Malachi.Metrics
  alias Malachi.Telemetry
  alias Malachi.Test.UnknownMessages
  alias Malachi.UnexpectedMessage

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

  test "a storage failure is counted by reason, with an unfamiliar reason under other" do
    # Buckets rather than the raw reason, because a Prometheus label has to come from a closed set: a new
    # POSIX reason must land somewhere an alert already watches, not in a series nobody created.
    before = Metrics.get_system_metrics().operations
    segment = {{"events", 0}, 0}

    Telemetry.storage_failure(segment, :enospc)
    Telemetry.storage_failure(segment, :enospc)
    Telemetry.storage_failure(segment, :eio)
    Telemetry.storage_failure(segment, :eacces)
    Telemetry.storage_failure(segment, :einval)

    ops = Metrics.get_system_metrics().operations
    assert ops.storage_failure_enospc == before.storage_failure_enospc + 2
    assert ops.storage_failure_eio == before.storage_failure_eio + 1
    assert ops.storage_failure_eacces == before.storage_failure_eacces + 1
    assert ops.storage_failure_other == before.storage_failure_other + 1
  end

  test "an orphaned fence and its reconciliation are counted as a pair" do
    # A fence whose control-plane seal failed closes a range to writes until something reconciles it.
    # The detection alone says a split hit an `ra` error, which is survivable; it is detections that
    # nothing reconciles that mean a range is stuck, and only both counters together can say so.
    before = Metrics.get_system_metrics().operations

    Telemetry.orphaned_fence({{"events", 0}, 0}, :ra_timeout)

    assert Metrics.get_system_metrics().operations.orphaned_fences == before.orphaned_fences + 1
    assert Metrics.get_system_metrics().operations.fences_reconciled == before.fences_reconciled

    # A pass reconciles more than one segment at a time, so this counter advances by the measurement.
    Telemetry.fence_reconciled(2)

    assert Metrics.get_system_metrics().operations.fences_reconciled == before.fences_reconciled + 2
  end

  test "a degraded reconcile lands on the counter for its reason" do
    # The broker emits this from its own loop when a reconcile does not complete. It is the only signal
    # that the node is serving from a view that is no longer being refreshed.
    before = degraded_counts()

    Telemetry.reconcile_degraded(:skipped)
    Telemetry.reconcile_degraded(:timeout)

    now = degraded_counts()

    assert now.skipped == before.skipped + 1
    assert now.timeout == before.timeout + 1
    assert now.down == before.down
  end

  defp degraded_counts do
    Metrics.get_system_metrics().operations.reconcile_degraded
    |> Map.new(fn %{reason: reason, count: count} -> {reason, count} end)
  end

  describe "storage flush" do
    # Flushes in (edge_lo, edge_hi]: the band between two exported edges, read off the cumulative buckets.
    defp flushes_between(edge_lo, edge_hi) do
      buckets = Map.new(Metrics.storage_flush_histogram().buckets)
      buckets[edge_hi] - buckets[edge_lo]
    end

    test "a flush event advances the count, sum and durability totals" do
      before = Metrics.storage_flush_histogram()

      Telemetry.storage_flush(1500, 4096, 10, "segment-0", "/tmp/seg")
      Telemetry.storage_flush(2500, 2048, 5, "segment-0", "/tmp/seg")

      after_flushes = Metrics.storage_flush_histogram()
      assert after_flushes.count == before.count + 2
      assert after_flushes.sum_us == before.sum_us + 4000
      assert after_flushes.bytes == before.bytes + 6144
      assert after_flushes.records == before.records + 15

      summary = Metrics.get_system_metrics().storage_flush
      assert summary.count == after_flushes.count
      assert summary.sum_us == after_flushes.sum_us
      assert summary.bytes == after_flushes.bytes
      assert summary.records == after_flushes.records
    end

    test "each flush lands in the bucket its duration belongs to" do
      # The band between the 2^(65/4) (~77.9ms) and 2^(66/4) (~92.7ms) edges. Comparing that band before
      # and after, rather than a percentile, keeps the assertion exact whatever else the suite flushed:
      # a slow disk would have to stall a test flush into this exact band to disturb it.
      lo = :math.pow(2, 65 / 4)
      hi = :math.pow(2, 66 / 4)
      before = flushes_between(lo, hi)

      for _ <- 1..25, do: Telemetry.storage_flush(80_000, 1024, 1, "segment-0", "/tmp/seg")
      Telemetry.storage_flush(95_000, 1024, 1, "segment-0", "/tmp/seg")

      assert flushes_between(lo, hi) == before + 25
    end

    test "the dashboard summary percentiles are in microseconds and ordered" do
      Telemetry.storage_flush(1500, 1, 1, "segment-0", "/tmp/seg")
      summary = Metrics.get_system_metrics().storage_flush

      assert summary.p50_us > 0.0
      assert summary.p50_us <= summary.p99_us
      assert summary.p99_us <= summary.p999_us
    end
  end

  describe "retention" do
    alias Malachi.Broker.Skip

    defp skip(offsets, overrides \\ []) do
      struct!(
        %Skip{range_id: {"t", 0}, source_range_id: {"t", 0}, from: 0, offsets: offsets, origin: :cursor, source: :self},
        overrides
      )
    end

    defp skips_of(topic), do: Enum.filter(Metrics.retention_snapshot().skips, &(&1.topic == topic))

    defp unique_topic, do: "retention_#{System.unique_integer([:positive])}"

    test "a skip counts one event and its offsets under its topic, reader, group, origin and span" do
      topic = unique_topic()

      Telemetry.retention_skip(topic, "billing", skip(5))
      Telemetry.retention_skip(topic, "billing", skip(2))
      Telemetry.retention_skip(topic, nil, skip(:unknown, source: :ancestor, origin: :start))

      assert Enum.sort_by(skips_of(topic), & &1.group) == [
               %{topic: topic, reader: :none, group: "", origin: :start, span: :unknown, events: 1, offsets: 0},
               %{topic: topic, reader: :group, group: "billing", origin: :cursor, span: :exact, events: 2, offsets: 7}
             ]
    end

    test "a group named like a reserved label keeps a series of its own" do
      # Nothing reserves a group name: a client can call its group "__other__" (the old overflow label)
      # or "" (what a fetch outside a group exports), and neither may be folded into those buckets.
      topic = unique_topic()

      Telemetry.retention_skip(topic, "__other__", skip(1))
      Telemetry.retention_skip(topic, "", skip(2))
      Telemetry.retention_skip(topic, nil, skip(4))

      rows = Map.new(skips_of(topic), &{{&1.reader, &1.group}, &1.offsets})
      assert rows == %{{:group, "__other__"} => 1, {:group, ""} => 2, {:none, ""} => 4}
    end

    test "past the cap on topic and group pairs, new groups fold into reader=other with no name" do
      topic = unique_topic()
      previous = Application.get_env(:malachi, :retention_metrics_max_groups)
      on_exit(fn -> restore_env(:retention_metrics_max_groups, previous) end)

      Telemetry.retention_skip(topic, "first", skip(1))
      # Whatever other tests admitted, the cap now leaves no room for one more pair.
      Application.put_env(:malachi, :retention_metrics_max_groups, Metrics.retention_group_count())

      Telemetry.retention_skip(topic, "second", skip(2))
      Telemetry.retention_skip(topic, "third", skip(3))
      # an admitted pair keeps its own series
      Telemetry.retention_skip(topic, "first", skip(4))
      # a group whose NAME is the old overflow label is admitted like any other, past the cap or not
      Telemetry.retention_skip(topic, "__other__", skip(8))

      rows = Map.new(skips_of(topic), &{{&1.reader, &1.group}, {&1.events, &1.offsets}})
      assert rows == %{{:group, "first"} => {2, 5}, {:other, ""} => {3, 13}}
    end

    test "an expired segment counts its bytes, a refusal counts under its reply and frees nothing" do
      topic = unique_topic()
      before = Metrics.retention_snapshot().failures

      Telemetry.retention_expire(topic, "s0", 100, :ok)
      Telemetry.retention_expire(topic, "s1", 50, :ok)
      Telemetry.retention_expire(topic, "s2", 999, :migrating)
      Telemetry.retention_expire(topic, "s3", 999, :no_such_segment)

      snapshot = Metrics.retention_snapshot()
      assert Enum.filter(snapshot.expired, &(&1.topic == topic)) == [%{topic: topic, segments: 2, bytes: 150}]
      assert snapshot.failures.migrating == before.migrating + 1
      assert snapshot.failures.other == before.other
    end

    test "a sweep lands in the duration histogram" do
      before = Metrics.retention_snapshot().sweeps.count

      Telemetry.retention_sweep(2_500, 1, 0)

      assert Metrics.retention_snapshot().sweeps.count == before + 1
    end

    defp restore_env(key, nil), do: Application.delete_env(:malachi, key)
    defp restore_env(key, value), do: Application.put_env(:malachi, key, value)
  end

  describe "a message a long-lived server had no clause for" do
    setup do
      # Emitted from this process on purpose, so the suite's guard must not count it.
      UnknownMessages.expect_from(self())
    end

    defp unexpected_count(server, kind) do
      Enum.find_value(Metrics.get_system_metrics().operations.unexpected_messages, fn
        %{server: ^server, kind: ^kind, count: count} -> count
        _other -> nil
      end)
    end

    test "is counted by server and kind" do
      before = unexpected_count(:replication, :cast)
      other_kinds = {unexpected_count(:replication, :info), unexpected_count(:membership, :cast)}

      Telemetry.unexpected_message(:replication, :cast, {:replica_append_v2, 8})
      Telemetry.unexpected_message(:replication, :cast, {:replica_append_v2, 8})

      assert unexpected_count(:replication, :cast) == before + 2
      assert {unexpected_count(:replication, :info), unexpected_count(:membership, :cast)} == other_kinds
    end

    test "from a server outside the known set is counted as other, so the label set stays closed" do
      before = unexpected_count(:other, :call)

      Telemetry.unexpected_message(:some_future_server, :call, :status)

      assert unexpected_count(:other, :call) == before + 1
      refute Enum.any?(Metrics.get_system_metrics().operations.unexpected_messages, &(&1.server == :some_future_server))
    end

    test "every server and kind is reported, zero included, so the series exists before the first drop" do
      reported =
        for %{server: server, kind: kind} <- Metrics.get_system_metrics().operations.unexpected_messages,
            do: {server, kind}

      expected =
        for server <- UnexpectedMessage.servers() ++ [:other], kind <- UnexpectedMessage.kinds(), do: {server, kind}

      assert reported == expected
      # One entry per server label plus `other`, times the three kinds. The count is asserted so a new
      # label has to be a deliberate change here rather than a silent widening of the exported series.
      assert length(reported) == 33
    end
  end
end
