defmodule Malachi.Retention.SkipReporterTest do
  # async: false: a global telemetry handler, and log capture.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Malachi.Broker.Skip
  alias Malachi.I18n
  alias Malachi.Retention.SkipReporter

  @event [:malachi, :retention, :skip]

  setup do
    parent = self()
    handler_id = "skip-reporter-test-#{System.unique_integer([:positive])}"
    :telemetry.attach(handler_id, @event, fn _event, m, meta, _config -> send(parent, {:skip_event, m, meta}) end, nil)
    on_exit(fn -> :telemetry.detach(handler_id) end)

    name = :"skip_reporter_#{System.unique_integer([:positive])}"
    now = :counters.new(1, [])
    start_supervised!({SkipReporter, name: name, clock: fn -> :counters.get(now, 1) end, window_ms: 1_000})
    %{reporter: name, now: now}
  end

  defp skip(overrides \\ []) do
    struct!(
      %Skip{
        range_id: {"orders", 0},
        source_range_id: {"orders", 0},
        from: 3,
        offsets: 2,
        origin: :cursor,
        source: :self
      },
      overrides
    )
  end

  # A cast is processed in order, so a call after it returns only once the cast was handled.
  defp flush(reporter), do: :sys.get_state(reporter)

  test "a skip becomes one telemetry event, labelled with the reader", %{reporter: reporter} do
    SkipReporter.report(reporter, "orders", "billing", [skip()])
    flush(reporter)

    assert_receive {:skip_event, %{count: 1, offsets: 2}, meta}

    assert meta == %{
             topic: "orders",
             group: "billing",
             range: {"orders", 0},
             source_range: {"orders", 0},
             origin: :cursor,
             span: :exact
           }
  end

  test "the same skip reported again (a re-read before the commit) is counted once", %{reporter: reporter} do
    SkipReporter.report(reporter, "orders", "billing", [skip()])
    SkipReporter.report(reporter, "orders", "billing", [skip()])
    flush(reporter)

    assert_receive {:skip_event, _measurements, _meta}
    refute_receive {:skip_event, _measurements, _meta}
  end

  test "the same skip read by two groups is counted for each", %{reporter: reporter} do
    SkipReporter.report(reporter, "orders", "billing", [skip()])
    SkipReporter.report(reporter, "orders", "audit", [skip()])
    flush(reporter)

    assert_receive {:skip_event, _measurements, %{group: "billing"}}
    assert_receive {:skip_event, _measurements, %{group: "audit"}}
  end

  test "an unknown span is counted with zero offsets and says so", %{reporter: reporter} do
    SkipReporter.report(reporter, "orders", nil, [skip(offsets: :unknown, source: :ancestor)])
    flush(reporter)

    assert_receive {:skip_event, %{count: 1, offsets: 0}, %{group: nil, span: :unknown}}
  end

  test "the first skip of a reader is logged through I18n, the next ones in the window are not",
       %{reporter: reporter, now: now} do
    log =
      capture_log(fn ->
        SkipReporter.report(reporter, "orders", "billing", [skip(), skip(from: 9, offsets: 1)])
        flush(reporter)
      end)

    assert log =~ expected_line(2, 0)
    assert length(String.split(log, "billing")) == 2, "the second skip in the window must not log"

    :counters.add(now, 1, 1_000)

    log =
      capture_log(fn ->
        SkipReporter.report(reporter, "orders", "billing", [skip(from: 20, offsets: 4)])
        flush(reporter)
      end)

    assert log =~ expected_line(4, 1)
  end

  defp expected_line(offsets, held) do
    I18n.t(:retention_consumer_skipped,
      group: inspect("billing"),
      topic: "orders",
      offsets: offsets,
      range: inspect({"orders", 0}),
      source_range: inspect({"orders", 0}),
      origin: :cursor,
      span: :exact,
      held: held
    )
  end

  test "an unexpected message is logged and the reporter keeps serving", %{reporter: reporter} do
    log =
      capture_log(fn ->
        GenServer.cast(reporter, :bogus)
        send(Process.whereis(reporter), :bogus_info)
        flush(reporter)
      end)

    assert log =~ I18n.t(:retention_reporter_unexpected_message, message: inspect(:bogus))
    assert log =~ I18n.t(:retention_reporter_unexpected_message, message: inspect(:bogus_info))

    SkipReporter.report(reporter, "orders", "billing", [skip()])
    flush(reporter)
    assert_receive {:skip_event, _measurements, _meta}
  end

  test "an empty report does nothing" do
    assert SkipReporter.report(:not_started_reporter, "orders", "billing", []) == :ok
    refute_receive {:skip_event, _measurements, _meta}
  end

  test "reporting to a reporter that is not running is dropped, never raised" do
    # A reporter restarting loses counts, not data: the read path must never fail because of it.
    assert SkipReporter.report(:not_started_reporter, "orders", "billing", [skip()]) == :ok
    assert SkipReporter.report(nil, "orders", "billing", [skip()]) == :ok
  end

  test "each named broker has its own reporter name, and an anonymous one has none" do
    assert SkipReporter.name_for(Malachi.LogBroker) == Malachi.LogBroker.SkipReporter
    assert SkipReporter.name_for(:"Elixir.Malachi.LogBroker.Shard1") == Malachi.LogBroker.Shard1.SkipReporter
    assert SkipReporter.name_for(self()) == nil
    assert SkipReporter.name_for({:via, Registry, {:reg, :broker}}) == nil
  end
end
