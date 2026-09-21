defmodule Malachi.DashboardRetentionMetricsTest do
  # The whole path an operator scrapes: retention telemetry folded in by the default reporter, exported
  # through an authenticated `GET /metrics`. The unit tests cover each piece; this is what catches a
  # reporter that was never attached, or an endpoint that never asks `Malachi.Metrics` for the retention
  # snapshot. async: false: node-global counters and the dashboard's shared rate limit bucket.
  use ExUnit.Case, async: false

  alias Malachi.Broker.Skip
  alias Malachi.Retention.SkipReporter
  alias Malachi.Telemetry
  alias Malachi.Test.DashboardHelper

  @user "retention_metrics_reader"
  @password "retention_pass_123"

  setup do
    Malachi.RateLimiter.reset_bucket("127.0.0.1", :dashboard_auth)
    _ = Malachi.Auth.remove_user(@user)
    :ok = Malachi.Auth.add_user(@user, @password, [:admin])

    on_exit(fn ->
      _ = Malachi.Auth.remove_user(@user)
      Malachi.RateLimiter.reset_bucket("127.0.0.1", :dashboard_auth)
    end)

    {:ok, token} = DashboardHelper.login(@user, @password, port: port())
    {:ok, token: token}
  end

  test "retention telemetry shows up in every retention series of an authenticated scrape", %{token: token} do
    topic = "retention_scrape_#{System.unique_integer([:positive])}"
    before = scrape(token)

    skip = %Skip{
      range_id: {topic, 0},
      source_range_id: {topic, 0},
      from: 0,
      offsets: 12,
      origin: :cursor,
      source: :self
    }

    Telemetry.retention_skip(topic, "billing", skip)
    Telemetry.retention_expire(topic, "segment-0", 4096, :ok)
    Telemetry.retention_expire(topic, "segment-1", 10, :migrating)
    Telemetry.retention_sweep(1_200, 1, 1)

    now = scrape(token)
    labels = ~s(topic="#{topic}",reader="group",group="billing",origin="cursor",span="exact")

    assert value(now, "malachi_retention_skips_total{#{labels}}") == 1
    assert value(now, "malachi_retention_offsets_skipped_total{#{labels}}") == 12
    assert value(now, ~s(malachi_retention_segments_expired_total{topic="#{topic}"})) == 1
    assert value(now, ~s(malachi_retention_bytes_expired_total{topic="#{topic}"})) == 4096

    # These two carry no topic label: they are node-global, and other tests (and a real coordinator on a
    # node that has retention configured) emit into them while this one runs. The bound is what this test
    # can prove, which is that the emit reached the endpoint; that it is counted exactly once is the
    # metrics reporter's test, where the counter is read directly.
    assert value(now, ~s(malachi_retention_expire_failures_total{reply="migrating"})) >=
             value(before, ~s(malachi_retention_expire_failures_total{reply="migrating"})) + 1

    assert value(now, "malachi_retention_sweep_duration_seconds_count") >=
             value(before, "malachi_retention_sweep_duration_seconds_count") + 1
  end

  test "the application runs a skip reporter beside its broker" do
    assert is_pid(Process.whereis(SkipReporter.name_for(Malachi.LogBroker)))
  end

  test "the retention series need the login like every other series" do
    {:ok, socket} = DashboardHelper.connect(port: port())
    {:ok, response} = DashboardHelper.request(socket, :GET, "/metrics", headers: %{"Accept" => "text/plain"})
    :gen_tcp.close(socket)

    refute response =~ "malachi_retention_"
  end

  defp scrape(token) do
    response = DashboardHelper.scrape_metrics(token, port: port())
    assert response =~ "HTTP/1.1 200 OK"
    response
  end

  defp value(text, series) do
    value = DashboardHelper.metric_value(text, series)
    assert value, "#{series} is missing from the scrape"
    value
  end

  # The port the dashboard actually listens on, read at runtime so a run with MALACHI_DASHBOARD_PORT set
  # (parallel worktrees) scrapes its own node.
  defp port, do: Application.fetch_env!(:malachi, :dashboard_port)
end
