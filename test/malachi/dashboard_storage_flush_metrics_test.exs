defmodule Malachi.DashboardStorageFlushMetricsTest do
  # The whole path the harnesses scrape: a produce through the running broker flushes a segment, the
  # default reporter folds the flush event in, and an authenticated `GET /metrics` asking for text/plain
  # shows it. The unit tests cover each piece; this is what catches a reporter that was never attached or
  # a histogram `Malachi.Metrics` never created. async: false, because it reads node-global counters and
  # uses the dashboard's shared rate limit bucket.
  use ExUnit.Case, async: false

  alias Malachi.BrokerServer
  alias Malachi.Log.Record
  alias Malachi.Test.DashboardHelper

  @user "flush_metrics_reader"
  @password "flush_pass_123"

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

  test "a produce shows up in the flush series of an authenticated scrape", %{token: token} do
    before = scrape(token)

    topic = "flush_metrics_#{System.unique_integer([:positive])}"
    {:ok, _root} = BrokerServer.create_topic(Malachi.LogBroker, topic, 1)
    records = for i <- 1..5, do: Record.new("v#{i}", key: "k#{i}")
    {:ok, _placements} = BrokerServer.produce(Malachi.LogBroker, topic, records)

    after_produce = scrape(token)

    assert value(after_produce, "malachi_storage_flush_duration_seconds_count") >
             value(before, "malachi_storage_flush_duration_seconds_count")

    assert value(after_produce, "malachi_storage_flushed_records_total") >=
             value(before, "malachi_storage_flushed_records_total") + 5

    assert value(after_produce, ~s(malachi_storage_flush_duration_seconds_bucket{le="+Inf"})) ==
             value(after_produce, "malachi_storage_flush_duration_seconds_count")
  end

  test "the flush series need the login like every other series" do
    {:ok, socket} = DashboardHelper.connect(port: port())
    {:ok, response} = DashboardHelper.request(socket, :GET, "/metrics", headers: %{"Accept" => "text/plain"})
    :gen_tcp.close(socket)

    refute response =~ "malachi_storage_flush_duration_seconds"
  end

  # The same request a harness sends: a bearer token and an Accept header asking for the exposition.
  defp scrape(token) do
    {:ok, socket} = DashboardHelper.connect(port: port())

    :ok =
      :gen_tcp.send(
        socket,
        "GET /metrics HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer #{token}\r\n" <>
          "Accept: text/plain\r\nConnection: close\r\n\r\n"
      )

    response = read_all(socket, "")
    :gen_tcp.close(socket)
    assert response =~ "HTTP/1.1 200 OK"
    response
  end

  # The port the dashboard actually listens on, read at runtime so a run with MALACHI_DASHBOARD_PORT set
  # (parallel worktrees) scrapes its own node.
  defp port, do: Application.fetch_env!(:malachi, :dashboard_port)

  defp read_all(socket, acc) do
    case :gen_tcp.recv(socket, 0, 5000) do
      {:ok, data} -> read_all(socket, acc <> data)
      {:error, :closed} -> acc
    end
  end

  defp value(text, series) do
    line = text |> String.split("\n") |> Enum.find(&String.starts_with?(&1, series <> " "))
    assert line, "#{series} is missing from the scrape"
    line |> String.split(" ") |> List.last() |> String.to_integer()
  end
end
