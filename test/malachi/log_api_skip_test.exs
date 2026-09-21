defmodule Malachi.LogApiSkipTest do
  # Every consume entry point of `Malachi.LogApi` reports the data a reader was moved past, attributed to
  # the reader's group, through the skip reporter beside the broker it read from.
  use ExUnit.Case, async: true

  import Malachi.Test.PollingHelper
  import Malachi.Test.TeardownHelper

  alias Malachi.BrokerServer
  alias Malachi.Consumer.GroupCoordinator
  alias Malachi.LogApi
  alias Malachi.Metadata
  alias Malachi.Retention.SkipReporter

  @moduletag :tmp_dir

  # A named broker whose every produce rolls its own segment, the reporter beside it, a topic holding
  # `values` one per segment with the first `expired` of them removed by retention, and a telemetry
  # handler forwarding this topic's skip events.
  setup %{tmp_dir: directory} do
    topic = "skips_#{System.unique_integer([:positive])}"
    name = :"log_api_skip_#{System.unique_integer([:positive])}"
    start_supervised!({SkipReporter, name: SkipReporter.name_for(name)})

    {:ok, server} = BrokerServer.start_link(directory, name: name, segment_max_bytes: 1)
    on_exit(fn -> stop_quietly(server) end)
    :ok = LogApi.create_topic(name, topic)

    parent = self()
    handler_id = "log-api-skip-#{topic}"

    :telemetry.attach(
      handler_id,
      [:malachi, :retention, :skip],
      fn _event, measurements, metadata, _config ->
        if metadata.topic == topic, do: send(parent, {:skip_event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    for value <- ["v0", "v1", "v2"], do: {:ok, 1} = LogApi.produce(name, topic, [%{"key" => value, "value" => value}])
    [root_id] = BrokerServer.active_range_ids(name, topic)
    wait_until!(fn -> Enum.all?(segments(name, root_id), &(&1.state == :sealed)) end)

    for segment <- segments(name, root_id), segment.start_offset < 2 do
      :ok = BrokerServer.delete_segment(name, segment.id)
    end

    %{broker: name, topic: topic, root_id: root_id}
  end

  defp segments(broker, root_id), do: broker |> BrokerServer.metadata() |> Metadata.segments_of_range(root_id)

  defp commit_start(broker, topic, group, root_id),
    do: :ok = BrokerServer.commit_offset(broker, group, topic, %{root_id => {0, 0}})

  test "fetch reports a skip with no group", %{broker: broker, topic: topic, root_id: root_id} do
    cursor = LogApi.encode_cursor(%{root_id => {0, 0}})

    assert {:ok, [%{value: "v2"}], _next} = LogApi.fetch(broker, topic, cursor, 100)
    assert_receive {:skip_event, %{count: 1, offsets: 2}, %{group: nil, origin: :cursor, span: :exact}}
  end

  test "fetch_group reports a skip once however often the page is re-read before the commit",
       %{broker: broker, topic: topic, root_id: root_id} do
    commit_start(broker, topic, "billing", root_id)

    assert {:ok, [%{value: "v2"}], _next} = LogApi.fetch_group(broker, topic, "billing", 100)
    assert {:ok, [%{value: "v2"}], _next} = LogApi.fetch_group(broker, topic, "billing", 100)
    assert {:ok, [%{value: "v2"}], next} = LogApi.fetch_group(broker, topic, "billing", 100)

    assert_receive {:skip_event, %{offsets: 2}, %{group: "billing", origin: :cursor}}
    refute_receive {:skip_event, _measurements, _metadata}

    # Once committed past the gap, nothing is skipped any more.
    :ok = LogApi.commit(broker, topic, "billing", next)
    assert {:ok, [], _next} = LogApi.fetch_group(broker, topic, "billing", 100)
    refute_receive {:skip_event, _measurements, _metadata}
  end

  test "a group that never committed is reported as a fresh start", %{broker: broker, topic: topic} do
    assert {:ok, [%{value: "v2"}], _next} = LogApi.fetch_group(broker, topic, "newcomer", 100)
    assert_receive {:skip_event, %{offsets: 2}, %{group: "newcomer", origin: :start}}
  end

  test "fetch_member reports a skip with the member's group", %{broker: broker, topic: topic, root_id: root_id} do
    commit_start(broker, topic, "billing", root_id)

    {:ok, coordinator} =
      GroupCoordinator.start_link(
        ranges_fun: fn topic -> BrokerServer.active_range_ids(broker, topic) end,
        tick_ms: 3_600_000
      )

    on_exit(fn -> stop_quietly(coordinator) end)

    assert {:ok, [%{value: "v2"}], _next} = LogApi.fetch_member(broker, coordinator, topic, "billing", :m1, 100)
    assert_receive {:skip_event, %{offsets: 2}, %{group: "billing", origin: :cursor}}
  end

  test "subscribe reports the skip its first push delivered", %{broker: broker, topic: topic, root_id: root_id} do
    commit_start(broker, topic, "billing", root_id)

    :ok = LogApi.subscribe(broker, topic, "billing", 100, 100)

    assert_receive {:log_records, ^topic, [%{value: "v2"}], _positions}
    assert_receive {:skip_event, %{offsets: 2}, %{group: "billing", origin: :cursor}}
  end

  test "a broker with no registered name fetches normally and reports nowhere", %{tmp_dir: directory} do
    {:ok, anonymous} = BrokerServer.start_link(Path.join(directory, "anonymous"), segment_max_bytes: 1)
    on_exit(fn -> stop_quietly(anonymous) end)
    :ok = LogApi.create_topic(anonymous, "orders")
    {:ok, 1} = LogApi.produce(anonymous, "orders", [%{"key" => "a", "value" => "a"}])

    assert {:ok, [%{value: "a"}], _next} = LogApi.fetch(anonymous, "orders", :start, 100)
  end
end
