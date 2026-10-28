defmodule Malachi.StreamSkipTest do
  # A reader on the stream keys (`fetch_range` and a consume stream, read as `Malachi.ConsumeStreams`
  # reads them) is told about the data retention moved it past, through the skip reporter beside the
  # broker it read from. Streams carry no group (the group lives beside the data path), so the skip is
  # reported with none.
  use ExUnit.Case, async: true

  import Malachi.Test.PollingHelper
  import Malachi.Test.TeardownHelper

  alias Malachi.BrokerServer
  alias Malachi.ConsumeStreams
  alias Malachi.Log.Record
  alias Malachi.Metadata
  alias Malachi.Retention.SkipReporter
  alias Malachi.Test.ReadBack

  @moduletag :tmp_dir

  # A named broker whose every produce rolls its own segment, the reporter beside it, a topic holding v0,
  # v1 and v2 one per segment with the first two removed by retention, and a telemetry handler forwarding
  # this topic's skip events.
  setup %{tmp_dir: directory} do
    topic = "stream_skips_#{System.unique_integer([:positive])}"
    name = :"stream_skip_#{System.unique_integer([:positive])}"
    start_supervised!({SkipReporter, name: SkipReporter.name_for(name)})

    {:ok, server} = BrokerServer.start_link(directory, name: name, segment_max_bytes: 1)
    on_exit(fn -> stop_quietly(server) end)
    {:ok, _root} = BrokerServer.create_topic(name, topic, 4)

    parent = self()
    handler_id = "stream-skip-#{topic}"

    :telemetry.attach(
      handler_id,
      [:malachi, :retention, :skip],
      fn _event, measurements, metadata, _config ->
        if metadata.topic == topic, do: send(parent, {:skip_event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    for value <- ["v0", "v1", "v2"], do: {:ok, _} = BrokerServer.produce(name, topic, [Record.new(value, key: value)])
    [root_id] = BrokerServer.active_range_ids(name, topic)
    wait_until!(fn -> Enum.all?(segments(name, root_id), &(&1.state == :sealed)) end)

    for segment <- segments(name, root_id), segment.start_offset < 2 do
      :ok = BrokerServer.delete_segment(name, segment.id)
    end

    %{broker: name, topic: topic, root_id: root_id}
  end

  defp segments(broker, root_id), do: broker |> BrokerServer.metadata() |> Metadata.segments_of_range(root_id)

  test "a fetch_range from the start reports the skip, with no group", %{broker: broker, root_id: root_id} do
    assert [%{value: "v2"}] = ReadBack.range_records(broker, root_id, :earliest)
    assert_receive {:skip_event, %{count: 1, offsets: 2}, %{group: nil, origin: :cursor, span: :exact}}
  end

  test "a read from a group's checkpoint reports the skip, with no group either",
       %{broker: broker, topic: topic, root_id: root_id} do
    :ok = BrokerServer.commit_offset(broker, "billing", topic, %{root_id => {0, 0}}, 1)
    assert [%{value: "v2"}] = ReadBack.range_records(broker, root_id, {:committed, "billing"})
    # the start a checkpoint names is a position like any other: the skip is the cursor's, with no group
    assert_receive {:skip_event, %{offsets: 2}, %{group: nil, origin: :cursor, span: :exact}}
  end

  test "a consume stream reports the skip its first page moved past", %{broker: broker, root_id: root_id} do
    {:ok, token, position, _server} = BrokerServer.open_consume(broker, root_id, :earliest)
    assert_receive {:consume_wake, ^token, view, reporter}, 3_000
    assert {:ok, page, 1} = ConsumeStreams.read_page(view, root_id, position, 100, 1_048_576, reporter)
    assert page.expired > 0
    assert_receive {:skip_event, %{offsets: 2}, %{group: nil}}
  end

  test "a broker with no registered name reads past what retention removed, and reports nowhere", %{tmp_dir: directory} do
    {:ok, anonymous} = BrokerServer.start_link(Path.join(directory, "anonymous"), segment_max_bytes: 1)
    on_exit(fn -> stop_quietly(anonymous) end)
    {:ok, root_id} = BrokerServer.create_topic(anonymous, "orders", 4)

    for value <- ["a", "b", "c"],
        do: {:ok, _} = BrokerServer.produce(anonymous, "orders", [Record.new(value, key: value)])

    wait_until!(fn -> Enum.all?(segments(anonymous, root_id), &(&1.state == :sealed)) end)

    for segment <- segments(anonymous, root_id),
        segment.start_offset < 2,
        do: :ok = BrokerServer.delete_segment(anonymous, segment.id)

    parent = self()
    handler_id = "stream-skip-anonymous-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:malachi, :retention, :skip],
      fn _event, measurements, metadata, _config ->
        if metadata.topic == "orders", do: send(parent, {:anonymous_skip, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    # the read moves past the two removed records; with no reporter beside the broker, nobody is told
    assert ReadBack.values(anonymous, "orders") == ["c"]
    refute_receive {:anonymous_skip, _measurements, _metadata}, 200
  end
end
