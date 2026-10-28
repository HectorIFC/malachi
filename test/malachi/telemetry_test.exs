defmodule Malachi.TelemetryTest do
  # Exercises the hot-path telemetry events end to end: attach a handler that forwards each event to the
  # test process, drive the stream keys and auth against the live broker over a real socket, and assert the
  # measurements/metadata. async: false: it uses the shared broker, a global telemetry handler, and the
  # topology the consumer-group router publishes, which the topic routing reads (cleared here).
  use ExUnit.Case, async: false

  import Malachi.Test.StreamTCP

  alias Malachi.BrokerServer
  alias Malachi.DataPlaneRouter
  alias Malachi.Test.RouterTopology
  alias Malachi.Test.TCPHelper
  alias Malachi.Wire

  @events [
    [:malachi, :produce],
    [:malachi, :consume],
    [:malachi, :auth],
    [:malachi, :replication, :commit]
  ]

  setup do
    parent = self()
    handler_id = "telemetry-test-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler_id,
      @events,
      fn name, measurements, metadata, _config -> send(parent, {:telemetry, name, measurements, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok = RouterTopology.clear()

    {:ok, socket} = TCPHelper.connect()
    {:ok, _token} = TCPHelper.authenticate_wire(socket, "app", "app123")
    on_exit(fn -> :gen_tcp.close(socket) end)

    topic = "tele_#{System.unique_integer([:positive])}"
    {code, _} = TCPHelper.request(socket, Wire.create_topic_key(), 2, Wire.encode_create_topic_req(topic, 4))
    assert code == Wire.ok_code()

    %{socket: socket, topic: topic}
  end

  defp produce(socket, topic, values) do
    {:ok, %{stream_id: id}} = open(socket, topic)
    send_append(socket, id, 0, batch(values))
    assert {_ack, []} = acked_through(socket, 0)
  end

  test "an acknowledged append emits its record count and value bytes, and drives a quorum replication commit",
       ctx do
    produce(ctx.socket, ctx.topic, ["aa", "bb"])

    topic = ctx.topic
    assert_receive {:telemetry, [:malachi, :produce], %{count: 2, bytes: 4}, %{topic: ^topic}}
    # single-node still replicates to its local quorum, so a commit event fires
    assert_receive {:telemetry, [:malachi, :replication, :commit], %{count: count}, %{result: :ok}}
    assert count > 0
  end

  test "an append the broker refuses, or one refused before it is sent, emits no produce event", ctx do
    {:ok, _left, _right} = BrokerServer.split_range(DataPlaneRouter.shard_for(ctx.topic), {ctx.topic, 0})
    [_parent, left, _right] = routes(ctx.socket, ctx.topic).ranges
    {:ok, %{stream_id: id}} = open(ctx.socket, ctx.topic, range: left.range)
    outside = Enum.find(1..10_000, &(:erlang.phash2("k#{&1}", 16) >= left.key_end))

    send_append(ctx.socket, id, 0, batch(["v"], key: "k#{outside}"))
    send_append(ctx.socket, id, 1, batch(["gone"], tombstone: true, key: "k"))

    assert {_ack, [%{reason: "key_outside_range"}, %{reason: "tombstone_unsupported"}]} =
             acked_through(ctx.socket, 1)

    topic = ctx.topic
    refute_receive {:telemetry, [:malachi, :produce], _measurements, %{topic: ^topic}}, 200
  end

  test "a records push on a consume stream emits the page's record count", ctx do
    produce(ctx.socket, ctx.topic, ["a", "b", "c"])
    consumer = connect()
    {:ok, _stream} = open_consume(consumer, ctx.topic)
    {:records, page} = push(consumer, consumer_corr())
    assert length(page_records(page)) == 3

    topic = ctx.topic
    assert_receive {:telemetry, [:malachi, :consume], %{count: 3}, %{topic: ^topic}}
  end

  test "a fetch_range page emits its record count", ctx do
    produce(ctx.socket, ctx.topic, ["a", "b"])
    assert {:ok, page} = fetch_range(ctx.socket, ctx.topic)
    assert length(page_records(page)) == 2

    topic = ctx.topic
    assert_receive {:telemetry, [:malachi, :consume], %{count: 2}, %{topic: ^topic}}
  end

  defp connect do
    {:ok, socket} = TCPHelper.connect()
    {:ok, _token} = TCPHelper.authenticate_wire(socket, "app", "app123")
    on_exit(fn -> :gen_tcp.close(socket) end)
    socket
  end

  test "auth emits :ok on success and :error on failure" do
    {:ok, _token} = Malachi.Auth.authenticate("app", "app123")
    assert_receive {:telemetry, [:malachi, :auth], %{count: 1}, %{result: :ok}}

    {:error, _reason} = Malachi.Auth.authenticate("app", "definitely-wrong")
    assert_receive {:telemetry, [:malachi, :auth], %{count: 1}, %{result: :error}}
  end
end
