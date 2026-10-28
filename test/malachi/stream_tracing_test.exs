defmodule Malachi.StreamTracingTest do
  # The OpenTelemetry spans of the stream keys: an append on a producer stream is a `malachi.produce` span,
  # the parent of the broker's `malachi.broker.produce` and so the grandparent of `malachi.replication.commit`,
  # and every page read (a records push, a `fetch_range` answer) is a `malachi.consume` span. Ended spans are
  # routed to this process through the pid exporter (the test env uses the synchronous simple processor).
  #
  # async: false: it sets a global exporter, and the topic routing reads the topology the consumer-group
  # router publishes (cleared here).
  use ExUnit.Case, async: false
  require Record

  import Malachi.Test.StreamTCP

  alias Malachi.BrokerServer
  alias Malachi.DataPlaneRouter
  alias Malachi.Test.RouterTopology
  alias Malachi.Test.TCPHelper
  alias Malachi.Wire

  # The span is an Erlang record; extract its fields so we can read the name and attributes.
  @span_fields Record.extract(:span, from_lib: "opentelemetry/include/otel_span.hrl")
  Record.defrecordp(:span, @span_fields)

  setup do
    # Route ended spans to this test process. Each test re-sets it in setup, so no teardown is needed;
    # after this file, exports go to a dead pid (a harmless no-op send).
    :otel_simple_processor.set_exporter(:otel_exporter_pid, self())

    :ok = RouterTopology.clear()

    socket = connect()
    topic = "trace_#{System.unique_integer([:positive])}"
    {code, _} = TCPHelper.request(socket, Wire.create_topic_key(), 2, Wire.encode_create_topic_req(topic, 4))
    assert code == Wire.ok_code()

    %{socket: socket, topic: topic}
  end

  defp connect do
    {:ok, socket} = TCPHelper.connect()
    {:ok, _token} = TCPHelper.authenticate_wire(socket, "app", "app123")
    on_exit(fn -> :gen_tcp.close(socket) end)
    socket
  end

  defp attrs(s), do: :otel_attributes.map(span(s, :attributes))

  # Waits for an ended span named `name` whose `malachi.topic` is `topic`, skipping every other one.
  defp await_span(name, topic, timeout \\ 2_000) do
    receive do
      {:span, s} ->
        if span(s, :name) == name and attrs(s)["malachi.topic"] == topic, do: s, else: await_span(name, topic, timeout)
    after
      timeout -> flunk("no span named #{name} on #{topic} was exported")
    end
  end

  # Drains every span exported to this process within the quiet period.
  defp drain_spans(acc \\ []) do
    receive do
      {:span, s} -> drain_spans([s | acc])
    after
      500 -> acc
    end
  end

  defp produce(socket, topic, values) do
    {:ok, %{stream_id: id}} = open(socket, topic)
    send_append(socket, id, 0, batch(values))
    assert {_ack, []} = acked_through(socket, 0)
  end

  test "an append is a malachi.produce span with its topic, record count and value bytes", ctx do
    produce(ctx.socket, ctx.topic, ["aa", "bb"])

    a = attrs(await_span("malachi.produce", ctx.topic))
    assert a["malachi.records"] == 2
    assert a["malachi.bytes"] == 4
    refute Map.has_key?(a, "malachi.error")
  end

  test "an append's spans are one trace across processes: produce -> broker.produce -> replication.commit",
       ctx do
    produce(ctx.socket, ctx.topic, ["x"])

    spans = drain_spans()
    produce = Enum.find(spans, &(span(&1, :name) == "malachi.produce" and attrs(&1)["malachi.topic"] == ctx.topic))

    broker =
      Enum.find(spans, &(span(&1, :name) == "malachi.broker.produce" and attrs(&1)["malachi.topic"] == ctx.topic))

    assert produce && broker, "expected the append's span and the broker's"

    replication =
      Enum.find(
        spans,
        &(span(&1, :name) == "malachi.replication.commit" and span(&1, :parent_span_id) == span(broker, :span_id))
      )

    assert replication, "expected the replication commit under the broker's span"
    assert span(broker, :trace_id) == span(produce, :trace_id)
    assert span(replication, :trace_id) == span(produce, :trace_id)
    assert span(broker, :parent_span_id) == span(produce, :span_id)
  end

  test "an append the broker refuses ends its span with the error", ctx do
    {:ok, _left, _right} = BrokerServer.split_range(DataPlaneRouter.shard_for(ctx.topic), {ctx.topic, 0})
    [_parent, left, _right] = routes(ctx.socket, ctx.topic).ranges
    {:ok, %{stream_id: id}} = open(ctx.socket, ctx.topic, range: left.range)
    outside = Enum.find(1..10_000, &(:erlang.phash2("k#{&1}", 16) >= left.key_end))

    send_append(ctx.socket, id, 0, batch(["v"], key: "k#{outside}"))
    assert {_ack, [%{reason: "key_outside_range"}]} = acked_through(ctx.socket, 0)

    a = attrs(await_span("malachi.produce", ctx.topic))
    assert a["malachi.error"] == inspect(:key_outside_range)
    refute Map.has_key?(a, "malachi.records")
  end

  test "a records push on a consume stream is a malachi.consume span with the page's record count", ctx do
    produce(ctx.socket, ctx.topic, ["a", "b", "c"])
    consumer = connect()
    {:ok, _stream} = open_consume(consumer, ctx.topic)
    {:records, page} = push(consumer, consumer_corr())
    assert length(page_records(page)) == 3

    assert attrs(await_span("malachi.consume", ctx.topic))["malachi.records"] == 3
  end

  test "a fetch_range page is a malachi.consume span with its record count", ctx do
    produce(ctx.socket, ctx.topic, ["a"])
    assert {:ok, _page} = fetch_range(ctx.socket, ctx.topic)

    assert attrs(await_span("malachi.consume", ctx.topic))["malachi.records"] == 1
  end
end
