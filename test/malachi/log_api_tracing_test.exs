defmodule Malachi.LogApiTracingTest do
  # Verifies the OpenTelemetry spans on the client operations (O5a): route ended spans to this process via
  # the pid exporter (the test env uses the synchronous simple processor) and assert on their name and
  # attributes. async: false: it sets a global exporter.
  use ExUnit.Case, async: false
  require Record

  alias Malachi.LogApi

  # The span is an Erlang record; extract its fields so we can read the name and attributes.
  @span_fields Record.extract(:span, from_lib: "opentelemetry/include/otel_span.hrl")
  Record.defrecordp(:span, @span_fields)

  setup do
    # Route ended spans to this test process. Each test re-sets it in setup, so no teardown is needed;
    # after this file, exports go to a dead pid (a harmless no-op send).
    :otel_simple_processor.set_exporter(:otel_exporter_pid, self())
    %{broker: Malachi.LogBroker}
  end

  # Waits for an ended span with `name`, skipping any others.
  defp await_span(name, timeout \\ 2000) do
    receive do
      {:span, s} -> if span(s, :name) == name, do: s, else: await_span(name, timeout)
    after
      timeout -> flunk("no span named #{name} was exported")
    end
  end

  defp attrs(s), do: :otel_attributes.map(span(s, :attributes))

  # Drains every span currently exported to this process.
  defp drain_spans(acc \\ []) do
    receive do
      {:span, s} -> drain_spans([s | acc])
    after
      500 -> acc
    end
  end

  defp by_name(spans, name), do: Enum.find(spans, &(span(&1, :name) == name))

  test "produce creates a malachi.produce span with topic/records/bytes", %{broker: broker} do
    topic = "trace_#{System.unique_integer([:positive])}"
    :ok = LogApi.create_topic(broker, topic)
    {:ok, 2} = LogApi.produce(broker, topic, [%{"value" => "aa"}, %{"value" => "bb"}])

    a = attrs(await_span("malachi.produce"))
    assert a["malachi.topic"] == topic
    assert a["malachi.records"] == 2
    assert a["malachi.bytes"] == 4
  end

  test "produce spans are linked across processes: produce -> broker.produce -> replication.commit", %{broker: broker} do
    topic = "trace_#{System.unique_integer([:positive])}"
    :ok = LogApi.create_topic(broker, topic)
    {:ok, 1} = LogApi.produce(broker, topic, [%{"value" => "x"}])

    spans = drain_spans()
    produce = by_name(spans, "malachi.produce")
    broker_span = by_name(spans, "malachi.broker.produce")
    replication = by_name(spans, "malachi.replication.commit")

    assert produce && broker_span && replication, "expected all three spans"

    # one trace across the three processes (client -> broker -> replication)
    trace_id = span(produce, :trace_id)
    assert span(broker_span, :trace_id) == trace_id
    assert span(replication, :trace_id) == trace_id

    # parent chain: broker.produce is a child of produce; replication.commit is a child of broker.produce
    assert span(broker_span, :parent_span_id) == span(produce, :span_id)
    assert span(replication, :parent_span_id) == span(broker_span, :span_id)
  end

  test "fetch creates a malachi.consume span with the topic and record count", %{broker: broker} do
    topic = "trace_#{System.unique_integer([:positive])}"
    :ok = LogApi.create_topic(broker, topic)
    {:ok, 1} = LogApi.produce(broker, topic, [%{"value" => "x"}])

    {:ok, records, _cursor} = LogApi.fetch(broker, topic, :start, 100)
    assert length(records) == 1

    a = attrs(await_span("malachi.consume"))
    assert a["malachi.topic"] == topic
    assert a["malachi.records"] == 1
  end
end
