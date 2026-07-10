defmodule Malachi.LogApiTracingTest do
  # Verifies the OpenTelemetry spans on the client operations (O5a): route ended spans to this process via
  # the pid exporter (the test env uses the synchronous simple processor) and assert on their name and
  # attributes. async: false — it sets a global exporter.
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

  test "produce creates a malachi.produce span with topic/records/bytes", %{broker: broker} do
    topic = "trace_#{System.unique_integer([:positive])}"
    :ok = LogApi.create_topic(broker, topic)
    {:ok, 2} = LogApi.produce(broker, topic, [%{"value" => "aa"}, %{"value" => "bb"}])

    a = attrs(await_span("malachi.produce"))
    assert a["malachi.topic"] == topic
    assert a["malachi.records"] == 2
    assert a["malachi.bytes"] == 4
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
