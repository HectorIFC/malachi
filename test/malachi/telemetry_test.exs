defmodule Malachi.TelemetryTest do
  # Exercises the hot-path telemetry events end to end (in-process): attach a handler that forwards each
  # event to the test process, drive produce/consume/auth against the live broker, and assert the
  # measurements/metadata. async: false: it uses the shared broker and a global telemetry handler.
  use ExUnit.Case, async: false

  alias Malachi.LogApi

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
    %{broker: Malachi.LogBroker}
  end

  test "produce emits count + value bytes, and drives a quorum replication commit", %{broker: broker} do
    topic = "tele_#{System.unique_integer([:positive])}"
    :ok = LogApi.create_topic(broker, topic)

    {:ok, 2} = LogApi.produce(broker, topic, [%{"value" => "aa"}, %{"value" => "bb"}])

    assert_receive {:telemetry, [:malachi, :produce], %{count: 2, bytes: 4}, %{topic: ^topic}}
    # single-node still replicates to its local quorum, so a commit event fires
    assert_receive {:telemetry, [:malachi, :replication, :commit], %{count: count}, %{result: :ok}}
    assert count > 0
  end

  test "consume emits the record count", %{broker: broker} do
    topic = "tele_#{System.unique_integer([:positive])}"
    :ok = LogApi.create_topic(broker, topic)
    {:ok, 3} = LogApi.produce(broker, topic, [%{"value" => "a"}, %{"value" => "b"}, %{"value" => "c"}])

    {:ok, records, _cursor} = LogApi.fetch(broker, topic, :start, 100)
    assert length(records) == 3
    assert_receive {:telemetry, [:malachi, :consume], %{count: 3}, %{topic: ^topic}}
  end

  test "auth emits :ok on success and :error on failure" do
    {:ok, _token} = Malachi.Auth.authenticate("app", "app123")
    assert_receive {:telemetry, [:malachi, :auth], %{count: 1}, %{result: :ok}}

    {:error, _reason} = Malachi.Auth.authenticate("app", "definitely-wrong")
    assert_receive {:telemetry, [:malachi, :auth], %{count: 1}, %{result: :error}}
  end
end
