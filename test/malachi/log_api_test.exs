defmodule Malachi.LogApiTest do
  use ExUnit.Case, async: true

  alias Malachi.BrokerServer
  alias Malachi.LogApi

  @moduletag :tmp_dir

  defp start_broker(directory) do
    {:ok, server} = BrokerServer.start_link(directory)
    on_exit(fn -> if Process.alive?(server), do: BrokerServer.stop(server) end)
    server
  end

  test "produce by key then fetch by opaque cursor returns the records", %{tmp_dir: directory} do
    server = start_broker(directory)
    :ok = LogApi.create_topic(server, "events")

    records = for index <- 0..4, do: %{"key" => "k#{index}", "value" => "v#{index}"}
    assert {:ok, 5} = LogApi.produce(server, "events", records)

    assert {:ok, fetched, cursor} = LogApi.fetch(server, "events", :start, 100)
    assert Enum.map(fetched, & &1.value) |> Enum.sort() == Enum.map(records, & &1["value"]) |> Enum.sort()

    # the cursor is an opaque string; passing it back yields no more records (caught up)
    assert is_binary(cursor)
    assert {:ok, [], ^cursor} = LogApi.fetch(server, "events", cursor, 100)

    # a record produced after the cursor is delivered on the next fetch
    assert {:ok, 1} = LogApi.produce(server, "events", [%{"key" => "k9", "value" => "v9"}])
    assert {:ok, [%{value: "v9"}], _next} = LogApi.fetch(server, "events", cursor, 100)
  end

  test "the client never sees offsets; the cursor carries position opaquely", %{tmp_dir: directory} do
    server = start_broker(directory)
    :ok = LogApi.create_topic(server, "events")
    {:ok, 2} = LogApi.produce(server, "events", [%{"value" => "a"}, %{"value" => "b"}])

    {:ok, _records, cursor} = LogApi.fetch(server, "events", :start, 100)
    # opaque token — not an integer offset the client interprets
    refute is_integer(cursor)
    assert is_binary(cursor)
  end

  test "a tampered or malformed cursor is rejected (safe decode)", %{tmp_dir: directory} do
    server = start_broker(directory)
    :ok = LogApi.create_topic(server, "events")

    assert {:error, :invalid_cursor} = LogApi.fetch(server, "events", "not-base64!!", 100)
    assert {:error, :invalid_cursor} = LogApi.fetch(server, "events", Base.url_encode64("garbage"), 100)
    # a validly-encoded but wrong-shaped term is rejected too
    bad = Base.url_encode64(:erlang.term_to_binary(%{"not" => "a position"}))
    assert {:error, :invalid_cursor} = LogApi.fetch(server, "events", bad, 100)
  end

  test "an invalid record (missing/non-binary value) is rejected", %{tmp_dir: directory} do
    server = start_broker(directory)
    :ok = LogApi.create_topic(server, "events")

    assert {:error, :invalid_record} = LogApi.produce(server, "events", [%{"key" => "k"}])
    assert {:error, :invalid_record} = LogApi.produce(server, "events", [%{"value" => 123}])
  end

  test "producing to an unknown topic fails", %{tmp_dir: directory} do
    server = start_broker(directory)
    assert {:error, :no_such_topic} = LogApi.produce(server, "nope", [%{"value" => "a"}])
  end
end
