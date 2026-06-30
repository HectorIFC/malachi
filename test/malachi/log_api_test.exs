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

  test "a consumer group resumes from its durably committed position", %{tmp_dir: directory} do
    server = start_broker(directory)
    :ok = LogApi.create_topic(server, "events")
    {:ok, 3} = LogApi.produce(server, "events", [%{"value" => "a"}, %{"value" => "b"}, %{"value" => "c"}])

    {:ok, records, cursor} = LogApi.fetch_group(server, "events", "g1", 100)
    assert records |> Enum.map(& &1.value) |> Enum.sort() == ["a", "b", "c"]

    # committing advances the group's durable position past those records
    assert :ok = LogApi.commit(server, "events", "g1", cursor)
    assert {:ok, [], _cursor} = LogApi.fetch_group(server, "events", "g1", 100)

    # later records are delivered to the committed group
    {:ok, 1} = LogApi.produce(server, "events", [%{"value" => "d"}])
    assert {:ok, [%{value: "d"}], _next} = LogApi.fetch_group(server, "events", "g1", 100)

    # a different group still starts from the beginning
    assert {:ok, fresh, _} = LogApi.fetch_group(server, "events", "g2", 100)
    assert length(fresh) == 4
  end

  test "without a commit, a group re-reads from its last committed position (at-least-once)", %{tmp_dir: directory} do
    server = start_broker(directory)
    :ok = LogApi.create_topic(server, "events")
    {:ok, 2} = LogApi.produce(server, "events", [%{"value" => "a"}, %{"value" => "b"}])

    {:ok, first, _cursor} = LogApi.fetch_group(server, "events", "g", 100)
    {:ok, again, _cursor} = LogApi.fetch_group(server, "events", "g", 100)
    assert Enum.map(first, & &1.value) == Enum.map(again, & &1.value)
  end

  test "commit rejects a malformed cursor", %{tmp_dir: directory} do
    server = start_broker(directory)
    :ok = LogApi.create_topic(server, "events")
    assert {:error, :invalid_cursor} = LogApi.commit(server, "events", "g", "not-base64!!")
  end

  test "a forged cursor with an out-of-range source_index is handled gracefully (no crash)", %{tmp_dir: directory} do
    server = start_broker(directory)
    :ok = LogApi.create_topic(server, "events")
    {:ok, 1} = LogApi.produce(server, "events", [%{"value" => "a"}])

    # passes shape validation, but the position points past the range's sources: empty, not a crash
    forged = Base.url_encode64(:erlang.term_to_binary(%{{"events", 0} => {9999, 0}}))
    assert {:ok, [], _cursor} = LogApi.fetch(server, "events", forged, 100)

    # the server is still alive and serves a normal fetch
    assert {:ok, [%{value: "a"}], _next} = LogApi.fetch(server, "events", :start, 100)
  end

  test "split-aware consume: records produced before a split are still delivered", %{tmp_dir: directory} do
    server = start_broker(directory)
    :ok = LogApi.create_topic(server, "events")

    # produced before the split — these live in the parent's segments, which leave the active set
    before_split = for index <- 0..19, do: %{"key" => "k#{index}", "value" => "v#{index}"}
    {:ok, 20} = LogApi.produce(server, "events", before_split)

    [root_id] = BrokerServer.active_range_ids(server, "events")
    {:ok, _left, _right} = BrokerServer.split_range(server, root_id)

    after_split = for index <- 20..39, do: %{"key" => "k#{index}", "value" => "v#{index}"}
    {:ok, 20} = LogApi.produce(server, "events", after_split)

    # a fresh consumer sees every record (pre- and post-split); none are lost to the sealed parent
    {:ok, records, _cursor} = LogApi.fetch_group(server, "events", "g_fresh", 100)
    values = Enum.map(records, & &1.value)
    assert length(values) == 40
    assert Enum.sort(values) == Enum.sort(Enum.map(before_split ++ after_split, & &1["value"]))
  end

  test "after a split, a committed group reprocesses its slice from the active children", %{tmp_dir: directory} do
    server = start_broker(directory)
    :ok = LogApi.create_topic(server, "events")

    before_split = for index <- 0..9, do: %{"key" => "k#{index}", "value" => "v#{index}"}
    {:ok, 10} = LogApi.produce(server, "events", before_split)

    # the group consumes the root range and commits its position
    {:ok, first, cursor} = LogApi.fetch_group(server, "events", "g", 100)
    assert length(first) == 10
    :ok = LogApi.commit(server, "events", "g", cursor)
    assert {:ok, [], _cursor} = LogApi.fetch_group(server, "events", "g", 100)

    # the range the group was reading splits; the children carry no inherited position
    [root_id] = BrokerServer.active_range_ids(server, "events")
    {:ok, _left, _right} = BrokerServer.split_range(server, root_id)

    # resuming reprocesses the slice via the active children (at-least-once, no loss)
    {:ok, again, _cursor} = LogApi.fetch_group(server, "events", "g", 100)
    assert again |> Enum.map(& &1.value) |> Enum.sort() == Enum.sort(Enum.map(before_split, & &1["value"]))
  end
end
