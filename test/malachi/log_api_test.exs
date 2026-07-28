defmodule Malachi.LogApiTest do
  use ExUnit.Case, async: true

  alias Malachi.BrokerServer
  alias Malachi.Consumer.GroupCoordinator
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
    # opaque token: not an integer offset the client interprets
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

  test "fetch with wait blocks until a record is produced (long-poll)", %{tmp_dir: directory} do
    server = start_broker(directory)
    :ok = LogApi.create_topic(server, "events")

    task = Task.async(fn -> LogApi.fetch(server, "events", :start, 100, 2_000) end)
    wait_for_park(server)

    {:ok, 1} = LogApi.produce(server, "events", [%{"value" => "a"}])

    assert {:ok, [%{value: "a"}], _cursor} = Task.await(task)
  end

  test "fetch with wait returns empty after the timeout when nothing is produced", %{tmp_dir: directory} do
    server = start_broker(directory)
    :ok = LogApi.create_topic(server, "events")
    assert {:ok, [], _cursor} = LogApi.fetch(server, "events", :start, 100, 50)
  end

  defp wait_for_park(server, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 2_000

    cond do
      :sys.get_state(server).waiters != [] -> :ok
      System.monotonic_time(:millisecond) > deadline -> flunk("expected a parked waiter")
      true -> Process.sleep(2) && wait_for_park(server, deadline)
    end
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

    # produced before the split: these live in the parent's segments, which leave the active set
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

  defp start_coordinator(server) do
    {:ok, coord} =
      GroupCoordinator.start_link(
        ranges_fun: fn topic -> BrokerServer.active_range_ids(server, topic) end,
        tick_ms: 3_600_000
      )

    on_exit(fn -> if Process.alive?(coord), do: GenServer.stop(coord) end)
    coord
  end

  test "fetch_member consumes only the member's ranges: two members split the topic disjointly",
       %{tmp_dir: directory} do
    server = start_broker(directory)
    :ok = LogApi.create_topic(server, "events")

    # split the root into two ranges so members can own different ones
    [root] = BrokerServer.active_range_ids(server, "events")
    {:ok, _left, _right} = BrokerServer.split_range(server, root)

    records = for i <- 0..29, do: %{"key" => "k#{i}", "value" => "v#{i}"}
    {:ok, 30} = LogApi.produce(server, "events", records)

    coord = start_coordinator(server)

    # register both members first so the assignment is a stable 2-member split before either fetches
    {:ok, _, _} = GroupCoordinator.poll(coord, "g", "events", :m1)
    {:ok, _, _} = GroupCoordinator.poll(coord, "g", "events", :m2)

    {:ok, r1, _} = LogApi.fetch_member(server, coord, "events", "g", :m1, 100)
    {:ok, r2, _} = LogApi.fetch_member(server, coord, "events", "g", :m2, 100)
    v1 = Enum.map(r1, & &1.value)
    v2 = Enum.map(r2, & &1.value)

    # disjoint (each record consumed by exactly one member) and complete (together, all records)
    assert v1 -- v2 == v1
    assert Enum.sort(v1 ++ v2) == Enum.sort(Enum.map(records, & &1["value"]))
  end

  test "fetch_group with no member still consumes the whole group (backward compatible)",
       %{tmp_dir: directory} do
    server = start_broker(directory)
    :ok = LogApi.create_topic(server, "events")
    records = for i <- 0..9, do: %{"key" => "k#{i}", "value" => "v#{i}"}
    {:ok, 10} = LogApi.produce(server, "events", records)

    {:ok, all, _} = LogApi.fetch_group(server, "events", "g", 100)
    assert length(all) == 10
  end
end
