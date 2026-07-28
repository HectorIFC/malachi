defmodule Malachi.BrokerServerStreamingTest do
  # B2-a: streaming subscribers with a credit window and durable group commit, in-process (the test is
  # the subscriber, receiving {:log_records, ...} into its own mailbox).
  use ExUnit.Case, async: false

  alias Malachi.BrokerServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Consumer.GroupCoordinator
  alias Malachi.Log.Record
  alias Malachi.LogApi

  setup do
    dir = Path.join(System.tmp_dir!(), "malachi_stream_#{System.unique_integer([:positive])}")
    repl = :"repl_#{System.unique_integer([:positive])}"
    start_supervised!({ReplicationServer, name: repl, directory: Path.join(dir, "repl")}, id: repl)
    {:ok, broker} = BrokerServer.start_link(Path.join(dir, "b"), brokers: [repl])
    {:ok, _root} = BrokerServer.create_topic(broker, "t", 4)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{broker: broker}
  end

  defp produce(broker, values) do
    {:ok, _} = BrokerServer.produce(broker, "t", Enum.map(values, &Record.new/1))
  end

  # receive one push, returning {values, positions}
  defp recv_push do
    receive do
      {:log_records, "t", records, positions} -> {Enum.map(records, & &1.value), positions}
    after
      1_000 -> flunk("expected a {:log_records, ...} push")
    end
  end

  test "subscribe pushes the backlog, then a later produce pushes the new record", %{broker: broker} do
    produce(broker, ["a", "b"])
    :ok = BrokerServer.subscribe(broker, "t", "g", 10, 100)

    assert {["a", "b"], _positions} = recv_push()

    produce(broker, ["c"])
    assert {["c"], _positions} = recv_push()
  end

  test "the credit window bounds in-flight records until acked", %{broker: broker} do
    produce(broker, ["a", "b", "c", "d", "e"])
    :ok = BrokerServer.subscribe(broker, "t", "g", 2, 100)

    # only the window's worth is pushed, though 5 are available
    assert {["a", "b"], positions} = recv_push()
    refute_receive {:log_records, _, _, _}, 100

    # acking 2 returns 2 credit → the next 2 are pushed
    :ok = BrokerServer.stream_ack(broker, "t", "g", positions, 2)
    assert {["c", "d"], _positions} = recv_push()
  end

  test "ack commits the group's position durably", %{broker: broker} do
    produce(broker, ["a", "b", "c"])
    :ok = BrokerServer.subscribe(broker, "t", "g", 2, 100)
    assert {["a", "b"], positions} = recv_push()

    :ok = BrokerServer.stream_ack(broker, "t", "g", positions, 2)

    # the committed position is durable and equals what was acked
    assert BrokerServer.committed_offsets(broker, "g", "t") == positions
  end

  test "a fresh subscription resumes from the group's committed position", %{broker: broker} do
    produce(broker, ["a", "b", "c"])

    # first subscriber consumes and acks "a","b"
    :ok = BrokerServer.subscribe(broker, "t", "g", 2, 100)
    assert {["a", "b"], positions} = recv_push()
    :ok = BrokerServer.stream_ack(broker, "t", "g", positions, 2)
    _ = recv_push()
    :ok = BrokerServer.unsubscribe(broker, "t")

    # a fresh subscription of the same group resumes past the committed "a","b"
    flush()
    :ok = BrokerServer.subscribe(broker, "t", "g", 10, 100)
    assert {["c"], _positions} = recv_push()
  end

  test "a dead subscriber is dropped from the topic (via :DOWN)", %{broker: broker} do
    {sub, ref} =
      spawn_monitor(fn ->
        BrokerServer.subscribe(broker, "t", "g", 10, 100)
        receive do: (:never -> :ok)
      end)

    wait_until(fn -> length(subscribers(broker)) == 1 end)
    Process.exit(sub, :kill)
    assert_receive {:DOWN, ^ref, :process, _pid, _reason}

    wait_until(fn -> subscribers(broker) == [] end)
  end

  defp subscribers(broker), do: Map.get(:sys.get_state(broker).subscribers, "t", [])

  defp flush do
    receive do
      _ -> flush()
    after
      0 -> :ok
    end
  end

  defp wait_until(fun, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 1_000

    cond do
      fun.() -> :ok
      System.monotonic_time(:millisecond) > deadline -> flunk("condition not met in time")
      true -> Process.sleep(5) && wait_until(fun, deadline)
    end
  end

  # --- consumer-group member scoping (streaming) ---

  defp start_coordinator(broker) do
    {:ok, coord} =
      GroupCoordinator.start_link(
        ranges_fun: fn topic -> BrokerServer.active_range_ids(broker, topic) end,
        tick_ms: 3_600_000
      )

    on_exit(fn -> if Process.alive?(coord), do: GenServer.stop(coord) end)
    coord
  end

  # Spawns a group member whose own process subscribes to the scoped push stream and forwards the values
  # it receives (over `collect_ms`) back to the test as `{member, values}`.
  defp member_stream(broker, coord, group, member, test, collect_ms \\ 300) do
    spawn(fn ->
      :ok = LogApi.subscribe_member(broker, coord, "t", group, member, 1_000, 1_000)
      send(test, {member, collect_values([], collect_ms)})
    end)
  end

  defp collect_values(acc, timeout) do
    receive do
      {:log_records, "t", records, _positions} -> collect_values(acc ++ Enum.map(records, & &1.value), timeout)
    after
      timeout -> acc
    end
  end

  test "two group members get disjoint, complete push streams", %{broker: broker} do
    [root] = BrokerServer.active_range_ids(broker, "t")
    {:ok, _left, _right} = BrokerServer.split_range(broker, root)

    records = for i <- 0..19, do: Record.new("v#{i}", key: "k#{i}")
    {:ok, _} = BrokerServer.produce(broker, "t", records)

    coord = start_coordinator(broker)
    # pre-register both so the assignment is a stable two-member split before either subscribes
    {:ok, _, _} = GroupCoordinator.poll(coord, "g", "t", :m1)
    {:ok, _, _} = GroupCoordinator.poll(coord, "g", "t", :m2)

    member_stream(broker, coord, "g", :m1, self())
    member_stream(broker, coord, "g", :m2, self())

    v1 = receive do: ({:m1, v} -> v), after: (2_000 -> flunk("no push for m1"))
    v2 = receive do: ({:m2, v} -> v), after: (2_000 -> flunk("no push for m2"))

    assert v1 -- v2 == v1
    assert Enum.sort(v1 ++ v2) == Enum.sort(Enum.map(records, & &1.value))
  end

  test "a member's subscriber process exiting leaves the group", %{broker: broker} do
    coord = start_coordinator(broker)

    pid =
      spawn(fn ->
        :ok = LogApi.subscribe_member(broker, coord, "t", "g", :m1, 10, 10)
        Process.sleep(:infinity)
      end)

    wait_until(fn -> match?({:ok, _, _}, GroupCoordinator.assignment(coord, "g", "t", :m1)) end)

    Process.exit(pid, :kill)
    # the broker's :DOWN spawns an async task to leave the group; the member eventually disappears
    wait_until(fn -> GroupCoordinator.assignment(coord, "g", "t", :m1) == {:error, :unknown_member} end)
  end

  test "a member ack refreshes the subscriber's coordinator so the :DOWN leave targets the current owner",
       %{broker: broker} do
    # coord1 = the owner at subscribe time; coord2 = the new owner after a (simulated) leadership change
    coord1 = start_coordinator(broker)
    coord2 = start_coordinator(broker)
    test = self()

    pid =
      spawn(fn ->
        :ok = LogApi.subscribe_member(broker, coord1, "t", "g", :m1, 10, 10)
        # the member re-resolves to coord2 and acks there (a heartbeat): this must refresh sub.coordinator
        :ok = LogApi.stream_ack_member(broker, coord2, "t", "g", :m1, nil, 0)
        send(test, :acked)
        Process.sleep(:infinity)
      end)

    assert_receive :acked, 2_000
    # the ack registered the member on the new owner (coord2)
    wait_until(fn -> match?({:ok, _, _}, GroupCoordinator.assignment(coord2, "g", "t", :m1)) end)

    Process.exit(pid, :kill)
    # the leave must follow the refreshed ref to coord2 (not the stale coord1), so m1 leaves coord2
    wait_until(fn -> GroupCoordinator.assignment(coord2, "g", "t", :m1) == {:error, :unknown_member} end)
  end
end
