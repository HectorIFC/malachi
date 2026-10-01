defmodule Malachi.BrokerServerStreamingTest do
  # B2-a: streaming subscribers with a credit window and durable group commit, in-process (the test is
  # the subscriber, receiving {:log_records, ...} into its own mailbox).
  use ExUnit.Case, async: false

  import Malachi.Test.PollingHelper
  import Malachi.Test.TeardownHelper

  alias Malachi.BrokerServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Consumer.GroupCoordinator
  alias Malachi.Log.Record
  alias Malachi.LogApi
  alias Malachi.Test.TmpDir

  setup do
    dir = TmpDir.path("malachi_stream")
    repl = :"repl_#{System.unique_integer([:positive])}"
    start_supervised!({ReplicationServer, name: repl, directory: Path.join(dir, "repl")}, id: repl)
    {:ok, broker} = BrokerServer.start_link(Path.join(dir, "b"), brokers: [repl])
    {:ok, _root} = BrokerServer.create_topic(broker, "t", 4)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{broker: broker}
  end

  defp produce(broker, values, topic \\ "t") do
    {:ok, _} = BrokerServer.produce(broker, topic, Enum.map(values, &Record.new/1))
  end

  # receive one push for `topic`, returning {values, positions}
  defp recv_push(topic \\ "t") do
    receive do
      {:log_records, ^topic, records, positions} -> {Enum.map(records, & &1.value), positions}
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

    wait_until!(fn -> length(subscribers(broker)) == 1 end)
    Process.exit(sub, :kill)
    assert_receive {:DOWN, ^ref, :process, _pid, _reason}

    wait_until!(fn -> subscribers(broker) == [] end)
  end

  # The pids subscribed to `topic`, sorted. The only helper in this file that reads the broker's state: a
  # dead plain subscriber is otherwise invisible (a push to a dead pid is dropped silently). It relies on
  # nothing but `subscribers` being a per-topic list of maps carrying `:pid`, the shape #275 keeps.
  defp subscribers(broker, topic \\ "t") do
    :sys.get_state(broker).subscribers |> Map.get(topic, []) |> Enum.map(& &1.pid) |> Enum.sort()
  end

  defp flush do
    receive do
      _ -> flush()
    after
      0 -> :ok
    end
  end

  # --- consumer-group member scoping (streaming) ---

  defp start_coordinator(broker) do
    {:ok, coord} =
      GroupCoordinator.start_link(
        ranges_fun: fn topic -> BrokerServer.active_range_ids(broker, topic) end,
        tick_ms: 3_600_000
      )

    on_exit(fn -> stop_quietly(coord) end)
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

    wait_until!(fn -> match?({:ok, _, _}, GroupCoordinator.assignment(coord, "g", "t", :m1)) end)

    Process.exit(pid, :kill)
    # the broker's :DOWN spawns an async task to leave the group; the member eventually disappears
    wait_until!(fn -> GroupCoordinator.assignment(coord, "g", "t", :m1) == {:error, :unknown_member} end)
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
    wait_until!(fn -> match?({:ok, _, _}, GroupCoordinator.assignment(coord2, "g", "t", :m1)) end)

    Process.exit(pid, :kill)
    # the leave must follow the refreshed ref to coord2 (not the stale coord1), so m1 leaves coord2
    wait_until!(fn -> GroupCoordinator.assignment(coord2, "g", "t", :m1) == {:error, :unknown_member} end)
  end

  # --- subscriber teardown across topics ---

  # Spawns a process that subscribes to every topic in `topics` and forwards each push to the test as
  # `{:pushed, pid, topic, values}`. It unsubscribes one topic on `{:unsubscribe, topic, from}` and replies
  # `{:unsubscribed, pid}`. Returns once every subscription is in place; the process is killed on exit.
  defp spawn_subscriber(broker, topics, group, window) do
    test = self()

    pid =
      spawn(fn ->
        for topic <- topics, do: :ok = BrokerServer.subscribe(broker, topic, group, window, 100)
        send(test, {:subscribed, self()})
        forward_pushes(broker, test)
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)
    assert_receive {:subscribed, ^pid}, 1_000
    pid
  end

  defp forward_pushes(broker, test) do
    receive do
      {:log_records, topic, records, _positions} ->
        send(test, {:pushed, self(), topic, Enum.map(records, & &1.value)})

      {:unsubscribe, topic, from} ->
        :ok = BrokerServer.unsubscribe(broker, topic)
        send(from, {:unsubscribed, self()})
    end

    forward_pushes(broker, test)
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, _pid, _reason}
  end

  describe "multi-topic subscriber teardown" do
    setup %{broker: broker} do
      {:ok, _root} = BrokerServer.create_topic(broker, "u", 4)
      :ok
    end

    test "a dead subscriber on one topic leaves the other topic's subscribers in place and still pushing",
         %{broker: broker} do
      # the test process subscribes to "u" with one record of its window of two already in flight
      produce(broker, ["u0"], "u")
      :ok = BrokerServer.subscribe(broker, "u", "gu", 2, 100)
      assert {["u0"], _positions} = recv_push("u")

      # the survivor shares the dead one's topic and also has one record of its window of two in flight
      dead = spawn_subscriber(broker, ["t"], "g1", 10)
      survivor = spawn_subscriber(broker, ["t"], "g2", 2)
      assert subscribers(broker, "t") == Enum.sort([dead, survivor])
      produce(broker, ["t0"])
      assert_receive {:pushed, ^survivor, "t", ["t0"]}, 1_000

      kill(dead)

      wait_until!(fn -> subscribers(broker, "t") == [survivor] end)
      assert subscribers(broker, "u") == [self()]

      # the "u" subscriber kept its position and its in-flight count: one credit left, so one record
      produce(broker, ["u1", "u2"], "u")
      assert {["u1"], positions} = recv_push("u")
      refute_receive {:log_records, "u", _, _}, 100

      :ok = BrokerServer.stream_ack(broker, "u", "gu", positions, 2)
      assert {["u2"], _positions} = recv_push("u")

      # the survivor on the dead one's topic kept its position (no t0 again) and its credit (no t2 yet)
      produce(broker, ["t1", "t2"])
      assert_receive {:pushed, ^survivor, "t", ["t1"]}, 1_000
      refute_receive {:pushed, ^survivor, "t", _}, 100
    end

    test "a process subscribed to two topics is removed from both when it dies", %{broker: broker} do
      both = spawn_subscriber(broker, ["t", "u"], "g", 10)
      on_t = spawn_subscriber(broker, ["t"], "gt", 10)
      on_u = spawn_subscriber(broker, ["u"], "gu", 10)
      assert subscribers(broker, "t") == Enum.sort([both, on_t])
      assert subscribers(broker, "u") == Enum.sort([both, on_u])

      kill(both)

      wait_until!(fn -> subscribers(broker, "t") == [on_t] and subscribers(broker, "u") == [on_u] end)

      produce(broker, ["t1"])
      produce(broker, ["u1"], "u")
      assert_receive {:pushed, ^on_t, "t", ["t1"]}, 1_000
      assert_receive {:pushed, ^on_u, "u", ["u1"]}, 1_000
    end

    test "unsubscribing one topic keeps the other subscription, and the process's death still removes it",
         %{broker: broker} do
      both = spawn_subscriber(broker, ["t", "u"], "g", 10)

      send(both, {:unsubscribe, "t", self()})
      assert_receive {:unsubscribed, ^both}, 1_000
      assert subscribers(broker, "t") == []
      assert subscribers(broker, "u") == [both]

      kill(both)

      wait_until!(fn -> subscribers(broker, "u") == [] end)
      assert subscribers(broker, "t") == []
    end

    test "a member on two topics leaves both groups on death, and a member on another topic stays",
         %{broker: broker} do
      coord = start_coordinator(broker)
      test = self()

      member = fn member, topics ->
        pid =
          spawn(fn ->
            for topic <- topics, do: :ok = LogApi.subscribe_member(broker, coord, topic, "g", member, 10, 10)
            send(test, {:subscribed, member})
            Process.sleep(:infinity)
          end)

        on_exit(fn -> Process.exit(pid, :kill) end)
        pid
      end

      m1 = member.(:m1, ["t", "u"])
      assert_receive {:subscribed, :m1}, 2_000
      _m2 = member.(:m2, ["u"])
      assert_receive {:subscribed, :m2}, 2_000

      assigned? = fn topic, member -> match?({:ok, _, _}, GroupCoordinator.assignment(coord, "g", topic, member)) end

      gone? = fn topic, member ->
        GroupCoordinator.assignment(coord, "g", topic, member) == {:error, :unknown_member}
      end

      assert assigned?.("t", :m1) and assigned?.("u", :m1) and assigned?.("u", :m2)

      kill(m1)

      # the broker's :DOWN leaves each of the dead member's groups in an async task, one per subscription
      wait_until!(fn -> gone?.("t", :m1) and gone?.("u", :m1) end)
      # a leave aimed at the wrong subscriber would land within the same burst of tasks
      assert wait_until(fn -> gone?.("u", :m2) end, timeout: 200) == {:error, :timeout}
      assert assigned?.("u", :m2)
    end
  end
end
