defmodule Malachi.BrokerServerStreamingTest do
  # B2-a: streaming subscribers with a credit window and durable group commit, in-process (the test is
  # the subscriber: it runs the reads the broker hands it, as a connection does, through StreamPush).
  use ExUnit.Case, async: false

  import Malachi.Test.PollingHelper
  import Malachi.Test.TeardownHelper

  alias Malachi.BrokerServer
  alias Malachi.BrokerServer.Subscribers
  alias Malachi.Cluster.DSRSM
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Consumer.GroupCoordinator
  alias Malachi.Keyspace
  alias Malachi.Log.Record
  alias Malachi.LogApi
  alias Malachi.Retention.SkipReporter
  alias Malachi.Test.StreamPush
  alias Malachi.Test.TmpDir

  setup do
    dir = TmpDir.path("malachi_stream")
    repl = :"repl_#{System.unique_integer([:positive])}"
    start_supervised!({ReplicationServer, name: repl, directory: Path.join(dir, "repl")}, id: repl)
    {:ok, broker} = BrokerServer.start_link(Path.join(dir, "b"), brokers: [repl])
    {:ok, _root} = BrokerServer.create_topic(broker, "t", 4)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{broker: broker, repl: repl, dir: dir}
  end

  defp produce(broker, values, topic \\ "t") do
    {:ok, _} = BrokerServer.produce(broker, topic, Enum.map(values, &Record.new/1))
  end

  # receive one push for `topic`, returning {values, positions}
  defp recv_push(topic \\ "t") do
    case StreamPush.recv() do
      {:log_records, ^topic, records, positions} -> {Enum.map(records, & &1.value), positions}
      other -> flunk("expected a push to #{topic}, got #{inspect(other)}")
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
    assert StreamPush.recv(100) == :timeout

    # acking 2 returns 2 credit → the next 2 are pushed
    :ok = BrokerServer.stream_ack(broker, "t", "g", positions, 2)
    assert {["c", "d"], _positions} = recv_push()
  end

  describe "reads run in the subscriber, not in the broker's loop" do
    test "a subscriber that has not run its read is handed no second one, and appends keep going",
         %{broker: broker} do
      :ok = BrokerServer.subscribe(broker, "t", "g", 100, 100)

      # nothing to read yet, but the first read is handed out at subscribe and left unrun
      for i <- 1..20, do: produce(broker, ["v#{i}"])

      assert {:messages, messages} = Process.info(self(), :messages)
      assert [{:log_read, _plan}] = Enum.filter(messages, &match?({:log_read, _}, &1))

      # running it delivers nothing (it was planned before the produces); the wakes that came meanwhile
      # hand out exactly one more read, which delivers everything produced since
      assert {:log_records, "t", records, _positions} = StreamPush.recv()
      assert Enum.map(records, & &1.value) == Enum.map(1..20, &"v#{&1}")
      assert StreamPush.recv(100) == :timeout
    end

    test "the window bounds a push across every range of the topic, not each range", %{broker: broker} do
      [root] = BrokerServer.active_range_ids(broker, "t")
      {:ok, _left, _right} = BrokerServer.split_range(broker, root)
      produce_keyed(broker, 40)

      :ok = BrokerServer.subscribe(broker, "t", "g", 2, 100)

      assert {:log_records, "t", records, _positions} = StreamPush.recv()
      assert length(records) <= 2
    end

    test "with less credit than ranges, successive pushes take turns across the ranges", %{broker: broker} do
      [root] = BrokerServer.active_range_ids(broker, "t")
      {:ok, left, right} = BrokerServer.split_range(broker, root)
      produce_keyed(broker, 40)

      :ok = BrokerServer.subscribe(broker, "t", "g", 1, 100)

      pushed_ranges =
        for _ <- 1..4 do
          assert {:log_records, "t", [record], positions} = StreamPush.recv()
          :ok = BrokerServer.stream_ack(broker, "t", "g", positions, 1)
          range_of(broker, record, [left, right])
        end

      assert pushed_ranges in [[left, right, left, right], [right, left, right, left]]
    end
  end

  describe "reads that fail, or would read through a view that went stale" do
    test "a read that fails delivers nothing, moves nothing, and the subscriber is read for again",
         %{broker: broker, repl: repl, dir: dir} do
      produce(broker, ["a", "b"])
      stop_supervised!(repl)

      :ok = BrokerServer.subscribe(broker, "t", "g", 10, 100)

      # the primary is gone: the read fails, and the broker is told so (or it would wait on it forever)
      assert StreamPush.recv(200) == :timeout
      wait_until!(fn -> match?([%{reading: false, in_flight: 0}], subscription(broker)) end)
      assert [%{positions: positions}] = subscription(broker)
      assert positions == %{}

      start_supervised!({ReplicationServer, name: repl, directory: Path.join(dir, "repl")}, id: repl)
      :ok = BrokerServer.stream_ack(broker, "t", "g", %{}, 0)
      assert {["a", "b"], _positions} = recv_push()
    end

    test "a read planned before an ancestor segment was deleted does not move the reader past what is left",
         %{repl: repl, dir: dir} do
      server = start_small_segments(repl, dir, 1)
      attach_skips(server)
      [root] = BrokerServer.active_range_ids(server, "s")
      keys = left_half_keys(3)
      for key <- keys, do: {:ok, _} = BrokerServer.produce(server, "s", [Record.new(key, key: key)])
      wait_until!(fn -> length(sealed(server, root)) == 3 end)
      {:ok, _left, _right} = BrokerServer.split_range(server, root)

      :ok = BrokerServer.subscribe(server, "s", "g", 100, 100)
      assert_receive {:log_read, plan}, 1_000

      # retention deletes the first ancestor segment, metadata then files, after the plan was handed out
      [gone | _] = server |> sealed(root) |> Enum.sort_by(& &1.start_offset)
      :ok = BrokerServer.delete_segment(server, gone.id)
      :ok = ReplicationServer.delete(repl, gone.id)

      # the plan still lists it: its read must fail, not hand the reader to the next source
      assert LogApi.execute_push(plan) == :nothing

      # nothing moved, and the deletion's wake hands out the next read on its own, with no produce or ack
      assert_receive {:log_read, next}, 1_000
      assert next.positions == %{}
      assert [%{positions: positions}] = subscription(server, "s")
      assert positions == %{}

      # that read sees the deletion and delivers what the ancestor still holds for this child
      send(self(), {:log_read, next})
      assert {:log_records, "s", records, _positions} = StreamPush.recv()
      assert Enum.map(records, & &1.value) == Enum.drop(keys, 1)

      # and what it stepped over is reported, attributed to the group, not skipped in silence
      assert_receive {:skip_event, %{offsets: 1}, %{group: "g", span: :upper_bound}}
    end

    test "a push never takes more than its budget, even across a segment boundary", %{repl: repl, dir: dir} do
      server = start_small_segments(repl, dir, 3)
      [root] = BrokerServer.active_range_ids(server, "s")

      # three records fill the first segment, which seals before the next three open the second, so
      # the push below has to cross a sealed edge three records in
      for i <- 1..3, do: {:ok, _} = BrokerServer.produce(server, "s", [Record.new("v#{i}", key: "k0")])
      wait_until!(fn -> match?([%{length: 3}], sealed(server, root)) end)
      for i <- 4..6, do: {:ok, _} = BrokerServer.produce(server, "s", [Record.new("v#{i}", key: "k0")])

      :ok = BrokerServer.subscribe(server, "s", "g", 4, 4)
      assert {:log_records, "s", records, _positions} = StreamPush.recv()
      assert length(records) == 4
      assert [%{in_flight: 4}] = subscription(server, "s")
    end
  end

  describe "the budget of a page" do
    test "a fetch reads up to max from every range, as it always has", %{broker: broker} do
      [root] = BrokerServer.active_range_ids(broker, "t")
      {:ok, _left, _right} = BrokerServer.split_range(broker, root)
      produce_keyed(broker, 40)

      {records, _positions, _skips} = BrokerServer.consume(broker, "t", %{}, 1, 0)
      assert length(records) == 2
    end

    test "a push hands the share a range left unused to the range that has more", %{broker: broker} do
      [root] = BrokerServer.active_range_ids(broker, "t")
      {:ok, _left, _right} = BrokerServer.split_range(broker, root)
      {:ok, _} = BrokerServer.produce(broker, "t", for(key <- left_half_keys(6), do: Record.new(key, key: key)))

      :ok = BrokerServer.subscribe(broker, "t", "g", 4, 100)
      assert {:log_records, "t", records, _positions} = StreamPush.recv()
      assert length(records) == 4
    end
  end

  # The pushed-to subscriptions of `topic`, as the broker holds them.
  defp subscription(broker, topic \\ "t"), do: :sys.get_state(broker).subscribers |> Subscribers.list(topic)

  # A broker on topic "s" whose segments hold `per_segment` records each (records of two-character values
  # and keys), with a skip reporter beside it, as the application starts one.
  defp start_small_segments(repl, dir, per_segment) do
    name = :"small_segments_#{System.unique_integer([:positive])}"
    start_supervised!({SkipReporter, name: SkipReporter.name_for(name)})
    segment_max_bytes = per_segment * Record.encoded_size(Record.new("k0", key: "k0"))

    {:ok, server} =
      BrokerServer.start_link(Path.join(dir, "small"),
        name: name,
        brokers: [repl],
        segment_max_bytes: segment_max_bytes
      )

    {:ok, _root} = BrokerServer.create_topic(server, "s", 4)
    on_exit(fn -> stop_quietly(server) end)
    server
  end

  defp attach_skips(_server) do
    test = self()
    handler_id = "skips-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:malachi, :retention, :skip],
      fn _event, measurements, metadata, _config ->
        if metadata.topic == "s", do: send(test, {:skip_event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp sealed(server, range_id) do
    server
    |> BrokerServer.metadata()
    |> Malachi.Metadata.segments_of_range(range_id)
    |> Enum.filter(&(&1.state == :sealed))
  end

  # `n` keys of the lower half of a 4-bit keyspace: the left child after a split.
  defp left_half_keys(n) do
    Stream.iterate(0, &(&1 + 1))
    |> Stream.map(&"k#{&1}")
    |> Stream.filter(&(Keyspace.position_of(&1, 16) < 8))
    |> Enum.take(n)
  end

  # Records with distinct keys, so a split topic gets some in each range.
  defp produce_keyed(broker, n) do
    {:ok, _} = BrokerServer.produce(broker, "t", for(i <- 1..n, do: Record.new("v#{i}", key: "k#{i}")))
  end

  # The range of `range_ids` that owns `record`'s key. The setup creates "t" with 4 keyspace bits.
  defp range_of(broker, record, range_ids) do
    position = Keyspace.position_of(record.key, 16)
    dsrsm = :sys.get_state(broker).broker.dsrsm

    Enum.find(range_ids, fn range_id ->
      range = DSRSM.get_range(dsrsm, "t", range_id)
      Keyspace.within?(position, range.key_start, range.key_end)
    end)
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
  # nothing but each topic's subscribers being a list of maps carrying `:pid`.
  defp subscribers(broker, topic \\ "t") do
    :sys.get_state(broker).subscribers |> Subscribers.list(topic) |> Enum.map(& &1.pid) |> Enum.sort()
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
    case StreamPush.recv(timeout) do
      {:log_records, "t", records, _positions} -> collect_values(acc ++ Enum.map(records, & &1.value), timeout)
      :timeout -> acc
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
      {:log_read, plan} ->
        case LogApi.execute_push(plan) do
          {:ok, topic, records, _positions} -> send(test, {:pushed, self(), topic, Enum.map(records, & &1.value)})
          :nothing -> :ok
        end

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
      assert StreamPush.recv(100) == :timeout

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
