defmodule Malachi.BrokerServerRaTest do
  # async: false: ra is global/stateful (one data dir, on-disk Raft logs).
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Malachi.Test.PollingHelper
  import Malachi.Test.TeardownHelper

  alias Malachi.BrokerServer
  alias Malachi.Cluster.DSRSM
  alias Malachi.Cluster.HealCoordinator
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record
  alias Malachi.Metadata
  alias Malachi.Storage.Layout
  alias Malachi.Test.AliveMembersStub
  alias Malachi.Test.FaultySegmentStore
  alias Malachi.Test.SilentRaMember
  alias Malachi.Test.UnknownMessages

  setup_all do
    :ok
  end

  defp start_replication do
    directory = Path.join(System.tmp_dir!(), "malachi_ra_bs_repl_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    start_supervised!({ReplicationServer, directory: directory}, id: {:repl, System.unique_integer([:positive])})
  end

  test "metadata mutations are committed to the Raft cluster, and the cache matches it" do
    cluster = :"bs_meta_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)

    brokers = for _ <- 1..3, do: start_replication()

    {:ok, control} =
      BrokerServer.start_link("unused",
        brokers: brokers,
        replication_factor: 3,
        segment_max_bytes: Record.encoded_size(Record.new("value", key: "key")),
        metadata_cluster: cluster
      )

    {:ok, root} = BrokerServer.create_topic(control, "events", 4)
    {:ok, _placements} = BrokerServer.produce(control, "events", [Record.new("v", key: "k")])

    # the topic and segment live in the replicated Raft state (queried directly), proving the
    # control-plane mutations went through the log rather than only a local map
    {:ok, replicated} = MetadataServer.query({cluster, node()}, & &1)
    assert Metadata.get_topic(replicated, "events").name == "events"
    assert Metadata.segments_of_range(replicated, root) != []

    # and the broker's local cache is exactly the replicated state (read-your-writes)
    assert BrokerServer.metadata(control) == replicated

    :ok = BrokerServer.stop(control)
  end

  test "a restarted broker serves reads of pre-restart data (range state recovery)" do
    cluster = :"bs_meta_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)
    repl = start_replication()

    {:ok, first} = BrokerServer.start_link("unused", brokers: [repl], metadata_cluster: cluster)
    {:ok, root} = BrokerServer.create_topic(first, "events", 4)
    {:ok, _} = BrokerServer.produce(first, "events", for(i <- 1..5, do: Record.new("v#{i}", key: "k#{i}")))
    :ok = BrokerServer.stop(first)

    # A fresh broker over the SAME metadata cluster and replication server, the restart shape the
    # chaos harness exercises. Before range-state recovery its empty offsets map clamped every read
    # to :eof at offset 0, so durable pre-restart data was unreadable until the next produce.
    {:ok, second} = BrokerServer.start_link("unused", brokers: [repl], metadata_cluster: cluster)

    {:ok, records} = BrokerServer.read(second, root, 0, 100)
    assert Enum.map(records, & &1.value) == for(i <- 1..5, do: "v#{i}")

    # The consume/fetch path (what the chaos verify uses) works too.
    {consumed, _next, _skips} = BrokerServer.consume(second, "events", %{}, 100, 0)
    assert length(consumed) == 5

    # And producing continues cleanly after the restart (recovered offsets + segment seq floor).
    {:ok, _} = BrokerServer.produce(second, "events", [Record.new("v6", key: "k6")])
    {:ok, all} = BrokerServer.read(second, root, 0, 100)
    assert length(all) == 6

    :ok = BrokerServer.stop(second)
  end

  test "a restarted broker AND replication server serve reads of pre-restart data" do
    # The test above restarts the broker while keeping the replication server alive, so its segment
    # logs stay open and the recovered read horizon is right by accident. A container restart takes
    # both down, and then the recovery asked a server whose logs map was empty, got told the segment
    # held nothing, and set the horizon to zero. Every read then clamped to :eof before it could reach
    # the cold-segment recovery in the read path, which is the deadlock: no read, so no open segment,
    # so no horizon, so no read. Only a produce broke it.
    cluster = :"bs_meta_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)
    directory = Path.join(System.tmp_dir!(), "malachi_cold_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)

    # A REGISTERED server, so its ref is {name, node} and survives the restart. An unregistered one is
    # referenced by pid, which a restart changes, and the recovery would then be asking a dead process
    # rather than a cold one: a different failure that would hide this one.
    name = :"cold_repl_#{System.unique_integer([:positive])}"
    {:ok, repl} = ReplicationServer.start_link(directory: directory, name: name)
    {:ok, first} = BrokerServer.start_link("unused", brokers: [{name, node()}], metadata_cluster: cluster)
    {:ok, root} = BrokerServer.create_topic(first, "events", 4)
    {:ok, _} = BrokerServer.produce(first, "events", for(i <- 1..5, do: Record.new("v#{i}", key: "k#{i}")))
    :ok = BrokerServer.stop(first)
    :ok = GenServer.stop(repl)

    # Both come back over the same directory and the same metadata cluster, holding nothing in memory.
    {:ok, cold_repl} = ReplicationServer.start_link(directory: directory, name: name)
    {:ok, second} = BrokerServer.start_link("unused", brokers: [{name, node()}], metadata_cluster: cluster)

    assert {:ok, records} = BrokerServer.read(second, root, 0, 100)
    assert Enum.map(records, & &1.value) == for(i <- 1..5, do: "v#{i}")

    {consumed, _next, _skips} = BrokerServer.consume(second, "events", %{}, 100, 0)
    assert length(consumed) == 5, "a cold replication server must not report durable records as drained"

    :ok = BrokerServer.stop(second)
    :ok = GenServer.stop(cold_repl)
  end

  test "a restarted broker whose active segment's primary copy failed in storage still starts" do
    # Recovery asks the primary for the active segment's durable end. A copy latched as failed answers an
    # error, which must count as no answer (seated at zero, retried next tick), not crash the broker's init.
    cluster = :"bs_failed_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)
    directory = Path.join(System.tmp_dir!(), "malachi_failed_#{System.unique_integer([:positive])}")

    on_exit(fn ->
      FaultySegmentStore.clear(directory)
      File.rm_rf!(directory)
    end)

    name = :"failed_repl_#{System.unique_integer([:positive])}"
    {:ok, repl} = ReplicationServer.start_link(directory: directory, name: name, store: FaultySegmentStore)
    on_exit(fn -> stop_quietly(repl) end)

    {:ok, first} = BrokerServer.start_link("unused", brokers: [{name, node()}], metadata_cluster: cluster)
    {:ok, root} = BrokerServer.create_topic(first, "events", 4)
    {:ok, _} = BrokerServer.produce(first, "events", for(i <- 1..5, do: Record.new("v#{i}", key: "k#{i}")))
    :ok = BrokerServer.stop(first)

    segment_id = {root, 0}
    FaultySegmentStore.fail(Layout.segment_directory(directory, segment_id), :sync, {:error, :eio})

    ExUnit.CaptureLog.capture_log(fn ->
      late = [Record.new("late", key: "late")]
      assert {:error, {:storage, :eio}} = ReplicationServer.follow({name, node()}, segment_id, 5, late)
      assert {:error, {:storage, :eio}} = ReplicationServer.durable_end({name, node()}, segment_id, 0)

      {:ok, second} = BrokerServer.start_link("unused", brokers: [{name, node()}], metadata_cluster: cluster)
      assert Process.alive?(second)
      assert BrokerServer.active_range_ids(second, "events") == [root]
      :ok = BrokerServer.stop(second)
    end)
  end

  test "a subscriber is pushed records produced through a different frontend" do
    # A streaming subscriber used to be pushed only on subscribe, on its own ack, and on a produce
    # through the broker it subscribed to. A produce through another frontend woke that frontend's
    # subscribers and not this one's, so a topic written on one node and streamed from another
    # delivered nothing, with no error anywhere and both nodes healthy. An ack cannot recover it,
    # having no records to acknowledge.
    cluster = :"bs_sub_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)

    name = :"sub_repl_#{System.unique_integer([:positive])}"
    directory = Path.join(System.tmp_dir!(), "malachi_sub_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    {:ok, repl} = ReplicationServer.start_link(directory: directory, name: name)
    on_exit(fn -> stop_quietly(repl) end)

    opts = [brokers: [{name, node()}], metadata_cluster: cluster, brokers_refresh_interval: 100]
    {:ok, writer} = BrokerServer.start_link("unused", opts)
    {:ok, streamer} = BrokerServer.start_link("unused", opts)

    {:ok, _root} = BrokerServer.create_topic(writer, "events", 4)
    :ok = BrokerServer.subscribe(streamer, "events", "g", 100, 10)

    # Produced through the OTHER broker, so nothing on this path wakes the subscription.
    {:ok, _} = BrokerServer.produce(writer, "events", [Record.new("v1", key: "k1")])

    assert_receive {:log_records, "events", records, _positions}, 3_000
    assert Enum.map(records, & &1.value) == ["v1"]

    :ok = BrokerServer.stop(writer)
    :ok = BrokerServer.stop(streamer)
  end

  test "a broker that loses the control plane stays ready, and keeps serving what it already knew" do
    # Readiness answers whether this node can serve, not whether the control plane is answering. A
    # refresh that fails leaves the last view in place and reads keep working from it, so reporting
    # unready would pull a working node out of rotation. Every node sees the same outage at the same
    # moment, so that answer would empty the load balancer exactly when all of its backends still work.
    cluster = :"bs_ready_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "malachi_ready_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, repl} = ReplicationServer.start_link(directory: dir)
    on_exit(fn -> stop_quietly(repl) end)

    {:ok, server} =
      BrokerServer.start_link("unused", brokers: [repl], metadata_cluster: cluster, brokers_refresh_interval: 50)

    {:ok, root} = BrokerServer.create_topic(server, "events", 4)
    {:ok, _} = BrokerServer.produce(server, "events", [Record.new("v1", key: "k1")])
    assert BrokerServer.metadata_ready?(server)

    # The control plane goes away. Several refreshes fail before the assertions below.
    MetadataServer.delete(cluster)
    Process.sleep(300)

    assert BrokerServer.metadata_ready?(server), "a node serving from its retained view is ready"
    assert {:ok, [record]} = BrokerServer.read(server, root, 0, 100)
    assert record.value == "v1"
    assert {[_], _positions, []} = BrokerServer.consume(server, "events", %{}, 100, 0)

    :ok = BrokerServer.stop(server)
  end

  test "a sharded broker stays ready when a vnode it had already read stops answering" do
    # The case that distinguishes this policy from the previous one. A vnode that was never reachable
    # leaves the node unready under both, because there is no view of it to serve from. One that WAS
    # read and then went silent used to report unready, and now does not: the retained view is what it
    # serves, and it is still serving.
    #
    # `bootstrap_orchestrator` is forced off so nothing recreates the vnode's cluster underneath the
    # assertion, which is what this node would otherwise do a tick later as the sole member.
    suffix = System.unique_integer([:positive])
    vnode = :"bs_gone_#{suffix}"
    on_exit(fn -> MetadataServer.delete(vnode) end)
    {:ok, _server_id} = MetadataServer.start(vnode, [node()])

    {:ok, server} =
      BrokerServer.start_link("unused",
        brokers: [start_replication()],
        metadata_vnodes: [{vnode, 0, [node()]}],
        bootstrap_orchestrator: fn -> false end,
        brokers_refresh_interval: 50
      )

    {:ok, _root} = BrokerServer.create_topic(server, "events", 4)
    Process.sleep(200)
    assert BrokerServer.metadata_ready?(server), "the vnode answered, so the node is ready"

    MetadataServer.delete(vnode)
    Process.sleep(300)

    assert BrokerServer.metadata_ready?(server), "a vnode that went silent does not unready the node"
    assert BrokerServer.active_range_ids(server, "events") != [], "and its topics are still there"

    :ok = BrokerServer.stop(server)
  end

  test "a topic whose metadata vnode never answered is refused, not reported as drained" do
    # Two vnodes: one on this node, one routed at a node that does not exist, so its ra cluster can
    # never be read. That is the shape of a broker whose control plane is partly unreachable, which is
    # also the shape of a broker that just restarted and whose vnodes have not come up yet.
    #
    # The absent node lives at a literal address, as the other tests' `nonexistent@127.0.0.1` does. It
    # was `absent@nowhere` once, and every attempt to reach it then resolved `nowhere` through the OS
    # resolver before failing: with the resolver slow, a single attempt took the whole 5s the ra query
    # allows, the broker loop sat in it, and the consume below timed out (once in 192 CI runs; every
    # time with the resolver blackholed). An address needs no resolving, so the attempt fails at once.
    suffix = System.unique_integer([:positive])
    reachable = {:"bs_reach_#{suffix}", 0, [node()]}
    ghost = {:"bs_ghost_#{suffix}", div(Integer.pow(2, 32), 2), [:"absent@127.0.0.1"]}
    on_exit(fn -> MetadataServer.delete(elem(reachable, 0)) end)

    {:ok, control} =
      BrokerServer.start_link("unused", brokers: [start_replication()], metadata_vnodes: [reachable, ghost])

    # Names divide between the two vnodes by hash. A name on the reachable vnode reads normally; a name
    # on the ghost must NOT read as an empty topic, because this broker cannot know whether it is empty.
    results = for i <- 0..19, do: BrokerServer.consume(control, "gate_t#{i}", %{}, 100, 0)

    refused = Enum.filter(results, &match?({:error, :metadata_unavailable}, &1))
    answered = Enum.filter(results, &match?({[], _positions, []}, &1))

    assert refused != [], "expected topics routed to the unreachable vnode to be refused"
    assert answered != [], "expected topics routed to the reachable vnode to still be served"

    # And the node says it is not ready, so an orchestrator stops routing to it rather than letting it
    # answer half its keyspace with a successful lie.
    refute BrokerServer.metadata_ready?(control)

    :ok = BrokerServer.stop(control)
  end

  # --- #178: the reconcile must not stall the clients of the vnodes that do answer ---

  # A live vnode and a silent one at opposite ends of the ring, with the bootstrap step off so nothing
  # tries to form the silent one's cluster underneath the assertions. Names split between the two by
  # hash, exactly as in the "never answered" test above.
  defp live_and_silent_vnodes(read_timeout, opts \\ []) do
    suffix = System.unique_integer([:positive])
    live = :"bs_live_#{suffix}"
    silent = :"bs_mute_#{suffix}"

    {:ok, _server_id} = MetadataServer.start(live, [node()])
    on_exit(fn -> MetadataServer.delete(live) end)

    # A vnode whose name is not registered at all fails its read at once (noproc), so a test that wants
    # a cheap boot and an expensive tick registers the silent member only after the broker is up.
    {silent_at_boot?, opts} = Keyword.pop(opts, :silent_at_boot, true)
    if silent_at_boot?, do: start_silent(silent)

    vnodes = [{live, 0, [node()]}, {silent, div(Integer.pow(2, 32), 2), [node()]}]

    broker_opts =
      Keyword.merge(
        [
          brokers: [start_replication()],
          metadata_vnodes: vnodes,
          bootstrap_orchestrator: fn -> false end,
          reconcile_read_timeout: read_timeout
        ],
        opts
      )

    {live, silent, broker_opts}
  end

  # `start_link/2` returns once `init/1` has, while the boot `handle_continue` still has a reconcile to
  # run on the loop. Any call queues behind it, so this is how a test knows boot is over before it makes
  # the control plane slow: otherwise the boot pass itself pays the new cost and eats the assertion's
  # window, which passed in isolation and failed behind fifteen other tests.
  defp await_boot(server), do: BrokerServer.metadata(server)

  # A topic name that routes to `vnode` on this broker's ring. Names split between the vnodes by hash,
  # and a name on the SILENT vnode would block the write itself, which is a different problem.
  defp topic_on(control, vnode, prefix) do
    ring = :sys.get_state(control).broker.dsrsm.ring

    Enum.find(
      Stream.map(0..80, &"#{prefix}_#{&1}"),
      fn name -> match?({:ok, ^vnode}, DSRSM.vnode_for(%DSRSM{ring: ring, vnodes: %{}}, name)) end
    )
  end

  # The ref of the reconcile task currently in flight, once there is one. Driving these tests off the
  # task's own identity rather than off a sleep is what makes them deterministic: the window under test
  # is exactly "between this task starting and its result landing".
  defp task_ref!(control) do
    wait_until!(fn -> :sys.get_state(control).reconcile_task != nil end)
    :sys.get_state(control).reconcile_task.ref
  end

  # Returns once the task identified by `ref` is no longer the one in flight, which is when its result
  # has been applied (or it was given up on).
  defp await_task_result!(control, ref) do
    wait_until!(
      fn ->
        case :sys.get_state(control).reconcile_task do
          nil -> true
          %{ref: ^ref} -> false
          _other -> true
        end
      end,
      timeout: 15_000
    )
  end

  defp start_silent(name) do
    {:ok, _pid} = SilentRaMember.start_link(name)
    on_exit(fn -> SilentRaMember.stop(name) end)
    :ok
  end

  # Forwards every `[:malachi, :cluster, :reconcile_degraded]` reason to the test as `{:degraded, r}`.
  defp watch_degraded do
    handler_id = {__MODULE__, make_ref()}
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:malachi, :cluster, :reconcile_degraded],
        fn _event, _measurements, %{reason: reason}, _config -> send(test_pid, {:degraded, reason}) end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok
  end

  test "a vnode that answers nothing does not stall the clients of the vnode that does" do
    # The defect. The reconcile used to make its control plane reads INSIDE this server's loop, so
    # while one vnode swallowed a query every client call on the node queued behind it: up to ra's 5s
    # per silent vnode, every tick. The node held a view it could serve from and did not answer.
    #
    # The tick is 50ms here, so on the old code the loop was blocked essentially continuously: each
    # pass rescheduled before it blocked, so the next `:reconcile` was already in the mailbox when the
    # previous one gave up. A consume issued at any moment waited seconds.
    {_live, _silent, opts} = live_and_silent_vnodes(1_000, brokers_refresh_interval: 50)
    {:ok, control} = BrokerServer.start_link("unused", opts)
    on_exit(fn -> stop_quietly(control) end)

    # Let a few ticks go by, so the assertion lands while a reconcile is in flight rather than before
    # the first one started.
    Process.sleep(200)

    task = Task.async(fn -> for i <- 0..19, do: BrokerServer.consume(control, "stall_t#{i}", %{}, 100, 0) end)
    results = Task.await(task, 1_000)

    # Half the names route to the silent vnode and must still be refused rather than reported drained:
    # moving the reconcile off the loop changes WHEN the node answers, never WHAT it answers.
    assert Enum.any?(results, &match?({:error, :metadata_unavailable}, &1)),
           "topics on the silent vnode must still be refused"

    assert Enum.any?(results, &match?({[], _positions, []}, &1)),
           "topics on the live vnode must still be served"
  end

  test "boot does not wait out the control plane either" do
    # `init/1` snapshots the vnodes and `handle_continue` reconciles once more before the first client
    # call is served, both on this loop. Read sequentially at ra's default that was 5s per silent vnode
    # in each of them; bounded and concurrent it is one short read. Readiness still has to be answerable
    # by the time the first call is: that is what the continue is for, and why it stays on the loop.
    {_live, _silent, opts} = live_and_silent_vnodes(500)

    {elapsed_us, control} =
      :timer.tc(fn ->
        {:ok, control} = BrokerServer.start_link("unused", opts)
        # Queued behind the boot `handle_continue`, so this answers only once that reconcile landed.
        _ = BrokerServer.metadata(control)
        control
      end)

    on_exit(fn -> stop_quietly(control) end)

    assert elapsed_us < 4_000_000,
           "boot took #{div(elapsed_us, 1000)}ms; the two boot reads are waiting out the control plane"

    # And the live vnode was read, so its topics are servable at once rather than after the first tick.
    served = for i <- 0..19, do: BrokerServer.consume(control, "boot_t#{i}", %{}, 100, 0)
    assert Enum.any?(served, &match?({[], _positions, []}, &1))
  end

  test "a reconcile that overruns its deadline is killed, counted and logged" do
    # The reads are bounded, but bootstrapping a vnode reaches `:ra.start_cluster`, whose `rpc:call/4`
    # has no timeout at all. Without this deadline a task wedged there would hold the one-at-a-time slot
    # for good and no reconcile would ever run again, with nothing in the log to say so. A read timeout
    # far longer than the deadline puts the task in exactly that position: still running when it fires.
    {_live, silent, opts} =
      live_and_silent_vnodes(3_000,
        silent_at_boot: false,
        brokers_refresh_interval: 50,
        reconcile_deadline_ms: 50
      )

    watch_degraded()

    log =
      capture_log(fn ->
        {:ok, control} = BrokerServer.start_link("unused", opts)
        on_exit(fn -> stop_quietly(control) end)
        _ = await_boot(control)
        start_silent(silent)

        assert_receive {:degraded, :timeout}, 10_000

        # The kill freed the slot rather than taking the broker with it, so ticking carries on: a
        # second overrun proves a later tick started a task of its own.
        assert_receive {:degraded, :timeout}, 10_000
        assert Process.alive?(control)

        # And the node is still serving from the view it holds, which is the whole point of the design
        # this deadline protects.
        assert BrokerServer.metadata_ready?(control) in [true, false]
      end)

    assert log =~ "control plane reconcile overran"
  end

  test "a reply from the task a deadline gave up on is dropped without being counted as unknown" do
    # The deadline kills the task, but its answer may already be in the mailbox. A reply with no clause
    # is a counted drop (`Malachi.UnexpectedMessage`), which the suite guard turns into a failed run, so
    # the one slot that remembers the abandoned ref is what keeps a normal race from reading as a bug.
    {_live, silent, opts} =
      live_and_silent_vnodes(3_000,
        silent_at_boot: false,
        brokers_refresh_interval: 50,
        reconcile_deadline_ms: 50
      )

    watch_degraded()

    capture_log(fn ->
      {:ok, control} = BrokerServer.start_link("unused", opts)
      on_exit(fn -> stop_quietly(control) end)
      _ = await_boot(control)
      start_silent(silent)

      assert_receive {:degraded, :timeout}, 10_000

      # Let the member answer from here on, so no further deadline fires and the remembered ref stays
      # the one whose task was just killed.
      :ok = SilentRaMember.release(silent)
      abandoned = :sys.get_state(control).abandoned_ref
      assert is_reference(abandoned)

      {drops, _log} =
        UnknownMessages.drops(control, fn -> send(control, {abandoned, {:reconciled, 0, nil}}) end)

      assert drops == []
      assert :sys.get_state(control).abandoned_ref == nil
    end)
  end

  test "a write made while the reconcile task was reading survives its result" do
    # The task reads the control plane, and the read it brings back was taken BEFORE anything the loop
    # handled meanwhile. Installing it REPLACES a reachable vnode's metadata, so without replaying what
    # was applied in between, a topic created during the window is undone: produce is then refused as
    # :no_such_topic and consume answers a successful empty page, which is the one answer a client
    # cannot tell from the truth.
    {live, silent, opts} = live_and_silent_vnodes(2_000, silent_at_boot: false, brokers_refresh_interval: 50)

    {:ok, control} = BrokerServer.start_link("unused", opts)
    on_exit(fn -> stop_quietly(control) end)
    _ = await_boot(control)

    topic = topic_on(control, live, "survive_#{System.unique_integer([:positive])}")
    start_silent(silent)

    # Inside the window by construction: the write happens while THIS task is reading, and the
    # assertions run once THIS task's result has been applied.
    ref = task_ref!(control)
    {:ok, _root} = BrokerServer.create_topic(control, topic, 4)
    assert BrokerServer.active_range_ids(control, topic) != [], "the topic must exist the moment it is created"

    await_task_result!(control, ref)

    assert BrokerServer.active_range_ids(control, topic) != [],
           "the reconcile result discarded a topic created while it was reading"

    assert {:ok, _placements} = BrokerServer.produce(control, topic, [Record.new("v", key: "k")])
  end

  test "a committed group position is not rolled back by the reconcile result" do
    # The same replacement, on the metadata a consumer group depends on. `committed_offsets/3` reads
    # the cache, and a position that goes backwards is redelivery: at ten million messages a day a
    # window as long as one read is thousands of messages consumed twice, and a position that comes
    # back empty is the whole range consumed again.
    {live, silent, opts} = live_and_silent_vnodes(2_000, silent_at_boot: false, brokers_refresh_interval: 50)

    {:ok, control} = BrokerServer.start_link("unused", opts)
    on_exit(fn -> stop_quietly(control) end)
    _ = await_boot(control)

    topic = topic_on(control, live, "commits_#{System.unique_integer([:positive])}")
    {:ok, root} = BrokerServer.create_topic(control, topic, 4)
    group = "billing"

    :ok = BrokerServer.commit_offset(control, group, topic, %{root => 100})
    start_silent(silent)

    # Commit inside the window of one identified task, then read the position back once that task's
    # result has landed. Without the replay this reads 100, the position the task saw before the commit.
    ref = task_ref!(control)
    :ok = BrokerServer.commit_offset(control, group, topic, %{root => 900})
    assert Map.get(BrokerServer.committed_offsets(control, group, topic), root) == 900

    await_task_result!(control, ref)

    assert Map.get(BrokerServer.committed_offsets(control, group, topic), root) == 900,
           "the reconcile result rolled a committed group position backwards"
  end

  test "the bootstrap pass checks the vnodes concurrently, so silent ones do not add up" do
    # The orchestrator asks every vnode whether its cluster is formed before the metadata is read at
    # all, and this pass runs ON THIS LOOP at boot and in `reconcile_now/2`. Asked in sequence, three
    # silent vnodes cost three read timeouts before anything else happens; asked together, one.
    suffix = System.unique_integer([:positive])
    silent = for i <- 0..2, do: :"bs_boot_#{i}_#{suffix}"

    vnodes =
      silent
      |> Enum.with_index()
      |> Enum.map(fn {name, i} -> {name, i * div(Integer.pow(2, 32), 3), [node()]} end)

    # The orchestrator is held off until the silent members hold the names: left on, boot would form
    # real clusters under them, which is a different (and fast) pass from the one being measured.
    gate = :counters.new(1, [])

    {:ok, control} =
      BrokerServer.start_link("unused",
        brokers: [start_replication()],
        metadata_vnodes: vnodes,
        bootstrap_orchestrator: fn -> :counters.get(gate, 1) == 1 end,
        reconcile_read_timeout: 1_000,
        brokers_refresh_interval: 60_000
      )

    on_exit(fn -> stop_quietly(control) end)
    _ = await_boot(control)
    Enum.each(silent, &start_silent/1)

    # From here the pass runs as it does on the node that holds the role, which is exactly the node the
    # sequential version made slowest.
    :counters.put(gate, 1, 1)

    # A pass driven by hand, so what is measured is one pass and not a tick landing mid-measurement.
    {elapsed_us, :ok} = :timer.tc(fn -> BrokerServer.reconcile_now(control) end)

    assert elapsed_us < 2_500_000,
           "one pass over three silent vnodes took #{div(elapsed_us, 1000)}ms; the readiness checks are in sequence"
  end

  test "a reconcile that crashes is logged and counted, and does not take the broker with it" do
    # `bootstrap_orchestrator` is an injected seam (in production the membership leader, which is a call
    # into another process). It runs inside the reconcile now, so what it raises lands in a task rather
    # than on this loop: the broker survives, the tick keeps its rhythm, and an operator gets a line.
    suffix = System.unique_integer([:positive])
    live = :"bs_boom_#{suffix}"
    {:ok, _server_id} = MetadataServer.start(live, [node()])
    on_exit(fn -> MetadataServer.delete(live) end)

    # It raises from the first TICK on, not at boot: the boot pass runs on this loop, so a raise there
    # fails `init/1` and the supervisor restarts the broker, which is the right answer for a node that
    # cannot complete its very first reconcile and is not what this test is about.
    calls = :counters.new(1, [])

    orchestrator = fn ->
      :counters.add(calls, 1, 1)
      if :counters.get(calls, 1) > 1, do: raise("membership is not answering"), else: true
    end

    watch_degraded()

    log =
      capture_log(fn ->
        {:ok, control} =
          BrokerServer.start_link("unused",
            brokers: [start_replication()],
            metadata_vnodes: [{live, 0, [node()]}],
            bootstrap_orchestrator: orchestrator,
            brokers_refresh_interval: 50
          )

        on_exit(fn -> stop_quietly(control) end)

        assert_receive {:degraded, :down}, 10_000
        assert Process.alive?(control)

        # And it is still serving: this is the node keeping its retained view while the control plane
        # work fails beside it, which is the whole point of taking that work off this loop.
        assert {[], _positions, []} = BrokerServer.consume(control, "crash_t", %{}, 100, 0)
      end)

    assert log =~ "control plane reconcile crashed"
  end

  test "a reconcile tick reaching a broker with in-memory metadata does nothing" do
    # The tick timer is only ever armed on the replicated path, so this shape does not arise on its own.
    # It arrives from outside: `:reconcile` used to be the documented way for a test to drive a pass, and
    # a stray one must be a no-op rather than start a task with nothing to read.
    directory = Path.join(System.tmp_dir!(), "malachi_bs_tick_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)

    {:ok, control} = BrokerServer.start_link(directory)
    on_exit(fn -> stop_quietly(control) end)

    send(control, :reconcile)

    assert :sys.get_state(control).reconcile_task == nil
    assert BrokerServer.metadata_ready?(control)
  end

  test "a tick that finds the previous reconcile still running counts itself instead of piling on" do
    # One reconcile at a time. Against a control plane slow enough that a pass outlives the tick, the
    # passes would otherwise stack up, each with its own vnode bootstrap. The skipped tick is counted
    # because what it costs is a view that keeps ageing, which nothing else reports.
    {_live, silent, opts} = live_and_silent_vnodes(3_000, silent_at_boot: false, brokers_refresh_interval: 20)

    watch_degraded()

    {:ok, control} = BrokerServer.start_link("unused", opts)
    on_exit(fn -> stop_quietly(control) end)
    _ = await_boot(control)
    start_silent(silent)

    assert_receive {:degraded, :skipped}, 10_000
  end

  test "a deadline for a task that already finished is ignored, not counted as an unknown message" do
    # The deadline timer is cancelled when the task answers, but cancelling loses a race with a timer
    # that already fired. The clause that swallows it must exist, or the message reaches the catch-all
    # and is counted as a drop, which the suite guard turns into a failed run.
    cluster = :"bs_deadline_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)

    {:ok, control} = BrokerServer.start_link("unused", brokers: [start_replication()], metadata_cluster: cluster)
    on_exit(fn -> stop_quietly(control) end)

    {drops, _log} = UnknownMessages.drops(control, fn -> send(control, {:reconcile_deadline, make_ref()}) end)

    assert drops == []
    assert BrokerServer.metadata_ready?(control)
  end

  test "reconcile_now is a no-op on a broker with in-memory metadata" do
    # No control plane to read, so the barrier the tests use has nothing to wait for and must not
    # depend on one existing.
    directory = Path.join(System.tmp_dir!(), "malachi_bs_inmem_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)

    {:ok, control} = BrokerServer.start_link(directory)
    on_exit(fn -> stop_quietly(control) end)

    assert BrokerServer.reconcile_now(control) == :ok
    assert BrokerServer.metadata_ready?(control)
  end

  test "a rejected control-plane command surfaces the Raft machine error" do
    cluster = :"bs_meta_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)

    {:ok, control} =
      BrokerServer.start_link("unused", brokers: [start_replication()], metadata_cluster: cluster)

    {:ok, _root} = BrokerServer.create_topic(control, "events", 4)
    assert {:error, :already_exists} = BrokerServer.create_topic(control, "events", 4)

    :ok = BrokerServer.stop(control)
  end

  test "shards the control plane across vnodes: each topic's metadata lives in its own ra cluster" do
    # two vnodes at opposite ends of the ring, each its own ra cluster over the local node (this node
    # is the sole node, so it is the bootstrap orchestrator by default)
    vnodes =
      for i <- 0..1, do: {:"bs_vn_#{i}_#{System.unique_integer([:positive])}", i * div(Integer.pow(2, 32), 2), [node()]}

    on_exit(fn -> Enum.each(vnodes, fn {name, _token, _nodes} -> MetadataServer.delete(name) end) end)

    {:ok, control} =
      BrokerServer.start_link("unused", brokers: [start_replication()], metadata_vnodes: vnodes)

    names = for i <- 0..9, do: "t#{i}"
    for name <- names, do: assert({:ok, _root} = BrokerServer.create_topic(control, name, 4))

    # each topic is retrievable through the broker's cache, and its metadata was committed to exactly
    # the ra cluster its name routes to (queried directly), and to no other vnode
    home = fn name ->
      Enum.filter(vnodes, fn {vnode, _token, _nodes} ->
        match?(%{name: ^name}, elem(MetadataServer.query({vnode, node()}, &Metadata.get_topic(&1, name)), 1))
      end)
    end

    homes =
      Map.new(names, fn name ->
        assert BrokerServer.active_range_ids(control, name) == [{name, 0}]
        assert [{owner, _token, _nodes}] = home.(name), "topic #{name} must live in exactly one vnode's cluster"
        {name, owner}
      end)

    assert homes |> Map.values() |> Enum.uniq() |> length() == 2, "expected topics to shard across both vnodes"

    :ok = BrokerServer.stop(control)
  end

  test "the membership leader bootstraps the vnodes via the reconcile loop" do
    # membership where this node is the sole (thus lowest) live member → it is the bootstrap leader
    {:ok, membership} = AliveMembersStub.start_link([{Malachi.LogMembership, node()}])
    vnode = :"bs_ml_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(vnode) end)

    {:ok, control} =
      BrokerServer.start_link("unused",
        brokers: [start_replication()],
        metadata_vnodes: [{vnode, 0, [node()]}],
        bootstrap_orchestrator: Malachi.Application.membership_leader(membership)
      )

    # nothing is started at boot (build_replicated only routes); the reconcile loop on the leader
    # bootstraps the vnode's cluster, so a create_topic through the broker then commits
    assert {:ok, _root} = BrokerServer.create_topic(control, "events", 4)
    assert %{name: "events"} = Metadata.get_topic(BrokerServer.metadata(control), "events")
    assert MetadataServer.ready?({vnode, node()})

    :ok = BrokerServer.stop(control)
  end

  test "produces across a 3-broker replica set and reads the records back (replicated data plane)" do
    cluster = :"bs_meta_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)

    # The data-plane wiring D2 sets up: several ReplicationServers as the broker set + a replication
    # factor, with ra as the control plane. This is the shape Malachi.Application builds when clustered.
    brokers = for _ <- 1..3, do: start_replication()

    {:ok, control} =
      BrokerServer.start_link("unused", brokers: brokers, replication_factor: 3, metadata_cluster: cluster)

    {:ok, _root} = BrokerServer.create_topic(control, "events", 4)

    records = for index <- 0..4, do: Record.new("v#{index}", key: "k#{index}")
    {:ok, _placements} = BrokerServer.produce(control, "events", records)

    # a segment landed on a 3-broker replica set (placement across the whole broker set)
    [range_id] = BrokerServer.active_range_ids(control, "events")
    [segment] = Metadata.segments_of_range(BrokerServer.metadata(control), range_id)
    assert length(segment.replica_set) == 3

    # committed (quorum-durable) records read back through the broker's primary
    {read, _cursor, _skips} = BrokerServer.consume(control, "events", %{}, 100, 0)
    assert read |> Enum.map(& &1.value) |> Enum.sort() == Enum.map(records, & &1.value) |> Enum.sort()

    :ok = BrokerServer.stop(control)
  end

  test "a split fences the parent even when the splitting frontend never wrote to it" do
    # ISSUE #41. A frontend produces straight into its cached active segment, and the fence a split
    # performs used to be read out of that same cache (`Broker.active_roll/2`). So a split run on a node
    # that had never produced to the parent fenced NOTHING and reported success, while the node holding
    # the segment kept appending to the now-sealed parent.
    #
    # That inverts per-key order, because a child reads its ancestors FIRST (`history_sources/2`): a
    # record written to the parent AFTER the split is served BEFORE a record the child already holds.
    # And it is not the one-refresh-interval window the issue estimated. The split seals the RANGE and
    # not its segments, and nothing else ever closes an active segment on a sealed range: failover only
    # seals segments whose primary is DEAD, retention and healing only touch sealed ones, and
    # `drop_stale_active_segments/1` matches on the SEGMENT's state. So the parent stays writable, and
    # the inversion grows for as long as that frontend keeps producing.
    cluster = :"bs_fence41_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)

    # A REGISTERED replication server, so both frontends address the same primary by `{name, node()}`
    # and place segments on a ref that compares equal on both sides.
    name = :"fence41_repl_#{System.unique_integer([:positive])}"
    directory = Path.join(System.tmp_dir!(), "malachi_fence41_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    {:ok, repl} = ReplicationServer.start_link(directory: directory, name: name)
    on_exit(fn -> stop_quietly(repl) end)

    # One shared primary and one shared control plane, two frontends. The refresh interval is long
    # enough that nothing reconciles on its own, so every convergence below is driven explicitly and the
    # window the bug lives in is deterministic rather than timed.
    opts = [
      brokers: [{name, node()}],
      metadata_cluster: cluster,
      group_commit: false,
      brokers_refresh_interval: 60_000
    ]

    {:ok, writer} = BrokerServer.start_link("unused", opts)
    on_exit(fn -> stop_quietly(writer) end)
    {:ok, splitter} = BrokerServer.start_link("unused", opts)
    on_exit(fn -> stop_quietly(splitter) end)

    {:ok, root} = BrokerServer.create_topic(writer, "events", 4)

    # ONE key throughout, so per-key order is exactly what the child's history has to preserve.
    key = "k"
    assert {:ok, _} = BrokerServer.produce(writer, "events", [Record.new("early", key: key)])

    # The asymmetry that IS issue #41: the splitter can see the parent's write head in the CONTROL
    # PLANE, and holds nothing for it in its own cache, having never produced.
    reconcile!(splitter)
    [parent_segment] = Metadata.segments_of_range(BrokerServer.metadata(splitter), root)
    assert parent_segment.state == :active

    assert :sys.get_state(splitter).broker.segments == %{},
           "the splitting frontend must not hold the parent's segment: that is the whole scenario"

    assert {:ok, left, right} = BrokerServer.split_range(splitter, root)

    # A write to the CHILD, through the frontend that performed the split.
    assert {:ok, placements} = BrokerServer.produce(splitter, "events", [Record.new("mid", key: key)])
    [child] = Map.keys(placements)
    assert child in [left, right]

    # And the write that used to invert the order. The writer has not reconciled, so it still routes
    # this key at its cached PARENT segment.
    late = BrokerServer.produce(writer, "events", [Record.new("late", key: key)])

    assert {:error, {:sealed, 1}} = late,
           "the parent's primary must refuse the write and seat the frontend at the fenced edge"

    reconcile!(splitter)
    values = splitter |> drain_history(child) |> Enum.map(& &1.value)

    assert values == ["early", "mid"],
           "per-key order inverted: a record produced to the sealed parent after the split reads " <>
             "ahead of the child's own records (issue #41); got #{inspect(values)}"

    # The STORE is fenced, not merely this frontend's bookkeeping: a direct append is refused too, so a
    # node that never learns about the split still cannot get a record into the parent.
    assert {:error, {:sealed, 1}} =
             ReplicationServer.append(
               {name, node()},
               parent_segment.id,
               parent_segment.replica_set,
               parent_segment.start_offset,
               [Record.new("direct", key: key)]
             )

    # And the refusal converges rather than wedging: once the writer sees the split it routes to the
    # child, and the retried record lands AFTER "mid", which is the order the client wrote them in.
    reconcile!(writer)
    assert {:ok, _} = BrokerServer.produce(writer, "events", [Record.new("late", key: key)])
    reconcile!(splitter)

    assert splitter |> drain_history(child) |> Enum.map(& &1.value) == ["early", "mid", "late"]

    :ok = BrokerServer.stop(writer)
    :ok = BrokerServer.stop(splitter)
  end

  test "a fence whose control-plane seal never landed stops wedging the range (issue #121)" do
    # THE REPRODUCTION. `BrokerServer.fence_and_seal/2` closes the segment on the primary and only then
    # records the seal. On a generic error from that second step (an `ra` timeout is the ordinary way to
    # get one) `Broker.record_seal/5` returns the broker UNCHANGED: no new metadata, and no roll owed,
    # so nothing retries. The store is closed, the control plane still calls the segment active, and the
    # produce path loops forever between adopting that segment and being refused by its store.
    #
    # Nothing else rescued it. `Failover` requires the primary to be DEAD and here it is alive and
    # answering; healing and retention only touch sealed segments; `drop_stale_active_segments/1` keys
    # off the segment being sealed in the metadata, which is exactly what did not happen.
    cluster = :"bs_orphan121_#{System.unique_integer([:positive])}"
    on_exit(fn -> MetadataServer.delete(cluster) end)

    name = :"orphan121_repl_#{System.unique_integer([:positive])}"
    directory = Path.join(System.tmp_dir!(), "malachi_orphan121_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    {:ok, repl} = ReplicationServer.start_link(directory: directory, name: name)
    on_exit(fn -> stop_quietly(repl) end)
    primary = {name, node()}

    {:ok, broker} =
      BrokerServer.start_link("unused",
        brokers: [primary],
        metadata_cluster: cluster,
        group_commit: false,
        brokers_refresh_interval: 60_000
      )

    on_exit(fn -> stop_quietly(broker) end)

    {:ok, root} = BrokerServer.create_topic(broker, "events", 4)
    assert {:ok, _} = BrokerServer.produce(broker, "events", [Record.new("early", key: "k")])

    [segment] = Metadata.segments_of_range(BrokerServer.metadata(broker), root)

    # Put the data plane in exactly that state: fence the store, leave the metadata alone. Then drop
    # this frontend's cache, which is what a restart (or any other node) would face.
    assert {:ok, 1, sealed_bytes} = ReplicationServer.seal(primary, segment.id, segment.start_offset)
    reconcile!(broker)
    assert Metadata.get_segment(BrokerServer.metadata(broker), segment.id).state == :active

    # The loop, verbatim from the issue: every produce readopts the segment the metadata still calls
    # active, and every one of them is refused at the same offset. A successor is never opened.
    for _attempt <- 1..3 do
      assert {:error, {:sealed, 1}} = BrokerServer.produce(broker, "events", [Record.new("blocked", key: "k")])
    end

    assert [{_id, :active}] =
             BrokerServer.metadata(broker)
             |> Metadata.segments_of_range(root)
             |> Enum.map(&{&1.id, &1.state})

    # One reconciling pass, wired exactly as `Malachi.Application` wires it.
    coordinator =
      start_supervised!(
        {HealCoordinator,
         live_brokers: fn -> [primary] end,
         metadata_source: fn -> BrokerServer.metadata(broker) end,
         apply_command: fn command -> BrokerServer.apply_heal(broker, [command]) end,
         replication_factor: 1,
         interval: 60_000},
        id: {:heal121, System.unique_integer([:positive])}
      )

    assert [{:seal_segment, segment_id, 1, ^sealed_bytes, _at}] = HealCoordinator.heal_now(coordinator).applied
    assert segment_id == segment.id

    # Convergence, which is what the issue asks for in place of the three refusals: the parent is
    # sealed at the end its own store reported, and the next produce opens a SUCCESSOR there.
    parent = Metadata.get_segment(BrokerServer.metadata(broker), segment.id)
    assert parent.state == :sealed
    assert parent.length == 1

    # And in the RAFT LOG, not only in this frontend's cache. That distinction is the whole reason the
    # reconciliation is a level-triggered pass rather than a retry at the source: a retry converges only
    # while the process that owed the seal is alive, and what makes the pass hold across a restart is
    # that its seal is replicated. A consistent query reads it from the leader rather than from a copy.
    {:ok, replicated} = MetadataServer.query({cluster, node()}, &Function.identity/1)
    assert %{state: :sealed, length: 1} = Metadata.get_segment(replicated, segment.id)

    assert {:ok, placements} = BrokerServer.produce(broker, "events", [Record.new("after", key: "k")])
    assert %{^root => {1, 1}} = placements

    successor = Metadata.segments_of_range(BrokerServer.metadata(broker), root) |> Enum.find(&(&1.state == :active))
    assert successor.id != segment.id
    assert successor.start_offset == 1

    # And the range reads as one contiguous history across the fence, which is the point of sealing at
    # the store's own answer rather than at a number measured beside it.
    assert broker |> drain_history(root) |> Enum.map(& &1.value) == ["early", "after"]
  end

  # Drives one metadata reconcile and waits for it to land. It used to be `send(server, :reconcile)`
  # plus a `:sys.get_state/1` queued behind it in the same mailbox; that stopped proving anything once
  # the periodic reconcile moved off the loop (#178), because the tick only STARTS a task. The public
  # call runs the same pass on the loop and answers when its result has been applied.
  defp reconcile!(server), do: BrokerServer.reconcile_now(server)

  defp drain_history(server, range_id, cursor \\ :start, accumulated \\ []) do
    case BrokerServer.stream_history(server, range_id, cursor, 3) do
      {:ok, records, :done} -> [records | accumulated] |> Enum.reverse() |> List.flatten()
      {:ok, records, next} -> drain_history(server, range_id, next, [records | accumulated])
    end
  end
end
