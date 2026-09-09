defmodule Malachi.Cluster.HealCoordinatorTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.HealCoordinator
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record
  alias Malachi.Metadata

  defp start_broker do
    directory = Path.join(System.tmp_dir!(), "malachi_healco_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    start_supervised!({ReplicationServer, directory: directory}, id: {:repl, System.unique_integer([:positive])})
  end

  defp records(values), do: for(value <- values, do: Record.new(value, key: value))

  defp read_values(ref, segment_id) do
    case ReplicationServer.read(ref, segment_id, 0, 100) do
      {:ok, records} -> Enum.map(records, & &1.value)
      :eof -> []
    end
  end

  # A metadata held in an Agent, with the source/apply seams the coordinator needs.
  defp metadata_store(metadata) do
    {:ok, agent} = start_supervised({Agent, fn -> metadata end}, id: {:meta, System.unique_integer([:positive])})
    source = fn -> Agent.get(agent, & &1) end
    apply = fn command -> Agent.update(agent, fn meta -> elem(Metadata.apply(meta, command), 0) end) end
    {source, apply}
  end

  defp start_coordinator(opts) do
    defaults = [interval: 60_000, replication_factor: 3]
    start_supervised!({HealCoordinator, Keyword.merge(defaults, opts)}, id: {:co, System.unique_integer([:positive])})
  end

  # metadata with one sealed segment over `replica_set`, with `source` seeded with its data
  defp sealed_segment(replica_set, source, values) do
    {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 4})
    segment_id = {root, 0}
    {metadata, :ok} = Metadata.apply(metadata, {:register_segment, root, segment_id, replica_set, 0})
    {metadata, :ok} = Metadata.apply(metadata, {:seal_segment, segment_id, length(values), 0, 0})
    {:ok, _last} = ReplicationServer.replicate(source, segment_id, [source], 0, records(values))
    {metadata, segment_id}
  end

  test "heals an under-replicated sealed segment against the live broker set and applies it" do
    a = start_broker()
    d = start_broker()
    {metadata, segment_id} = sealed_segment([a, :b, :c], a, ["x", "y", "z"])
    {source, apply} = metadata_store(metadata)

    # :c has left; the live set is a, :b, d
    coordinator = start_coordinator(live_brokers: fn -> [a, :b, d] end, metadata_source: source, apply_command: apply)

    result = HealCoordinator.heal_now(coordinator)

    assert [{:set_segment_replicas, ^segment_id, new_set}] = result.applied
    assert Enum.sort(new_set) == Enum.sort([a, :b, d])

    # the new replica d was backfilled, and the applied command landed in the metadata
    assert read_values(d, segment_id) == ["x", "y", "z"]
    assert Metadata.get_segment(source.(), segment_id).replica_set == new_set

    # the loop is closed: a second pass has nothing to do
    assert HealCoordinator.heal_now(coordinator) == %{applied: [], failed: [], repaired: []}
  end

  test "reports a segment with no live source as failed and applies nothing" do
    a = start_broker()
    d = start_broker()
    {metadata, segment_id} = sealed_segment([a, :b, :c], a, ["x"])
    {source, apply} = metadata_store(metadata)

    # only d is live, and it has no copy
    coordinator = start_coordinator(live_brokers: fn -> [d] end, metadata_source: source, apply_command: apply)

    result = HealCoordinator.heal_now(coordinator)
    assert result.applied == []
    assert result.failed == [{segment_id, :no_live_source}]
    assert read_values(d, segment_id) == []
  end

  test "heals automatically on its interval" do
    a = start_broker()
    d = start_broker()
    {metadata, segment_id} = sealed_segment([a, :b, :c], a, ["x", "y"])
    {source, apply} = metadata_store(metadata)

    start_coordinator(
      live_brokers: fn -> [a, :b, d] end,
      metadata_source: source,
      apply_command: apply,
      interval: 15
    )

    assert eventually(fn -> read_values(d, segment_id) == ["x", "y"] end)
  end

  # An ACTIVE segment over `replica_set`, with `holders` seeded with `values` through the directed
  # follow path (so a replica holds data without being the set's primary).
  defp active_segment(replica_set, holders, values) do
    {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 4})
    segment_id = {root, 0}
    {metadata, :ok} = Metadata.apply(metadata, {:register_segment, root, segment_id, replica_set, 0})
    for holder <- holders, do: {:ok, _last} = ReplicationServer.follow(holder, segment_id, 0, records(values))
    {metadata, segment_id}
  end

  test "the default probe FENCES every replica that answers, then seals at the highest end" do
    # The probe and the fence are one act. A replica that reports what it holds and stays writable has
    # described a moving target, and sealing at that number is a promise the pass cannot keep.
    b = start_broker()
    c = start_broker()
    {metadata, segment_id} = active_segment([:dead_primary, b, c], [b, c], ["x", "y", "z"])
    {source, apply} = metadata_store(metadata)

    coordinator =
      start_coordinator(
        live_brokers: fn -> [b, c] end,
        metadata_source: source,
        apply_command: apply,
        probe_timeout: 500
      )

    HealCoordinator.heal_now(coordinator)

    sealed = Metadata.get_segment(source.(), segment_id)
    assert sealed.state == :sealed
    assert sealed.length == 3

    # Both answering replicas are fenced, so nothing can grow the segment past the length just recorded.
    for replica <- [b, c] do
      assert {:error, {:sealed, 3}} = ReplicationServer.replicate(replica, segment_id, [replica], 0, records(["late"]))
    end
  end

  test "below a majority nothing is sealed and nothing is fenced, so the pass leaves no debt" do
    # A fence has no inverse. Closing the one replica that answered would keep it refusing writes
    # after the primary returns, and at rf=2 that is terminal: one follower is never a majority, so no
    # pass seals, and once the primary is alive again the segment stops being a candidate, so no later
    # pass ever finishes. Every produce would then fail quorum against a follower nothing can reopen.
    # Measuring is free of that, so the pass measures first and only fences what it is about to seal.
    b = start_broker()
    {metadata, segment_id} = active_segment([:dead_primary, b, :gone_c, :gone_d, :gone_e], [b], ["x", "y"])
    {source, apply} = metadata_store(metadata)

    coordinator =
      start_coordinator(
        live_brokers: fn -> [b] end,
        metadata_source: source,
        apply_command: apply,
        probe_timeout: 500
      )

    HealCoordinator.heal_now(coordinator)

    assert Metadata.get_segment(source.(), segment_id).state == :active
    # Still writable: this is the assertion the previous behavior got backwards.
    assert {:ok, 2} = ReplicationServer.replicate(b, segment_id, [b], 0, records(["late"]))
  end

  test "a pass below a majority does not call the fence at all" do
    # The seam, watched directly, because the test above can only observe the absence of an effect and
    # would still pass if the fence were called and happened to fail.
    {:ok, fenced} = Agent.start_link(fn -> [] end)
    b = start_broker()
    {metadata, segment_id} = active_segment([:dead_primary, b, :gone_c, :gone_d, :gone_e], [b], ["x", "y"])
    {source, apply} = metadata_store(metadata)

    coordinator =
      start_coordinator(
        live_brokers: fn -> [b] end,
        metadata_source: source,
        apply_command: apply,
        fence: fn replica, _segment, _base ->
          Agent.update(fenced, &[replica | &1])
          {2, 0}
        end,
        probe_timeout: 500
      )

    HealCoordinator.heal_now(coordinator)

    assert Agent.get(fenced, & &1) == []
    assert Metadata.get_segment(source.(), segment_id).state == :active
  end

  test "a primary that comes back after the pass cannot get a batch acknowledged" do
    # The race the fence closes, and the sentence in `Malachi.Cluster.Failover`'s moduledoc that used to
    # be false. The old primary's own log is not fenced (the probe never reached it), so it appends
    # locally as it always would; what it can no longer do is close a quorum, because every follower it
    # reaches refuses the push and acks an error.
    old_primary = start_broker()
    b = start_broker()
    c = start_broker()
    replica_set = [old_primary, b, c]

    {metadata, segment_id} = active_segment(replica_set, [old_primary, b, c], ["x", "y"])
    {source, apply} = metadata_store(metadata)

    coordinator =
      start_coordinator(
        live_brokers: fn -> [b, c] end,
        metadata_source: source,
        apply_command: apply,
        probe_timeout: 500
      )

    HealCoordinator.heal_now(coordinator)
    assert Metadata.get_segment(source.(), segment_id).length == 2

    assert {:error, :no_quorum} =
             ReplicationServer.replicate(old_primary, segment_id, replica_set, 0, records(["ghost"]))

    # And nothing above the sealed edge became readable on the replicas that hold the truth.
    for replica <- [b, c], do: assert(read_values(replica, segment_id) == ["x", "y"])
  end

  describe "the orphaned-fence pass (issue #121)" do
    test "seals a segment whose store is fenced while the metadata still calls it active" do
      # The state a failed `record_seal/5` leaves behind after a successful fence. Nothing else
      # converges it: the primary is ALIVE, so failover does not look at the segment, and healing and
      # retention only touch sealed ones. Until this pass runs, every produce to the range is refused
      # with `{:error, {:sealed, N}}` forever.
      primary = start_broker()
      {metadata, segment_id} = active_segment([primary, :b, :c], [primary], ["x", "y", "z"])
      {source, apply} = metadata_store(metadata)

      # Fence the store and leave the metadata untouched, which is exactly what a split does when its
      # control-plane write times out.
      assert {:ok, 3, bytes} = ReplicationServer.seal(primary, segment_id, 0)
      assert Metadata.get_segment(source.(), segment_id).state == :active

      coordinator =
        start_coordinator(
          live_brokers: fn -> [primary, :b, :c] end,
          metadata_source: source,
          apply_command: apply,
          probe_timeout: 500
        )

      result = HealCoordinator.heal_now(coordinator)

      # The seal is recorded at the length the STORE reported, not at anything measured beside it.
      assert result.applied == [{:seal_segment, segment_id, 3, bytes, result.applied |> hd() |> elem(4)}]
      sealed = Metadata.get_segment(source.(), segment_id)
      assert sealed.state == :sealed
      assert sealed.length == 3
      assert sealed.byte_size == bytes

      # Level-triggered: a second pass has nothing left to do.
      assert HealCoordinator.heal_now(coordinator).applied == []
    end

    test "leaves a merely active segment alone, and above all does not fence it" do
      # The pass visits EVERY active segment, so a probe that fenced would not wedge one range but the
      # whole workload. The store is asserted still writable AFTER the pass, which is the effect that
      # matters, and the next test watches the seam itself.
      primary = start_broker()
      {metadata, segment_id} = active_segment([primary, :b, :c], [primary], ["x"])
      {source, apply} = metadata_store(metadata)

      coordinator =
        start_coordinator(
          live_brokers: fn -> [primary, :b, :c] end,
          metadata_source: source,
          apply_command: apply,
          probe_timeout: 500
        )

      assert HealCoordinator.heal_now(coordinator).applied == []

      assert Metadata.get_segment(source.(), segment_id).state == :active
      assert {:ok, 1} = ReplicationServer.replicate(primary, segment_id, [primary], 0, records(["late"]))
    end

    test "a seal this pass could not land is reported as pending, not counted as reconciled" do
      # The counters are an alerting pair, so `fence_reconciled` climbing above `orphaned_fence` while
      # a range still refuses every write would invert the one signal they exist to give. The seal is
      # applied into a control plane that drops it, which is the same `ra` timeout that created the
      # divergence in the first place, arriving a second time.
      primary = start_broker()
      {metadata, segment_id} = active_segment([primary, :b], [primary], ["x"])
      {source, _apply} = metadata_store(metadata)
      {:ok, 1, _bytes} = ReplicationServer.seal(primary, segment_id, 0)

      handler = "orphan-pending-#{System.unique_integer([:positive])}"
      test_pid = self()

      :telemetry.attach(
        handler,
        [:malachi, :cluster, :fence_reconciled],
        fn _e, m, _md, _c -> send(test_pid, m) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      coordinator =
        start_coordinator(
          live_brokers: fn -> [primary, :b] end,
          metadata_source: source,
          apply_command: fn _command -> :dropped end,
          probe_timeout: 500
        )

      log = ExUnit.CaptureLog.capture_log(fn -> HealCoordinator.heal_now(coordinator) end)

      assert log =~ "could not record the seal"
      assert log =~ inspect(segment_id)
      refute log =~ "reconciled 1 segment"
      refute_received %{count: _any}

      # Level-triggered: the pass finds it again rather than treating it as done.
      assert [{:seal_segment, ^segment_id, 1, _bytes, _at}] = HealCoordinator.heal_now(coordinator).applied
    end

    test "the pass never calls the fence seam, whatever it finds" do
      # The seam watched directly, because the assertions above can only observe the absence of an
      # effect and would still pass if the fence were called and happened to fail. Both states are put
      # in front of the same pass: one segment already fenced, one merely active.
      {:ok, fenced} = Agent.start_link(fn -> [] end)
      primary = start_broker()
      {metadata, segment_id} = active_segment([primary, :b, :c], [primary], ["x", "y"])
      {source, apply} = metadata_store(metadata)
      {:ok, 2, _bytes} = ReplicationServer.seal(primary, segment_id, 0)

      coordinator =
        start_coordinator(
          live_brokers: fn -> [primary, :b, :c] end,
          metadata_source: source,
          apply_command: apply,
          fence: fn replica, _segment, _base ->
            Agent.update(fenced, &[replica | &1])
            {2, 0}
          end,
          probe_timeout: 500
        )

      HealCoordinator.heal_now(coordinator)

      assert Agent.get(fenced, & &1) == []
      assert Metadata.get_segment(source.(), segment_id).state == :sealed
    end

    test "a primary that cannot be reached costs the pass nothing and does not stop the others" do
      # The default seam is asked about every active segment on every pass, so it meets refs that are
      # not processes at all as a matter of course. Answering nothing (rather than exiting) is what
      # keeps one absent primary from taking the reconciliation of every other segment down with it.
      primary = start_broker()
      {metadata, reachable} = active_segment([primary, :b], [primary], ["x", "y"])
      {:ok, 2, _bytes} = ReplicationServer.seal(primary, reachable, 0)

      {metadata, {:ok, orders}} = Metadata.apply(metadata, {:create_topic, "orders", 4})
      absent = {orders, 0}
      {metadata, :ok} = Metadata.apply(metadata, {:register_segment, orders, absent, [:ghost], 0})
      {source, apply} = metadata_store(metadata)

      coordinator =
        start_coordinator(
          live_brokers: fn -> [primary, :b, :ghost] end,
          metadata_source: source,
          apply_command: apply,
          probe_timeout: 100
        )

      assert [{:seal_segment, ^reachable, 2, _bytes, _at}] = HealCoordinator.heal_now(coordinator).applied

      # No answer, no seal: the segment nothing could be learned about is left exactly as it was.
      assert Metadata.get_segment(source.(), absent).state == :active
    end

    test "does not touch a segment whose primary is dead: that is failover's case, with its own rule" do
      # The two policies are disjoint by construction. Sealing a dead primary's segment needs the
      # majority rule `Malachi.Cluster.Failover` applies, and this pass has none, so it must never be
      # the one to reach such a segment.
      b = start_broker()
      c = start_broker()
      {metadata, segment_id} = active_segment([:dead_primary, b, c], [b, c], ["x", "y"])
      {source, apply} = metadata_store(metadata)

      coordinator =
        start_coordinator(
          live_brokers: fn -> [b, c] end,
          metadata_source: source,
          apply_command: apply,
          seal_state: fn replica, _segments -> flunk("orphaned-fence pass probed #{inspect(replica)}") end,
          probe_timeout: 500
        )

      # Failover still seals it, on its own majority rule.
      HealCoordinator.heal_now(coordinator)
      assert Metadata.get_segment(source.(), segment_id).state == :sealed
    end
  end

  test "a non-leader ticks but skips healing (only the leader acts)" do
    a = start_broker()
    d = start_broker()
    {metadata, segment_id} = sealed_segment([a, :b, :c], a, ["x", "y"])
    {source, apply} = metadata_store(metadata)

    start_coordinator(
      live_brokers: fn -> [a, :b, d] end,
      metadata_source: source,
      apply_command: apply,
      interval: 15,
      leader?: fn -> false end
    )

    # not the membership leader → no healing happens; d never receives the segment
    refute eventually(fn -> read_values(d, segment_id) == ["x", "y"] end, 300)
  end

  defp eventually(check, remaining_ms \\ 2_000) do
    cond do
      check.() -> true
      remaining_ms <= 0 -> false
      true -> Process.sleep(15) && eventually(check, remaining_ms - 15)
    end
  end
end
