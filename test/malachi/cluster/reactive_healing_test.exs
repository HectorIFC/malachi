defmodule Malachi.Cluster.ReactiveHealingTest do
  # End-to-end wiring (slice 1a): a control node (BrokerServer) over N data brokers
  # (ReplicationServers), with a HealCoordinator re-replicating sealed segments against a live set,
  # and the membership -> broker-ref bridge that feeds it.
  use ExUnit.Case, async: false

  alias Malachi.BrokerServer
  alias Malachi.Cluster.HealCoordinator
  alias Malachi.Cluster.MembershipServer
  alias Malachi.Cluster.Placement
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record
  alias Malachi.Metadata

  # :temporary so GenServer.stop/1 permanently removes a broker (simulating a node death).
  defp start_replication do
    n = System.unique_integer([:positive])
    directory = Path.join(System.tmp_dir!(), "malachi_rh_#{n}")
    on_exit(fn -> File.rm_rf!(directory) end)
    spec = %{id: {:repl, n}, start: {ReplicationServer, :start_link, [[directory: directory]]}, restart: :temporary}
    start_supervised!(spec)
  end

  defp one_record_bytes, do: Record.encoded_size(Record.new("value", key: "key"))

  defp segment_values(replica, segment_id, segment) do
    case ReplicationServer.read(replica, segment_id, segment.start_offset, 100) do
      {:ok, records} -> Enum.map(records, & &1.value)
      _ -> []
    end
  end

  test "a dead data broker's sealed segments are re-replicated to the live set" do
    brokers = for _ <- 1..4, do: start_replication()

    {:ok, control} =
      BrokerServer.start_link("unused", brokers: brokers, replication_factor: 3, segment_max_bytes: one_record_bytes())

    {:ok, root} = BrokerServer.create_topic(control, "events", 4)

    # each single-record produce rolls a new sealed segment, placed on 3 of the 4 brokers
    for index <- 0..7 do
      {:ok, _placements} = BrokerServer.produce(control, "events", [Record.new("v#{index}", key: "k#{index}")])
    end

    {:ok, live_agent} = start_supervised({Agent, fn -> brokers end}, id: :live)

    {:ok, coordinator} =
      start_supervised(
        {HealCoordinator,
         live_brokers: fn -> Agent.get(live_agent, & &1) end,
         metadata_source: fn -> BrokerServer.metadata(control) end,
         apply_command: fn command -> BrokerServer.apply_heal(control, [command]) end,
         replication_factor: 3,
         interval: 60_000}
      )

    # all four alive: nothing to heal
    assert HealCoordinator.heal_now(coordinator) == %{applied: [], failed: [], repaired: []}

    # kill a broker that actually hosts segments (HRW excludes one broker per segment, so a fixed
    # broker is not guaranteed to host any) and drop it from the live set
    victim = most_used_broker(BrokerServer.metadata(control), root)
    live = brokers -- [victim]
    Agent.update(live_agent, fn _ -> live end)
    :ok = GenServer.stop(victim)

    result = HealCoordinator.heal_now(coordinator)
    metadata = BrokerServer.metadata(control)

    # everything the victim hosted has been re-replicated; nothing is under-replicated anymore
    assert result.failed == []
    refute result.applied == []
    assert Placement.under_replicated(metadata, live, 3) == []

    # each healed segment's replicas all hold its data (backfill succeeded) and exclude the dead one
    for {:set_segment_replicas, segment_id, new_set} <- result.applied do
      segment = Metadata.get_segment(metadata, segment_id)
      refute victim in new_set
      expected = segment_values(hd(new_set), segment_id, segment)
      assert expected != []
      for replica <- new_set, do: assert(segment_values(replica, segment_id, segment) == expected)
    end
  end

  defp most_used_broker(metadata, range_id) do
    metadata
    |> Metadata.segments_of_range(range_id)
    |> Enum.flat_map(& &1.replica_set)
    |> Enum.frequencies()
    |> Enum.max_by(fn {_broker, count} -> count end)
    |> elem(0)
  end

  test "new segments are placed only on live brokers once the set refreshes" do
    brokers = [r1, r2, r3, r4] = for _ <- 1..4, do: start_replication()

    {:ok, live_agent} = start_supervised({Agent, fn -> brokers end}, id: :live2)

    {:ok, control} =
      BrokerServer.start_link("unused",
        brokers: brokers,
        replication_factor: 3,
        segment_max_bytes: one_record_bytes(),
        live_brokers: fn -> Agent.get(live_agent, & &1) end,
        brokers_refresh_interval: 15
      )

    {:ok, root} = BrokerServer.create_topic(control, "events", 4)

    # r3 dies and is dropped from the live set
    Agent.update(live_agent, fn _ -> [r1, r2, r4] end)
    :ok = GenServer.stop(r3)

    # after the placement set refreshes, a freshly produced segment lands only on live brokers
    assert eventually(fn ->
             case BrokerServer.produce(control, "events", [Record.new("z", key: "z")]) do
               {:ok, _placements} ->
                 segments = Metadata.segments_of_range(BrokerServer.metadata(control), root)
                 latest = Enum.max_by(segments, & &1.start_offset)
                 r3 not in latest.replica_set

               {:error, _reason} ->
                 false
             end
           end)
  end

  test "failover seals the active segment when its primary dies, and writing rolls to a new one" do
    brokers = [_r1, _r2, _r3, _r4] = for _ <- 1..4, do: start_replication()
    {:ok, live_agent} = start_supervised({Agent, fn -> brokers end}, id: :live_fo)

    {:ok, control} =
      BrokerServer.start_link("unused",
        brokers: brokers,
        replication_factor: 3,
        # large, so the segment stays active across both produces
        segment_max_bytes: 1_000_000,
        live_brokers: fn -> Agent.get(live_agent, & &1) end,
        # Short, so the segment opened after the seal is placed on the survivors rather than on a
        # cached set that still lists the dead broker.
        brokers_refresh_interval: 15
      )

    {:ok, root} = BrokerServer.create_topic(control, "events", 4)
    {:ok, _placements} = BrokerServer.produce(control, "events", [Record.new("a", key: "a")])

    [segment] = Metadata.segments_of_range(BrokerServer.metadata(control), root)
    [primary | _] = segment.replica_set

    {:ok, coordinator} =
      start_supervised(
        {HealCoordinator,
         live_brokers: fn -> Agent.get(live_agent, & &1) end,
         metadata_source: fn -> BrokerServer.metadata(control) end,
         apply_command: fn command -> BrokerServer.apply_heal(control, [command]) end,
         replication_factor: 3,
         interval: 60_000}
      )

    # The primary leaves the live set. It is dropped rather than killed on purpose: failover keys off
    # membership, not process liveness, so this exercises the decision under test without dragging in a
    # dead pid, whose silently discarded casts turn every unlucky placement into a five second produce
    # timeout and made this test flaky. A primary whose process is truly gone is covered by the
    # re-replication test above and by the chaos drill.
    Agent.update(live_agent, fn live -> live -- [primary] end)

    result = HealCoordinator.heal_now(coordinator)

    # Sealed rather than promoted: a promoted replica could be behind the offsets the dead primary
    # already acknowledged, and would then reissue them. A sealed segment can never hand an offset out
    # twice, and the store refuses appends to it even if the old primary comes back.
    assert [{:seal_segment, sealed_id, 1, _bytes, _at}, {:set_segment_replicas, _same, [live_head | _]}] =
             result.applied

    # The sealed segment is read from a live replica right away: reads route at the head, and the head
    # was the broker that just died.
    refute live_head == primary
    assert sealed_id == segment.id

    [sealed] = Metadata.segments_of_range(BrokerServer.metadata(control), root)
    assert sealed.state == :sealed
    assert sealed.length == 1

    # A second pass repairs the segment just sealed: `heal_sealed` only acts on sealed segments, so the
    # pass that seals cannot also re-replicate it. Recovery therefore takes two ticks (seal, then
    # repair), which the periodic loop supplies on its own.
    repair = HealCoordinator.heal_now(coordinator)
    assert Enum.any?(repair.applied, &match?({:set_segment_replicas, ^sealed_id, _set}, &1))

    [repaired] = Enum.filter(Metadata.segments_of_range(BrokerServer.metadata(control), root), &(&1.id == sealed_id))
    [sealed_primary | _] = repaired.replica_set
    refute sealed_primary == primary, "the sealed segment must be readable from a live primary"

    # writes continue on a NEW segment of the same range, and both records read back. Retried only for
    # the placement set to refresh off the live seam, which is milliseconds.
    assert eventually(fn -> match?({:ok, _}, BrokerServer.produce(control, "events", [Record.new("b", key: "b")])) end)

    segments = Metadata.segments_of_range(BrokerServer.metadata(control), root)
    assert length(segments) == 2, "the produce must open a fresh segment, not reopen the sealed one"
    assert Enum.any?(segments, &(&1.state == :active and &1.id != sealed_id))
    # the new segment starts where the sealed one ended: no offset is ever assigned twice
    [rolled] = Enum.filter(segments, &(&1.state == :active))
    assert rolled.start_offset == 1

    # Nothing was lost across the roll. Asserted through consume, which chains a range's segments (and
    # is what the chaos drill's verify uses); `read/4` answers from one segment at a time, so it stops
    # at the sealed segment's end by design rather than because a record went missing.
    assert eventually(fn ->
             case BrokerServer.consume(control, "events", %{}, 100, 0) do
               {records, _next} when is_list(records) -> Enum.map(records, & &1.value) == ["a", "b"]
               _ -> false
             end
           end)

    # And each record sits in its own segment, at an offset assigned exactly once.
    assert {:ok, [%{value: "a", offset: 0}]} = BrokerServer.read(control, root, 0, 100)
    assert {:ok, [%{value: "b", offset: 1}]} = BrokerServer.read(control, root, 1, 100)
  end

  test "live_brokers bridges membership member ids to broker references" do
    suffix = System.unique_integer([:positive])
    a = :"rh_ms_a_#{suffix}"
    b = :"rh_ms_b_#{suffix}"
    broker_a = start_replication()
    broker_b = start_replication()
    bridge = %{a => broker_a, b => broker_b}

    timings = [protocol_period: 15, ack_timeout: 15, suspicion_timeout: 90]
    start_supervised!({MembershipServer, [name: a, peers: [b]] ++ timings}, id: a)
    start_supervised!({MembershipServer, [name: b, peers: [a]] ++ timings}, id: b)

    live_brokers = fn -> for member <- MembershipServer.alive_members(a), ref = bridge[member], do: ref end

    assert eventually(fn -> Enum.sort(live_brokers.()) == Enum.sort([broker_a, broker_b]) end)

    :ok = stop_supervised!(b)
    assert eventually(fn -> live_brokers.() == [broker_a] end)
  end

  # The reproduction of issue #75: an acknowledged write is lost when failover hands the segment to a
  # replica that never received it. A batch commits once a MAJORITY has it durably, so with rf=3 one
  # replica can legitimately be behind; promoting that one let it append at offsets the dead primary
  # had already acknowledged, silently replacing them. Sealing instead keeps every acknowledged offset
  # assigned exactly once, which is what this asserts.
  test "an acknowledged write survives failover even when a replica missed it" do
    # Registered names, so a replica keeps its identity across a restart (a pid would not) and the
    # replica set still points at it when it comes back holding less than the others.
    names = for index <- 1..3, do: :"rh_behind_#{index}_#{System.unique_integer([:positive])}"
    directories = Map.new(names, &{&1, Path.join(System.tmp_dir!(), "malachi_behind_#{&1}")})
    on_exit(fn -> Enum.each(Map.values(directories), &File.rm_rf!/1) end)

    start_named = fn name ->
      spec = %{
        id: {:behind, name},
        start: {ReplicationServer, :start_link, [[directory: directories[name], name: name]]},
        restart: :temporary
      }

      start_supervised!(spec)
      {name, node()}
    end

    refs = Map.new(names, &{&1, start_named.(&1)})
    brokers = Map.values(refs)
    {:ok, live_agent} = start_supervised({Agent, fn -> brokers end}, id: :live_behind)

    {:ok, control} =
      BrokerServer.start_link("unused",
        brokers: brokers,
        replication_factor: 3,
        segment_max_bytes: 1_000_000,
        live_brokers: fn -> Agent.get(live_agent, & &1) end,
        # Short, so the segment opened after the failover is placed on the surviving brokers rather
        # than on a cached set that still lists the dead one.
        brokers_refresh_interval: 15
      )

    {:ok, root} = BrokerServer.create_topic(control, "events", 4)
    {:ok, _} = BrokerServer.produce(control, "events", [Record.new("first", key: "k0")])

    [segment] = Metadata.segments_of_range(BrokerServer.metadata(control), root)
    [primary, second | _] = segment.replica_set
    behind_name = Enum.find(names, &(refs[&1] == second))

    # The replica that failover used to promote (first live in replica-set order) goes down, so it
    # misses the next batch entirely. The batch still commits: primary plus the third replica is a
    # majority of three.
    :ok = GenServer.stop(second)
    {:ok, _} = BrokerServer.produce(control, "events", [Record.new("acked", key: "k1")])

    # It comes back with the same identity and a log that ends one record short, and then the primary
    # leaves the live set. Dropped rather than killed for the same reason as the test above: what
    # failover reads is membership, and a dead pid only adds five second produce timeouts to a test
    # that is about which offsets end up where.
    start_named.(behind_name)
    Agent.update(live_agent, fn live -> live -- [primary] end)

    {:ok, coordinator} =
      start_supervised(
        {HealCoordinator,
         live_brokers: fn -> Agent.get(live_agent, & &1) end,
         metadata_source: fn -> BrokerServer.metadata(control) end,
         apply_command: fn command -> BrokerServer.apply_heal(control, [command]) end,
         replication_factor: 3,
         interval: 60_000}
      )

    # The seal is placed at the furthest replica's end (2 records), not the behind one's (1). Promoting
    # the behind replica would have reopened offset 1, where "acked" already lives.
    # Deliberately not asserting the command shape here: what this test is about is the acknowledged
    # record, so the invariant below carries the proof. Under the old mechanism (promote the first live
    # replica) this pass still "succeeds" and the data is what goes missing.
    HealCoordinator.heal_now(coordinator)

    # Second pass: `heal_sealed` only acts on already-sealed segments, so the pass that seals cannot
    # also move the sealed segment onto a live primary. The periodic loop supplies that tick on its own.
    HealCoordinator.heal_now(coordinator)

    # Until the placement set refreshes, the segment opened after the seal can still list the dead
    # broker and the produce times out on it. Retried a bounded number of times rather than with
    # `eventually`, whose millisecond budget does not account for a check that itself blocks for the
    # replication timeout. A produce that timed out wrote nothing, so a retry cannot duplicate a
    # record, and the offsets asserted below would catch it if it did.
    assert {:ok, _} =
             Enum.reduce_while(1..6, nil, fn _attempt, last ->
               case BrokerServer.produce(control, "events", [Record.new("after", key: "k2")]) do
                 {:ok, _} = ok ->
                   {:halt, ok}

                 error ->
                   Process.sleep(150)
                   {:cont, error || last}
               end
             end)

    assert eventually(fn ->
             case BrokerServer.consume(control, "events", %{}, 100, 0) do
               {records, _next} when is_list(records) -> Enum.map(records, & &1.value) == ["first", "acked", "after"]
               _ -> false
             end
           end)

    # The acknowledged record kept its offset, and the post-failover write did not reuse it.
    assert {:ok, [%{value: "acked", offset: 1}]} = BrokerServer.read(control, root, 1, 1)
    assert {:ok, [%{value: "after", offset: 2}]} = BrokerServer.read(control, root, 2, 1)
  end

  defp eventually(check, remaining_ms \\ 3_000) do
    cond do
      check.() -> true
      remaining_ms <= 0 -> false
      true -> Process.sleep(20) && eventually(check, remaining_ms - 20)
    end
  end
end
