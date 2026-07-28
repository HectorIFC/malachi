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
    assert HealCoordinator.heal_now(coordinator) == %{applied: [], failed: []}

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

  test "failover promotes a live replica when an active segment's primary dies" do
    brokers = [r1, r2, r3, r4] = for _ <- 1..4, do: start_replication()
    {:ok, live_agent} = start_supervised({Agent, fn -> brokers end}, id: :live_fo)

    {:ok, control} =
      BrokerServer.start_link("unused",
        brokers: brokers,
        replication_factor: 3,
        # large, so the segment stays active across both produces
        segment_max_bytes: 1_000_000,
        live_brokers: fn -> Agent.get(live_agent, & &1) end,
        brokers_refresh_interval: 60_000
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

    # the segment's primary dies
    Agent.update(live_agent, fn live -> live -- [primary] end)
    :ok = GenServer.stop(primary)

    result = HealCoordinator.heal_now(coordinator)
    assert [{:set_segment_replicas, _id, _set}] = result.applied

    # the active segment now has a live primary (different from the dead one)
    [healed] = Metadata.segments_of_range(BrokerServer.metadata(control), root)
    [new_primary | _] = healed.replica_set
    refute new_primary == primary
    assert new_primary in ([r1, r2, r3, r4] -- [primary])

    # writes continue to the new primary, and both records read back
    {:ok, _placements} = BrokerServer.produce(control, "events", [Record.new("b", key: "b")])

    assert eventually(fn ->
             case BrokerServer.read(control, root, 0, 100) do
               {:ok, records} -> Enum.map(records, & &1.value) == ["a", "b"]
               _ -> false
             end
           end)
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

  defp eventually(check, remaining_ms \\ 3_000) do
    cond do
      check.() -> true
      remaining_ms <= 0 -> false
      true -> Process.sleep(20) && eventually(check, remaining_ms - 20)
    end
  end
end
