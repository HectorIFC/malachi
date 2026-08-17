defmodule Malachi.BrokerServerTest do
  use ExUnit.Case, async: true

  import Malachi.Test.PollingHelper

  alias Malachi.Broker
  alias Malachi.BrokerServer
  alias Malachi.Cluster.DSRSM
  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.ReplicatedDSRSM
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.RingTopology
  alias Malachi.Log.Record
  alias Malachi.Metadata

  @moduletag :tmp_dir

  defp record(value, key), do: Record.new(value, key: key)

  defp start(directory, opts \\ []) do
    {:ok, server} = BrokerServer.start_link(directory, opts)
    on_exit(fn -> if Process.alive?(server), do: BrokerServer.stop(server) end)
    server
  end

  defp with_topic(directory, opts \\ []) do
    server = start(directory, opts)
    {:ok, root_id} = BrokerServer.create_topic(server, "events", 4)
    {server, root_id}
  end

  defp read_all(server, range_id) do
    read_all(server, range_id, 0, [])
  end

  defp read_all(server, range_id, offset, accumulated) do
    case BrokerServer.read(server, range_id, offset, 100) do
      :eof -> accumulated |> Enum.reverse() |> List.flatten()
      {:ok, records} -> read_all(server, range_id, offset + length(records), [records | accumulated])
    end
  end

  # Deterministically wait until `count` long-poll waiters are parked (avoids sleep-based flakiness).
  defp wait_for_park(server, count \\ 1, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 2_000

    cond do
      length(:sys.get_state(server).waiters) >= count -> :ok
      System.monotonic_time(:millisecond) > deadline -> flunk("expected #{count} parked waiter(s)")
      true -> Process.sleep(2) && wait_for_park(server, count, deadline)
    end
  end

  describe "long-poll consume" do
    test "consume returns immediately when wait_ms is 0", %{tmp_dir: directory} do
      {server, _root} = with_topic(directory)
      assert {[], _positions} = BrokerServer.consume(server, "events", %{}, 100, 0)
    end

    test "consume with wait blocks until a produce wakes it", %{tmp_dir: directory} do
      {server, _root} = with_topic(directory)

      task = Task.async(fn -> BrokerServer.consume(server, "events", %{}, 100, 5_000) end)
      wait_for_park(server)

      {:ok, _placements} = BrokerServer.produce(server, "events", [record("a", "k0")])

      assert {records, _positions} = Task.await(task)
      assert Enum.map(records, & &1.value) == ["a"]
    end

    test "consume with wait returns empty after the timeout when nothing is produced", %{tmp_dir: directory} do
      {server, _root} = with_topic(directory)
      assert {[], _positions} = BrokerServer.consume(server, "events", %{}, 100, 50)
    end

    test "a produce wakes only waiters on the produced topic", %{tmp_dir: directory} do
      server = start(directory)
      {:ok, _} = BrokerServer.create_topic(server, "events", 4)
      {:ok, _} = BrokerServer.create_topic(server, "other", 4)

      events_task = Task.async(fn -> BrokerServer.consume(server, "events", %{}, 100, 300) end)
      other_task = Task.async(fn -> BrokerServer.consume(server, "other", %{}, 100, 300) end)
      wait_for_park(server, 2)

      {:ok, _} = BrokerServer.produce(server, "events", [record("a", "k0")])

      # the events waiter wakes with data; the other waiter is untouched and times out empty
      assert {[%{value: "a"}], _} = Task.await(events_task)
      assert {[], _} = Task.await(other_task, 1_000)
    end
  end

  describe "durability" do
    test "records are durable on return: no explicit sync needed", %{tmp_dir: directory} do
      {server, root_id} = with_topic(directory)

      {:ok, _placements} = BrokerServer.produce(server, "events", [record("a", "k0"), record("b", "k1")])

      # the replication server fsynced on a quorum before the call returned
      assert read_all(server, root_id) |> Enum.map(& &1.value) == ["a", "b"]
    end

    test "delete_segment drops a sealed segment from the control plane (retention)", %{tmp_dir: directory} do
      one_record = Record.encoded_size(record("value", "key"))
      {server, root_id} = with_topic(directory, segment_max_bytes: one_record)

      # each record fills a segment, sealing it and rolling to the next
      {:ok, _} = BrokerServer.produce(server, "events", [record("value", "k0")])
      {:ok, _} = BrokerServer.produce(server, "events", [record("value", "k1")])

      sealed = Metadata.segments_of_range(BrokerServer.metadata(server), root_id) |> Enum.find(&(&1.state == :sealed))
      assert sealed != nil

      assert BrokerServer.delete_segment(server, sealed.id) == :ok
      refute Enum.any?(Metadata.segments_of_range(BrokerServer.metadata(server), root_id), &(&1.id == sealed.id))
      # (refusing the active segment is covered by the Metadata unit test)
    end

    test "sync is a no-op and safe to call", %{tmp_dir: directory} do
      {server, root_id} = with_topic(directory)
      {:ok, _placements} = BrokerServer.produce(server, "events", [record("a", "k0")])
      :ok = BrokerServer.sync(server)
      assert {:ok, [record_read]} = BrokerServer.read(server, root_id, 0, 10)
      assert record_read.value == "a"
    end
  end

  describe "concurrency" do
    test "serializes concurrent produces without losing records", %{tmp_dir: directory} do
      {server, root_id} = with_topic(directory)

      1..50
      |> Enum.map(fn index ->
        Task.async(fn -> BrokerServer.produce(server, "events", [record("v#{index}", "k#{index}")]) end)
      end)
      |> Enum.each(fn task -> assert {:ok, _placements} = Task.await(task) end)

      assert server |> read_all(root_id) |> length() == 50
    end
  end

  describe "async produce (non-blocking frontend)" do
    # The non-group-commit produce path plans in the loop, fires replication as casts, and replies from
    # the results, so the broker loop never blocks on replication. These prove the reply semantics hold.

    defp start_repl(directory, index) do
      name = :"bsrv_repl_#{System.unique_integer([:positive])}_#{index}"
      {:ok, _} = ReplicationServer.start_link(name: name, directory: Path.join(directory, "r#{index}"))
      name
    end

    test "concurrent rf=3 produces all commit and read back consistently", %{tmp_dir: directory} do
      repls = for index <- 1..3, do: start_repl(directory, index)
      server = start(Path.join(directory, "broker"), brokers: repls, replication_factor: 3)
      {:ok, root_id} = BrokerServer.create_topic(server, "events", 4)

      results =
        1..20
        |> Task.async_stream(
          fn i -> BrokerServer.produce(server, "events", [record("v#{i}", "k#{i}")]) end,
          max_concurrency: 20,
          timeout: 15_000
        )
        |> Enum.map(fn {:ok, r} -> r end)

      assert Enum.all?(results, &match?({:ok, _}, &1))
      assert server |> read_all(root_id) |> length() == 20
    end

    test "two frontends interleaving on one range both succeed by adopting primary-assigned offsets",
         %{tmp_dir: directory} do
      # The cluster scenario reproduced locally: two BrokerServer frontends share one ReplicationServer
      # (the range's primary). Their in-memory metadata is separate but segment ids are deterministic,
      # so both address the same segment and their precomputed offsets interleave. Before offset
      # adoption ~half of these produces died with offset_mismatch; now the primary's assignment is
      # the truth and every produce must succeed.
      repl = start_repl(directory, 1)
      front_a = start(Path.join(directory, "a"), brokers: [repl])
      front_b = start(Path.join(directory, "b"), brokers: [repl])
      {:ok, root_id} = BrokerServer.create_topic(front_a, "events", 4)
      {:ok, ^root_id} = BrokerServer.create_topic(front_b, "events", 4)

      results =
        1..40
        |> Task.async_stream(
          fn i ->
            front = if rem(i, 2) == 0, do: front_a, else: front_b
            BrokerServer.produce(front, "events", [record("v#{i}", "k#{i}")])
          end,
          max_concurrency: 40,
          timeout: 15_000
        )
        |> Enum.map(fn {:ok, r} -> r end)

      assert Enum.all?(results, &match?({:ok, _}, &1)),
             "every interleaved produce must succeed, got: #{inspect(Enum.filter(results, &match?({:error, _}, &1)))}"

      # All 40 records landed contiguously on the shared primary (either frontend can read them).
      values = front_a |> read_all(root_id) |> Enum.map(& &1.value)
      assert length(values) == 40
      assert Enum.sort(values) == Enum.sort(for i <- 1..40, do: "v#{i}")

      # And each produce's placement points at its own records: the offsets the reply reported hold
      # exactly the produced value on the primary. (Read via the producing frontend: a frontend's read
      # horizon is its local counter, so the other frontend only sees this offset after its own next
      # produce/refresh, a pre-existing visibility bound, not an adoption artifact.)
      {:ok, placements} = BrokerServer.produce(front_b, "events", [record("probe", "kp")])
      [{first, last}] = Map.values(placements)
      assert first == last
      {:ok, [probe]} = BrokerServer.read(front_b, root_id, first, 1)
      assert probe.value == "probe"
    end

    test "unreachable replicas fail the produce gracefully and the broker survives", %{tmp_dir: directory} do
      # Replica refs that were never registered: the replication casts vanish, so the produce must
      # complete via a real error (no_quorum from the primary's timer, replication_timeout from the
      # broker's safety timer, or unreachable), never by crashing the caller or the broker.
      live = start_repl(directory, 1)
      dead1 = :"bsrv_dead_#{System.unique_integer([:positive])}"
      dead2 = :"bsrv_dead_#{System.unique_integer([:positive])}"
      server = start(Path.join(directory, "broker"), brokers: [live, dead1, dead2], replication_factor: 3)
      {:ok, _root} = BrokerServer.create_topic(server, "events", 4)

      assert {:error, reason} = BrokerServer.produce(server, "events", [record("v", "k")])
      assert reason in [:no_quorum, :replication_timeout, :unreachable]
      assert Process.alive?(server), "the broker must survive replication failure"

      # And it still serves other topics afterwards.
      assert {:ok, _} = BrokerServer.create_topic(server, "healthy", 4)
    end
  end

  describe "operations" do
    test "split, produce and read through the server", %{tmp_dir: directory} do
      {server, root_id} = with_topic(directory)
      {:ok, left_id, right_id} = BrokerServer.split_range(server, root_id)
      assert Enum.sort(BrokerServer.active_range_ids(server, "events")) == Enum.sort([left_id, right_id])

      records = for index <- 0..19, do: record("v#{index}", "k#{index}")
      {:ok, _placements} = BrokerServer.produce(server, "events", records)

      left = read_all(server, left_id) |> Enum.map(& &1.value)
      right = read_all(server, right_id) |> Enum.map(& &1.value)
      assert Enum.sort(left ++ right) == Enum.sort(Enum.map(records, & &1.value))
    end

    test "merge and cross-epoch history through the server", %{tmp_dir: directory} do
      {server, root_id} = with_topic(directory)
      parent_records = for index <- 0..9, do: record("v#{index}", "k#{index}")
      {:ok, _placements} = BrokerServer.produce(server, "events", parent_records)
      {:ok, left_id, right_id} = BrokerServer.split_range(server, root_id)

      left = drain_history(server, left_id)
      right = drain_history(server, right_id)
      assert Enum.sort(Enum.map(left ++ right, & &1.value)) == Enum.sort(Enum.map(parent_records, & &1.value))

      assert {:ok, _child_id} = BrokerServer.merge_ranges(server, left_id, right_id)
    end

    test "producing to an unknown topic fails", %{tmp_dir: directory} do
      server = start(directory)
      assert BrokerServer.produce(server, "nope", [record("a", "k")]) == {:error, :no_such_topic}
    end
  end

  describe "rack-aware placement wiring" do
    test "the periodic refresh pulls broker attributes from the source into placement", %{tmp_dir: directory} do
      attributes = %{a1: %{"rack" => "a"}, b1: %{"rack" => "b"}}

      server =
        start(directory,
          brokers: [:a1, :b1],
          replication_factor: 2,
          spread_by: "rack",
          broker_attributes: fn -> attributes end,
          brokers_refresh_interval: 5
        )

      wait_until!(fn -> :sys.get_state(server).broker.broker_attributes == attributes end)
    end
  end

  defp drain_history(server, range_id, cursor \\ :start, accumulated \\ []) do
    case BrokerServer.stream_history(server, range_id, cursor, 3) do
      {:ok, records, :done} -> [records | accumulated] |> Enum.reverse() |> List.flatten()
      {:ok, records, next_cursor} -> drain_history(server, range_id, next_cursor, [records | accumulated])
    end
  end

  describe "adopt_topology/2 (pure metadata-routing adoption)" do
    test "grows the routing to the new ring, keeps existing vnode metadata, starts a new vnode empty" do
      # a sharded broker cache with one vnode v0 holding a topic
      {:ok, ring0} = HashRing.add_vnode(HashRing.new(), :v0, 0)
      {meta0, {:ok, _root}} = Metadata.apply(Metadata.new(), {:create_topic, "orders", 4})

      {:ok, broker} =
        Broker.open(dsrsm: DSRSM.seed(ring0, %{v0: meta0}), command_fun: fn dsrsm, _t, _c -> {dsrsm, :ok} end)

      # adopt a topology that adds v1
      {:ok, ring1} = HashRing.add_vnode(ring0, :v1, div(Integer.pow(2, 32), 2))
      topology = %RingTopology{version: 1, ring: ring1, placements: %{v0: [node()], v1: [node()]}}

      adopted = BrokerServer.adopt_topology(broker, topology)

      # the cache adopted the new ring, with both vnodes
      assert adopted.dsrsm.ring == ring1
      assert adopted.dsrsm |> DSRSM.vnode_ids() |> Enum.sort() == [:v0, :v1]
      # v0 keeps its cached metadata; v1 starts empty until the next refresh from ra
      assert Metadata.get_topic(adopted.dsrsm.vnodes[:v0], "orders").name == "orders"
      assert adopted.dsrsm.vnodes[:v1] == Metadata.new()
      # the write router was rebuilt (over the new server map)
      assert is_function(adopted.command_fun, 3)
    end

    test "handle_cast adopt_topology rebuilds routing and the refresh source, so a reconcile won't revert" do
      {:ok, ring0} = HashRing.add_vnode(HashRing.new(), :v0, 0)

      {:ok, broker} =
        Broker.open(dsrsm: DSRSM.seed(ring0, %{v0: Metadata.new()}), command_fun: fn d, _t, _c -> {d, :ok} end)

      state = %{
        broker: broker,
        metadata_refresh: fn -> :stale end,
        bootstrap: %{
          orchestrator?: true,
          vnodes: [],
          replicated: %ReplicatedDSRSM{ring: ring0, vnodes: %{v0: {:v0, node()}}}
        }
      }

      {:ok, ring1} = HashRing.add_vnode(ring0, :v1, div(Integer.pow(2, 32), 2))
      topology = %RingTopology{version: 1, ring: ring1, placements: %{v0: [node()], v1: [node()]}}

      {:noreply, adopted} = BrokerServer.handle_cast({:adopt_topology, topology}, state)

      # metadata routing adopted the new ring
      assert adopted.broker.dsrsm.ring == ring1
      # the refresh source (bootstrap.replicated, which the rebuilt metadata_refresh snapshots) targets the
      # new ring, so the periodic reconcile re-seeds against it instead of reverting to the boot ring
      assert adopted.bootstrap.replicated.ring == ring1
      assert adopted.bootstrap.replicated.vnodes == %{v0: {:v0, node()}, v1: {:v1, node()}}
      assert is_function(adopted.metadata_refresh, 0)
    end
  end
end
