defmodule Malachi.BrokerServerTest do
  use ExUnit.Case, async: true

  import Malachi.Test.PollingHelper
  import Malachi.Test.TeardownHelper

  alias Malachi.Broker
  alias Malachi.BrokerServer
  alias Malachi.Cluster.DSRSM
  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.ReplicatedDSRSM
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.RingTopology
  alias Malachi.Log.Record
  alias Malachi.Metadata
  alias Malachi.Test.UnfenceablePrimary

  @moduletag :tmp_dir

  defp record(value, key), do: Record.new(value, key: key)

  # A primary that serves everything but the fence, so a FAILED fence can be told apart from a dead
  # primary (which would fail the produce too and make the two outcomes indistinguishable).
  defp start_unfenceable(id, opts \\ []) do
    directory = Path.join(System.tmp_dir!(), "malachi_unfenceable_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    start_supervised!({UnfenceablePrimary, [directory: directory] ++ opts}, id: id)
  end

  defp start(directory, opts \\ []) do
    {:ok, server} = BrokerServer.start_link(directory, opts)
    on_exit(fn -> stop_quietly(server) end)
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

      first_sealed = fn ->
        Metadata.segments_of_range(BrokerServer.metadata(server), root_id) |> Enum.find(&(&1.state == :sealed))
      end

      # A roll's seal lands when its fence answers, which is asynchronous to the produce that tripped it.
      wait_until!(fn -> first_sealed.() != nil end)
      sealed = first_sealed.()

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

  # The seal path end to end, in BOTH produce modes: `:group_commit` false is the default and is where
  # the 519-record loss reproduced, and true takes a different code path to the same fence.
  for group_commit <- [false, true] do
    describe "fenced seals (group_commit: #{group_commit})" do
      @describetag group_commit: group_commit

      defp seal_opts(unquote(group_commit), directory) do
        one_record = Record.encoded_size(Record.new("value", key: "key"))

        {directory, [segment_max_bytes: one_record, group_commit: unquote(group_commit), group_commit_interval_ms: 5]}
      end

      test "every acknowledged offset is served exactly once across many rolls", %{tmp_dir: directory} do
        # THE REGRESSION, with the chaos harness's randomness removed. A sealed length that outran or
        # fell short of the store put acknowledged records in offsets the next segment owned, and the
        # consume cursor stepped over them without ever delivering them.
        {directory, opts} = seal_opts(unquote(group_commit), directory)
        {server, root_id} = with_topic(directory, opts)

        acked =
          Enum.flat_map(0..19, fn index ->
            records = [record("v#{index}a", "k#{index}a"), record("v#{index}b", "k#{index}b")]
            {:ok, placements} = BrokerServer.produce(server, "events", records)
            {first, last} = Map.fetch!(placements, root_id)
            Enum.to_list(first..last)
          end)

        segments = Metadata.segments_of_range(BrokerServer.metadata(server), root_id)
        assert length(segments) > 1, "the threshold must actually have rolled several segments"

        served = server |> read_all(root_id) |> Enum.map(& &1.offset)
        assert served == Enum.sort(acked)
        assert served == Enum.uniq(served)
      end

      test "the sealed length and byte size are what the fence answered", %{tmp_dir: directory} do
        # The property the whole design turns on: the control plane's numbers are the store's, not a
        # frontend's tally. The byte half is what finally lets SelfHealing's integrity probe compare
        # like with like, since it measures `stored_bytes/3` on the other side.
        {directory, opts} = seal_opts(unquote(group_commit), directory)
        {server, root_id} = with_topic(directory, opts)

        for index <- 0..4 do
          {:ok, _placements} = BrokerServer.produce(server, "events", [record("value", "key#{index}")])
        end

        replication = BrokerServer.replication_ref(server)

        sealed_segments = fn ->
          BrokerServer.metadata(server) |> Metadata.segments_of_range(root_id) |> Enum.filter(&(&1.state == :sealed))
        end

        # A roll's fence answers asynchronously, so its seal lands shortly after the produce that tripped it.
        wait_until!(fn -> sealed_segments.() != [] end)
        sealed = sealed_segments.()

        for segment <- sealed do
          durable = ReplicationServer.durable_end(replication, segment.id, segment.start_offset)
          assert segment.length == durable - segment.start_offset
          assert segment.byte_size == ReplicationServer.stored_bytes(replication, segment.id)
        end
      end

      test "a segment sealed short is impossible when another frontend interleaved", %{tmp_dir: directory} do
        # THE REGRESSION, and it needs TWO frontends to exist at all. With one frontend the tally and the
        # store's end agree by construction, so a single-frontend version of this test passes even on a
        # tree with the bug: it witnesses nothing. The divergence the bug needs is a frontend that seals
        # a segment whose records it did not all produce.
        #
        # A and B share one primary, and their segment ids are deterministic, so both address the same
        # segment while counting separately. A produces 2, B produces 3 (the primary assigns them above
        # A's, and A never counts them), then A produces 3 more and crosses its own threshold. A's tally
        # says 5; the store holds 8. Sealing on the tally recorded length 5 and put three acknowledged
        # records above the sealed edge, where the next segment owns their offsets and no read can reach
        # them. Sealing on the counter the primary corrected records 8.
        # Measured from the record shape this test produces, not from a larger one: a threshold taken
        # from a bigger record is never crossed and the test passes without ever rolling a segment.
        one_record = Record.encoded_size(record("v0", "k0"))
        repl = start_repl(directory, 1)

        opts = [
          brokers: [repl],
          segment_max_bytes: 5 * one_record,
          group_commit: unquote(group_commit),
          group_commit_interval_ms: 5
        ]

        front_a = start(Path.join(directory, "a"), opts)
        front_b = start(Path.join(directory, "b"), opts)
        {:ok, root_id} = BrokerServer.create_topic(front_a, "events", 4)
        {:ok, ^root_id} = BrokerServer.create_topic(front_b, "events", 4)

        {:ok, first} = BrokerServer.produce(front_a, "events", for(i <- 0..1, do: record("v#{i}", "k#{i}")))
        {:ok, _} = BrokerServer.produce(front_b, "events", for(i <- 2..4, do: record("v#{i}", "k#{i}")))
        {:ok, third} = BrokerServer.produce(front_a, "events", for(i <- 5..7, do: record("v#{i}", "k#{i}")))

        # A produced 5 of the segment's 8 records, and B's three sit between them. That is what makes
        # the assertion below non-vacuous: 8 is a number A cannot reach by counting its own work, so a
        # seal derived from a per-frontend tally records 5 and this test fails.
        assert Map.fetch!(first, root_id) == {0, 1}
        assert Map.fetch!(third, root_id) == {5, 7}

        front_a_sealed = fn ->
          BrokerServer.metadata(front_a) |> Metadata.segments_of_range(root_id) |> Enum.filter(&(&1.state == :sealed))
        end

        wait_until!(fn -> front_a_sealed.() != [] end)
        [sealed] = front_a_sealed.()

        # 8, the store's end, not 5, A's tally. This is the assertion a baseline tree fails.
        assert sealed.length == 8
        assert sealed.start_offset == 0

        # And the three records above A's tally are still reachable, which is what the length being
        # wrong actually cost: with the read clamped to the sealed length, a short seal hides them.
        assert {:ok, [%{offset: 5, value: "v5"} | _rest]} = BrokerServer.read(front_a, root_id, 5, 100)
        assert front_a |> read_all(root_id) |> Enum.map(& &1.value) == Enum.map(0..7, &"v#{&1}")
      end

      test "a frontend that has not seen a roll gets no write acknowledged above the sealed edge",
           %{tmp_dir: directory} do
        # THE REGRESSION the storage chaos drill found (#147). A roll used to be a metadata command taken from
        # the rolling frontend's counter, with nothing telling the store. A frontend still caching the segment
        # as its write head kept appending to it, the primary kept acknowledging, and those records sat above
        # the edge the control plane recorded: stored, acknowledged, and unreachable by any read. Now the roll
        # fences the store first, so the stale frontend is refused instead of acknowledged.
        #
        # Two frontends with separate metadata over one primary, as in the test above: segment ids are
        # deterministic, so both address the same segment, and B never learns A's seal from the metadata.
        one_record = Record.encoded_size(record("v0", "k0"))
        repl = start_repl(directory, 1)

        opts = [
          brokers: [repl],
          segment_max_bytes: 2 * one_record,
          group_commit: unquote(group_commit),
          group_commit_interval_ms: 5
        ]

        front_a = start(Path.join(directory, "a"), opts)
        front_b = start(Path.join(directory, "b"), opts)
        {:ok, root_id} = BrokerServer.create_topic(front_a, "events", 4)
        {:ok, ^root_id} = BrokerServer.create_topic(front_b, "events", 4)

        # B opens the range's first segment and caches it as its write head, below its own threshold.
        {:ok, _} = BrokerServer.produce(front_b, "events", [record("v0", "k0")])

        # A appends to the same segment and crosses ITS threshold: the roll, sealed at the primary's end, 3.
        {:ok, _} = BrokerServer.produce(front_a, "events", [record("v1", "k1"), record("v2", "k2")])

        wait_until!(fn ->
          match?([%{state: :sealed, length: 3}], BrokerServer.metadata(front_a) |> Metadata.segments_of_range(root_id))
        end)

        # B still treats segment 0 as its write head. Before the fence this append was acknowledged, at offset
        # 3, above the edge A had recorded.
        assert {:error, {:sealed, 3}} = BrokerServer.produce(front_b, "events", [record("v3", "k3")])

        # A's next write opens the successor at the sealed edge, and every acknowledged record is readable.
        {:ok, _} = BrokerServer.produce(front_a, "events", [record("v4", "k4")])
        assert front_a |> read_all(root_id) |> Enum.map(& &1.value) == ["v0", "v1", "v2", "v4"]
      end

      test "a produce behind this frontend's own fence waits for the answer and lands in the successor",
           %{tmp_dir: directory} do
        # THE REGRESSION the async fence introduced (#147). The rolling frontend's next produce reaches the
        # primary after the fence it sent, so the primary refuses it; that is this frontend overtaking itself,
        # not a stale writer, and failing it back made every roll cost a client error. The double holds the
        # fence's answer, so the produce is provably held while the answer is out.
        {directory, opts} = seal_opts(unquote(group_commit), directory)
        # Records the size of the one `seal_opts/2` measures, so each one crosses the threshold.
        primary = start_unfenceable(:"hold_fence_#{unquote(group_commit)}", fence: :hold)
        {server, root_id} = with_topic(directory, Keyword.put(opts, :brokers, [primary]))

        assert {:ok, %{^root_id => {0, 0}}} = BrokerServer.produce(server, "events", [record("val_0", "ke0")])

        second = Task.async(fn -> BrokerServer.produce(server, "events", [record("val_1", "ke1")]) end)
        wait_until!(fn -> Map.has_key?(:sys.get_state(server).fence_parked, root_id) end)
        assert Task.yield(second, 50) == nil

        :ok = UnfenceablePrimary.release_fences(primary)

        assert {:ok, %{^root_id => {1, 1}}} = Task.await(second)
        assert [%{state: :sealed, length: 1}, %{start_offset: 1} | _] = segments_of(server, root_id)
        assert server |> read_all(root_id) |> Enum.map(& &1.value) == ["val_0", "val_1"]
        assert :sys.get_state(server).fence_parked == %{}
      end

      test "a produce behind a fence that never answers fails with the refusal once the retry window passes",
           %{tmp_dir: directory} do
        # The bound on the hold: an answer that was lost must not park a client forever. The window is the
        # one after which the frontend resends the fence, so no produce waits on an answer longer than that.
        {directory, opts} = seal_opts(unquote(group_commit), directory)
        primary = start_unfenceable(:"mute_fence_#{unquote(group_commit)}", fence: :hold)
        {server, root_id} = with_topic(directory, Keyword.put(opts, :brokers, [primary]))

        assert {:ok, _} = BrokerServer.produce(server, "events", [record("val_0", "ke0")])

        {elapsed_us, reply} = :timer.tc(fn -> BrokerServer.produce(server, "events", [record("val_1", "ke1")]) end)

        assert reply == {:error, {:sealed, 1}}
        assert elapsed_us >= 900_000, "failed after #{div(elapsed_us, 1000)}ms, before the retry window"
        assert :sys.get_state(server).fence_parked == %{}
        assert server |> read_all(root_id) |> Enum.map(& &1.value) == ["val_0"]
      end
    end
  end

  defp segments_of(server, range_id) do
    server |> BrokerServer.metadata() |> Metadata.segments_of_range(range_id) |> Enum.sort_by(& &1.start_offset)
  end

  describe "a produce behind its own roll fence (group_commit: false)" do
    test "is planned again only once: a second refusal fails it", %{tmp_dir: directory} do
      # The fence answers, but its seal cannot be recorded, so the control plane still calls the segment
      # active. The held produce is planned again, adopts that same segment, and is refused a second time;
      # holding it again would loop for as long as the metadata stays behind.
      one_record = Record.encoded_size(record("v0", "k0"))
      primary = start_unfenceable(:hold_fence_record_fails, fence: :hold)
      {server, root_id} = with_topic(directory, brokers: [primary], segment_max_bytes: one_record)

      failing = fn dsrsm, topic, command ->
        case command do
          {:seal_segment, _id, _length, _bytes, _at} -> {dsrsm, {:error, :ra_timeout}}
          _other -> DSRSM.command(dsrsm, topic, command)
        end
      end

      :sys.replace_state(server, fn state -> put_in(state.broker.command_fun, failing) end)

      assert {:ok, _} = BrokerServer.produce(server, "events", [record("v0", "k0")])

      second = Task.async(fn -> BrokerServer.produce(server, "events", [record("v1", "k1")]) end)
      wait_until!(fn -> Map.has_key?(:sys.get_state(server).fence_parked, root_id) end)

      ExUnit.CaptureLog.capture_log(fn ->
        :ok = UnfenceablePrimary.release_fences(primary)
        assert Task.await(second) == {:error, {:sealed, 1}}
      end)

      assert :sys.get_state(server).fence_parked == %{}
      assert [%{state: :active}] = segments_of(server, root_id)

      # And the answer ended the wait: the next refusal fails at once rather than being held for a window.
      {elapsed_us, reply} = :timer.tc(fn -> BrokerServer.produce(server, "events", [record("v2", "k2")]) end)
      assert reply == {:error, {:sealed, 1}}
      assert elapsed_us < 500_000
    end

    test "a failed fence holds nothing", %{tmp_dir: directory} do
      # A fence that answers with an error leaves the segment open, so a produce refused meanwhile was not
      # refused by it: nothing is held, and the roll stays owed.
      one_record = Record.encoded_size(record("v0", "k0"))
      primary = start_unfenceable(:hold_fence_fails, fence: :hold)
      {server, root_id} = with_topic(directory, brokers: [primary], segment_max_bytes: one_record)

      assert {:ok, _} = BrokerServer.produce(server, "events", [record("v0", "k0")])
      [roll] = Broker.due_rolls(:sys.get_state(server).broker)
      assert Broker.awaiting_fence?(:sys.get_state(server).broker, root_id, roll.segment_id, now(), 1_000)

      ExUnit.CaptureLog.capture_log(fn ->
        send(server, {:seal_result, {:roll_fence, roll}, {:error, :unreachable}})

        wait_until!(fn ->
          not Broker.awaiting_fence?(:sys.get_state(server).broker, root_id, roll.segment_id, now(), 1_000)
        end)
      end)

      assert [%{segment_id: segment_id}] = Broker.due_rolls(:sys.get_state(server).broker)
      assert segment_id == roll.segment_id
      assert :sys.get_state(server).fence_parked == %{}
    end

    test "several produces held behind one fence go again in the order they arrived", %{tmp_dir: directory} do
      one_record = Record.encoded_size(record("val_0", "ke0"))
      primary = start_unfenceable(:hold_fence_many, fence: :hold)
      {server, root_id} = with_topic(directory, brokers: [primary], segment_max_bytes: one_record)

      assert {:ok, %{^root_id => {0, 0}}} = BrokerServer.produce(server, "events", [record("val_0", "ke0")])

      second = Task.async(fn -> BrokerServer.produce(server, "events", [record("val_1", "ke1")]) end)
      wait_until!(fn -> parked_count(server, root_id) == 1 end)
      third = Task.async(fn -> BrokerServer.produce(server, "events", [record("val_2", "ke2")]) end)
      wait_until!(fn -> parked_count(server, root_id) == 2 end)

      :ok = UnfenceablePrimary.release_fences(primary)

      assert {:ok, %{^root_id => {1, 1}}} = Task.await(second)
      assert {:ok, %{^root_id => {2, 2}}} = Task.await(third)
      assert server |> read_all(root_id) |> Enum.map(& &1.value) == ["val_0", "val_1", "val_2"]
    end

    test "a held produce that already finished is dropped when its fence answers", %{tmp_dir: directory} do
      # Its safety timer fired while it was held: the client has its answer, and replanning it now would
      # store records nobody is waiting for.
      one_record = Record.encoded_size(record("val_0", "ke0"))
      primary = start_unfenceable(:hold_fence_finished, fence: :hold)
      {server, root_id} = with_topic(directory, brokers: [primary], segment_max_bytes: one_record)

      assert {:ok, _} = BrokerServer.produce(server, "events", [record("val_0", "ke0")])

      second = Task.async(fn -> BrokerServer.produce(server, "events", [record("val_1", "ke1")]) end)
      wait_until!(fn -> parked_count(server, root_id) == 1 end)

      [ref] = Map.keys(:sys.get_state(server).async_produces)
      send(server, {:produce_timeout, ref})
      assert Task.await(second) == {:error, :replication_timeout}

      :ok = UnfenceablePrimary.release_fences(primary)
      wait_until!(fn -> :sys.get_state(server).fence_parked == %{} end)

      assert server |> read_all(root_id) |> Enum.map(& &1.value) == ["val_0"]
      assert {:ok, %{^root_id => {1, 1}}} = BrokerServer.produce(server, "events", [record("val_2", "ke2")])
    end

    test "a held produce whose new plan fails gets that failure", %{tmp_dir: directory} do
      one_record = Record.encoded_size(record("val_0", "ke0"))
      primary = start_unfenceable(:hold_fence_replan_fails, fence: :hold)
      {server, root_id} = with_topic(directory, brokers: [primary], segment_max_bytes: one_record)

      assert {:ok, _} = BrokerServer.produce(server, "events", [record("val_0", "ke0")])

      second = Task.async(fn -> BrokerServer.produce(server, "events", [record("val_1", "ke1")]) end)
      wait_until!(fn -> parked_count(server, root_id) == 1 end)

      # The seal records, but the successor cannot be registered.
      failing = fn dsrsm, topic, command ->
        case command do
          {:register_segment, _range, _id, _set, _offset} -> {dsrsm, {:error, :ra_timeout}}
          _other -> DSRSM.command(dsrsm, topic, command)
        end
      end

      :sys.replace_state(server, fn state -> put_in(state.broker.command_fun, failing) end)
      :ok = UnfenceablePrimary.release_fences(primary)

      assert Task.await(second) == {:error, :ra_timeout}
      assert :sys.get_state(server).async_produces == %{}
      assert [%{state: :sealed, length: 1}] = segments_of(server, root_id)
    end

    test "a stale park timeout releases nothing", %{tmp_dir: directory} do
      {server, root_id} = with_topic(directory)
      send(server, {:fence_park_timeout, root_id, make_ref()})
      assert :sys.get_state(server).fence_parked == %{}
    end
  end

  defp now, do: System.monotonic_time(:millisecond)

  defp parked_count(server, range_id) do
    case Map.get(:sys.get_state(server).fence_parked, range_id) do
      nil -> 0
      parked -> length(parked.entries)
    end
  end

  describe "fenced seals (failure paths)" do
    test "a primary that never answers a fence delays no produce and seals nothing on a guess", %{tmp_dir: directory} do
      # Two guards in one. This double answers every request except the fence, which it swallows forever.
      #
      # The fence is sent as a cast, so a mute primary puts no wait on the loop that serializes every client
      # of this node: a waited-on fence would have each produce below pay the fence timeout, and the
      # frontend would degrade to about one produce per timeout for every owed roll.
      #
      # And the seal waits for the fence's answer, so a primary that never gives one leaves the segment
      # ACTIVE and taking writes past its soft threshold, rather than sealed at a length nobody measured.
      # Sealing from the frontend's own counter is what let a writer that had not yet seen the seal land
      # acknowledged records above the recorded edge.
      one_record = Record.encoded_size(Record.new("value", key: "key"))
      unfenceable = start_unfenceable(:unfenceable_roll)

      {server, root_id} = with_topic(directory, brokers: [unfenceable], segment_max_bytes: one_record)

      {elapsed_us, results} =
        :timer.tc(fn -> for i <- 0..1, do: BrokerServer.produce(server, "events", [record("value", "key#{i}")]) end)

      assert Enum.all?(results, &match?({:ok, _}, &1))
      assert elapsed_us < 500_000, "two produces took #{div(elapsed_us, 1000)}ms: something waited on the fence"

      assert [%{state: :active, start_offset: 0}] = BrokerServer.metadata(server) |> Metadata.segments_of_range(root_id)
      assert server |> read_all(root_id) |> length() == 2

      # The roll stays owed, to be sent again, instead of being settled without an answer.
      assert [%{segment_id: {^root_id, 0}}] = :sys.get_state(server).broker |> Broker.due_rolls()
    end

    test "split_range/2 fences the parent before the split, and reports a fence failure", %{tmp_dir: directory} do
      {server, root_id} = with_topic(directory)
      {:ok, _placements} = BrokerServer.produce(server, "events", [record("v", "k")])

      replication = BrokerServer.replication_ref(server)
      [segment] = BrokerServer.metadata(server) |> Metadata.segments_of_range(root_id)

      assert {:ok, _left, _right} = BrokerServer.split_range(server, root_id)

      # Fenced before either child existed: a node that has not seen the split can no longer get a
      # record into the parent, which is the write half of the split gap.
      assert %{state: :sealed, length: 1} = BrokerServer.metadata(server) |> Metadata.get_segment(segment.id)

      assert {:error, {:sealed, 1}} =
               ReplicationServer.append(replication, segment.id, segment.replica_set, 0, [record("late", "k")])
    end

    test "split_range/2 refuses the split when the parent cannot be fenced", %{tmp_dir: directory} do
      unfenceable = start_unfenceable(:unfenceable_split)
      {server, root_id} = with_topic(directory, brokers: [unfenceable], fence_timeout: 50)

      assert {:ok, _placements} = BrokerServer.produce(server, "events", [record("v", "k")])
      assert Broker.active_roll(:sys.get_state(server).broker, root_id) != :none

      assert {:error, {:fence_failed, ^root_id, :unreachable}} = BrokerServer.split_range(server, root_id)

      # And the range is intact: no children, parent still active.
      assert BrokerServer.active_range_ids(server, "events") == [root_id]
    end

    test "a fence whose seal command fails is reported loudly, not swallowed (issue #121)", %{tmp_dir: directory} do
      # The store is fenced and the metadata is not, which stops the range from accepting any write at
      # all until `Malachi.Cluster.OrphanedFence` reconciles it. `Broker.record_seal/5` returns the
      # broker UNCHANGED on this branch, so nothing here retries and nothing else notices: reporting
      # only that the split failed left an operator unable to tell this apart from a split that
      # changed nothing.
      {server, root_id} = with_topic(directory)
      {:ok, _placements} = BrokerServer.produce(server, "events", [record("v", "k")])
      [segment] = BrokerServer.metadata(server) |> Metadata.segments_of_range(root_id)

      # An `ra` timeout on the seal command, injected into the RUNNING broker's own `:command_fun`
      # seam. `BrokerServer` builds its broker options itself rather than forwarding the caller's, and
      # widening its option list to reach one failure mode from a test would put a seam in production
      # code that only tests use.
      failing = fn dsrsm, topic, command ->
        case command do
          {:seal_segment, _id, _length, _bytes, _at} -> {dsrsm, {:error, :ra_timeout}}
          _other -> DSRSM.command(dsrsm, topic, command)
        end
      end

      :sys.replace_state(server, fn state -> put_in(state.broker.command_fun, failing) end)

      handler = "orphaned-fence-test-#{System.unique_integer([:positive])}"
      test_pid = self()

      :telemetry.attach(
        handler,
        [:malachi, :cluster, :orphaned_fence],
        fn _event, measurements, metadata, _config -> send(test_pid, {:orphaned, measurements, metadata}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:error, {:fence_failed, ^root_id, :ra_timeout}} = BrokerServer.split_range(server, root_id)
        end)

      assert log =~ inspect(segment.id)
      assert log =~ "ra_timeout"

      assert_received {:orphaned, %{count: 1}, %{segment: segment_id, reason: :ra_timeout}}
      assert segment_id == segment.id

      # The state the report describes, pinned: the store refuses writes while the control plane still
      # calls the segment active, which is precisely what the reconciling pass looks for.
      assert %{state: :active} = BrokerServer.metadata(server) |> Metadata.get_segment(segment.id)

      assert {:error, {:sealed, 1}} =
               ReplicationServer.append(
                 BrokerServer.replication_ref(server),
                 segment.id,
                 segment.replica_set,
                 0,
                 [record("late", "k")]
               )
    end

    test "a reconcile evicts a cached segment another node sealed", %{tmp_dir: directory} do
      # Level-triggered convergence: before this, only the node running the heal coordinator dropped
      # such a segment, so every other frontend kept routing produces at a segment the metadata had
      # already sealed.
      {server, root_id} = with_topic(directory)
      {:ok, _placements} = BrokerServer.produce(server, "events", [record("v", "k")])

      broker = :sys.get_state(server).broker
      [segment] = BrokerServer.metadata(server) |> Metadata.segments_of_range(root_id)

      {dsrsm, :ok} = DSRSM.command(broker.dsrsm, "events", {:seal_segment, segment.id, 1, 4, 900})
      converged = Broker.drop_stale_active_segments(%{broker | dsrsm: dsrsm})

      refute Map.has_key?(converged.segments, root_id)
      assert converged.offsets[root_id] == 1
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

  describe "a read that fails" do
    test "is reported, not returned as an empty page", %{tmp_dir: tmp_dir} do
      replica = start_supervised!({ReplicationServer, directory: Path.join(tmp_dir, "replica")})
      server = start(Path.join(tmp_dir, "broker"), brokers: [replica])

      {:ok, _root} = BrokerServer.create_topic(server, "events", 4)
      {:ok, _placements} = BrokerServer.produce(server, "events", [record("v1", "k1"), record("v2", "k2")])
      assert {[_, _], _positions} = BrokerServer.consume(server, "events", %{}, 100, 0)

      # The segment's only replica goes away, so the records are real and durable but this broker
      # cannot reach them. Answering `{[], positions}` here is what a consumer reads as "caught up",
      # and it would commit past records it never saw.
      :ok = stop_supervised!(ReplicationServer)

      assert {:error, :unreachable} = BrokerServer.consume(server, "events", %{}, 100, 0)
    end

    test "is reported rather than parked until the long poll expires", %{tmp_dir: tmp_dir} do
      replica = start_supervised!({ReplicationServer, directory: Path.join(tmp_dir, "replica")})
      server = start(Path.join(tmp_dir, "broker"), brokers: [replica])

      {:ok, _root} = BrokerServer.create_topic(server, "events", 4)
      {:ok, _placements} = BrokerServer.produce(server, "events", [record("v1", "k1")])
      :ok = stop_supervised!(ReplicationServer)

      # With wait_ms set, an empty page parks the caller. A failure must not: parking would hide the
      # error behind a timeout and then hand back the same empty page.
      #
      # The wait is long and the assertion is on the clock, so the test states what it means. The
      # shape alone would catch a regression to parking, because a long-poll timeout replies
      # `{[], positions}` and not an error, but it would catch it five seconds late and say only that
      # the answer was wrong, not that the caller had been held.
      task = Task.async(fn -> BrokerServer.consume(server, "events", %{}, 100, 5_000) end)

      assert {:error, :unreachable} = Task.await(task, 1_000)
    end
  end
end
