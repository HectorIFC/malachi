defmodule Malachi.Cluster.ReplicationServerTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record
  alias Malachi.Storage.Layout

  @segment {{"events", 0}, 0}

  defp start_broker(opts \\ []) do
    name = :"repl_#{System.unique_integer([:positive])}"
    directory = Path.join(System.tmp_dir!(), "malachi_repl_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    start_supervised!({ReplicationServer, [name: name, directory: directory] ++ opts}, id: name)
    name
  end

  defp records(values), do: for(value <- values, do: Record.new(value, key: value))

  defp read_values(ref, segment, offset \\ 0) do
    case ReplicationServer.read(ref, segment, offset, 100) do
      {:ok, records} -> Enum.map(records, & &1.value)
      :eof -> []
    end
  end

  test "the primary replicates to all followers and commits the batch" do
    [primary, f1, f2] = replica_set = [start_broker(), start_broker(), start_broker()]

    assert {:ok, 1} = ReplicationServer.replicate(primary, @segment, replica_set, 0, records(["a", "b"]))

    # Every replica (primary included) stores the same records at the same offsets. The reply comes on
    # QUORUM durability (2 of 3), so the third replica may still be applying: wait for convergence.
    for ref <- [primary, f1, f2] do
      assert eventually(fn -> read_values(ref, @segment) == ["a", "b"] end),
             "#{inspect(ref)} should converge, got #{inspect(read_values(ref, @segment))}"
    end
  end

  test "cluster-shaped {name, node} replica refs are accepted as primary (regression)" do
    # A clustered broker builds replica sets as `{name, node}` tuples (Application.broker_refs/1). The
    # server's own ref must match that shape, or every clustered produce dies with :not_primary, which
    # is exactly what happened before refs were canonicalized: a registered server kept a bare-atom ref
    # that never equaled the tuple, so this append (and so every produce on a real cluster) failed.
    name = start_broker()
    tuple_set = [{name, node()}]

    assert {:ok, 0} = ReplicationServer.append({name, node()}, @segment, tuple_set, 0, records(["a"]))
    assert {:ok, 1} = ReplicationServer.replicate({name, node()}, @segment, tuple_set, 1, records(["b"]))
    assert read_values(name, @segment) == ["a", "b"]
  end

  test "atom and {name, node} refs are interchangeable in one replica set" do
    # Single-node callers use bare atoms, cluster callers use tuples; both name the same server, so a
    # mixed set must behave identically (the atom is canonicalized to {name, node()} on receipt).
    [primary, follower] = [start_broker(), start_broker()]
    mixed_set = [{primary, node()}, follower]

    assert {:ok, 1} = ReplicationServer.replicate(primary, @segment, mixed_set, 0, records(["a", "b"]))
    assert read_values(primary, @segment) == ["a", "b"]
    assert read_values(follower, @segment) == ["a", "b"]
  end

  describe "pipelined fan-out" do
    @sa {{"deadlock_a", 0}, 0}
    @sb {{"deadlock_b", 0}, 0}

    test "mutual primaries never deadlock (regression: circular follower waits)" do
      # A is primary of one segment with B as follower, and B is primary of another with A as
      # follower. Under the old synchronous fan-out each primary's loop blocked waiting on the other's
      # :follow, a circular wait that stalled every batch to its 5s timeout. With the pipelined push
      # the loops never block, so a burst in both directions completes orders of magnitude faster.
      [a, b] = [start_broker(), start_broker()]

      t0 = System.monotonic_time(:millisecond)

      results =
        1..20
        |> Task.async_stream(
          fn i ->
            if rem(i, 2) == 0 do
              ReplicationServer.replicate(a, @sa, [a, b], 0, records(["a#{i}"]))
            else
              ReplicationServer.replicate(b, @sb, [b, a], 0, records(["b#{i}"]))
            end
          end,
          max_concurrency: 20,
          timeout: 15_000
        )
        |> Enum.map(fn {:ok, r} -> r end)

      elapsed = System.monotonic_time(:millisecond) - t0

      assert Enum.all?(results, &match?({:ok, _}, &1)), "every cross-primary batch must commit"
      assert elapsed < 2_000, "cross-primary replication took #{elapsed}ms; the loops are blocking again"
    end

    test "concurrent batches to one segment stay contiguous and ordered on every replica" do
      [primary, f1, f2] = replica_set = [start_broker(), start_broker(), start_broker()]

      results =
        1..30
        |> Task.async_stream(
          fn i -> ReplicationServer.replicate(primary, @segment, replica_set, 0, records(["v#{i}"])) end,
          max_concurrency: 30,
          timeout: 15_000
        )
        |> Enum.map(fn {:ok, r} -> r end)

      assert Enum.all?(results, &match?({:ok, _}, &1))

      # 30 records landed contiguously (offsets 0..29) and identically on all three replicas: the
      # per-pair FIFO push preserves the expected_first chain even with a full window of batches.
      # Replies come on quorum (2 of 3), so the non-quorum replica may still be applying: converge.
      primary_values = read_values(primary, @segment)
      assert length(primary_values) == 30

      assert eventually(fn -> read_values(f1, @segment) == primary_values end),
             "f1 should converge to the primary, got #{inspect(read_values(f1, @segment))}"

      assert eventually(fn -> read_values(f2, @segment) == primary_values end),
             "f2 should converge to the primary, got #{inspect(read_values(f2, @segment))}"
    end

    test "a window of one (degenerate pipelining) still commits everything in order" do
      [primary, follower] = [start_broker(replication_window: 1), start_broker()]

      for i <- 1..10 do
        assert {:ok, _} = ReplicationServer.replicate(primary, @segment, [primary, follower], 0, records(["w#{i}"]))
      end

      assert read_values(follower, @segment) == for(i <- 1..10, do: "w#{i}")
    end

    test "a committed batch cancels its no-quorum timer (no stale timeout messages)" do
      # Trace the primary's received messages: after a batch commits normally, its follow_timeout
      # timer must have been cancelled, so no {:replicate_timeout, ...} ever arrives. Before the
      # cancel, every committed batch fired a stale message that walked the inflight and pending
      # structures for nothing.
      primary = start_broker(follow_timeout: 150)
      follower = start_broker()
      pid = Process.whereis(primary)

      # No untrace cleanup needed: trace flags are cleared automatically when the traced process exits
      # (and the supervised server dies with the test).
      :erlang.trace(pid, true, [:receive])

      assert {:ok, 0} = ReplicationServer.replicate(primary, @segment, [primary, follower], 0, records(["a"]))

      # Give a stale timer (150ms) ample time to fire if it was not cancelled.
      Process.sleep(300)

      stale =
        receive do
          {:trace, ^pid, :receive, {:replicate_timeout, _, _} = msg} -> msg
        after
          0 -> nil
        end

      assert stale == nil, "committed batch left a stale timeout: #{inspect(stale)}"
    end

    test "an ack arriving after the no-quorum timeout does not answer the batch a second time" do
      # The mirror of the test above, and the other half of the exactly-once invariant: there, an
      # ack cancels the timer; here the timer wins and the ack arrives late. A parked batch must be
      # answered EXACTLY once, so the late ack must find the batch gone and reply nothing. This is
      # the interleaving the Concuerror spike targeted and could not explore (see
      # docs/ARCHITECTURE.md), so it is pinned deterministically instead: a stub follower that
      # never acks on its own lets the timeout fire first, and the ack is then injected by hand.
      primary = start_broker(follow_timeout: 20)
      silent_follower = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(silent_follower, :kill) end)

      :ok =
        ReplicationServer.replicate_async(
          primary,
          @segment,
          [primary, silent_follower],
          0,
          records(["a"]),
          self(),
          :late
        )

      assert_receive {:replicate_result, :late, {:error, :no_quorum}}, 1_000

      # The follower's ack, delayed past the timeout (its offset is the batch's last).
      GenServer.cast(primary, {:replica_ack, @segment, silent_follower, {:ok, 0}})

      refute_receive {:replicate_result, :late, _second}, 200

      # Not a vacuous pass: the late ack really was processed (the tracker recorded the follower's
      # offset), so the silence above is the timeout handler having consumed the batch, not the ack
      # being discarded as unknown.
      tracker = :sys.get_state(Process.whereis(primary)).trackers[@segment]
      assert tracker.match[silent_follower] == 0

      # The server is still healthy after the stale ack: it keeps serving new batches.
      assert {:ok, 1} = ReplicationServer.replicate(primary, @segment, [primary], 1, records(["b"]))
    end

    test "quorum holds with one dead follower, fails without a majority" do
      # Dead follower = a never-registered name: the cast vanishes, no ack ever arrives. A short
      # follow_timeout keeps the no-quorum case fast.
      primary = start_broker(follow_timeout: 300)
      live = start_broker()
      dead1 = :"dead_#{System.unique_integer([:positive])}"
      dead2 = :"dead_#{System.unique_integer([:positive])}"

      # 2 of 3 durable (primary + live): committed.
      assert {:ok, 0} = ReplicationServer.replicate(primary, @segment, [primary, live, dead1], 0, records(["a"]))

      # 1 of 3 durable (primary only): the batch times out with no_quorum.
      seg2 = {{"quorumless", 0}, 0}

      assert {:error, :no_quorum} =
               ReplicationServer.replicate(primary, seg2, [primary, dead1, dead2], 0, records(["b"]))
    end

    test "a behind follower nacks (does not fake quorum) and catches up in the background" do
      [primary, live] = [start_broker(), start_broker()]
      behind = start_broker()

      # Seed the primary and the live follower past offset 0 while `behind` misses the batches.
      assert {:ok, 1} = ReplicationServer.replicate(primary, @segment, [primary, live], 0, records(["a", "b"]))

      # Now include `behind`: it nacks (out_of_sync) and pulls from the primary in the background; the
      # batch still commits via primary + live (2 of 3).
      assert {:ok, 2} = ReplicationServer.replicate(primary, @segment, [primary, live, behind], 0, records(["c"]))

      # The catch-up converges: eventually the behind replica has the full history.
      assert eventually(fn -> read_values(behind, @segment) == ["a", "b", "c"] end),
             "behind replica should backfill to a/b/c, got #{inspect(read_values(behind, @segment))}"
    end
  end

  test "a replica restarted over its persisted directory recovers instead of crashing" do
    # After a power-loss restart the segment files are already on disk; the first append used to blow
    # up the server with an :already_exists MatchError (Log.open creating over existing files), which
    # crash-looped the whole replication server on a restarted node, exactly what the chaos harness
    # caught. With recover, the replica resumes at its durable end and keeps serving.
    directory = Path.join(System.tmp_dir!(), "malachi_repl_restart_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    name = :"repl_restart_#{System.unique_integer([:positive])}"

    {:ok, first} = ReplicationServer.start_link(name: name, directory: directory)
    assert {:ok, 1} = ReplicationServer.replicate(name, @segment, [name], 0, records(["a", "b"]))
    GenServer.stop(first)

    {:ok, _second} = ReplicationServer.start_link(name: name, directory: directory)

    # The restarted replica accepts appends continuing its durable end (this crashed before the fix)
    # and still serves the pre-restart records.
    assert {:ok, 2} = ReplicationServer.replicate(name, @segment, [name], 0, records(["c"]))
    assert read_values(name, @segment) == ["a", "b", "c"]
  end

  test "a restarted replica serves COLD reads of its durable segments (no append needed first)" do
    # The storage-chaos harness read 0 of 4592 acked records off a healthy cluster: the read
    # handler only served segments already open in memory, and only the append path opened them,
    # so after a restart every pre-restart record answered :eof until some write touched its
    # segment. A read must recover from disk on its own.
    directory = Path.join(System.tmp_dir!(), "malachi_repl_cold_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    name = :"repl_cold_#{System.unique_integer([:positive])}"

    {:ok, first} = ReplicationServer.start_link(name: name, directory: directory)
    assert {:ok, 1} = ReplicationServer.replicate(name, @segment, [name], 0, records(["a", "b"]))
    GenServer.stop(first)

    {:ok, _second} = ReplicationServer.start_link(name: name, directory: directory)

    # first interaction is a READ, not an append
    assert read_values(name, @segment) == ["a", "b"]

    # a segment this server never stored still answers :eof, without creating files as a side effect
    unknown = {{"cold_none", 0}, 0}
    assert ReplicationServer.read(name, unknown, 0, 10) == :eof
    assert ReplicationServer.stored_bytes(name, unknown) == 0
  end

  test "opening a corrupt sealed copy warns and emits an integrity event naming the segment" do
    # Recovery knows the copy is damaged the moment it scans it. Reporting here, at the one place a
    # segment is opened, is what makes the damage visible immediately: the background scrub verifies
    # everything eventually, but on a slow cadence, so a node could otherwise serve short reads for
    # days without a word. async: false is not needed: the handler is scoped to this test's pid.
    directory = Path.join(System.tmp_dir!(), "malachi_repl_integrity_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    name = :"repl_integrity_#{System.unique_integer([:positive])}"

    {:ok, first} = ReplicationServer.start_link(name: name, directory: directory)
    assert {:ok, 2} = ReplicationServer.replicate(name, @segment, [name], 0, records(["a", "b", "c"]))
    GenServer.stop(first)

    # seal it (the scrub's subject: an immutable copy nothing re-scans) and rot a byte in the middle
    segment_directory = Layout.segment_directory(directory, @segment)
    [log_file] = Path.wildcard(Path.join(segment_directory, "*.log"))
    File.touch!(String.replace_suffix(log_file, ".log", ".sealed"))

    {pairs, _valid} = Record.decode_all(File.read!(log_file))
    {_record, position} = Enum.at(pairs, 1)
    flip_at = position + 12
    <<head::binary-size(flip_at), byte, tail::binary>> = File.read!(log_file)
    File.write!(log_file, <<head::binary, Bitwise.bxor(byte, 0xFF), tail::binary>>)

    parent = self()
    handler_id = "integrity-test-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:malachi, :storage, :integrity],
      fn name, measurements, metadata, _config -> send(parent, {:telemetry, name, measurements, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        {:ok, _second} = ReplicationServer.start_link(name: name, directory: directory)
        # the first read is what opens (recovers) the segment
        assert read_values(name, @segment) == ["a"]
      end)

    assert log =~ "failed verification at byte #{position}"
    assert log =~ "bad_crc"
    assert log =~ "sealed segment"

    assert_receive {:telemetry, [:malachi, :storage, :integrity], %{position: ^position, unreadable_bytes: unreadable},
                    %{result: :bad_crc, sealed: true, source: :recover, segment: @segment}}

    assert unreadable > 0
  end

  test "stored_bytes reads the on-disk size without opening; durable_end recovers where end_offset cannot" do
    directory = Path.join(System.tmp_dir!(), "malachi_repl_probe_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    name = :"repl_probe_#{System.unique_integer([:positive])}"

    {:ok, first} = ReplicationServer.start_link(name: name, directory: directory)

    # nothing stored yet: both probes answer the empty shape
    assert ReplicationServer.stored_bytes(name, @segment) == 0

    assert {:ok, 1} = ReplicationServer.replicate(name, @segment, [name], 0, records(["a", "b"]))
    bytes = ReplicationServer.stored_bytes(name, @segment)
    assert bytes > 0
    GenServer.stop(first)

    {:ok, _second} = ReplicationServer.start_link(name: name, directory: directory)

    # the restarted server has not opened the segment: end_offset says :empty (its documented
    # contract), stored_bytes still sees the durable files, and durable_end recovers the true end,
    # which is exactly the gap the sealed-copy integrity probe needs closed
    assert ReplicationServer.end_offset(name, @segment) == :empty
    assert ReplicationServer.stored_bytes(name, @segment) == bytes
    assert ReplicationServer.durable_end(name, @segment, 0) == 2
  end

  # Counts the fsyncs that actually happen (a sync with nothing buffered is a no-op and does not
  # count), so a test can prove that group commit coalesces them. Kept local to this file with its own
  # table: sharing a global counter with other test modules would couple async tests through mutable
  # global state.
  defmodule CountingStore do
    @behaviour Malachi.Storage.SegmentStore
    alias Malachi.Storage.ElixirStore

    @impl true
    def sync(handle) do
      if ElixirStore.pending?(handle), do: :ets.update_counter(:repl_gc_syncs, :n, 1)
      ElixirStore.sync(handle)
    end

    @impl true
    def open(dir, id, opts), do: ElixirStore.open(dir, id, opts)
    @impl true
    def recover(dir, id, opts), do: ElixirStore.recover(dir, id, opts)
    @impl true
    def open_read(dir, id, opts), do: ElixirStore.open_read(dir, id, opts)
    @impl true
    def append(handle, records), do: ElixirStore.append(handle, records)
    @impl true
    def read(handle, offset, max), do: ElixirStore.read(handle, offset, max)
    @impl true
    def seal(handle), do: ElixirStore.seal(handle)
    @impl true
    def next_offset(handle), do: ElixirStore.next_offset(handle)
    @impl true
    def sealed?(handle), do: ElixirStore.sealed?(handle)
    @impl true
    def pending?(handle), do: ElixirStore.pending?(handle)
    @impl true
    def should_seal?(handle, now_ms), do: ElixirStore.should_seal?(handle, now_ms)
    @impl true
    def close(handle), do: ElixirStore.close(handle)
    @impl true
    def verify(dir, id, opts), do: ElixirStore.verify(dir, id, opts)
    @impl true
    def integrity(handle), do: ElixirStore.integrity(handle)
  end

  describe "group commit under replication" do
    test "grouped replicate commits durably on every replica and coalesces fsyncs" do
      :ets.new(:repl_gc_syncs, [:named_table, :public, :set])
      :ets.insert(:repl_gc_syncs, {:n, 0})

      gc = [group_commit: true, group_commit_interval_ms: 40, store: CountingStore]
      [primary, f1, f2] = replica_set = [start_broker(gc), start_broker(gc), start_broker(gc)]

      results =
        1..30
        |> Task.async_stream(
          fn i -> ReplicationServer.replicate(primary, @segment, replica_set, 0, records(["g#{i}"])) end,
          max_concurrency: 30,
          timeout: 15_000
        )
        |> Enum.map(fn {:ok, r} -> r end)

      assert Enum.all?(results, &match?({:ok, _}, &1))

      primary_values = read_values(primary, @segment)
      assert length(primary_values) == 30

      # The reply arrives on QUORUM durability (primary + one follower), so the other follower may
      # still hold everything in its group-commit buffer until its own flush tick fires, and buffered
      # records are not readable (read clamps to the flushed end). Wait for both to converge instead
      # of racing the second follower's tick, which is exactly what flaked on slower CI scheduling.
      assert eventually(fn -> read_values(f1, @segment) == primary_values end),
             "f1 should converge to the primary, got #{inspect(read_values(f1, @segment))}"

      assert eventually(fn -> read_values(f2, @segment) == primary_values end),
             "f2 should converge to the primary, got #{inspect(read_values(f2, @segment))}"

      # The point: 30 batches across 3 replicas is 90 per-batch fsyncs without coalescing; with the
      # burst landing in a couple of flush ticks it must be far fewer. Allow slack for tick straddling.
      [{:n, fsyncs}] = :ets.lookup(:repl_gc_syncs, :n)
      assert fsyncs > 0
      assert fsyncs <= 18, "expected coalesced fsyncs, saw #{fsyncs} for 30 batches x 3 replicas"
    end

    test "the reply waits for the flush (never precedes durability), including with no followers" do
      primary = start_broker(group_commit: true, group_commit_interval_ms: 100)

      t0 = System.monotonic_time(:millisecond)
      assert {:ok, 0} = ReplicationServer.replicate(primary, @segment, [primary], 0, records(["solo"]))
      elapsed = System.monotonic_time(:millisecond) - t0

      assert elapsed >= 50, "replied in #{elapsed}ms, before the ~100ms flush tick could have fsynced"
      assert read_values(primary, @segment) == ["solo"]
    end

    test "no_quorum still fails fast under group commit when a majority is dead" do
      primary = start_broker(group_commit: true, group_commit_interval_ms: 20, follow_timeout: 300)
      dead1 = :"dead_#{System.unique_integer([:positive])}"
      dead2 = :"dead_#{System.unique_integer([:positive])}"

      assert {:error, :no_quorum} =
               ReplicationServer.replicate(primary, @segment, [primary, dead1, dead2], 0, records(["x"]))
    end
  end

  test "delete removes a stored segment's data and is idempotent" do
    ref = start_broker()
    assert {:ok, 1} = ReplicationServer.replicate(ref, @segment, [ref], 0, records(["a", "b"]))
    assert read_values(ref, @segment) == ["a", "b"]

    assert ReplicationServer.delete(ref, @segment) == :ok
    # the segment is gone. A read finds nothing (:eof -> [])
    assert read_values(ref, @segment) == []

    # deleting an already-removed (now unknown) segment is still :ok
    assert ReplicationServer.delete(ref, @segment) == :ok
  end

  test "delete on an unreachable replica is best-effort (:ok, does not crash the caller)" do
    ref = start_broker()
    :ok = stop_supervised!(ref)

    # a dead/unreachable replica (a down cluster node during a retention sweep) must not crash us
    assert ReplicationServer.delete(ref, @segment) == :ok
  end

  test "a write still commits with one follower down (quorum tolerated)" do
    [primary, f1, f2] = replica_set = [start_broker(), start_broker(), start_broker()]
    :ok = stop_supervised!(f2)

    # primary + f1 = 2 of 3 is a quorum
    assert {:ok, 0} = ReplicationServer.replicate(primary, @segment, replica_set, 0, records(["a"]))
    assert read_values(primary, @segment) == ["a"]
    assert read_values(f1, @segment) == ["a"]
  end

  test "a write fails to commit when a quorum is unreachable" do
    [primary, f1, f2] = replica_set = [start_broker(), start_broker(), start_broker()]
    :ok = stop_supervised!(f1)
    :ok = stop_supervised!(f2)

    assert {:error, :no_quorum} = ReplicationServer.replicate(primary, @segment, replica_set, 0, records(["a"]))
  end

  test "a single-replica segment commits on the primary alone" do
    primary = start_broker()
    assert {:ok, 2} = ReplicationServer.replicate(primary, @segment, [primary], 0, records(["a", "b", "c"]))
    assert read_values(primary, @segment) == ["a", "b", "c"]
  end

  test "successive batches replicate with contiguous offsets" do
    [primary | _] = replica_set = [start_broker(), start_broker(), start_broker()]

    assert {:ok, 1} = ReplicationServer.replicate(primary, @segment, replica_set, 0, records(["a", "b"]))
    assert {:ok, 3} = ReplicationServer.replicate(primary, @segment, replica_set, 0, records(["c", "d"]))

    for ref <- replica_set do
      assert read_values(ref, @segment) == ["a", "b", "c", "d"]
    end
  end

  test "a segment opens at its base_offset so offsets continue the range" do
    [primary, f1, _f2] = replica_set = [start_broker(), start_broker(), start_broker()]

    # this segment starts at range-relative offset 100
    assert {:ok, 101} = ReplicationServer.replicate(primary, @segment, replica_set, 100, records(["a", "b"]))

    # records live at offsets 100..101 on every replica; nothing exists below the base
    for ref <- [primary, f1] do
      assert read_values(ref, @segment, 100) == ["a", "b"]
      assert {:error, :out_of_range} = ReplicationServer.read(ref, @segment, 0, 100)
    end
  end

  test "replicate on a non-primary is rejected" do
    [primary, f1, _f2] = replica_set = [start_broker(), start_broker(), start_broker()]
    assert {:error, :not_primary} = ReplicationServer.replicate(f1, @segment, replica_set, 0, records(["a"]))
    # nothing was stored anywhere
    assert read_values(primary, @segment) == []
  end

  test "an empty batch is rejected" do
    [primary | _] = replica_set = [start_broker(), start_broker(), start_broker()]
    assert {:error, :empty} = ReplicationServer.replicate(primary, @segment, replica_set, 0, [])
  end

  test "an empty replica set is rejected (no crash)" do
    primary = start_broker()
    assert {:error, :empty_replica_set} = ReplicationServer.replicate(primary, @segment, [], 0, records(["a"]))
    # the server is still alive and usable afterwards
    assert {:ok, 0} = ReplicationServer.replicate(primary, @segment, [primary], 0, records(["a"]))
  end

  test "a duplicated replica set does not deadlock the primary" do
    [primary, f1] = [start_broker(), start_broker()]
    # primary listed twice: must not make the server call itself
    assert {:ok, 0} =
             ReplicationServer.replicate(primary, @segment, [primary, primary, f1], 0, records(["a"]))

    assert read_values(primary, @segment) == ["a"]
    assert read_values(f1, @segment) == ["a"]
  end

  test "a follower that fell behind is automatically caught up from the primary" do
    [primary, behind, other] = full = [start_broker(), start_broker(), start_broker()]

    # all three get the first batch
    assert {:ok, 1} = ReplicationServer.replicate(primary, @segment, full, 0, records(["a", "b"]))

    # `behind` misses the next batch (excluded from this call's replica set, as if it were down)
    assert {:ok, 3} = ReplicationServer.replicate(primary, @segment, [primary, other], 0, records(["c", "d"]))
    assert read_values(behind, @segment) == ["a", "b"]

    # the next fan-out reaches `behind` past its end → it triggers a background catch-up and the
    # write still commits via primary + other
    assert {:ok, 5} = ReplicationServer.replicate(primary, @segment, full, 0, records(["e", "f"]))

    # out of band, `behind` pulls the gap from the primary and reaches the full log
    assert eventually(fn -> read_values(behind, @segment) == ~w(a b c d e f) end)

    # having rejoined, a later batch appends to `behind` directly within the fan-out
    assert {:ok, 7} = ReplicationServer.replicate(primary, @segment, full, 0, records(["g", "h"]))
    assert read_values(behind, @segment) == ~w(a b c d e f g h)
  end

  test "a brand-new replica added to an active segment backfills from the start and joins" do
    [a, b, c] = [start_broker(), start_broker(), start_broker()]

    # the segment is active on [a, b]
    assert {:ok, 1} = ReplicationServer.replicate(a, @segment, [a, b], 0, records(["w", "x"]))

    # c is added to the replica set; the next write fans out to c, which holds none of the segment
    assert {:ok, 3} = ReplicationServer.replicate(a, @segment, [a, b, c], 0, records(["y", "z"]))

    # c opens at the segment's base, sees the start gap, and backfills [0, head) out of band
    assert eventually(fn -> read_values(c, @segment) == ~w(w x y z) end)

    # having converged on the head, c now follows live within the fan-out
    assert {:ok, 5} = ReplicationServer.replicate(a, @segment, [a, b, c], 0, records(["p", "q"]))
    assert eventually(fn -> read_values(c, @segment) == ~w(w x y z p q) end)
  end

  test "a path-unsafe topic in a replicated segment id cannot escape the base directory" do
    name = :"repl_#{System.unique_integer([:positive])}"
    base = Path.join(System.tmp_dir!(), "malachi_repl_pt_#{System.unique_integer([:positive])}")
    # A topic carrying a single-level path traversal, as a compromised peer could send over replication.
    # The escape target lands next to `base` under the (writable) tmp dir, so without the fix it would be
    # created there; unique-suffix it to avoid colliding with other tests.
    evil_topic = "../evil_#{System.unique_integer([:positive])}"
    evil_segment = {{evil_topic, 0}, 0}
    escaped = Path.expand(Path.join(base, "#{evil_topic}-r0-s0"))

    on_exit(fn ->
      File.rm_rf!(base)
      File.rm_rf!(escaped)
    end)

    start_supervised!({ReplicationServer, name: name, directory: base}, id: name)

    assert {:ok, 1} = ReplicationServer.replicate(name, evil_segment, [name], 0, records(["a", "b"]))

    # The segment must not be written at the traversal target outside `base`.
    refute File.exists?(escaped)
    # It is still stored and readable, just under a safe encoded directory inside `base`.
    assert read_values(name, evil_segment) == ["a", "b"]
  end

  test "a follower that never held a non-zero-base segment is caught up from its base offset" do
    [primary, behind, other] = full = [start_broker(), start_broker(), start_broker()]

    # this segment starts at range-relative offset 100; `behind` is excluded from the first batches
    assert {:ok, 101} =
             ReplicationServer.replicate(primary, @segment, [primary, other], 100, records(["a", "b"]))

    assert {:ok, 103} =
             ReplicationServer.replicate(primary, @segment, [primary, other], 100, records(["c", "d"]))

    # `behind` holds nothing of this segment yet
    assert ReplicationServer.end_offset(behind, @segment) == :empty

    # the next fan-out reaches `behind` past its (empty) end → it triggers a background catch-up, which
    # must start from the segment's base_offset (100), not 0; the write still commits via primary + other
    assert {:ok, 105} =
             ReplicationServer.replicate(primary, @segment, full, 100, records(["e", "f"]))

    # `behind` backfills the whole segment from base 100 and converges on the head
    assert eventually(fn -> read_values(behind, @segment, 100) == ~w(a b c d e f) end)
  end

  defp eventually(check, remaining_ms \\ 2_000) do
    cond do
      check.() -> true
      remaining_ms <= 0 -> false
      true -> Process.sleep(20) && eventually(check, remaining_ms - 20)
    end
  end
end
