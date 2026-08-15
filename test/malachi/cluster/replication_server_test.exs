defmodule Malachi.Cluster.ReplicationServerTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record

  @segment {{"events", 0}, 0}

  defp start_broker do
    name = :"repl_#{System.unique_integer([:positive])}"
    directory = Path.join(System.tmp_dir!(), "malachi_repl_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    start_supervised!({ReplicationServer, name: name, directory: directory}, id: name)
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

    # every replica (primary included) durably stored the same records at the same offsets
    for ref <- [primary, f1, f2] do
      assert read_values(ref, @segment) == ["a", "b"]
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
