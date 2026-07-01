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

  test "delete removes a stored segment's data and is idempotent" do
    ref = start_broker()
    assert {:ok, 1} = ReplicationServer.replicate(ref, @segment, [ref], 0, records(["a", "b"]))
    assert read_values(ref, @segment) == ["a", "b"]

    assert ReplicationServer.delete(ref, @segment) == :ok
    # the segment is gone — a read finds nothing (:eof -> [])
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

  defp eventually(check, remaining_ms \\ 2_000) do
    cond do
      check.() -> true
      remaining_ms <= 0 -> false
      true -> Process.sleep(20) && eventually(check, remaining_ms - 20)
    end
  end
end
