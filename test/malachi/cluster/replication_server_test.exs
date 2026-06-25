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

  defp read_values(ref, segment) do
    case ReplicationServer.read(ref, segment, 0, 100) do
      {:ok, records} -> Enum.map(records, & &1.value)
      :eof -> []
    end
  end

  test "the primary replicates to all followers and commits the batch" do
    [primary, f1, f2] = replica_set = [start_broker(), start_broker(), start_broker()]

    assert {:ok, 1} = ReplicationServer.replicate(primary, @segment, replica_set, records(["a", "b"]))

    # every replica (primary included) durably stored the same records at the same offsets
    for ref <- [primary, f1, f2] do
      assert read_values(ref, @segment) == ["a", "b"]
    end
  end

  test "a write still commits with one follower down (quorum tolerated)" do
    [primary, f1, f2] = replica_set = [start_broker(), start_broker(), start_broker()]
    :ok = stop_supervised!(f2)

    # primary + f1 = 2 of 3 is a quorum
    assert {:ok, 0} = ReplicationServer.replicate(primary, @segment, replica_set, records(["a"]))
    assert read_values(primary, @segment) == ["a"]
    assert read_values(f1, @segment) == ["a"]
  end

  test "a write fails to commit when a quorum is unreachable" do
    [primary, f1, f2] = replica_set = [start_broker(), start_broker(), start_broker()]
    :ok = stop_supervised!(f1)
    :ok = stop_supervised!(f2)

    assert {:error, :no_quorum} = ReplicationServer.replicate(primary, @segment, replica_set, records(["a"]))
  end

  test "a single-replica segment commits on the primary alone" do
    primary = start_broker()
    assert {:ok, 2} = ReplicationServer.replicate(primary, @segment, [primary], records(["a", "b", "c"]))
    assert read_values(primary, @segment) == ["a", "b", "c"]
  end

  test "successive batches replicate with contiguous offsets" do
    [primary | _] = replica_set = [start_broker(), start_broker(), start_broker()]

    assert {:ok, 1} = ReplicationServer.replicate(primary, @segment, replica_set, records(["a", "b"]))
    assert {:ok, 3} = ReplicationServer.replicate(primary, @segment, replica_set, records(["c", "d"]))

    for ref <- replica_set do
      assert read_values(ref, @segment) == ["a", "b", "c", "d"]
    end
  end

  test "replicate on a non-primary is rejected" do
    [primary, f1, _f2] = replica_set = [start_broker(), start_broker(), start_broker()]
    assert {:error, :not_primary} = ReplicationServer.replicate(f1, @segment, replica_set, records(["a"]))
    # nothing was stored anywhere
    assert read_values(primary, @segment) == []
  end

  test "an empty batch is rejected" do
    [primary | _] = replica_set = [start_broker(), start_broker(), start_broker()]
    assert {:error, :empty} = ReplicationServer.replicate(primary, @segment, replica_set, [])
  end

  test "an empty replica set is rejected (no crash)" do
    primary = start_broker()
    assert {:error, :empty_replica_set} = ReplicationServer.replicate(primary, @segment, [], records(["a"]))
    # the server is still alive and usable afterwards
    assert {:ok, 0} = ReplicationServer.replicate(primary, @segment, [primary], records(["a"]))
  end

  test "a duplicated replica set does not deadlock the primary" do
    [primary, f1] = [start_broker(), start_broker()]
    # primary listed twice: must not make the server call itself
    assert {:ok, 0} =
             ReplicationServer.replicate(primary, @segment, [primary, primary, f1], records(["a"]))

    assert read_values(primary, @segment) == ["a"]
    assert read_values(f1, @segment) == ["a"]
  end
end
