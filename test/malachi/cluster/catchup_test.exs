defmodule Malachi.Cluster.CatchupTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.Catchup
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record

  @segment {{"events", 0}, 0}

  defp start_broker do
    directory = Path.join(System.tmp_dir!(), "malachi_catchup_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    start_supervised!({ReplicationServer, directory: directory}, id: {:repl, System.unique_integer([:positive])})
  end

  defp records(values), do: for(value <- values, do: Record.new(value, key: value))

  # Seeds a source replica with `values` for the segment, starting at `base`.
  defp seed_source(values, base \\ 0) do
    source = start_broker()
    {:ok, _last} = ReplicationServer.replicate(source, @segment, [source], base, records(values))
    source
  end

  defp read_values(ref, offset, count) do
    case ReplicationServer.read(ref, @segment, offset, count) do
      {:ok, records} -> Enum.map(records, & &1.value)
      :eof -> []
    end
  end

  test "backfills an entire segment onto a fresh replica" do
    source = seed_source(["a", "b", "c", "d"])
    target = start_broker()

    assert {:ok, 4} = Catchup.run(target, source, @segment, 0, 4)
    assert read_values(target, 0, 100) == ["a", "b", "c", "d"]
  end

  test "catches a behind follower up over only the missing gap" do
    values = ["a", "b", "c", "d", "e"]
    source = seed_source(values)

    target = start_broker()
    # the follower already has the first two records
    {:ok, 1} = ReplicationServer.follow(target, @segment, 0, records(["a", "b"]))
    assert ReplicationServer.end_offset(target, @segment) == 2

    assert {:ok, 5} = Catchup.run(target, source, @segment, 2, 5)
    assert read_values(target, 0, 100) == values
  end

  test "preserves the segment's base offset when backfilling" do
    source = seed_source(["a", "b"], 100)
    target = start_broker()

    assert {:ok, 102} = Catchup.run(target, source, @segment, 100, 102)
    assert read_values(target, 100, 100) == ["a", "b"]
    assert ReplicationServer.end_offset(target, @segment) == 102
  end

  test "stops at what the source has when the source is itself behind" do
    source = seed_source(["a", "b"])
    target = start_broker()

    # ask to reach 5, but the source only holds 2
    assert {:ok, 2} = Catchup.run(target, source, @segment, 0, 5)
    assert read_values(target, 0, 100) == ["a", "b"]
  end

  test "is a no-op when the target is already at or past the target offset" do
    source = seed_source(["a", "b", "c"])
    target = start_broker()
    {:ok, 2} = ReplicationServer.follow(target, @segment, 0, records(["a", "b", "c"]))

    assert {:ok, 3} = Catchup.run(target, source, @segment, 3, 3)
    assert read_values(target, 0, 100) == ["a", "b", "c"]
  end

  test "end_offset reports :empty for a segment the replica does not hold" do
    target = start_broker()
    assert ReplicationServer.end_offset(target, @segment) == :empty
  end
end
