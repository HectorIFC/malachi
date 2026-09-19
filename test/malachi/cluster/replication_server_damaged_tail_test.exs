defmodule Malachi.Cluster.ReplicationServerDamagedTailTest do
  @moduledoc """
  A copy whose ACTIVE segment recovered with a preserved tail (rot, or a frame in a format this release
  does not know) must never be appended over, whether the write comes as a produce on the primary or
  as a replica append on a follower. The store refuses with `:damaged_tail`, and the server takes that
  as the storage failure it is: the caller gets the error, the copy is reported failed (which is what
  makes failover seal the segment on the intact copies, see `Malachi.Cluster.Failover`), and the bytes
  on disk stay exactly as they were.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record
  alias Malachi.Storage.Layout

  @segment {{"events", 0}, 0}

  defp records(values), do: for(value <- values, do: Record.new(value, key: value))

  defp start_server(name, directory),
    do: start_supervised!({ReplicationServer, [name: name, directory: directory]}, id: name)

  # Five records, one frame each, then a byte flipped inside the third frame's payload with the server
  # stopped, so the restart recovers the segment with rot after the second record.
  defp rotted_copy do
    name = :"damaged_#{System.unique_integer([:positive])}"
    directory = Path.join(System.tmp_dir!(), "malachi_damaged_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)

    start_server(name, directory)

    for {value, offset} <- Enum.with_index(~w(a b c d e)) do
      assert {:ok, ^offset} = ReplicationServer.replicate(name, @segment, [name], offset, records([value]))
    end

    :ok = stop_supervised(name)

    [log_file] = Path.wildcard(Path.join(Layout.segment_directory(directory, @segment), "*.log"))
    {frames, _valid_bytes} = Record.decode_all(File.read!(log_file))
    {_record, third} = Enum.at(frames, 2)
    flip_byte(log_file, third + 12)

    start_server(name, directory)
    {name, directory, log_file}
  end

  defp flip_byte(path, position) do
    {:ok, fd} = :file.open(path, [:read, :write, :raw, :binary])
    {:ok, <<byte>>} = :file.pread(fd, position, 1)
    :ok = :file.pwrite(fd, position, <<Bitwise.bxor(byte, 0xFF)>>)
    :ok = :file.close(fd)
  end

  test "a produce on the primary is refused and the damaged bytes stay on disk" do
    {name, _directory, log_file} = rotted_copy()
    before = File.read!(log_file)

    capture_log(fn ->
      assert ReplicationServer.replicate(name, @segment, [name], 0, records(["overwrite"])) ==
               {:error, {:storage, :damaged_tail}}
    end)

    assert File.read!(log_file) == before
    assert ReplicationServer.failed_segments(name, [@segment]) == {:ok, MapSet.new([@segment])}
  end

  test "a replica append on a follower is acked with the error and the damaged bytes stay on disk" do
    {follower, _directory, log_file} = rotted_copy()
    before = File.read!(log_file)

    capture_log(fn ->
      # The follower recovered two records, so the next append it expects starts at offset 2.
      GenServer.cast(follower, {:replica_append, @segment, 0, 2, records(["overwrite"]), -1, self()})
      assert_receive {:"$gen_cast", {:replica_ack, @segment, _ref, {:error, {:storage, :damaged_tail}}}}, 2_000
    end)

    assert File.read!(log_file) == before
    assert ReplicationServer.failed_segments(follower, [@segment]) == {:ok, MapSet.new([@segment])}
  end

  test "a restart does not clear the refusal, because the damage is still there" do
    {name, directory, log_file} = rotted_copy()
    before = File.read!(log_file)

    capture_log(fn ->
      assert {:error, {:storage, :damaged_tail}} =
               ReplicationServer.replicate(name, @segment, [name], 0, records(["overwrite"]))

      :ok = stop_supervised(name)
      start_server(name, directory)

      assert {:error, {:storage, :damaged_tail}} =
               ReplicationServer.replicate(name, @segment, [name], 0, records(["overwrite"]))
    end)

    assert File.read!(log_file) == before
  end
end
