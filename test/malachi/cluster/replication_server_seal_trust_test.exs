defmodule Malachi.Cluster.ReplicationServerSealTrustTest do
  @moduledoc """
  A server that restarts over a sealed copy trusts the seal marker only after the segment's directory has
  been fsynced (`Malachi.Log.recover/2`), and a failed fsync takes the copy out of service instead of
  answering it as fenced. The fsync leaves nothing on disk to inspect, so the calls are watched.

  async: false because call tracing is set per function, globally for the VM (`Malachi.Test.CallTrace`).
  """
  use ExUnit.Case, async: false

  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record
  alias Malachi.Storage.Directory
  alias Malachi.Storage.Layout
  alias Malachi.Test.CallTrace
  alias Malachi.Test.StorageFaults
  alias Malachi.Test.TmpDir

  @segment {{"events", 0}, 0}

  setup do
    directory = TmpDir.path("malachi_seal_trust")
    on_exit(fn -> File.rm_rf!(directory) end)

    %{directory: directory, name: :"seal_trust_#{System.unique_integer([:positive])}"}
  end

  test "a restart over a sealed copy whose directory cannot be fsynced refuses writes and latches the copy", %{
    directory: directory,
    name: name
  } do
    start_server(name, directory)
    seal_one_record!(name)
    stop_server(name)

    StorageFaults.make_unsyncable!(Layout.segment_directory(directory, @segment))
    start_server(name, directory)

    ExUnit.CaptureLog.capture_log(fn ->
      assert ReplicationServer.fenced_segments(name, [{@segment, 0}]) == {:ok, %{}}
    end)

    assert {:ok, failed} = ReplicationServer.failed_segments(name, [@segment])
    assert MapSet.member?(failed, @segment)

    assert ReplicationServer.replicate(name, @segment, [name], 0, records(["b"])) ==
             {:error, {:storage, {:seal_marker_unsynced, :eacces}}}
  end

  test "a restart over a sealed copy fsyncs its directory once, and later fenced checks do not repeat it", %{
    directory: directory,
    name: name
  } do
    start_server(name, directory)
    seal_one_record!(name)
    stop_server(name)
    pid = start_server(name, directory)
    segment_directory = Layout.segment_directory(directory, @segment)

    first =
      CallTrace.calls([{Directory, :sync, 1}], [pids: [pid]], fn ->
        assert {:ok, %{@segment => {1, _bytes}}} = ReplicationServer.fenced_segments(name, [{@segment, 0}])
      end)

    assert first == [{Directory, :sync, [segment_directory]}]

    second =
      CallTrace.calls([{Directory, :sync, 1}], [pids: [pid]], fn ->
        assert {:ok, %{@segment => {1, _bytes}}} = ReplicationServer.fenced_segments(name, [{@segment, 0}])
      end)

    assert second == []
  end

  test "a restarted copy whose segment directory cannot be examined answers an error, not end of log", %{
    directory: directory,
    name: name
  } do
    start_server(name, directory)
    seal_one_record!(name)
    stop_server(name)

    StorageFaults.make_inaccessible!(Layout.segment_directory(directory, @segment))
    start_server(name, directory)

    ExUnit.CaptureLog.capture_log(fn ->
      assert {:error, {:storage, _reason}} = ReplicationServer.read(name, @segment, 0, 1)
    end)
  end

  test "a restarted copy whose seal marker cannot be examined is not read as unfenced, and is not latched", %{
    directory: directory,
    name: name
  } do
    start_server(name, directory)
    seal_one_record!(name)
    stop_server(name)

    segment_directory = Layout.segment_directory(directory, @segment)
    StorageFaults.make_inaccessible!(segment_directory)
    start_server(name, directory)

    # Not reported as fenced (it cannot be examined, so the open fails closed), and not latched either: the
    # stat error may clear, so the copy stays in service and is looked at again on the next request.
    ExUnit.CaptureLog.capture_log(fn ->
      assert ReplicationServer.fenced_segments(name, [{@segment, 0}]) == {:ok, %{}}
    end)

    # A write to the copy is refused, with the storage error, rather than taken as unfenced.
    assert {:error, {:storage, _reason}} = ReplicationServer.replicate(name, @segment, [name], 0, records(["b"]))

    assert {:ok, failed} = ReplicationServer.failed_segments(name, [@segment])
    refute MapSet.member?(failed, @segment)
  end

  test "a copy whose directory fsync keeps failing is latched by its first refused write", %{
    directory: directory,
    name: name
  } do
    start_server(name, directory)
    seal_one_record!(name)
    stop_server(name)

    StorageFaults.make_unsyncable!(Layout.segment_directory(directory, @segment))
    start_server(name, directory)

    ExUnit.CaptureLog.capture_log(fn ->
      assert {:error, {:storage, {:seal_marker_unsynced, _reason}}} =
               ReplicationServer.replicate(name, @segment, [name], 0, records(["b"]))
    end)

    assert {:ok, failed} = ReplicationServer.failed_segments(name, [@segment])
    assert MapSet.member?(failed, @segment)
  end

  test "fenced probes of a copy whose directory fsync keeps failing latch it at once", %{
    directory: directory,
    name: name
  } do
    start_server(name, directory)
    seal_one_record!(name)
    stop_server(name)

    StorageFaults.make_unsyncable!(Layout.segment_directory(directory, @segment))
    start_server(name, directory)

    ExUnit.CaptureLog.capture_log(fn ->
      assert ReplicationServer.fenced_segments(name, [{@segment, 0}]) == {:ok, %{}}
    end)

    assert {:ok, failed} = ReplicationServer.failed_segments(name, [@segment])
    assert MapSet.member?(failed, @segment)
  end

  test "a read of a copy whose directory fsync keeps failing latches it at once", %{
    directory: directory,
    name: name
  } do
    start_server(name, directory)
    seal_one_record!(name)
    stop_server(name)

    StorageFaults.make_unsyncable!(Layout.segment_directory(directory, @segment))
    start_server(name, directory)

    ExUnit.CaptureLog.capture_log(fn ->
      assert {:error, {:storage, _reason}} = ReplicationServer.read(name, @segment, 0, 1)
    end)

    assert {:ok, failed} = ReplicationServer.failed_segments(name, [@segment])
    assert MapSet.member?(failed, @segment)
  end

  test "a copy whose directory cannot be listed is refused on every write and never latched",
       %{directory: directory, name: name} do
    start_server(name, directory)
    seal_one_record!(name)
    stop_server(name)

    StorageFaults.make_inaccessible!(Layout.segment_directory(directory, @segment))
    start_server(name, directory)

    ExUnit.CaptureLog.capture_log(fn ->
      for _attempt <- 1..5 do
        assert {:error, {:storage, _reason}} = ReplicationServer.replicate(name, @segment, [name], 0, records(["b"]))
      end
    end)

    assert {:ok, failed} = ReplicationServer.failed_segments(name, [@segment])
    refute MapSet.member?(failed, @segment)
  end

  test "a copy whose directory failed to list once is read again after the failure clears, not latched", %{
    directory: directory,
    name: name
  } do
    start_server(name, directory)
    seal_one_record!(name)
    stop_server(name)

    segment_directory = Layout.segment_directory(directory, @segment)
    StorageFaults.make_inaccessible!(segment_directory)
    start_server(name, directory)

    ExUnit.CaptureLog.capture_log(fn ->
      assert {:error, {:storage, _reason}} = ReplicationServer.read(name, @segment, 0, 1)
    end)

    File.chmod!(segment_directory, 0o755)

    assert {:ok, [_record]} = ReplicationServer.read(name, @segment, 0, 1)
    assert {:ok, failed} = ReplicationServer.failed_segments(name, [@segment])
    refute MapSet.member?(failed, @segment)
  end

  test "a read given no time to answer reports the replica unreachable instead of waiting", %{
    directory: directory,
    name: name
  } do
    start_server(name, directory)

    # Suspended, so the server cannot answer at all: only the timeout can end the call. The five-second default
    # would also answer :unreachable, so the elapsed time is what shows the given timeout was used.
    :ok = :sys.suspend(name)
    {micros, answer} = :timer.tc(fn -> ReplicationServer.read(name, @segment, 0, 1, 50) end)
    :ok = :sys.resume(name)

    assert answer == {:error, :unreachable}
    assert micros < 2_000_000
  end

  test "a restart over an active segment fsyncs no directory on its first write", %{directory: directory, name: name} do
    start_server(name, directory)
    assert {:ok, 0} = ReplicationServer.replicate(name, @segment, [name], 0, records(["a"]))
    stop_server(name)
    pid = start_server(name, directory)

    calls =
      CallTrace.calls([{Directory, :sync, 1}], [pids: [pid]], fn ->
        assert {:ok, 1} = ReplicationServer.replicate(name, @segment, [name], 1, records(["b"]))
      end)

    assert calls == []
  end

  defp start_server(name, directory) do
    start_supervised!({ReplicationServer, [name: name, directory: directory]}, id: name)
  end

  defp stop_server(name), do: stop_supervised!(name)

  defp seal_one_record!(name) do
    assert {:ok, 0} = ReplicationServer.replicate(name, @segment, [name], 0, records(["a"]))
    assert {:ok, 1, _bytes} = ReplicationServer.seal(name, @segment, 0)
  end

  defp records(values), do: for(value <- values, do: Record.new(value, key: value))
end
