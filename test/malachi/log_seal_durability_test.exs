defmodule Malachi.LogSealDurabilityTest do
  @moduledoc """
  The whole-log seal marker is the fence after a restart, so it has to exist after a power failure,
  not only have durable content. That takes an fsync of the log's directory after the marker is
  written, which leaves nothing on disk to inspect, so the calls are watched instead. Recovery is the
  other half: a marker found on disk is believed only after the same fsync, and a failed fsync fails the
  recovery rather than unfencing the log.

  async: false because call tracing is set per function, globally for the VM (`Malachi.Test.CallTrace`).
  """
  use ExUnit.Case, async: false

  alias Malachi.Log
  alias Malachi.Log.Record
  alias Malachi.Storage.Directory
  alias Malachi.Test.CallTrace
  alias Malachi.Test.StorageFaults

  @moduletag :tmp_dir

  test "sealing fsyncs the log directory after writing the seal marker", %{tmp_dir: directory} do
    {:ok, log} = Log.open(directory)
    {:ok, log, _first, _last} = Log.append(log, [Record.new("a")])
    marker = Log.seal_marker_path(directory)

    calls =
      CallTrace.calls([{File, :write, 3}, {Directory, :sync, 1}], fn ->
        assert {:ok, _sealed} = Log.seal(log)
      end)

    marker_written = Enum.find_index(calls, &match?({File, :write, [^marker | _]}, &1))
    assert marker_written, "the seal marker was not written through File.write/3"
    assert {Directory, :sync, [directory]} in Enum.drop(calls, marker_written + 1)
  end

  test "sealing a log that is already sealed fsyncs its directory again", %{tmp_dir: directory} do
    # The retry of a seal whose marker was written and whose directory fsync failed: the marker is
    # visible, and this :ok is what tells the caller the fence is final.
    {:ok, log} = Log.open(directory)
    {:ok, log, _first, _last} = Log.append(log, [Record.new("a")])
    {:ok, sealed} = Log.seal(log)

    calls =
      CallTrace.calls([{File, :write, 3}, {Directory, :sync, 1}], fn -> assert {:ok, ^sealed} = Log.seal(sealed) end)

    assert calls == [{Directory, :sync, [directory]}]
  end

  describe "recovering a sealed log" do
    test "a visible seal marker fsyncs the log directory before the log is answered as sealed", %{tmp_dir: directory} do
      seal_log_with_one_record(directory)

      calls = CallTrace.calls([{Directory, :sync, 1}], fn -> assert {:ok, _recovered} = Log.recover(directory) end)

      assert calls == [{Directory, :sync, [directory]}]
      assert {:ok, recovered} = Log.recover(directory)
      assert Log.sealed?(recovered)
    end

    test "recovering a log with no seal marker fsyncs nothing", %{tmp_dir: directory} do
      {:ok, log} = Log.open(directory)
      {:ok, log, _first, _last} = Log.append(log, [Record.new("a")])
      {:ok, _log} = Log.sync(log)

      calls = CallTrace.calls([{Directory, :sync, 1}], fn -> assert {:ok, _recovered} = Log.recover(directory) end)

      assert calls == []
      assert {:ok, recovered} = Log.recover(directory)
      refute Log.sealed?(recovered)
    end

    test "a seal marker whose directory cannot be fsynced fails the recovery instead of trusting it", %{
      tmp_dir: directory
    } do
      seal_log_with_one_record(directory)
      StorageFaults.make_unsyncable!(directory)

      assert {:error, {:seal_marker_unsynced, :eacces}} = Log.recover(directory)
    end

    test "a seal marker whose directory fsync fails is reported at once, with a single attempt", %{
      tmp_dir: directory
    } do
      seal_log_with_one_record(directory)
      StorageFaults.make_unsyncable!(directory)

      calls =
        CallTrace.calls([{Directory, :sync, 1}], fn ->
          assert {:error, {:seal_marker_unsynced, :eacces}} = Log.recover(directory)
        end)

      assert calls == [{Directory, :sync, [directory]}]
    end

    test "a seal marker whose directory fsync failed is not trusted, and the next recovery syncs it again", %{
      tmp_dir: directory
    } do
      seal_log_with_one_record(directory)
      attempts = :counters.new(1, [])

      # The first directory fsync fails; every later one succeeds.
      sync_fun = fn dir ->
        :counters.add(attempts, 1, 1)

        if :counters.get(attempts, 1) == 1,
          do: {:error, :eio},
          else: Directory.sync(dir)
      end

      assert {:error, {:seal_marker_unsynced, :eio}} = Log.recover(directory, sync_fun: sync_fun)
      assert {:ok, recovered} = Log.recover(directory, sync_fun: sync_fun)
      assert Log.sealed?(recovered)
      assert :counters.get(attempts, 1) == 2
    end

    test "a seal marker with no segment files is fsynced and trusted as a sealed empty log", %{tmp_dir: directory} do
      File.write!(Log.seal_marker_path(directory), <<>>)

      calls = CallTrace.calls([{Directory, :sync, 1}], fn -> assert {:ok, _recovered} = Log.recover(directory) end)

      assert calls == [{Directory, :sync, [directory]}]
      assert {:ok, recovered} = Log.recover(directory)
      assert Log.sealed?(recovered)
    end

    test "a seal marker with no segment files that cannot be fsynced fails the recovery", %{tmp_dir: directory} do
      File.write!(Log.seal_marker_path(directory), <<>>)
      StorageFaults.make_unsyncable!(directory)

      assert {:error, {:seal_marker_unsynced, :eacces}} = Log.recover(directory)
    end

    test "a seal marker that cannot be examined fails the recovery instead of recovering an unsealed log", %{
      tmp_dir: directory
    } do
      seal_log_with_one_record(directory)
      StorageFaults.make_inaccessible!(directory)

      assert {:error, {:seal_marker_unreadable, :eacces}} = Log.recover(directory)
    end

    test "a segment directory that cannot be listed fails the recovery instead of recovering an empty log", %{
      tmp_dir: directory
    } do
      {:ok, log} = Log.open(directory)
      {:ok, log, _first, _last} = Log.append(log, [Record.new("a")])
      {:ok, _log} = Log.sync(log)
      # Search permission stays, so the segment file can be looked up by name but not listed.
      StorageFaults.make_unsyncable!(directory)

      assert {:error, {:segments_unreadable, :eacces}} = Log.recover(directory)
    end

    test "a hidden file named like a segment is not a segment", %{tmp_dir: directory} do
      # `Path.wildcard/1` skips dotfiles, so the listing must too: a hidden `.<digits>.log` would otherwise
      # be parsed as a base offset, and that parse raises.
      seal_log_with_one_record(directory)
      File.write!(Path.join(directory, ".00000000000000000000.log"), <<>>)

      assert {:ok, recovered} = Log.recover(directory)
      assert recovered.next_offset == 1
    end

    test "a recovered seal keeps its end offset", %{tmp_dir: directory} do
      seal_log_with_one_record(directory)

      assert {:ok, recovered} = Log.recover(directory)
      assert Log.sealed?(recovered)
      assert recovered.next_offset == 1
      assert {:ok, again} = Log.recover(directory)
      assert again.next_offset == 1
    end
  end

  defp seal_log_with_one_record(directory) do
    {:ok, log} = Log.open(directory)
    {:ok, log, _first, _last} = Log.append(log, [Record.new("a")])
    {:ok, sealed} = Log.seal(log)
    sealed
  end
end
