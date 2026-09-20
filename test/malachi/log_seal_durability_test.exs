defmodule Malachi.LogSealDurabilityTest do
  @moduledoc """
  The whole-log seal marker is the fence after a restart, so it has to exist after a power failure,
  not only have durable content. That takes an fsync of the log's directory after the marker is
  written, which leaves nothing on disk to inspect, so the calls are watched instead.

  async: false because call tracing is set per function, globally for the VM (`Malachi.Test.CallTrace`).
  """
  use ExUnit.Case, async: false

  alias Malachi.Log
  alias Malachi.Log.Record
  alias Malachi.Storage.Directory
  alias Malachi.Test.CallTrace

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
end
