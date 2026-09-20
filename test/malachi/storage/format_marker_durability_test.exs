defmodule Malachi.Storage.FormatMarkerDurabilityTest do
  @moduledoc """
  The marker is only as durable as its directory entry: a rename that a power failure can undo brings
  back the previous marker, or none, after new-format bytes were written. These tests watch the calls
  and assert the directory is fsynced after the rename, on both paths that write the marker.

  async: false because call tracing is set per function, globally for the VM (`Malachi.Test.CallTrace`).
  """
  use ExUnit.Case, async: false

  alias Malachi.Storage.Directory
  alias Malachi.Storage.FormatMarker
  alias Malachi.Test.CallTrace

  @moduletag :tmp_dir

  @traced [{File, :rename, 2}, {Directory, :sync, 1}]

  test "creating the marker fsyncs its directory after the rename", %{tmp_dir: dir} do
    calls = CallTrace.calls(@traced, fn -> assert FormatMarker.write(dir, 1, "0.12.0") == :ok end)

    assert calls == [
             {File, :rename, [Path.join(dir, "malachi.format.tmp"), FormatMarker.path(dir)]},
             {Directory, :sync, [dir]}
           ]
  end

  test "raising the marker fsyncs its directory after the rename", %{tmp_dir: dir} do
    :ok = FormatMarker.write(dir, 1, "0.12.0")

    calls =
      CallTrace.calls(@traced, fn ->
        assert FormatMarker.raise_to(dir, 2, supported: 2, requires: "0.13.0", version: "0.13.0") == :ok
      end)

    assert calls == [
             {File, :rename, [Path.join(dir, "malachi.format.tmp"), FormatMarker.path(dir)]},
             {Directory, :sync, [dir]}
           ]
  end

  test "raising to the level already on disk fsyncs the directory again", %{tmp_dir: dir} do
    # The retry of a raise whose rename landed and whose fsync failed: the marker reads as the level
    # asked for, and answering :ok is what lets the caller write the first new-format byte.
    :ok = FormatMarker.write(dir, 1, "0.12.0")

    calls = CallTrace.calls(@traced, fn -> assert FormatMarker.raise_to(dir, 1) == :ok end)

    assert calls == [{Directory, :sync, [dir]}]
  end

  test "accepting an existing marker at boot fsyncs its directory", %{tmp_dir: dir} do
    :ok = FormatMarker.write(dir, 1, "0.12.0")

    calls = CallTrace.calls(@traced, fn -> assert FormatMarker.enforce(dir) == :ok end)

    assert calls == [{Directory, :sync, [dir]}]
  end

  test "a directory that cannot be fsynced refuses the start", %{tmp_dir: dir} do
    :ok = FormatMarker.write(dir, 1, "0.12.0")
    # Write and traverse, but no read: the marker file still opens, the directory does not, so the
    # failure is the fsync of the trust path and nothing else.
    File.chmod!(dir, 0o300)
    on_exit(fn -> File.chmod(dir, 0o700) end)

    # Root ignores the permission bits, so the refusal can only be observed as a regular user.
    if Directory.sync(dir) == {:error, :eacces} do
      assert {:refuse, {:io, :eacces, _path}} = FormatMarker.enforce(dir)
    end
  end

  describe "Directory.sync/1" do
    test "fsyncs an existing directory", %{tmp_dir: dir} do
      assert Directory.sync(dir) == :ok
    end

    test "answers the error for a directory that does not exist", %{tmp_dir: tmp} do
      assert Directory.sync(Path.join(tmp, "missing")) == {:error, :enoent}
    end
  end
end
