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

  describe "Directory.sync/1" do
    test "fsyncs an existing directory", %{tmp_dir: dir} do
      assert Directory.sync(dir) == :ok
    end

    test "answers the error for a directory that does not exist", %{tmp_dir: tmp} do
      assert Directory.sync(Path.join(tmp, "missing")) == {:error, :enoent}
    end
  end
end
