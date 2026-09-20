defmodule Malachi.Storage.FormatGateIntegrationTest do
  @moduledoc """
  The rollback the format marker exists for, end to end on real segment files: a directory written by
  a newer release (an unknown frame at the tail of an active segment, a marker one level above what
  this binary reads) must be refused before any segment is opened, and must come out of the refusal
  byte-identical.

  The last test is the other half. It opens the same directory with the storage layer directly, the
  way an older release that never reads the marker would, and shows what the gate is protecting: the
  unknown frame is classified as rot, and the store no longer appends over it.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO
  import ExUnit.CaptureLog

  alias Malachi.Log.Record
  alias Malachi.Storage.ElixirStore
  alias Malachi.Storage.FormatMarker

  @moduletag :tmp_dir

  @store_opts [prealloc_bytes: 0]

  # Two shards' worth of segments, each written and synced by the real store, then one frame this
  # release cannot read appended to the end of the first shard's active segment.
  defp newer_directory(root) do
    for shard <- ["shard_0", "shard_1"] do
      {:ok, store} = ElixirStore.open(Path.join(root, shard), "segment-0", @store_opts)
      {:ok, store, _first, _last} = ElixirStore.append(store, [Record.new("a"), Record.new("b")])
      {:ok, store} = ElixirStore.sync(store)
      :ok = ElixirStore.close(store)
    end

    segment = Path.join([root, "shard_0", "segment-0.log"])
    File.write!(segment, unknown_frame(), [:append])
    File.write!(FormatMarker.path(root), FormatMarker.render(%{format: 2, written_by: "0.13.0", requires: "0.13.0"}))
    segment
  end

  # A well-formed frame whose magic this release does not know: what a format change looks like to it.
  defp unknown_frame do
    <<_magic::16, rest::binary>> = Record.encode(%{Record.new("from the future") | offset: 2})
    <<0x4D52::16, rest::binary>>
  end

  defp hashes(root) do
    root
    |> Path.join("**")
    |> Path.wildcard(match_dot: true)
    |> Enum.filter(&File.regular?/1)
    |> Map.new(&{&1, :crypto.hash(:sha256, File.read!(&1))})
  end

  test "the application's own boot ran the gate on the configured log data directory" do
    # The suite boots the application once (test_helper.exs) on a per-run directory, so the marker there
    # can only have been written by `Malachi.Application.start/2`.
    dir = Application.fetch_env!(:malachi, :log_data_dir)
    assert {:ok, {:ok, %{format: 1}}} = FormatMarker.read(dir)
  end

  test "the node refuses to start and every file is byte-identical afterwards", %{tmp_dir: root} do
    newer_directory(root)
    before = hashes(root)
    parent = self()

    stderr =
      capture_io(:stderr, fn ->
        capture_log(fn -> Malachi.Application.ensure_data_format(root, &send(parent, {:halted, &1})) end)
      end)

    assert_received {:halted, 78}
    assert stderr =~ "REFUSING TO START (exit 78):"
    assert stderr =~ "format 2"
    assert stderr =~ "at most format 1"
    assert stderr =~ "Start release 0.13.0 or newer"
    assert hashes(root) == before
  end

  test "a directory this release can read starts, and its marker covers every shard", %{tmp_dir: root} do
    newer_directory(root)
    File.rm!(FormatMarker.path(root))

    capture_log(fn ->
      assert Malachi.Application.ensure_data_format(root, fn status -> flunk("halted #{status}") end) == :ok
    end)

    assert {:ok, {:ok, %{format: 1}}} = FormatMarker.read(root)
    refute File.exists?(FormatMarker.path(Path.join(root, "shard_0")))
    refute File.exists?(FormatMarker.path(Path.join(root, "shard_1")))
  end

  test "without the gate, the store reads the unknown frame as rot and refuses to append over it", %{tmp_dir: root} do
    segment = newer_directory(root)
    before = File.read!(segment)

    {:ok, store} = ElixirStore.recover(Path.join(root, "shard_0"), "segment-0", @store_opts)

    assert %{reason: :bad_magic} = ElixirStore.integrity(store)
    assert ElixirStore.append(store, [Record.new("overwrite")]) == {:error, :damaged_tail}
    assert {:ok, [%Record{value: "a"}, %Record{value: "b"}]} = ElixirStore.read(store, 0, 10)
    :ok = ElixirStore.close(store)
    assert File.read!(segment) == before
  end
end
