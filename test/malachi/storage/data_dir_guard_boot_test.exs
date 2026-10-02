defmodule Malachi.Storage.DataDirGuardBootTest do
  @moduledoc """
  The application's own boot runs `Malachi.Storage.DataDirGuard` (#273), end to end: a peer node boots
  the whole application, with this suite's configuration, over a log directory holding a segment its
  control plane, formed now, would not know. The peer must halt (the refusal's `System.halt/1`) before
  anything serves, and the segment must still be there. A control boot over an empty directory starts.

  The guard's decisions are tested on their own in `Malachi.Storage.DataDirGuardTest`; this is what
  holds the boot to calling it, which no in-process test can do: the suite's application booted once,
  over a fresh directory, where the guard has nothing to say.

  async: false and tagged, like every test that starts peer nodes.
  """
  use ExUnit.Case, async: false

  @moduletag :multinode
  # Each test boots a whole application on a peer, which can take tens of seconds on a loaded runner.
  @moduletag timeout: 180_000

  alias Malachi.Storage.Layout
  alias Malachi.Test.Distribution
  alias Malachi.Test.TmpDir

  setup_all do
    :ok = Distribution.ensure_started()
  end

  # A peer configured exactly like this suite's node, with its own data directories, then the whole
  # application started on it. Returns the peer and what starting answered, or :halted when the peer went
  # down.
  defp boot_peer(log_dir, overrides \\ [], before_boot \\ fn _node -> :ok end) do
    {peer, node, name} = Distribution.start_peer("guard_boot")
    on_exit(fn -> Distribution.stop_peer(peer) end)
    ra_dir = TmpDir.path("malachi_guard_boot_ra_#{name}")
    on_exit(fn -> File.rm_rf!(ra_dir) end)

    env =
      Application.get_all_env(:malachi)
      |> Keyword.merge(log_data_dir: log_dir, ra_data_dir: ra_dir)
      |> Keyword.merge(overrides)

    :ok = :erpc.call(node, Application, :put_all_env, [[malachi: env]])
    :ok = before_boot.(node)

    try do
      {node, :erpc.call(node, Application, :ensure_all_started, [:malachi], 60_000)}
    catch
      :error, {:erpc, :noconnection} -> {node, :halted}
    end
  end

  defp segment_dir(log_dir) do
    path = Layout.segment_directory(log_dir, {{"orders", 0}, 0})
    File.mkdir_p!(path)
    path
  end

  test "a boot over a segment its control plane does not know halts before it serves, and keeps it" do
    log_dir = TmpDir.path("malachi_guard_boot_log")
    on_exit(fn -> File.rm_rf!(log_dir) end)
    segment = segment_dir(log_dir)

    assert {_node, :halted} = boot_peer(log_dir)
    assert File.dir?(segment)
  end

  # A sharded control plane has no unsharded member to be unknown: what refuses there is the ring store,
  # read before the boot starts it. Read after, it would always be known, and this boot would start.
  test "a sharded boot over a segment its ring store never knew halts too, and keeps it" do
    log_dir = TmpDir.path("malachi_guard_boot_sharded")
    on_exit(fn -> File.rm_rf!(log_dir) end)
    segment = segment_dir(log_dir)

    assert {_node, :halted} = boot_peer(log_dir, log_vnodes: 2)
    assert File.dir?(segment)
  end

  test "a boot over an empty log directory starts" do
    log_dir = TmpDir.path("malachi_guard_boot_empty")
    on_exit(fn -> File.rm_rf!(log_dir) end)

    assert {_node, {:ok, _apps}} = boot_peer(log_dir)
  end

  test "a sharded boot over an empty log directory starts" do
    log_dir = TmpDir.path("malachi_guard_boot_empty_sharded")
    on_exit(fn -> File.rm_rf!(log_dir) end)

    assert {_node, {:ok, _apps}} = boot_peer(log_dir, log_vnodes: 2)
  end

  # A single node asked for broker group commit with the default replication factor (3) does not get it,
  # and the boot is where it is told: the line goes through the application's own boot, not a direct call.
  test "a boot asked for group commit with a factor above 1 warns that it does not apply" do
    log_dir = TmpDir.path("malachi_guard_boot_gc")
    on_exit(fn -> File.rm_rf!(log_dir) end)
    log_file = TmpDir.path("malachi_guard_boot_gc_log")
    on_exit(fn -> File.rm_rf!(log_file) end)

    handler = %{config: %{file: String.to_charlist(log_file)}, level: :warning}

    before_boot = fn node ->
      :ok = :erpc.call(node, :logger, :add_handler, [:guard_boot_file, :logger_std_h, handler])
    end

    assert {node, {:ok, _apps}} = boot_peer(log_dir, [group_commit: true, log_replication_factor: 3], before_boot)
    # A file handler writes on its own schedule; make what the boot logged readable now.
    :ok = :erpc.call(node, :logger_std_h, :filesync, [:guard_boot_file])
    assert File.read!(log_file) =~ "MALACHI_LOG_REPLICATION_FACTOR=3"
  end
end
