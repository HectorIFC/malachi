Code.require_file("../../../benchmark/support/flush_regime.exs", __DIR__)

defmodule Malachi.Bench.FlushRegimeTest do
  use ExUnit.Case, async: true

  alias Malachi.Bench.FlushRegime

  @moduletag :tmp_dir

  # Lines in the shape the kernel writes them (proc(5)), including optional fields before the `-`.
  @mountinfo """
  22 1 259:2 / / rw,relatime shared:1 - ext4 /dev/nvme0n1p2 rw,errors=remount-ro
  25 22 0:23 / /tmp rw,nosuid,nodev shared:5 - tmpfs tmpfs rw,size=8G
  26 25 259:3 / /tmp/disk rw,relatime shared:6 - xfs /dev/nvme1n1 rw,attr2
  27 22 0:24 / /dev/shm rw,nosuid,nodev shared:7 - tmpfs tmpfs rw
  28 22 259:4 / /mnt/with\\040space rw,relatime shared:8 - btrfs /dev/sdb1 rw
  29 22 259:2 /srv/data /bound rw,relatime shared:1 - ext4 /dev/nvme0n1p2 rw
  30 22 0:25 / /var/lib/docker/overlay2/abc/merged rw master:9 - overlay overlay rw,lowerdir=/l
  31 22 0:26 / /ram rw - ramfs ramfs rw
  32 22 0:27 / /stacked rw - ext4 /dev/sdc1 rw
  33 32 0:28 / /stacked rw - tmpfs tmpfs rw
  not a mountinfo line
  """

  describe "label/4" do
    test "states the values per request, the commit and the preallocation" do
      assert FlushRegime.label(1000, 100, false, 0) ==
               "batch 1000 x 100B (97.7KB of values per request, group commit off, segment preallocation off)"
    end

    test "uses the ceiling's byte formatting at every batch the scale sweep runs" do
      assert FlushRegime.label(10, 100, false, 0) =~ "batch 10 x 100B (1000B of values per request,"
      assert FlushRegime.label(100, 100, false, 0) =~ "batch 100 x 100B (9.8KB of values per request,"
    end

    test "names group commit on and a preallocation size" do
      assert FlushRegime.label(1000, 100, true, 64 * 1024 * 1024) ==
               "batch 1000 x 100B (97.7KB of values per request, group commit on, segment preallocation 64MB)"
    end

    test "refuses values that describe no regime" do
      for {batch, value_bytes, group_commit, prealloc} <- [
            {0, 100, false, 0},
            {-1, 100, false, 0},
            {1000, 0, false, 0},
            {1000, 100, false, -1},
            {1000, 100, :off, 0},
            {1.5, 100, false, 0}
          ] do
        assert_raise FunctionClauseError, fn -> FlushRegime.label(batch, value_bytes, group_commit, prealloc) end
      end
    end
  end

  describe "label/3 and label/5" do
    test "the pinned regime ends with the filesystem" do
      assert FlushRegime.label(1000, 100, "ext4") ==
               "batch 1000 x 100B (97.7KB of values per request, group commit off, " <>
                 "segment preallocation off), on ext4"
    end

    test "an unreadable mount table is said, not hidden" do
      assert FlushRegime.label(1000, 100, :unknown) =~ ~r/segment preallocation off\), on an unknown filesystem$/
    end

    test "label/5 appends the filesystem to label/4" do
      assert FlushRegime.label(10, 256, true, 0, "xfs") == FlushRegime.label(10, 256, true, 0) <> ", on xfs"
    end

    test "an empty filesystem type is refused" do
      assert_raise FunctionClauseError, fn -> FlushRegime.label(1000, 100, "") end
    end
  end

  describe "pinned options" do
    test "the servers get the same settings the label states" do
      assert FlushRegime.replication_opts() == [prealloc_bytes: 0]
      assert FlushRegime.broker_opts() == [group_commit: false]

      assert FlushRegime.label(1000, 100, "ext4") ==
               FlushRegime.label(
                 1000,
                 100,
                 FlushRegime.broker_opts()[:group_commit],
                 FlushRegime.replication_opts()[:prealloc_bytes],
                 "ext4"
               )
    end
  end

  describe "mountinfo_fstype/2" do
    test "the root mount covers what nothing else does" do
      assert FlushRegime.mountinfo_fstype(@mountinfo, "/home/me/bench") == "ext4"
      assert FlushRegime.mountinfo_fstype(@mountinfo, "/") == "ext4"
    end

    test "a mount point covers itself and what is under it" do
      assert FlushRegime.mountinfo_fstype(@mountinfo, "/tmp") == "tmpfs"
      assert FlushRegime.mountinfo_fstype(@mountinfo, "/tmp/bench") == "tmpfs"
      assert FlushRegime.mountinfo_fstype(@mountinfo, "/dev/shm/x") == "tmpfs"
      assert FlushRegime.mountinfo_fstype(@mountinfo, "/ram/x") == "ramfs"
    end

    test "the deepest mount wins over the one it is nested in" do
      assert FlushRegime.mountinfo_fstype(@mountinfo, "/tmp/disk") == "xfs"
      assert FlushRegime.mountinfo_fstype(@mountinfo, "/tmp/disk/bench") == "xfs"
    end

    test "a prefix that stops inside a name is not a covering mount" do
      assert FlushRegime.mountinfo_fstype(@mountinfo, "/tmpfoo/bench") == "ext4"
      assert FlushRegime.mountinfo_fstype(@mountinfo, "/tmp/diskette") == "tmpfs"
    end

    test "escaped characters in a mount point are decoded" do
      assert FlushRegime.mountinfo_fstype(@mountinfo, "/mnt/with space/bench") == "btrfs"
      assert FlushRegime.mountinfo_fstype(@mountinfo, "/mnt/with\\040space/bench") == "ext4"
    end

    test "a bind mount reports the filesystem it exposes, whatever its source directory" do
      assert FlushRegime.mountinfo_fstype(@mountinfo, "/bound/x") == "ext4"
    end

    test "an overlay is reported as overlay" do
      assert FlushRegime.mountinfo_fstype(@mountinfo, "/var/lib/docker/overlay2/abc/merged/tmp") == "overlay"
    end

    test "of mounts stacked on one point, the last one listed is the visible one" do
      assert FlushRegime.mountinfo_fstype(@mountinfo, "/stacked/x") == "tmpfs"
    end

    test "a table covering nothing, or with no usable line, gives unknown" do
      assert FlushRegime.mountinfo_fstype("", "/tmp") == :unknown
      assert FlushRegime.mountinfo_fstype("garbage\n1 2 3\n", "/tmp") == :unknown
      assert FlushRegime.mountinfo_fstype("25 22 0:23 / /tmp rw - tmpfs tmpfs rw\n", "/home") == :unknown
    end
  end

  describe "filesystem/2" do
    test "reads the table it is given", %{tmp_dir: dir} do
      table = Path.join(dir, "mountinfo")
      File.write!(table, @mountinfo)

      assert FlushRegime.filesystem("/tmp/disk", table) == "xfs"
    end

    test "an unreadable table is unknown", %{tmp_dir: dir} do
      assert FlushRegime.filesystem("/tmp", Path.join(dir, "missing")) == :unknown
    end
  end

  describe "durability/2" do
    test "a disk filesystem runs without a warning" do
      for fstype <- ~w(ext4 xfs btrfs overlay) do
        assert FlushRegime.durability(fstype, false) == {:ok, []}
      end
    end

    test "memory-backed storage is refused unless allowed" do
      for fstype <- ~w(tmpfs ramfs devtmpfs) do
        assert {:error, reason} = FlushRegime.durability(fstype, false)
        assert reason =~ "on #{fstype}, which is not the durable path"
        assert reason =~ "BENCH_ALLOW_TMPFS=1"

        assert {:ok, [warning]} = FlushRegime.durability(fstype, true)
        assert warning =~ "on #{fstype}, which is not the durable path"
      end
    end

    test "an unknown filesystem runs with a warning" do
      assert {:ok, [warning]} = FlushRegime.durability(:unknown, false)
      assert warning =~ "filesystem is unknown"
    end
  end

  describe "prepare/2" do
    setup %{tmp_dir: dir} do
      table = Path.join(dir, "mountinfo")
      bench = Path.join(dir, "bench")
      File.mkdir_p!(bench)

      File.mkdir_p!(Path.join(dir, "ram"))
      # The kernel lists the resolved path, with a space written as \\040.
      ram_point = dir |> Path.join("ram") |> real() |> String.replace(" ", "\\040")

      File.write!(table, """
      1 0 0:1 / / rw - ext4 /dev/root rw
      2 1 0:2 / #{ram_point} rw - tmpfs tmpfs rw
      """)

      %{table: table, bench: bench}
    end

    test "uses BENCH_DIR and reports its filesystem", %{table: table, bench: bench} do
      assert {:ok, %{dir: dir, filesystem: "ext4", warnings: []}} =
               FlushRegime.prepare(%{"BENCH_DIR" => bench}, table)

      assert dir == real(bench)
    end

    test "defaults to the system temp dir", %{table: table} do
      for env <- [%{}, %{"BENCH_DIR" => ""}] do
        assert {:ok, %{dir: dir}} = FlushRegime.prepare(env, table)
        assert dir == real(System.tmp_dir!())
      end
    end

    test "a relative BENCH_DIR is taken from the current directory", %{table: table} do
      assert {:ok, %{dir: dir}} = FlushRegime.prepare(%{"BENCH_DIR" => "."}, table)
      assert dir == real(File.cwd!())
    end

    test "a missing BENCH_DIR, or a file, is refused", %{table: table, bench: bench, tmp_dir: tmp} do
      file = Path.join(tmp, "file")
      File.write!(file, "")

      for value <- [Path.join(bench, "missing"), file] do
        assert {:error, message} = FlushRegime.prepare(%{"BENCH_DIR" => value}, table)
        assert message == "BENCH_DIR #{value} is not an existing directory"
      end
    end

    test "memory-backed storage is refused, and allowed with BENCH_ALLOW_TMPFS=1", %{table: table, tmp_dir: tmp} do
      ram = Path.join(tmp, "ram")

      for allow <- [nil, "", "0"] do
        env = %{"BENCH_DIR" => ram} |> Map.merge(if allow, do: %{"BENCH_ALLOW_TMPFS" => allow}, else: %{})
        assert {:error, message} = FlushRegime.prepare(env, table)
        assert message =~ "#{real(ram)} is on tmpfs, which is not the durable path"
      end

      assert {:ok, %{filesystem: "tmpfs", warnings: [warning]}} =
               FlushRegime.prepare(%{"BENCH_DIR" => ram, "BENCH_ALLOW_TMPFS" => "1"}, table)

      assert warning =~ "BENCH_ALLOW_TMPFS=1 runs it anyway"
    end

    test "a link into memory-backed storage is judged by where it points", %{table: table, tmp_dir: tmp} do
      link = Path.join(tmp, "looks_like_disk")
      File.ln_s!(Path.join(tmp, "ram"), link)

      assert {:error, message} = FlushRegime.prepare(%{"BENCH_DIR" => link}, table)
      assert message =~ "is on tmpfs"
    end

    test "any other BENCH_ALLOW_TMPFS value is refused before the directory is looked at", %{table: table} do
      for value <- ["yes", "true", "2", " 1"] do
        assert {:error, message} =
                 FlushRegime.prepare(%{"BENCH_ALLOW_TMPFS" => value, "BENCH_DIR" => "/nonexistent"}, table)

        assert message == "BENCH_ALLOW_TMPFS must be 1 or unset, got #{inspect(value)}"
      end
    end

    test "an unreadable mount table runs with a warning", %{bench: bench, tmp_dir: tmp} do
      assert {:ok, %{filesystem: :unknown, warnings: [warning]}} =
               FlushRegime.prepare(%{"BENCH_DIR" => bench}, Path.join(tmp, "missing"))

      assert warning =~ "numbers only count on Linux"
    end
  end

  describe "resolve/1" do
    test "resolves absolute and relative links at any depth", %{tmp_dir: tmp} do
      real_dir = Path.join([real(tmp), "a", "b"])
      File.mkdir_p!(Path.join(real_dir, "c"))
      File.ln_s!(Path.join(real(tmp), "a"), Path.join(tmp, "abs"))
      File.ln_s!("a/b", Path.join(tmp, "rel"))

      assert FlushRegime.resolve(Path.join([tmp, "abs", "b", "c"])) == {:ok, Path.join(real_dir, "c")}
      assert FlushRegime.resolve(Path.join([tmp, "rel", "c"])) == {:ok, Path.join(real_dir, "c")}
    end

    test "a path with no links is returned as it is", %{tmp_dir: tmp} do
      assert FlushRegime.resolve(real(tmp)) == {:ok, real(tmp)}
    end

    test "a missing path resolves as far as it exists", %{tmp_dir: tmp} do
      assert FlushRegime.resolve(Path.join(real(tmp), "missing/deeper")) ==
               {:ok, Path.join(real(tmp), "missing/deeper")}
    end

    test "a link loop is an error, not a hang", %{tmp_dir: tmp} do
      File.ln_s!(Path.join(tmp, "two"), Path.join(tmp, "one"))
      File.ln_s!(Path.join(tmp, "one"), Path.join(tmp, "two"))

      assert FlushRegime.resolve(Path.join(tmp, "one")) == {:error, "too many symbolic links resolving BENCH_DIR"}
    end
  end

  # The fully resolved form of an existing path, from the system's realpath rather than from the code
  # under test. On macOS it differs from what tmp_dir reports (/var is a link to /private/var).
  defp real(path) do
    {resolved, 0} = System.cmd("realpath", [path])
    String.trim_trailing(resolved, "\n")
  end
end
