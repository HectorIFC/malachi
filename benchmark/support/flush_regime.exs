# The regime the end-to-end benchmarks run in, pinned and declared in one place: what a produce carries,
# whether group commit coalesces produces, whether segments are preallocated, and which filesystem the
# log lands on. Each of those moves the per-flush cost (#83, #141), so a number printed without them
# invites being compared with a number measured in another regime.
#
# The group commit and preallocation settings are PASSED to the servers from here, not inherited: left
# to the defaults, `MALACHI_GROUP_COMMIT=true` in the shell would change the regime while the label kept
# saying off. The label reuses `Malachi.Loadtest.Ceiling.regime_label/3`, so these scripts and the
# published ceiling pages count bytes the same way: bytes of values per request, not encoded frames.
#
# Malachi runs on Linux only, and so do these harnesses: the filesystem comes from
# /proc/self/mountinfo, and a log on tmpfs or ramfs is refused, because memory-backed storage has no
# journal, no delayed allocation and no real sync, which is the path these benchmarks exist to measure.
#
#   Code.require_file("support/flush_regime.exs", __DIR__)

defmodule Malachi.Bench.FlushRegime do
  alias Malachi.Loadtest.Ceiling

  @group_commit false
  @prealloc_bytes 0
  @memory_backed ~w(tmpfs ramfs devtmpfs)
  @mountinfo "/proc/self/mountinfo"
  @max_symlink_hops 40

  @doc "Options for `Malachi.Cluster.ReplicationServer.start_link/1` that pin the storage side of the regime."
  def replication_opts, do: [prealloc_bytes: @prealloc_bytes]

  @doc "Options for `Malachi.BrokerServer.start_link/2` that pin the commit side of the regime."
  def broker_opts, do: [group_commit: @group_commit]

  @doc "The pinned regime for `batch` records of `value_bytes` each, on `filesystem`."
  def label(batch, value_bytes, filesystem), do: label(batch, value_bytes, @group_commit, @prealloc_bytes, filesystem)

  @doc """
  A regime as one sentence fragment, for example
  `batch 1000 x 100B (97.7KB of values per request, group commit off), segment preallocation off`.
  """
  def label(batch, value_bytes, group_commit, prealloc_bytes)
      when is_integer(batch) and batch > 0 and is_integer(value_bytes) and value_bytes > 0 and
             is_boolean(group_commit) and is_integer(prealloc_bytes) and prealloc_bytes >= 0 do
    Ceiling.regime_label(batch, value_bytes, group_commit) <>
      ", segment preallocation " <> preallocation(prealloc_bytes)
  end

  @doc "`label/4` followed by the filesystem the log is on, or `:unknown` when it could not be read."
  def label(batch, value_bytes, group_commit, prealloc_bytes, filesystem) do
    label(batch, value_bytes, group_commit, prealloc_bytes) <> ", " <> on(filesystem)
  end

  defp preallocation(0), do: "off"
  defp preallocation(bytes), do: Ceiling.format_bytes(bytes)

  defp on(:unknown), do: "on an unknown filesystem"
  defp on(fstype) when is_binary(fstype) and fstype != "", do: "on " <> fstype

  @doc """
  Chooses the directory a benchmark writes under and checks it is on the durable path, printing what it
  found. Halts with status 2, before anything starts, when it cannot run as asked.

  `BENCH_DIR` names the directory (default: the system temp dir), which must already exist.
  `BENCH_ALLOW_TMPFS=1` runs on memory-backed storage anyway, with a warning.
  """
  def prepare!(env \\ System.get_env()) do
    case prepare(env, @mountinfo) do
      {:ok, %{warnings: warnings} = target} ->
        Enum.each(warnings, &IO.puts(:stderr, "WARNING: " <> &1))
        Map.delete(target, :warnings)

      {:error, message} ->
        IO.puts(:stderr, "ERROR: " <> message)
        System.halt(2)
    end
  end

  @doc """
  The decision behind `prepare!/1`, without printing or halting: `{:ok, %{dir, filesystem, warnings}}`
  or `{:error, message}`. `dir` is absolute with every symbolic link resolved, because a link is how a
  directory on one filesystem appears to live on another.
  """
  def prepare(env, mountinfo_path) do
    with {:ok, allow_memory?} <- allow_memory(Map.get(env, "BENCH_ALLOW_TMPFS")),
         {:ok, dir} <- bench_dir(Map.get(env, "BENCH_DIR")) do
      filesystem = filesystem(dir, mountinfo_path)

      case durability(filesystem, allow_memory?) do
        {:ok, warnings} -> {:ok, %{dir: dir, filesystem: filesystem, warnings: warnings}}
        {:error, reason} -> {:error, "#{dir} is #{reason}"}
      end
    end
  end

  defp allow_memory(value) when value in [nil, "", "0"], do: {:ok, false}
  defp allow_memory("1"), do: {:ok, true}
  defp allow_memory(other), do: {:error, "BENCH_ALLOW_TMPFS must be 1 or unset, got #{inspect(other)}"}

  defp bench_dir(value) when value in [nil, ""], do: bench_dir(System.tmp_dir!())

  defp bench_dir(value) do
    with {:ok, dir} <- resolve(Path.expand(value)) do
      if File.dir?(dir), do: {:ok, dir}, else: {:error, "BENCH_DIR #{value} is not an existing directory"}
    end
  end

  @doc false
  def durability(:unknown, _allow_memory?) do
    {:ok, ["could not read #{@mountinfo}, so the filesystem is unknown; numbers only count on Linux"]}
  end

  def durability(fstype, allow_memory?) when fstype in @memory_backed do
    if allow_memory? do
      {:ok, ["the log is on #{fstype}, which is not the durable path; BENCH_ALLOW_TMPFS=1 runs it anyway"]}
    else
      {:error,
       "on #{fstype}, which is not the durable path; set BENCH_DIR to a disk-backed directory, " <>
         "or BENCH_ALLOW_TMPFS=1 to run anyway"}
    end
  end

  def durability(fstype, _allow_memory?) when is_binary(fstype), do: {:ok, []}

  @doc """
  The type of the filesystem `dir` (absolute, links resolved) is on, from the mount table at
  `mountinfo_path`, or `:unknown` when the table cannot be read.
  """
  def filesystem(dir, mountinfo_path \\ @mountinfo) do
    case File.read(mountinfo_path) do
      {:ok, mountinfo} -> mountinfo_fstype(mountinfo, dir)
      {:error, _} -> :unknown
    end
  end

  @doc """
  The type of the filesystem `path` is on, given the text of a `/proc/<pid>/mountinfo`: the mount whose
  point is the longest whole-directory prefix of `path`, and of mounts stacked on the same point the last
  one listed, which is the one visible. `:unknown` when no line covers the path.
  """
  def mountinfo_fstype(mountinfo, path) do
    mountinfo
    |> String.split("\n", trim: true)
    |> Enum.flat_map(&parse_mount/1)
    |> Enum.filter(fn {point, _fstype} -> covers?(point, path) end)
    |> Enum.reduce(nil, &deeper_or_later/2)
    |> case do
      nil -> :unknown
      {_point, fstype} -> fstype
    end
  end

  # Mounts arrive in table order, so on an equally long point the later one wins.
  defp deeper_or_later(mount, nil), do: mount

  defp deeper_or_later({point, _} = mount, {best_point, _} = best) do
    if byte_size(point) >= byte_size(best_point), do: mount, else: best
  end

  # `36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw`: the mount point is the fifth
  # field, then a variable run of optional fields ends at `-`, and the filesystem type follows it.
  defp parse_mount(line) do
    with [_id, _parent, _dev, _root, point | rest] <- String.split(line, " "),
         [_separator, fstype | _] <- Enum.drop_while(rest, &(&1 != "-")) do
      [{unescape(point), fstype}]
    else
      _ -> []
    end
  end

  # The kernel writes space, tab, newline and backslash in a path as three octal digits.
  defp unescape(field) do
    Regex.replace(~r/\\([0-7]{3})/, field, fn _, octal -> <<String.to_integer(octal, 8)>> end)
  end

  defp covers?("/", _path), do: true
  defp covers?(point, path), do: path == point or String.starts_with?(path, point <> "/")

  @doc false
  def resolve(path), do: resolve(Path.split(path), [], @max_symlink_hops)

  defp resolve(_parts, _done, 0), do: {:error, "too many symbolic links resolving BENCH_DIR"}
  defp resolve([], done, _hops), do: {:ok, done |> Enum.reverse() |> Path.join()}

  defp resolve([part | rest], done, hops) do
    candidate = [part | done] |> Enum.reverse() |> Path.join()

    case File.read_link(candidate) do
      {:ok, target} ->
        # A relative target is relative to the directory holding the link; either way the target is
        # resolved again from the root, with the unresolved remainder after it.
        # `done` is never empty here: the root it starts from is not a link.
        target = Path.expand(target, done |> Enum.reverse() |> Path.join())
        resolve(Path.split(target) ++ rest, [], hops - 1)

      {:error, _not_a_link_or_missing} ->
        resolve(rest, [part | done], hops)
    end
  end
end
