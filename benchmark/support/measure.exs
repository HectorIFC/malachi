# Resource measurements the end-to-end benchmarks report, in one place so throughput_1m.exs and
# single_node_scale.exs cannot disagree about what a megabyte or a directory's size is.
#
#   Code.require_file("support/measure.exs", __DIR__)

defmodule Malachi.Bench.Measure do
  @doc "Bytes as megabytes (1MB = 1_048_576 bytes), rounded to one decimal."
  def mb(bytes), do: Float.round(bytes / 1_048_576, 1)

  @doc """
  The bytes held by the regular files under `dir`, at any depth, dotfiles included.

  Directory entries and symbolic links are not content, so they count nothing, and a link is never
  followed: a link out of the tree must not pull someone else's files into the figure. A path that
  vanishes while the tree is walked counts 0, as does a `dir` that does not exist. Any other error
  (a permission denied, say) raises, because a silently smaller figure would read as a result.
  """
  def dir_bytes(dir) do
    case File.lstat(dir) do
      {:ok, %File.Stat{type: :directory}} -> dir |> children() |> Enum.map(&dir_bytes/1) |> Enum.sum()
      {:ok, %File.Stat{type: :regular, size: size}} -> size
      {:ok, %File.Stat{}} -> 0
      {:error, :enoent} -> 0
    end
  end

  defp children(dir) do
    case File.ls(dir) do
      {:ok, names} -> Enum.map(names, &Path.join(dir, &1))
      {:error, :enoent} -> []
    end
  end
end
