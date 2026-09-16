Code.require_file("../../../benchmark/support/measure.exs", __DIR__)

defmodule Malachi.Bench.MeasureTest do
  use ExUnit.Case, async: true

  alias Malachi.Bench.Measure

  @moduletag :tmp_dir

  describe "mb/1" do
    test "rounds to one decimal in binary megabytes" do
      assert Measure.mb(0) == 0.0
      assert Measure.mb(1_048_576) == 1.0
      assert Measure.mb(1_572_864) == 1.5
      assert Measure.mb(1_100_000) == 1.0
      assert Measure.mb(1_200_000) == 1.1
    end
  end

  describe "dir_bytes/1" do
    test "sums regular files at every depth and nothing for the directories holding them", %{tmp_dir: dir} do
      write!(dir, "a", 10)
      write!(dir, "sub/b", 20)
      write!(dir, "sub/deeper/c", 30)

      assert Measure.dir_bytes(dir) == 60
    end

    test "counts dotfiles", %{tmp_dir: dir} do
      write!(dir, ".hidden", 7)
      write!(dir, ".dotdir/inner", 5)

      assert Measure.dir_bytes(dir) == 12
    end

    test "an empty directory is 0", %{tmp_dir: dir} do
      assert Measure.dir_bytes(dir) == 0
    end

    test "a directory that does not exist is 0", %{tmp_dir: dir} do
      assert Measure.dir_bytes(Path.join(dir, "missing")) == 0
    end

    test "symbolic links are neither counted nor followed", %{tmp_dir: dir} do
      tree = Path.join(dir, "tree")
      outside = Path.join(dir, "outside")
      write!(tree, "own", 3)
      write!(outside, "big", 1_000)
      File.ln_s!(outside, Path.join(tree, "to_dir"))
      File.ln_s!(Path.join(outside, "big"), Path.join(tree, "to_file"))
      File.ln_s!(Path.join(dir, "nowhere"), Path.join(tree, "dangling"))

      assert Measure.dir_bytes(tree) == 3
    end

    test "a regular file given as the root counts its own size", %{tmp_dir: dir} do
      write!(dir, "file", 42)

      assert Measure.dir_bytes(Path.join(dir, "file")) == 42
    end

    test "an unreadable directory raises instead of reporting a smaller figure", %{tmp_dir: dir} do
      locked = Path.join(dir, "locked")
      write!(locked, "secret", 9)
      File.chmod!(locked, 0o000)
      on_exit(fn -> File.chmod(locked, 0o700) end)

      # root reads anything, so the case only means something for an ordinary user.
      if File.ls(locked) == {:error, :eacces} do
        assert_raise CaseClauseError, fn -> Measure.dir_bytes(dir) end
      end
    end
  end

  defp write!(root, relative, size) do
    path = Path.join(root, relative)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, :binary.copy("x", size))
  end
end
