defmodule Malachi.Test.TmpDirTest do
  # Two `mix test` runs on one host (two worktrees) must never name the same scratch directory: one run's
  # cleanup would delete the files the other is using. TmpDir is what makes the name unique per host, and
  # the guard below keeps the suite from drifting back to the per-VM `System.unique_integer/1` alone.
  use ExUnit.Case, async: true

  alias Malachi.Test.TmpDir

  @test_root Path.expand("../..", __DIR__)

  describe "path/2" do
    test "lives under the system temporary directory and names this VM's operating system pid" do
      path = TmpDir.path("probe")

      # Compared through Path.join, which drops the trailing slash some systems give the directory.
      assert path == Path.join(System.tmp_dir!(), Path.basename(path))
      assert Path.basename(path) =~ ~r/\Aprobe_#{System.pid()}_\d+\z/
    end

    test "never repeats within a VM" do
      paths = for _ <- 1..1_000, do: TmpDir.path("probe")
      assert length(Enum.uniq(paths)) == 1_000
    end

    test "takes another base directory" do
      assert Path.dirname(TmpDir.path("probe", "/dev/shm")) == "/dev/shm"
    end

    test "creates nothing" do
      refute File.exists?(TmpDir.path("probe"))
    end
  end

  # A line that builds a path under the system temporary directory (or /dev/shm) from a per-VM counter or
  # a random number, without the pid, is the collision this module exists to prevent. The Concuerror
  # harness is left out: it is not part of `mix test` and runs one VM at a time.
  test "no test builds a scratch path from a per-VM counter or a random number alone" do
    offenders =
      for file <- Path.wildcard(Path.join(@test_root, "**/*.{ex,exs}")),
          not String.starts_with?(file, Path.join(@test_root, "concuerror")),
          file != __ENV__.file,
          {line, number} <- file |> File.read!() |> String.split("\n") |> Enum.with_index(1),
          line =~ ~r/tmp_dir!\(\)|@shm\b/ and line =~ ~r/unique_integer|:rand\./,
          do: "#{Path.relative_to(file, @test_root)}:#{number}: #{String.trim(line)}"

    assert offenders == [], "use Malachi.Test.TmpDir.path/2 instead:\n" <> Enum.join(offenders, "\n")
  end
end
