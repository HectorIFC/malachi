defmodule BenchLibTest do
  # scripts/bench_lib.sh turns a cpuset into the BEAM scheduler count of every harness that pins a
  # server and a load generator, and labels each recorded result with the tree that produced it. A
  # miscount boots the server with a fraction of its schedulers and the run reports a lower ceiling with
  # nothing flagged; a lost -dirty records numbers from uncommitted code under a clean commit. Every
  # cpu-list form (Docker's cpuset takes ids and ranges, taskset also strides) and both tree states are
  # checked here against bash itself.
  use ExUnit.Case, async: true

  @moduletag :tmp_dir

  @lib Path.expand("../../scripts/bench_lib.sh", __DIR__)

  # tree_label runs git, and so do the fixtures: the developer's own global and system git config
  # (commit signing, hooks, status.showUntrackedFiles) and a GIT_DIR inherited from a hook would otherwise
  # fail the fixtures or hide the untracked files the -dirty cases rely on (as pin_diff_test.exs does).
  @no_git_env [
    {"GIT_DIR", nil},
    {"GIT_WORK_TREE", nil},
    {"GIT_COMMON_DIR", nil},
    {"GIT_INDEX_FILE", nil},
    {"GIT_CONFIG_GLOBAL", "/dev/null"},
    {"GIT_CONFIG_NOSYSTEM", "1"}
  ]

  setup_all do
    for tool <- ~w(bash git) do
      System.find_executable(tool) || flunk("#{tool} is required to test scripts/bench_lib.sh")
    end

    :ok
  end

  defp call(fun, arg, opts \\ []) do
    opts = Keyword.update(opts, :env, @no_git_env, &(@no_git_env ++ &1))

    {out, 0} =
      System.cmd("bash", ["-c", ~s(set -o pipefail; source "$1"; #{fun} "$2"), "bash", @lib, arg], opts)

    String.trim(out)
  end

  describe "count_cpus" do
    for {cpuset, cores} <- [
          {"0", "1"},
          {"1,2,3", "3"},
          {"4-7", "4"},
          {"0-3,6", "5"},
          {"0-10:2", "6"},
          {"0,2-3,8-12:2", "6"},
          {"", "0"}
        ] do
      test "#{inspect(cpuset)} names #{cores} cores" do
        assert call("count_cpus", unquote(cpuset)) == unquote(cores)
      end
    end
  end

  describe "schedulers_for" do
    test "a comma list gets one scheduler per core" do
      assert call("schedulers_for", "1,2,3") == "+S 3:3"
    end

    test "a range counts every core in it, not one" do
      assert call("schedulers_for", "4-7") == "+S 4:4"
    end
  end

  describe "tree_label" do
    setup %{tmp_dir: dir} do
      git = fn args -> {_, 0} = System.cmd("git", args, cd: dir, env: @no_git_env, stderr_to_stdout: true) end
      git.(["init", "-q"])
      File.write!(Path.join(dir, "a"), "a")
      git.(["add", "a"])
      git.(["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "a"])
      {sha, 0} = System.cmd("git", ["rev-parse", "--short", "HEAD"], cd: dir, env: @no_git_env)
      %{dir: dir, sha: String.trim(sha)}
    end

    test "a clean tree is its commit", %{dir: dir, sha: sha} do
      assert call("tree_label", "", cd: dir) == sha
    end

    test "an untracked file marks it dirty", %{dir: dir, sha: sha} do
      File.write!(Path.join(dir, "b"), "b")
      assert call("tree_label", "", cd: dir) == sha <> "-dirty"
    end

    # A status longer than a pipe buffer is what used to lose -dirty: `git status | grep -q .` under
    # pipefail fails once grep exits before git has written everything.
    test "a status far longer than a pipe buffer still marks it dirty", %{dir: dir, sha: sha} do
      for i <- 1..3000, do: File.write!(Path.join(dir, "untracked_file_with_a_long_name_#{i}"), "")
      assert call("tree_label", "", cd: dir) == sha <> "-dirty"
    end

    test "outside a checkout it is unknown", %{tmp_dir: dir} do
      outside = Path.join(dir, "not_a_repo")
      File.mkdir_p!(outside)
      assert call("tree_label", "", cd: outside, env: [{"GIT_CEILING_DIRECTORIES", dir}]) == "unknown"
    end
  end
end
