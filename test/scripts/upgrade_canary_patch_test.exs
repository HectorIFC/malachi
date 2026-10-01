defmodule UpgradeCanaryPatchTest do
  # test/support/upgrade_canary.patch is what scripts/docker-upgrade-chaos.sh applies to the NEW image so the
  # drill has a capability, a metadata command at the next machine version, a replication cast and a data
  # format that no release has (issue #196). It lives outside lib/ on purpose, which means nothing compiles it
  # and a change to the files it touches can leave it behind without anyone noticing until the nightly drill
  # fails to build. This test notices on every run of the suite instead: the patch still applies, and the tree
  # it produces still compiles, which is what catches a rename of something it calls outside its own hunks.
  use ExUnit.Case, async: true

  @root Path.expand("../..", __DIR__)
  @patch Path.join(@root, "test/support/upgrade_canary.patch")

  # Every file the canary changes. A new entry here is a decision, not an accident: each one is drill-only
  # code the release never carries, so the list is kept exact rather than read back from the patch.
  @touched ~w(
    lib/malachi/application.ex
    lib/malachi/broker.ex
    lib/malachi/broker_server.ex
    lib/malachi/cluster/capabilities.ex
    lib/malachi/cluster/machine_version.ex
    lib/malachi/cluster/replication_server.ex
    lib/malachi/metadata.ex
    lib/malachi/storage/format_marker.ex
  )

  setup_all do
    System.find_executable("git") || flunk("git is required to check test/support/upgrade_canary.patch")
    :ok
  end

  test "still applies to the working tree" do
    assert {output, status} = git(["apply", "--check", @patch])
    assert status == 0, "the upgrade canary patch no longer applies; rebase it onto lib/:\n#{output}"
  end

  # A copy of what `mix compile` reads, with the build of this suite copied in so only the patched project is
  # compiled again, and `deps` linked rather than copied. The copy sits under this repository (tmp/), so git is
  # kept from finding the repository around it, as the drill does when it applies the patch.
  @tag :tmp_dir
  test "the tree it produces still compiles, warnings included", %{tmp_dir: dir} do
    for path <- ~w(mix.exs mix.lock config lib priv), do: File.cp_r!(Path.join(@root, path), Path.join(dir, path))
    File.mkdir_p!(Path.join(dir, "_build"))
    File.cp_r!(Path.join([@root, "_build", "test"]), Path.join([dir, "_build", "test"]))
    File.ln_s!(Path.join(@root, "deps"), Path.join(dir, "deps"))

    env = [{"GIT_CEILING_DIRECTORIES", Path.dirname(dir)}, {"MIX_ENV", "test"}]
    {output, status} = System.cmd("git", ["apply", @patch], cd: dir, env: env, stderr_to_stdout: true)
    assert status == 0, "the upgrade canary patch does not apply to a copy of the tree:\n#{output}"

    assert {output, status} =
             System.cmd("mix", ["compile", "--warnings-as-errors"], cd: dir, env: env, stderr_to_stdout: true)

    assert status == 0, "the tree the upgrade canary patch produces does not compile:\n#{output}"
  end

  test "touches exactly the files it is meant to, all under lib/" do
    assert {numstat, 0} = git(["apply", "--numstat", @patch])

    files =
      numstat
      |> String.split("\n", trim: true)
      |> Enum.map(&(&1 |> String.split("\t") |> List.last()))
      |> Enum.sort()

    assert files == Enum.sort(@touched)
  end

  test "marks every file it changes as canary code" do
    added_by_file =
      @patch
      |> File.read!()
      |> String.split(~r/^diff --git /m, trim: true)
      |> Map.new(fn section ->
        [header | _lines] = String.split(section, "\n", parts: 2)
        file = header |> String.split(" ") |> List.last() |> String.replace_prefix("b/", "")
        added = section |> String.split("\n") |> Enum.filter(&String.starts_with?(&1, "+"))
        {file, Enum.join(added, "\n")}
      end)

    for file <- @touched do
      assert added_by_file[file] =~ "UPGRADE CANARY", "#{file}: the canary change does not say it is one"
    end
  end

  test "declares what the drill depends on" do
    patch = File.read!(@patch)

    assert patch =~ "+  @capabilities [:upgrade_canary]"
    assert patch =~ "+  @code_version 5"
    assert patch =~ "+      {:canary_note, 3} => 5"
    assert patch =~ "+  @supported_format 2"
    assert patch =~ "FormatMarker.raise_to(log_data_dir(), 2)"
    added = patch |> String.split("\n") |> Enum.filter(&String.starts_with?(&1, "+"))

    # The canary cast, and the timer that sends it: armed once in init/1 and again on every tick. Either one
    # missing still compiles and still matches the cast, and only the drill would notice.
    assert Enum.any?(added, &(&1 =~ "GenServer.cast({Malachi.LogReplication, peer}, {:replica_canary, nil})"))
    assert Enum.count(added, &(&1 == "+    Process.send_after(self(), :upgrade_canary, 1_000)")) == 2
    assert patch =~ "def handle_call({:canary_note, topic, note}, _from, state)"
  end

  defp git(args), do: System.cmd("git", args, cd: @root, stderr_to_stdout: true)
end
