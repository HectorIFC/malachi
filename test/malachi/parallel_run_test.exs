defmodule Malachi.ParallelRunTest do
  # Two `mix test` runs on one host (two worktrees) used to serialize: both took the node name
  # `malachi_primary` in the host-global epmd, and both bound 4040 and 4041. This starts a second run
  # while this one holds its node name and listeners, and requires it to pass.
  #
  # Linux only, like every reproduction in this project. There, the kernel lets two sockets of one user
  # that both set `reuseport` share a port, so the second run's broker listener used to bind 4040 beside
  # this one and split its connections silently; the dashboard, without `reuseport`, is what failed.
  use ExUnit.Case, async: false

  alias Malachi.Dashboard
  alias Malachi.TCPAcceptorPool

  @moduletag :linux
  @moduletag timeout: 300_000

  @probe "test/malachi/parallel_probe_test.exs"

  defp second_run(env) do
    mix = System.find_executable("mix") || flunk("mix is required to start a second test run")

    parent = [
      {"MIX_ENV", "test"},
      {"MALACHI_PARENT_TCP_PORT", Integer.to_string(TCPAcceptorPool.port())},
      {"MALACHI_PARENT_DASHBOARD_PORT", Integer.to_string(Dashboard.port())}
    ]

    System.cmd(mix, ["test", "--only", "parallel_probe", @probe],
      cd: File.cwd!(),
      env: parent ++ env,
      stderr_to_stdout: true
    )
  end

  test "a second run on this host starts and passes while this one runs" do
    {output, status} = second_run([])
    assert status == 0, output
    assert output =~ "1 test, 0 failures"
  end

  test "a shell that exported a dev node's ports does not pin the second run to them" do
    # What `worktree.env` exports for the dev node: pointed at this run's ports, which are taken.
    env = [
      {"MALACHI_TCP_PORT", Integer.to_string(TCPAcceptorPool.port())},
      {"MALACHI_DASHBOARD_PORT", Integer.to_string(Dashboard.port())}
    ]

    {output, status} = second_run(env)
    assert status == 0, output
    assert output =~ "1 test, 0 failures"
  end
end
