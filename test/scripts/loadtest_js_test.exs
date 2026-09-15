defmodule LoadtestJsTest do
  # scripts/loadtest.js is one of the two generators the published ceiling comes from, and nothing tested
  # it: its only check was a histogram self-test CI never ran. These cases run the real script against
  # the test server the app starts in test_helper, and observe it from the server side (authentication
  # telemetry, the file system) rather than trusting what it reports about itself.
  #
  # Not async: the authentication counts would pick up other tests' connections.
  use ExUnit.Case, async: false

  alias Malachi.Test.LoadtestProbes

  @moduletag :tmp_dir

  @script Path.expand("../../scripts/loadtest.js", __DIR__)
  @port Application.compile_env(:malachi, :tcp_port, 4040)

  setup_all do
    # A missing runtime fails loudly instead of skipping: a skipped generator test reads as a passing one.
    node = System.find_executable("node") || flunk("node is required to test scripts/loadtest.js; install Node 20+")
    %{node: node}
  end

  setup do
    probe = LoadtestProbes.watch_auth()
    on_exit(fn -> LoadtestProbes.stop_auth(probe) end)
    :ok
  end

  defp run_js(ctx, args) do
    env = [
      {"MALACHI_HOST", "127.0.0.1"},
      {"MALACHI_PORT", Integer.to_string(@port)},
      {"MALACHI_USER", "admin"},
      {"MALACHI_PASS", "admin123"}
    ]

    System.cmd(ctx.node, [@script | args], env: env, stderr_to_stdout: false)
  end

  defp topic, do: "ltjs_#{System.unique_integer([:positive])}"

  describe "--measure-marker" do
    test "is created only after every connection authenticated", ctx do
      marker = Path.join(ctx.tmp_dir, "measure.marker")
      LoadtestProbes.watch_file(marker)

      args =
        ~w(--scenario produce --json --connections 3 --batch 2 --duration 1 --warmup 1 --topic) ++
          [topic(), "--measure-marker", marker]

      assert {output, 0} = run_js(ctx, args)
      assert_receive {:file_appeared, ^marker, appeared_at}, 1_000
      auths = LoadtestProbes.successful_auths()

      assert appeared_at >= List.last(auths) - LoadtestProbes.poll_ms()
      assert {:ok, %{"errors" => 0}} = Jason.decode(output)
    end

    test "is left out of the recorded command, value included", ctx do
      marker = Path.join(ctx.tmp_dir, "measure.marker")
      args = ~w(--scenario produce --json --connections 1 --duration 1 --topic) ++ [topic(), "--measure-marker", marker]

      assert {output, 0} = run_js(ctx, args)
      assert {:ok, %{"meta" => %{"command" => command}}} = Jason.decode(output)

      refute command =~ "measure-marker"
      refute command =~ marker
      assert command =~ "--connections 1"
    end

    test "a directory that does not exist fails before any connection opens", ctx do
      marker = Path.join([ctx.tmp_dir, "missing", "m"])

      assert {_output, 1} =
               run_js(ctx, ~w(--scenario produce --connections 2 --duration 1 --measure-marker) ++ [marker])

      assert LoadtestProbes.successful_auths() == []
    end

    test "a trailing flag with no path is refused, not ignored", ctx do
      assert {_output, 1} = run_js(ctx, ~w(--scenario produce --connections 1 --duration 1 --measure-marker))
      assert LoadtestProbes.successful_auths() == []
    end
  end
end
