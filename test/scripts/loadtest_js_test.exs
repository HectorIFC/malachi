defmodule LoadtestJsTest do
  # scripts/loadtest.js is one of the two generators the published ceiling comes from, and nothing tested
  # it: its only check was a histogram self-test CI never ran. These cases run the real script against
  # the test server the app starts in test_helper, and observe it from the server side (authentication
  # telemetry, the file system) rather than trusting what it reports about itself.
  #
  # Not async: the authentication counts would pick up other tests' connections.
  use ExUnit.Case, async: false

  alias Malachi.TCPAcceptorPool
  alias Malachi.Test.LoadtestProbes

  @moduletag :tmp_dir

  @script Path.expand("../../scripts/loadtest.js", __DIR__)

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

  # `stderr: true` folds the generator's error output into the returned string, for the cases that are
  # about what it refuses. The default keeps stderr out, so a run's JSON is the whole of stdout.
  # `user:` and `pass:` run it as someone other than admin.
  defp run_js(ctx, args, opts \\ []) do
    env = [
      {"MALACHI_HOST", "127.0.0.1"},
      {"MALACHI_PORT", Integer.to_string(TCPAcceptorPool.port())},
      {"MALACHI_USER", Keyword.get(opts, :user, "admin")},
      {"MALACHI_PASS", Keyword.get(opts, :pass, "admin123")}
    ]

    System.cmd(ctx.node, [@script | args], env: env, stderr_to_stdout: Keyword.get(opts, :stderr, false))
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

      # After the last authentication AND the whole warmup: the connections are kept through it, so the
      # last authentication happens before the warmup starts.
      assert appeared_at >= List.last(auths) + 1_000 - LoadtestProbes.poll_ms()
      assert {:ok, %{"errors" => 0, "error_reasons" => %{}}} = Jason.decode(output)
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

      assert {output, 1} =
               run_js(ctx, ~w(--scenario produce --connections 2 --duration 1 --measure-marker) ++ [marker],
                 stderr: true
               )

      assert output =~ "does not exist or is not writable"
      assert LoadtestProbes.successful_auths() == []
    end

    test "a marker that already exists is overwritten rather than refused", ctx do
      marker = Path.join(ctx.tmp_dir, "stale.marker")
      File.write!(marker, "from an earlier run")
      args = ~w(--scenario produce --json --connections 1 --duration 1 --topic) ++ [topic(), "--measure-marker", marker]

      assert {_output, 0} = run_js(ctx, args)
      assert File.read!(marker) == ""
    end

    test "a marker path that is a directory fails before any connection opens", ctx do
      # Checking only the parent directory let this through, and it then failed inside writeFileSync,
      # after every connection had authenticated and the warmup had run.
      marker = Path.join(ctx.tmp_dir, "marker.d")
      File.mkdir_p!(marker)

      assert {output, 1} =
               run_js(ctx, ~w(--scenario produce --connections 2 --duration 1 --measure-marker) ++ [marker],
                 stderr: true
               )

      assert output =~ "exists and is not a regular file"
      assert LoadtestProbes.successful_auths() == []
    end

    test "a marker that exists and cannot be written fails before any connection opens", ctx do
      marker = Path.join(ctx.tmp_dir, "read-only.marker")
      File.write!(marker, "")
      File.chmod!(marker, 0o444)
      on_exit(fn -> File.chmod(marker, 0o644) end)

      assert {output, 1} =
               run_js(ctx, ~w(--scenario produce --connections 2 --duration 1 --measure-marker) ++ [marker],
                 stderr: true
               )

      assert output =~ "exists and is not writable"
      assert LoadtestProbes.successful_auths() == []
    end

    test "a trailing flag with no path is refused, not ignored", ctx do
      assert {_output, 1} = run_js(ctx, ~w(--scenario produce --connections 1 --duration 1 --measure-marker))
      assert LoadtestProbes.successful_auths() == []
    end
  end

  describe "the flush regime" do
    test "the report records the batch size and record size it ran with, as fields of their own", ctx do
      args = ~w(--scenario produce --json --connections 1 --duration 1 --batch 7 --record-size 100 --topic) ++ [topic()]

      assert {output, 0} = run_js(ctx, args)
      assert {:ok, %{"batch" => 7, "record_size" => 100}} = Jason.decode(output)

      # This generator's own defaults, which differ from the Elixir one's; recorded, not assumed.
      assert {defaults, 0} =
               run_js(ctx, ~w(--scenario produce --json --connections 1 --duration 1 --topic) ++ [topic()])

      assert {:ok, %{"batch" => 1, "record_size" => 128}} = Jason.decode(defaults)
    end

    test "both generators record the same regime fields for the same flags", ctx do
      # The two generators mirror their flags explicitly; the ceiling sweep reads these fields from either
      # one through a single path, so a name or a value drifting in one of them would break it quietly.
      flags = %{"connections" => 2, "batch" => 3, "record_size" => 64}

      {node_output, 0} =
        run_js(
          ctx,
          ~w(--scenario produce --json --connections 2 --batch 3 --record-size 64 --duration 1 --topic) ++ [topic()]
        )

      elixir_report =
        ExUnit.CaptureIO.capture_io(fn ->
          Malachi.Loadtest.run(
            port: TCPAcceptorPool.port(),
            user: "admin",
            pass: "admin123",
            scenario: :produce,
            connections: 2,
            batch: 3,
            record_size: 64,
            duration: 1,
            warmup: 0,
            topic: topic(),
            json: true
          )
        end)

      regime = fn json -> json |> Jason.decode!() |> Map.take(Map.keys(flags)) end

      assert regime.(node_output) == flags
      assert regime.(elixir_report) == flags

      # The error breakdown too: same name, same shape (reason to count), empty on a clean run.
      assert %{"errors" => 0, "error_reasons" => %{}} = Jason.decode!(node_output)
      assert %{"errors" => 0, "error_reasons" => %{}} = Jason.decode!(elixir_report)
    end
  end

  describe "error reasons" do
    test "a refused fetch is counted under the reason the broker gave", ctx do
      # May produce, so the topic is created, but may not consume.
      {user, pass} = add_user([:produce])
      args = ~w(--scenario fetch --json --connections 2 --duration 1 --prepopulate 0 --topic) ++ [topic()]

      assert {output, 0} = run_js(ctx, args, user: user, pass: pass)
      report = Jason.decode!(output)

      assert report["errors"] > 0
      assert report["error_reasons"] == %{"permission_denied" => report["errors"]}
    end

    test "a refused topic creation fails the run and names the reason", ctx do
      # create_topic is gated by :produce, so a consume-only user is refused it.
      {user, pass} = add_user([:consume])
      args = ~w(--scenario produce --json --connections 1 --duration 1 --topic) ++ [topic()]

      assert {output, 1} = run_js(ctx, args, user: user, pass: pass, stderr: true)
      assert output =~ "permission_denied"
    end

    test "a topic that already exists is not a setup failure", ctx do
      args = ~w(--scenario produce --json --connections 1 --duration 1 --topic) ++ [topic()]

      assert {_first, 0} = run_js(ctx, args)
      assert {output, 0} = run_js(ctx, args)
      assert %{"errors" => 0, "error_reasons" => %{}} = Jason.decode!(output)
    end
  end

  # A user with `permissions` on the test server, removed when the test ends.
  defp add_user(permissions) do
    user = "ltjs_user_#{System.unique_integer([:positive])}"
    pass = "Ltjs-Pass-1!"
    Malachi.Auth.add_user(user, pass, permissions)
    on_exit(fn -> Malachi.Auth.remove_user(user) end)
    {user, pass}
  end

  describe "connections across the warmup" do
    test "a produce run authenticates each connection once and keeps it through the warmup", ctx do
      # Every connection used to be closed and reopened after the warmup, which only the stream scenario
      # needs: 1989 authentications against the Elixir generator's 997 on the same CI ladder, a second
      # auth storm inside every point, and a methodology the two generators no longer shared.
      args = ~w(--scenario produce --json --connections 4 --duration 1 --warmup 1 --topic) ++ [topic()]

      assert {_output, 0} = run_js(ctx, args)
      # The topic-creating admin connection, then one per worker.
      assert length(LoadtestProbes.successful_auths()) == 5
    end

    test "a mixed run keeps its connections too, since it subscribes to nothing", ctx do
      args =
        ~w(--scenario mixed --json --connections 2 --duration 1 --warmup 1 --prepopulate 50 --topic) ++ [topic()]

      assert {_output, 0} = run_js(ctx, args)
      # Admin, the prepopulating connection, then one per worker.
      assert length(LoadtestProbes.successful_auths()) == 4
    end

    test "a stream run still reconnects after the warmup, since a subscription ends only with its socket", ctx do
      args =
        ~w(--scenario stream --json --connections 2 --duration 1 --warmup 1 --prepopulate 50 --topic) ++ [topic()]

      assert {_output, 0} = run_js(ctx, args)
      # Admin, the prepopulating connection, then every worker twice.
      assert length(LoadtestProbes.successful_auths()) == 6
    end

    test "without a warmup nothing reconnects in any scenario", ctx do
      args = ~w(--scenario stream --json --connections 2 --duration 1 --prepopulate 50 --topic) ++ [topic()]

      assert {_output, 0} = run_js(ctx, args)
      assert length(LoadtestProbes.successful_auths()) == 4
    end
  end
end
