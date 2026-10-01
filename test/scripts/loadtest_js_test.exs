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
  alias Malachi.Test.SubscribeResetStub

  @moduletag :tmp_dir

  @script Path.expand("../../scripts/loadtest.js", __DIR__)

  setup_all do
    # A missing runtime fails loudly instead of skipping: a skipped generator test reads as a passing one.
    node = System.find_executable("node") || flunk("node is required to test scripts/loadtest.js; install Node 22.15+")
    %{node: node}
  end

  setup do
    probe = LoadtestProbes.watch_auth()
    on_exit(fn -> LoadtestProbes.stop_auth(probe) end)
    :ok
  end

  # `stderr: true` folds the generator's error output into the returned string, for the cases that are
  # about what it refuses. The default keeps stderr out, so a run's JSON is the whole of stdout.
  # `user:` and `pass:` run it as someone other than admin; `port:` points it at a server other than the
  # test one.
  defp run_js(ctx, args, opts \\ []) do
    env = [
      {"MALACHI_HOST", "127.0.0.1"},
      {"MALACHI_PORT", Integer.to_string(Keyword.get_lazy(opts, :port, &TCPAcceptorPool.port/0))},
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

  describe "dropped stream connections" do
    # Issue #218, mirrored from the Elixir generator: a stream connection lost before or after its
    # subscribe went out used to end the worker, counted in `errors` or (when the socket had already
    # closed) not at all, where the Elixir generator counts a drop and resubscribes.

    test "a stream worker whose subscribe send fails counts the drop and resubscribes", ctx do
      report = stream_on_reset_stub(ctx, :reconnect)

      assert report["dropped"] == 1, "the failed subscribe was not counted as a drop"
      assert report["reconnects"] == 1, "the worker should reconnect and resubscribe, not end"
      assert report["errors"] == 0, "a transport failure is a drop, not a server error"
      assert report["error_reasons"] == %{}
    end

    test "a stream worker that cannot reconnect after a failed subscribe gives up without crashing", ctx do
      report = stream_on_reset_stub(ctx, :give_up)

      assert report["dropped"] == 1, "the failed subscribe was not counted as a drop"
      assert report["reconnects"] == 0
      assert report["errors"] == 0
    end

    test "a stream connection lost after its subscribe went out counts the drop and resubscribes", ctx do
      # A FIN reaches the client as 'close' and a reset as 'error', so both of the client's paths for a
      # connection lost on a live subscription are covered.
      for how <- [:close, :reset] do
        report = stream_on_reset_stub(ctx, {:mid_stream, how}, 1)

        assert report["dropped"] == 1, "#{how}: the lost stream connection was not counted as a drop"
        assert report["reconnects"] == 1, "#{how}: the worker should reconnect and resubscribe, not end"
        assert report["errors"] == 0, "#{how}: a transport failure is a drop, not a server error"
      end
    end

    test "a reconnect that hangs at the deadline is abandoned instead of stretching the run", ctx do
      # The rates divide by the time the run took, so waiting out a stalled authentication (30s) would report
      # a fraction of the real rate and count pushes that arrived after the window.
      report = stream_on_reset_stub(ctx, :stall_reconnect)

      assert report["dropped"] == 1
      assert report["reconnects"] == 0
      assert report["duration_s"] < 1.5, "the run waited on the stalled reconnect past its 1s window"
    end

    # Without the fix this never finishes on its own: the refused reconnects keep sockets open, the process
    # outlives its report, and run_js waits on it until the test times out.
    @tag timeout: 30_000
    test "a reconnect whose authentication is refused leaves no socket holding the process open", ctx do
      report = stream_on_reset_stub(ctx, :refuse_reconnect_auth)

      assert report["dropped"] == 1
      assert report["reconnects"] == 0
      assert report["errors"] == 0, "a refused reconnect is not a stream error"
    end

    test "a refused subscription stays an error under its reason, not a drop", ctx do
      {user, pass} = add_user([:produce])
      args = ~w(--scenario stream --json --connections 2 --duration 1 --prepopulate 0 --topic) ++ [topic()]

      assert {output, 0} = run_js(ctx, args, user: user, pass: pass)
      report = Jason.decode!(output)

      assert report["errors"] == 2, "each worker subscribed once and each refusal counts once"
      assert report["error_reasons"] == %{"permission_denied" => 2}
      assert report["dropped"] == 0
      assert report["reconnects"] == 0
    end

    test "a healthy stream run that stays idle reports no drop", ctx do
      args = ~w(--scenario stream --json --connections 2 --duration 1 --prepopulate 0 --topic) ++ [topic()]

      assert {output, 0} = run_js(ctx, args)
      assert %{"dropped" => 0, "reconnects" => 0, "errors" => 0} = Jason.decode!(output)
    end

    test "outside the stream scenario the two counters are null, since only the stream driver counts them", ctx do
      args = ~w(--scenario produce --json --connections 1 --duration 1 --topic) ++ [topic()]

      assert {output, 0} = run_js(ctx, args)
      assert %{"dropped" => nil, "reconnects" => nil} = Jason.decode!(output)
    end
  end

  # A stream run against Malachi.Test.SubscribeResetStub, returning the decoded report. The connections open
  # one at a time and nothing is prepopulated, which is what the stub's ordering relies on: this generator
  # prepopulates on a connection of its own, which would take the stub's second place.
  defp stream_on_reset_stub(ctx, mode, connections \\ 2) do
    args =
      ~w(--scenario stream --json --connections #{connections} --connect-strategy bounded --connect-concurrency 1) ++
        ~w(--duration 1 --warmup 0 --prepopulate 0 --topic subscribe_reset)

    SubscribeResetStub.with_stub(mode, fn port ->
      assert {output, 0} = run_js(ctx, args, port: port)
      Jason.decode!(output)
    end)
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
