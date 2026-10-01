defmodule PolicyJsTest do
  # The Node side of the storage policy api keys: its codecs against the golden frames the Elixir codec is
  # tested with, its field table against the one the server validates with, and scripts/policy.js run
  # against the test server the app starts in test_helper.
  #
  # Not async: the policy store and broker are the application's.
  use ExUnit.Case, async: false

  alias Malachi.Cluster.Policy
  alias Malachi.Cluster.PolicyStore
  alias Malachi.TCPAcceptorPool

  @selftest Path.expand("../../scripts/lib/wire.selftest.js", __DIR__)
  @script Path.expand("../../scripts/policy.js", __DIR__)

  setup_all do
    # A missing runtime fails loudly instead of skipping: a skipped client test reads as a passing one.
    node = System.find_executable("node") || flunk("node is required to test scripts/policy.js; install Node 22.15+")
    %{node: node}
  end

  defp run_js(ctx, args, opts \\ []) do
    env = [
      {"MALACHI_HOST", "127.0.0.1"},
      {"MALACHI_PORT", Integer.to_string(TCPAcceptorPool.port())},
      {"MALACHI_USER", Keyword.get(opts, :user, "admin")},
      {"MALACHI_PASS", Keyword.get(opts, :pass, "admin123")}
    ]

    System.cmd(ctx.node, [@script | args], env: env, stderr_to_stdout: true)
  end

  test "the Node codecs read and write every golden frame byte for byte", ctx do
    assert {output, 0} = System.cmd(ctx.node, [@selftest], stderr_to_stdout: true)
    assert output =~ "passed 12 golden frames"
  end

  test "the Node field table is the server's field table", ctx do
    assert {json, 0} = System.cmd(ctx.node, [@selftest, "--fields"])

    expected = Map.new(Policy.fields(), &{&1.name, Atom.to_string(&1.type)})
    assert Jason.decode!(json) == expected
  end

  describe "scripts/policy.js against a running node" do
    setup do
      suffix = System.unique_integer([:positive])
      name = "js_policy_#{suffix}"
      topic = "js-topic-#{suffix}"
      :ok = Malachi.LogApi.create_topic(Malachi.DataPlaneRouter.shard_for(topic), topic)
      on_exit(fn -> PolicyStore.delete(name) end)
      %{name: name, topic: topic}
    end

    test "define, list, bind, get, a refused delete, unbind and delete", ctx do
      assert {out, 0} =
               run_js(ctx, ["define", ctx.name, "--set", "retention.max_bytes=0", "--off", "retention.max_age_ms"])

      assert out =~ "defined policy #{ctx.name}"
      assert PolicyStore.get(ctx.name) == %{retention: %{max_bytes: 0, max_age_ms: nil}}

      assert {out, 0} = run_js(ctx, ["list"])
      # The server lists fields in table order, whatever order they were defined in.
      assert out =~ "retention.max_age_ms=off retention.max_bytes=0"

      assert {_out, 0} = run_js(ctx, ["bind", ctx.topic, ctx.name])
      assert {out, 0} = run_js(ctx, ["get", ctx.topic])
      assert out =~ "policy\t#{ctx.name}\n"
      assert out =~ "retention.max_bytes\t0\t(policy)"
      assert out =~ "retention.max_age_ms\toff\t(policy)"

      assert {out, 1} = run_js(ctx, ["delete", ctx.name])
      assert out =~ "policy_in_use: #{ctx.topic}"

      assert {_out, 0} = run_js(ctx, ["unbind", ctx.topic])
      assert {_out, 0} = run_js(ctx, ["delete", ctx.name])
      assert PolicyStore.get(ctx.name) == nil
    end

    test "an option that expects a field refuses a missing one, or another option in its place", ctx do
      for args <- [["--off"], ["--off", "--force"], ["--set"], ["--set", "--force"]] do
        assert {out, 1} = run_js(ctx, ["define", ctx.name | args])
        assert out =~ "#{hd(args)} needs a value", "#{inspect(args)}: #{out}"
      end

      assert PolicyStore.get(ctx.name) == nil
    end

    test "a bad value is refused before connecting, and a non-admin is refused by the server", ctx do
      assert {out, 1} = run_js(ctx, ["define", ctx.name, "--set", "retention.max_bytes=1k"])
      assert out =~ "invalid value in --set retention.max_bytes=1k"

      assert {out, 1} = run_js(ctx, ["list"], user: "producer", pass: "producer123")
      assert out =~ "permission_denied"
    end
  end
end
