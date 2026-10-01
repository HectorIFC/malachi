defmodule PayloadJsTest do
  # The Node side of the load generator payloads (scripts/lib/payload.js): its self-test against the golden
  # vectors this suite also reads, its values compared live with Malachi.Loadtest.Payload rather than only
  # through the fixture (which a change made identically on both sides would pass), and its refusal to run
  # the compression checks on a runtime whose zlib has no zstd.
  use ExUnit.Case, async: true

  alias Malachi.Loadtest.Payload

  @selftest Path.expand("../../scripts/lib/payload.selftest.js", __DIR__)

  setup_all do
    # A missing runtime fails loudly instead of skipping: a skipped generator test reads as a passing one.
    node =
      System.find_executable("node") ||
        flunk("node is required to test scripts/lib/payload.js; install Node 22.15+")

    %{node: node}
  end

  test "the Node generator passes the golden vectors and the compression band", ctx do
    assert {output, 0} = System.cmd(ctx.node, [@selftest], stderr_to_stdout: true)
    assert output =~ "payload.selftest.js passed 19 checks"
  end

  test "the Node values are the Elixir values, computed live on both sides", ctx do
    cases = [{:json, 0, Payload.min_json_size(), 300}, {:json, 77, 500, 300}, {:random, 0xFFFF_FFFF, 33, 300}]

    for {mode, seed, size, count} <- cases do
      args = [@selftest, "--digest", Atom.to_string(mode), "#{seed}", "#{size}", "#{count}"]
      assert {digest, 0} = System.cmd(ctx.node, args)

      expected = :sha256 |> :crypto.hash(Payload.values(mode, seed, size, count)) |> Base.encode16(case: :lower)
      assert String.trim(digest) == expected, "#{mode} seed #{seed} size #{size}"
    end
  end

  test "the Node pool rules are the Elixir pool rules", ctx do
    assert {json, 0} = System.cmd(ctx.node, [@selftest, "--constants"])
    constants = Jason.decode!(json)

    assert constants["min_json_size"] == Payload.min_json_size()
    assert constants["default_seed"] == Payload.default_seed()
    assert constants["modes"] == Enum.map(Payload.modes(), &Atom.to_string/1)

    for {key, pool} <- constants["pool_size"] do
      [size, batch, connections, pipeline] = key |> String.split("x") |> Enum.map(&String.to_integer/1)
      assert pool == Payload.pool_size(size, batch, connections, pipeline), key
    end

    expected =
      for {i, b, c} <- [{0, 100, 8}, {3, 100, 8}, {7, 100, 8}, {5, 3, 10}, {9, 1, 4}], do: Payload.start_batch(i, b, c)

    assert constants["start_batch"] == expected
  end

  @tag :tmp_dir
  test "a runtime whose zlib has no zstd fails the checks and says why", ctx do
    stub = Path.join(ctx.tmp_dir, "no_zstd.js")
    File.write!(stub, "require('zlib').zstdCompressSync = undefined;\n")

    assert {output, 1} = System.cmd(ctx.node, ["--require", stub, @selftest], stderr_to_stdout: true)
    assert output =~ "needs a Node with zlib zstd (22.15 or later)"
  end
end
