defmodule ReadersJsTest do
  # The Node readers (scripts/lib/fetcher.js, consumer.js, read_lanes.js): their cases against a broker in
  # their own process (scripts/lib/testing/fake_broker.js), and a run against the test server over a topic
  # split after part of it was written, which both readers must read whole.
  use ExUnit.Case, async: false

  alias Malachi.BrokerServer
  alias Malachi.DataPlaneRouter
  alias Malachi.Log.Record
  alias Malachi.TCPAcceptorPool
  alias Malachi.Test.StreamsOn
  alias Malachi.Test.TCPHelper
  alias Malachi.Wire

  @selftest Path.expand("../../scripts/lib/readers.selftest.js", __DIR__)

  setup_all do
    # A missing runtime fails loudly instead of skipping: a skipped client test reads as a passing one.
    %{node: System.find_executable("node") || flunk("node is required; install Node 22.15+ (zstd in zlib)")}
  end

  test "the reader cases against a broker of their own", ctx do
    assert {output, 0} = System.cmd(ctx.node, [@selftest], stderr_to_stdout: true)
    assert output =~ "passed 26 reader cases"
  end

  test "against the server, across a split: both readers read every record", ctx do
    :ok = StreamsOn.setup(%{})
    topic = "readers_js_#{System.unique_integer([:positive])}"
    {:ok, socket} = TCPHelper.connect()
    {:ok, _token} = TCPHelper.authenticate_wire(socket, "admin", "admin123")
    {0, _} = TCPHelper.request(socket, Wire.create_topic_key(), 2, Wire.encode_create_topic_req(topic, 4))
    :gen_tcp.close(socket)

    shard = DataPlaneRouter.shard_for(topic)
    before = for i <- 1..20, do: "b#{i}"
    {:ok, _} = BrokerServer.produce(shard, topic, Enum.map(before, &Record.new(&1, key: &1)))
    {:ok, _left, _right} = BrokerServer.split_range(shard, {topic, 0})
    later = for i <- 1..20, do: "a#{i}"
    for value <- later, do: {:ok, _} = BrokerServer.produce(shard, topic, [Record.new(value, key: value)])

    env = [
      {"MALACHI_HOST", "127.0.0.1"},
      {"MALACHI_PORT", Integer.to_string(TCPAcceptorPool.port())},
      {"MALACHI_USER", "admin"},
      {"MALACHI_PASS", "admin123"}
    ]

    assert {output, 0} = System.cmd(ctx.node, [@selftest, "--live", topic, "40"], env: env, stderr_to_stdout: true)
    result = Jason.decode!(output)

    # every record once across both children (the children's histories hold the parent's): at least once
    assert MapSet.new(result["fetched"]) == MapSet.new(before ++ later)
    assert result["subscribed"] == %{"ok" => true}
    assert result["consumed"] >= 40
  end
end
