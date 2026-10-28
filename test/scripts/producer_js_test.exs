defmodule ProducerJsTest do
  # The Node producer (scripts/lib/producer.js): its cases against a broker in its own process
  # (scripts/lib/testing/fake_broker.js), and a run against the test server, where every value it reports
  # acknowledged must be read back, across a split made while it produces.
  use ExUnit.Case, async: false

  alias Malachi.BrokerServer
  alias Malachi.DataPlaneRouter
  alias Malachi.Loadtest.Conn
  alias Malachi.Loadtest.Router
  alias Malachi.TCPAcceptorPool
  alias Malachi.Test.StreamsOn
  alias Malachi.Test.TmpDir
  alias Malachi.Wire

  @selftest Path.expand("../../scripts/lib/producer.selftest.js", __DIR__)

  setup_all do
    Code.require_file("scripts/chaos_checker.exs")
    # A missing runtime fails loudly instead of skipping: a skipped client test reads as a passing one.
    %{node: System.find_executable("node") || flunk("node is required; install Node 22.15+ (zstd in zlib)")}
  end

  test "the producer cases against a broker of its own", ctx do
    assert {output, 0} = System.cmd(ctx.node, [@selftest], stderr_to_stdout: true)
    assert output =~ "passed 29 producer cases"
  end

  test "against the server, across a split: every value acknowledged is read back, once", ctx do
    :ok = StreamsOn.setup(%{})
    topic = "producer_js_#{System.unique_integer([:positive])}"
    {:ok, conn} = Conn.connect(port: TCPAcceptorPool.port())
    {:ok, conn} = Conn.authenticate(conn, user: "admin", pass: "admin123")
    {:ok, 0, _, conn} = Conn.request(conn, Wire.create_topic_key(), 2, Wire.encode_create_topic_req(topic, 4))
    {:ok, router, conn} = Router.bootstrap(Router.new({"127.0.0.1", TCPAcceptorPool.port()}), conn)
    Conn.close(conn)

    values = for i <- 1..600, do: "v#{i}"
    parent = self()

    spawn_link(fn ->
      Process.sleep(150)
      send(parent, {:split, BrokerServer.split_range(DataPlaneRouter.shard_for(topic), {topic, 0})})
    end)

    input = TmpDir.path("producer_js") <> ".txt"
    File.write!(input, Enum.join(values, "\n"))
    on_exit(fn -> File.rm(input) end)

    env = [
      {"MALACHI_HOST", "127.0.0.1"},
      {"MALACHI_PORT", Integer.to_string(TCPAcceptorPool.port())},
      {"MALACHI_USER", "admin"},
      {"MALACHI_PASS", "admin123"}
    ]

    assert {output, 0} =
             System.cmd("sh", ["-c", ~s(exec "$0" "$1" --live "$2" < "$3"), ctx.node, @selftest, topic, input],
               env: env
             )

    result = Jason.decode!(output)
    assert_receive {:split, {:ok, _left, _right}}, 5_000

    assert result["prepared"] == %{"ok" => true}
    assert result["waited"] == "idle"
    assert result["failed"] == []
    assert Enum.sort(result["acked"]) == Enum.sort(values), "every value handed over was acknowledged"
    assert result["stats"]["moved"] + result["stats"]["redirects"] >= 1, "the producer followed the split"

    {read, _router, scan} = ChaosChecker.read_topic(router, topic, System.monotonic_time(:millisecond) + 30_000)
    assert scan.status == :settled
    assert MapSet.subset?(MapSet.new(values), read)
  end
end
