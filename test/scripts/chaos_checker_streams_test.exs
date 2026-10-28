defmodule ChaosCheckerStreamsTest do
  # The checker's produce and verify paths against a real broker serving the stream keys: values produced
  # through producer streams under a window, and read back range by range with fetch_range.
  use ExUnit.Case, async: false

  alias Malachi.BrokerServer
  alias Malachi.DataPlaneRouter
  alias Malachi.Loadtest.Conn
  alias Malachi.Loadtest.Router
  alias Malachi.TCPAcceptorPool
  alias Malachi.Test.RouterTopology
  alias Malachi.Test.StreamStub
  alias Malachi.Wire

  setup_all do
    Code.require_file("scripts/chaos_checker.exs")
    :ok
  end

  setup do
    :ok = RouterTopology.clear()
    topic = "chaos_#{System.unique_integer([:positive])}"
    conn = connect!(TCPAcceptorPool.port())
    {:ok, 0, _, conn} = Conn.request(conn, Wire.create_topic_key(), 2, Wire.encode_create_topic_req(topic, 4))
    {:ok, router, conn} = Router.bootstrap(Router.new({"127.0.0.1", TCPAcceptorPool.port()}), conn)
    Conn.close(conn)
    %{topic: topic, router: router}
  end

  defp connect!(port) do
    {:ok, conn} = Conn.connect(port: port)
    {:ok, conn} = Conn.authenticate(conn, user: "admin", pass: "admin123")
    conn
  end

  defp now, do: System.monotonic_time(:millisecond)

  # The values a window acknowledged, in the order their acknowledgements came.
  defp produce(router, topic, window_ms, progress_ms \\ 60_000) do
    parent = self()
    opts = [report: &send(parent, {:report, &1}), progress_ms: progress_ms]
    produced = ChaosChecker.produce_window(router, topic, now() + window_ms, &send(parent, {:acked, &1}), opts)
    {produced, acked()}
  end

  defp acked do
    receive do
      {:acked, value} -> [value | acked()]
    after
      0 -> []
    end
  end

  test "every value the window acknowledged is read back", ctx do
    {produced, acked} = produce(ctx.router, ctx.topic, 1_000)

    assert produced > 0
    assert length(acked) == produced, "every value handed out on a healthy broker is acknowledged"
    assert acked == Enum.uniq(acked)

    {read, _router, scan} = ChaosChecker.read_topic(ctx.router, ctx.topic, now() + 30_000)
    assert scan.status == :settled
    assert MapSet.subset?(MapSet.new(acked), read)
  end

  test "a split during the window moves the producer, and every acknowledged value is read back across the children",
       ctx do
    parent = self()

    spawn_link(fn ->
      Process.sleep(400)
      send(parent, {:split, BrokerServer.split_range(DataPlaneRouter.shard_for(ctx.topic), {ctx.topic, 0})})
    end)

    {produced, acked} = produce(ctx.router, ctx.topic, 1_500)
    assert_receive {:split, {:ok, _left, _right}}, 5_000
    # every value handed out was acknowledged, and none twice
    assert length(acked) == produced
    assert acked == Enum.uniq(acked)

    {read, _router, scan} = ChaosChecker.read_topic(ctx.router, ctx.topic, now() + 30_000)
    assert scan.status == :settled
    # the producer moved to the children: a child was read past records of its own (an offset above zero
    # in a source past its parent's history), which only an append after the split can have put there
    assert Enum.any?(scan.pages, &(&1.range != 0 and match?({source, offset} when source >= 1 and offset > 0, &1.to)))
    assert MapSet.subset?(MapSet.new(acked), read)
  end

  test "appends still in flight when the window ends are waited for, and their values written" do
    # every acknowledgement takes 300 ms, so the window ends with appends on the wire
    stub = StreamStub.start(%{27 => {:delay, 300, :default}})
    on_exit(fn -> StreamStub.stop(stub) end)
    conn = connect!(stub.port)
    {:ok, router, conn} = Router.bootstrap(Router.new({"127.0.0.1", stub.port}), conn)
    Conn.close(conn)

    {produced, acked} = produce(router, "t", 200)
    assert produced > 0
    assert length(acked) == produced
    # the last report is written after the final wait, so it counts its acknowledgements too
    assert List.last(reports()) =~ "handed=#{produced} acked=#{produced} "
  end

  test "a window reports how it goes as it goes, and once at the end", ctx do
    {produced, acked} = produce(ctx.router, ctx.topic, 700, 200)
    lines = reports()

    # one about every 200 ms (two or three in the window), then the last, which counts every value the
    # window handed out and had acknowledged
    assert length(lines) in 3..4
    assert List.last(lines) =~ ~r/^PRODUCE t=\d+s handed=#{produced} acked=#{length(acked)} resend=0 routing=%\{/
    handed = for line <- lines, do: line |> String.replace(~r/.* handed=(\d+) .*/, "\\1") |> String.to_integer()
    assert handed == Enum.sort(handed)
  end

  defp reports do
    receive do
      {:report, line} -> [line | reports()]
    after
      0 -> []
    end
  end

  test "an append the broker refuses is sent again, and its value acknowledged once" do
    # the first append is refused, every later one acknowledged
    stub = StreamStub.start(%{27 => {:after, 1, {:refuse_append, "overloaded"}, :default}})
    on_exit(fn -> StreamStub.stop(stub) end)
    conn = connect!(stub.port)
    {:ok, router, conn} = Router.bootstrap(Router.new({"127.0.0.1", stub.port}), conn)
    Conn.close(conn)

    {produced, acked} = produce(router, "t", 300)

    assert StreamStub.refused(stub) == 1
    assert "c-1" in acked, "the refused value went out again and was acknowledged"
    assert acked == Enum.uniq(acked)
    assert length(acked) == produced
    # sent twice: once refused, once acknowledged
    assert length(StreamStub.appends(stub)) == produced + 1
  end
end
