defmodule CliJsTest do
  # The Node CLIs (scripts/producer.js, consumer.js, subscriber.js) against the test server, on the stream
  # and group keys: what one appends the others read, a group member resumes from the group's checkpoint,
  # and two members split a topic's ranges between them and follow the rebalance a second one causes. And
  # their cases against brokers of their own (scripts/lib/cli.selftest.js, group_member.selftest.js), for
  # what the server cannot be made to do from here: go down and come back, refuse, or not be there.
  use ExUnit.Case, async: false

  alias Malachi.BrokerServer
  alias Malachi.Consumer.Assignment
  alias Malachi.DataPlaneRouter
  alias Malachi.TCPAcceptorPool
  alias Malachi.Test.StreamsOn
  alias Malachi.Test.TCPHelper
  alias Malachi.Wire

  @scripts Path.expand("../../scripts", __DIR__)
  @selftests [{"cli.selftest.js", "passed 8 cli cases"}, {"group_member.selftest.js", "passed 23 group member cases"}]

  setup_all do
    # A missing runtime fails loudly instead of skipping: a skipped client test reads as a passing one.
    %{node: System.find_executable("node") || flunk("node is required; install Node 22.15+ (zstd in zlib)")}
  end

  setup do
    :ok = StreamsOn.setup(%{})
    %{topic: "cli_js_#{System.unique_integer([:positive])}"}
  end

  defp env do
    [
      {"MALACHI_HOST", "127.0.0.1"},
      {"MALACHI_PORT", Integer.to_string(TCPAcceptorPool.port())},
      {"MALACHI_USER", "admin"},
      {"MALACHI_PASS", "admin123"}
    ]
  end

  # A CLI run to its end: what it printed and its exit status. Started as `start/3` does, so one that never
  # ends is killed with the test rather than left running.
  defp run(ctx, script, args) do
    cli = start(ctx, script, args)
    collect(cli, cli.port, 60_000, "did not exit")
  end

  # A CLI that runs until it is interrupted: started, its output collected as it comes.
  defp start(ctx, script, args) do
    port =
      Port.open({:spawn_executable, ctx.node}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: [Path.join(@scripts, script) | args],
        env: Enum.map(env(), fn {k, v} -> {String.to_charlist(k), String.to_charlist(v)} end)
      ])

    {:os_pid, pid} = Port.info(port, :os_pid)
    on_exit(fn -> System.cmd("kill", ["-9", Integer.to_string(pid)], stderr_to_stdout: true) end)
    %{port: port, pid: pid, out: ""}
  end

  defp output(%{port: port} = cli) do
    receive do
      {^port, {:data, data}} -> output(%{cli | out: cli.out <> data})
    after
      0 -> cli
    end
  end

  # Waits until the CLI's output matches `pattern`, and returns it with what it printed so far.
  defp until_output(cli, pattern, timeout \\ 15_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    wait_for(cli, pattern, deadline)
  end

  defp wait_for(cli, pattern, deadline) do
    cli = output(cli)

    cond do
      cli.out =~ pattern -> cli
      System.monotonic_time(:millisecond) > deadline -> flunk("never printed #{inspect(pattern)}:\n#{cli.out}")
      true -> wait_more(fn -> wait_for(cli, pattern, deadline) end)
    end
  end

  defp wait_more(again) do
    Process.sleep(50)
    again.()
  end

  # Interrupts the CLI as Ctrl-C does and returns all it printed, once it exited.
  defp interrupt(%{port: port, pid: pid} = cli) do
    System.cmd("kill", ["-INT", Integer.to_string(pid)])
    collect(cli, port, 15_000, "did not exit after SIGINT")
  end

  defp collect(cli, port, timeout, why) do
    receive do
      {^port, {:data, data}} -> collect(%{cli | out: cli.out <> data}, port, timeout, why)
      {^port, {:exit_status, status}} -> {cli.out, status}
    after
      timeout -> flunk("#{why}:\n#{cli.out}")
    end
  end

  # Both members' output, until they printed `count` records between them.
  defp until_read(first, second, count, deadline) do
    {first, second} = {output(first), output(second)}

    cond do
      length(ids(first.out) ++ ids(second.out)) >= count -> {first, second}
      System.monotonic_time(:millisecond) > deadline -> flunk("read too few:\n#{first.out}\n---\n#{second.out}")
      true -> wait_more(fn -> until_read(first, second, count, deadline) end)
    end
  end

  # The values a CLI printed, one per record, from the JSON value producer.js writes.
  defp ids(output) do
    ~r/"id":(\d+)/ |> Regex.scan(output) |> Enum.map(fn [_, id] -> String.to_integer(id) end)
  end

  for {selftest, passed} <- @selftests do
    # the cli cases wait out a cluster that does not answer, ten seconds and more a case
    @tag timeout: 180_000
    test "the #{selftest} cases against brokers of their own", ctx do
      assert {output, 0} =
               System.cmd(ctx.node, [Path.join([@scripts, "lib", unquote(selftest)])], stderr_to_stdout: true)

      assert output =~ unquote(passed)
    end
  end

  test "what the producer appends, a consumer reads, and a subscriber streams", ctx do
    assert {out, 0} = run(ctx, "producer.js", [ctx.topic, "25", "--create"])
    assert out =~ "Appended 25 record(s)"

    assert {out, 0} = run(ctx, "consumer.js", [ctx.topic, "--max", "7"])
    assert out =~ "Done. consumed=25"
    assert Enum.sort(ids(out)) == Enum.to_list(1..25)

    sub = ctx |> start("subscriber.js", [ctx.topic, "--window", "5", "--max", "3"]) |> until_output(~r/\[25\]/)
    assert {out, 0} = interrupt(sub)
    assert out =~ "Stopped. received=25"
    assert Enum.sort(ids(out)) == Enum.to_list(1..25)
  end

  test "a group member commits as it reads: the next member of the group resumes past it", ctx do
    {_, 0} = run(ctx, "producer.js", [ctx.topic, "10", "--create"])

    assert {out, 0} = run(ctx, "consumer.js", [ctx.topic, "--group", "g", "--member", "m1"])
    assert out =~ "Done. consumed=10"

    {_, 0} = run(ctx, "producer.js", [ctx.topic, "5"])
    assert {out, 0} = run(ctx, "consumer.js", [ctx.topic, "--group", "g", "--member", "m2"])
    assert out =~ "Done. consumed=5", "resumed from the group's checkpoint, not from the start"
    assert Enum.sort(ids(out)) == Enum.to_list(1..5)
  end

  test "a subscriber in a group commits what it streams, and a consumer of the group resumes past it", ctx do
    {_, 0} = run(ctx, "producer.js", [ctx.topic, "8", "--create"])
    # each page's commit is sent before the next event is read, on the connection its leave goes over after
    sub = ctx |> start("subscriber.js", [ctx.topic, "--group", "s"]) |> until_output(~r/\[8\]/)
    {_, 0} = interrupt(sub)

    assert {out, 0} = run(ctx, "consumer.js", [ctx.topic, "--group", "s"])
    assert out =~ "Done. consumed=0"
  end

  test "reading once, a member the group hands no range finishes with nothing read", ctx do
    {_, 0} = run(ctx, "producer.js", [ctx.topic, "3", "--create"])
    # a topic of one range: the group hands it to one member, the other gets none
    idle =
      Enum.find(for(i <- 1..100, do: "idle#{i}"), fn name ->
        [{ctx.topic, 0}] |> Assignment.assign(["busy", name]) |> Map.fetch!(name) == []
      end)

    busy =
      ctx
      |> start("consumer.js", [ctx.topic, "--group", "q", "--member", "busy", "--follow"])
      |> until_output(~r/\[3\]/)

    assert {out, 0} = run(ctx, "consumer.js", [ctx.topic, "--group", "q", "--member", idle])
    assert out =~ "ranges []"
    assert out =~ "the group handed this member no range"
    assert out =~ "Done. consumed=0"
    {_, 0} = interrupt(busy)
  end

  for {script, follow} <- [{"consumer.js", ["--follow"]}, {"subscriber.js", []}] do
    test "two #{script} members split the ranges between them, following the rebalance the second one causes",
         ctx do
      two_members_split(ctx, unquote(script), unquote(follow))
    end
  end

  defp two_members_split(ctx, script, follow) do
    {:ok, socket} = TCPHelper.connect()
    {:ok, _token} = TCPHelper.authenticate_wire(socket, "admin", "admin123")
    {0, _} = TCPHelper.request(socket, Wire.create_topic_key(), 2, Wire.encode_create_topic_req(ctx.topic, 4))
    :gen_tcp.close(socket)
    shard = DataPlaneRouter.shard_for(ctx.topic)
    {:ok, left, right} = BrokerServer.split_range(shard, {ctx.topic, 0})
    # the assignment is rendezvous hashing, balanced only in expectation: a second member whose name hashes
    # one range to each, so the rebalance it causes moves a range
    second_name =
      Enum.find(for(i <- 1..100, do: "b#{i}"), fn name ->
        [left, right] |> Assignment.assign(["a", name]) |> Map.values() |> Enum.all?(&(length(&1) == 1))
      end)

    first =
      ctx
      |> start(script, [ctx.topic, "--group", "w", "--member", "a" | follow])
      |> until_output(~r/ranges \[\d+,\d+\]/)

    second =
      ctx
      |> start(script, [ctx.topic, "--group", "w", "--member", second_name | follow])
      |> until_output(~r/ranges \[\d+\]/)

    # the first member hears of the rebalance on its next heartbeat and keeps one range
    first = until_output(first, ~r/ranges \[\d+,\d+\][\s\S]*ranges \[\d+\]/, 20_000)

    {_, 0} = run(ctx, "producer.js", [ctx.topic, "40"])

    {first, second} = until_read(first, second, 40, System.monotonic_time(:millisecond) + 20_000)

    {a, 0} = interrupt(first)
    {b, 0} = interrupt(second)
    read = ids(a) ++ ids(b)
    # every record read, by one member or the other: each its own share, none read by both, which a member
    # still reading the range the rebalance took from it would
    assert MapSet.new(read) == MapSet.new(1..40)
    assert ids(a) != [] and ids(b) != []
    assert MapSet.disjoint?(MapSet.new(ids(a)), MapSet.new(ids(b)))
  end
end
