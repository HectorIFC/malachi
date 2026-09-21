defmodule Malachi.UnexpectedMessageTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record
  alias Malachi.Test.UnknownMessages
  alias Malachi.UnexpectedMessage

  @event [:malachi, :process, :unexpected_message]

  setup do
    # `drop/4` is called from this process in most tests here, which is where the event comes from.
    UnknownMessages.expect_from(self())
  end

  # Every drop this process reports while `fun` runs, as {server, kind, shape}.
  defp events(fun) do
    handler_id = {__MODULE__, make_ref()}
    test = self()

    :ok = :telemetry.attach(handler_id, @event, &__MODULE__.forward/4, %{pid: test, test: test})

    try do
      result = fun.()
      {result, collect([])}
    after
      :telemetry.detach(handler_id)
    end
  end

  @doc false
  def forward(_event, %{count: 1}, %{pid: pid} = metadata, %{pid: pid, test: test}),
    do: send(test, {:drop, metadata.server, metadata.kind, metadata.shape})

  def forward(_event, _measurements, _metadata, _config), do: :ok

  @doc false
  def count(_event, %{count: 1}, %{pid: pid}, %{pid: pid, counter: counter}), do: :counters.add(counter, 1, 1)
  def count(_event, _measurements, _metadata, _config), do: :ok

  defp collect(acc) do
    receive do
      {:drop, server, kind, shape} -> collect([{server, kind, shape} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp warning_lines(log, fragment),
    do: log |> String.split("\n", trim: true) |> Enum.filter(&(&1 =~ fragment))

  describe "the server labels" do
    test "the public type lists exactly what servers/0 returns" do
      # The type is the cross-module contract (`Malachi.Metrics` closes its label set on it) and dialyzer
      # does not catch a label added to the list and not to the union, which is how :skip_reporter ended
      # up in one and not the other.
      {:ok, types} = Code.Typespec.fetch_types(UnexpectedMessage)
      {:type, {:server, definition, []}} = Enum.find(types, &match?({:type, {:server, _, []}}, &1))
      {:type, _line, :union, members} = definition
      declared = for {:atom, _line, atom} <- members, do: atom

      assert Enum.sort(declared) == Enum.sort(UnexpectedMessage.servers())
    end
  end

  describe "shape/1" do
    test "names a tagged tuple by its tag and arity, and anything else by its type" do
      assert UnexpectedMessage.shape({:replica_append_v2, 1, 2}) == {:replica_append_v2, 3}
      assert UnexpectedMessage.shape({make_ref(), :ok}) == {:tuple, 2}
      assert UnexpectedMessage.shape({}) == {:tuple, 0}
      assert UnexpectedMessage.shape(:tick) == :tick
      assert UnexpectedMessage.shape([1, 2]) == :list
      assert UnexpectedMessage.shape(%{a: 1}) == :map
      assert UnexpectedMessage.shape(%URI{}) == :map
      assert UnexpectedMessage.shape("payload") == :binary
      assert UnexpectedMessage.shape(42) == :other
      assert UnexpectedMessage.shape(self()) == :other
    end
  end

  describe "drop/4" do
    test "counts every message but logs each {kind, shape} once per process" do
      {log, drops} =
        events(fn ->
          capture_log(fn ->
            seen = UnexpectedMessage.drop(MapSet.new(), :replication, :cast, {:new_push, 1})
            seen = UnexpectedMessage.drop(seen, :replication, :cast, {:new_push, 2})
            # same shape, another kind: a line of its own
            seen = UnexpectedMessage.drop(seen, :replication, :info, {:new_push, 3})
            assert MapSet.equal?(seen, MapSet.new([{:cast, {:new_push, 2}}, {:info, {:new_push, 2}}]))
          end)
        end)

      assert drops == [
               {:replication, :cast, {:new_push, 2}},
               {:replication, :cast, {:new_push, 2}},
               {:replication, :info, {:new_push, 2}}
             ]

      # Filtered by server and tag: this file is async, and the capture collects every process's lines.
      assert [_one] = warning_lines(log, "replication process dropping an unexpected cast: {:new_push")
      assert [_one] = warning_lines(log, "replication process ignoring an unexpected message: {:new_push")
    end

    test "stops logging new shapes past the limit, says so once, and keeps counting" do
      shapes = for i <- 1..40, do: {:"shape_#{i}", i}

      {{seen, log}, drops} =
        events(fn ->
          with_log(fn -> Enum.reduce(shapes, MapSet.new(), &UnexpectedMessage.drop(&2, :broker, :info, &1)) end)
        end)

      assert length(drops) == 40
      assert length(warning_lines(log, "broker process ignoring an unexpected message: {:shape_")) == 32
      assert [_once] = warning_lines(log, "broker process has logged 32 unexpected message shapes")
      refute log =~ "shape_33"
      # the marker is what keeps the limit line from repeating: 32 shapes plus the marker, no more
      assert MapSet.size(seen) == 33
      assert MapSet.member?(seen, :log_limit_reached)
    end

    test "logs the call kind with the reply the caller gets" do
      log = capture_log(fn -> UnexpectedMessage.drop(MapSet.new(), :membership, :call, {:status_v2}) end)

      assert log =~ "membership process answering {:error, :unknown_call} to an unexpected call"
      assert log =~ ":status_v2"
    end

    test "the log line never carries text: strings, charlists and integer lists are all elided" do
      message =
        {:t, [%{key: "user-9", value: "s3cret"}], ~c"s3cret-charlist", [115, 51, 99 | :improper], [7, 8, 9],
         <<115, 51, 99>>}

      log = capture_log(fn -> UnexpectedMessage.drop(MapSet.new(), :heal, :info, message) end)

      # The text checks run on the whole capture: the payload must not appear anywhere.
      refute log =~ "s3cret"
      refute log =~ "user-9"

      # The number checks run on the printed message only: the line's timestamp can hold any digits.
      assert [line] = warning_lines(log, "heal process ignoring an unexpected message: ")
      [_prefix, printed] = String.split(line, "heal process ignoring an unexpected message: ", parts: 2)

      # A list of maps used to raise inside the catch-all itself (see `redact/2`); now it is printed.
      assert printed =~ "{:t, [%{"

      for fragment <- ["115", "51", "99", "7, 8, 9"] do
        refute printed =~ fragment, "#{inspect(fragment)} reached the log line: #{line}"
      end
    end

    test "the log line never carries numbers either: an id in a scalar position is elided" do
      # A bounded prefix of a payload is still a payload, and a whole number is the whole payload. Record
      # keys and values are binaries today, so the reachable case is a message from a newer node whose
      # fields are numbers. Atoms stay: they come from code, not from user input, and they are what tells
      # two shapes with the same tag apart.
      message = {:user_event_v2, 90_210, :ssn_verified, %{account: 4_242_424_242, tier: :platinum}, 1.5}

      log = capture_log(fn -> UnexpectedMessage.drop(MapSet.new(), :broker, :info, message) end)

      assert [line] = warning_lines(log, "broker process ignoring an unexpected message: ")
      [_prefix, printed] = String.split(line, "broker process ignoring an unexpected message: ", parts: 2)

      assert printed =~ ":user_event_v2"
      assert printed =~ ":ssn_verified"

      for fragment <- ["90210", "90_210", "4242424242", "4_242_424_242", "1.5"] do
        refute printed =~ fragment, "#{inspect(fragment)} reached the log line: #{line}"
      end
    end

    test "a list of maps or an improper list does not crash the catch-all, whatever its depth" do
      for message <- [[%{a: "x"}], {:t, {:u, [%{a: "x"}]}}, [1 | 2], {:t, [:a | :b]}, [[[~c"x"]]]] do
        assert %MapSet{} = capture_log_result(fn -> UnexpectedMessage.drop(MapSet.new(), :scrubber, :cast, message) end)
      end
    end
  end

  test "the call reply is the documented cross-version contract" do
    assert UnexpectedMessage.unknown_call_reply() == {:error, :unknown_call}
  end

  # A newer primary pushing thousands of batches a second at an older follower must cost that follower
  # a count per message, not a log line per message: a logger pushed into synchronous mode would slow the
  # process that holds every log on the node.
  test "a flood of one unknown shape is one log line and a count per message, and the server keeps serving" do
    name = :"unexpected_flood_#{System.unique_integer([:positive])}"
    directory = Path.join(System.tmp_dir!(), "malachi_unexpected_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    start_supervised!({ReplicationServer, [name: name, directory: directory]}, id: name)
    segment = {{"flood", 0}, 0}
    assert {:ok, 0} = ReplicationServer.replicate(name, segment, [name], 0, [Record.new("kept", key: "k")])

    pid = Process.whereis(name)
    UnknownMessages.expect_from(pid)
    counter = :counters.new(1, [])
    handler_id = {__MODULE__, make_ref()}

    :ok = :telemetry.attach(handler_id, @event, &__MODULE__.count/4, %{pid: pid, counter: counter})

    on_exit(fn -> :telemetry.detach(handler_id) end)

    log =
      capture_log(fn ->
        for i <- 1..50_000, do: GenServer.cast(name, {:replica_append_v2, segment, i, UnknownMessages.secret()})
        assert {:ok, [%Record{value: "kept"}]} = ReplicationServer.read(name, segment, 0, 10)
      end)

    assert :counters.get(counter, 1) == 50_000
    assert [_one] = warning_lines(log, "replication process dropping an unexpected cast: {:replica_append_v2")
    refute log =~ UnknownMessages.secret()
    assert Process.whereis(name) == pid
  end

  defp capture_log_result(fun) do
    {result, _log} = with_log(fun)
    result
  end
end
