defmodule Malachi.Test.ReadBackTest do
  # ReadBack is how the in-process tests check what a broker holds, so a record it drops or counts twice
  # would pass a test that should fail: across a split, each record is read once, by the range that holds
  # its key now, though both children's histories hold the parent's records. And a read it cannot make is
  # an error, never an empty list: a range led on another node is read there, a move it cannot follow ends
  # in an error, and a server that is not a broker raises.
  use ExUnit.Case, async: false

  alias Malachi.BrokerServer
  alias Malachi.Log.Record
  alias Malachi.Metadata
  alias Malachi.Test.PollingHelper
  alias Malachi.Test.RaPeers
  alias Malachi.Test.ReadBack
  alias Malachi.Test.StreamCluster
  alias Malachi.Test.TmpDir

  # Answers every call as a server that is not a broker does (`Malachi.UnexpectedMessage`), but for the
  # metadata, which it answers empty or holding one topic, so that `ReadBack` gets as far as the range read.
  defmodule NotABroker do
    use GenServer

    def start_link(metadata), do: GenServer.start_link(__MODULE__, metadata)

    @impl true
    def init(metadata), do: {:ok, metadata}

    @impl true
    def handle_call(:metadata, _from, metadata), do: {:reply, metadata, metadata}
    def handle_call(_other, _from, metadata), do: {:reply, {:error, :unknown_call}, metadata}
  end

  # Answers every range read with a move to the replication server on its own node: a broker whose view
  # sends the reader back to itself, forever.
  defmodule MovesToItself do
    use GenServer

    def start_link(name), do: GenServer.start_link(__MODULE__, nil, name: name)

    @impl true
    def init(nil), do: {:ok, nil}

    @impl true
    def handle_call({:fetch_range, range_id, _start, _wait_ms}, _from, state),
      do: {:reply, {:moved, :elsewhere, [{range_id, {{range_id, 0}, {:some_repl, node()}}}]}, state}
  end

  describe "on one node" do
    setup do
      dir = TmpDir.path("read_back")
      {:ok, broker} = BrokerServer.start_link(dir, name: :"read_back_#{System.unique_integer([:positive])}")

      on_exit(fn ->
        if Process.alive?(broker), do: GenServer.stop(broker)
        File.rm_rf!(dir)
      end)

      {:ok, _} = BrokerServer.create_topic(broker, "t", 4)
      %{broker: broker}
    end

    test "reads every record of a topic once, in the order its range holds them", %{broker: broker} do
      values = for i <- 1..30, do: "v#{i}"
      {:ok, _} = BrokerServer.produce(broker, "t", Enum.map(values, &Record.new(&1, key: "k")))
      assert ReadBack.values(broker, "t") == values
    end

    test "across a split, each record once, by the range that holds its key", %{broker: broker} do
      before = for i <- 1..40, do: "b#{i}"
      {:ok, _} = BrokerServer.produce(broker, "t", Enum.map(before, &Record.new(&1, key: &1)))
      {:ok, _left, _right} = BrokerServer.split_range(broker, {"t", 0})
      later = for i <- 1..40, do: "a#{i}"
      for value <- later, do: {:ok, _} = BrokerServer.produce(broker, "t", [Record.new(value, key: value)])

      read = ReadBack.values(broker, "t")
      assert Enum.sort(read) == Enum.sort(before ++ later)
      assert length(read) == length(Enum.uniq(read))
    end

    test "a topic with nothing written reads as nothing", %{broker: broker} do
      assert ReadBack.values(broker, "t") == []
    end
  end

  describe "a server that answers what no broker does" do
    test "one that is not a broker raises, for a range read and for a topic with no ranges" do
      {:ok, server} = NotABroker.start_link(Metadata.new())

      assert_raise ArgumentError, ~r/not a broker/, fn -> ReadBack.range_records(server, {"t", 0}, :earliest) end
      assert_raise ArgumentError, ~r/not a broker/, fn -> ReadBack.values(server, "t") end
    end

    test "a broker that keeps moving the read back to itself ends it with an error" do
      name = :"read_back_moves_#{System.unique_integer([:positive])}"
      {:ok, _server} = MovesToItself.start_link(name)

      assert ReadBack.range_records(name, {"t", 0}, :earliest) == {:error, {:moved, :too_many_hops}}
      assert ReadBack.range_records({name, node()}, {"t", 0}, :earliest) == {:error, {:moved, :too_many_hops}}
    end
  end

  describe "across nodes" do
    @describetag :multinode
    @describetag timeout: 120_000

    setup do
      :ok = RaPeers.ensure_distribution()
      StreamCluster.start()
    end

    test "a range led on another node is read there, through that node's broker", ctx do
      broker = StreamCluster.broker()
      # opened where it is led, which places the range's first segment and says which node leads it
      {primary, segment_id} = open_where_led(broker, ctx.root, hd(ctx.nodes))
      records = for value <- ["a", "b"], do: Record.new(value, key: "k")
      # two records, below the three a segment holds before it asks to roll
      {:ok, _placements} = BrokerServer.produce({broker, primary}, "events", records)
      reader = Enum.find(ctx.nodes, &(&1 != primary))

      # the reader's view must name that segment as active, or it reads the range itself (it has no active
      # segment there) instead of answering the move this test is about
      :ok =
        PollingHelper.wait_until!(
          fn ->
            reader
            |> StreamCluster.metadata_on()
            |> Metadata.segments_of_range(ctx.root)
            |> Enum.any?(&(&1.id == segment_id and &1.state == :active))
          end,
          timeout: 10_000
        )

      assert {:moved, :elsewhere, [{_range, {^segment_id, {_repl, ^primary}}}]} =
               BrokerServer.fetch_range({broker, reader}, ctx.root, :earliest, 0)

      assert ReadBack.values({broker, reader}, "events") == ["a", "b"]
    end
  end

  # Opens (and closes) a stream on `range_id` where its segment is led, following the moves, and answers
  # that node and the segment. Asked again every 100 ms, up to 50 times, while the node's view does not hold
  # the new topic yet (`:no_such_range`, `:metadata_unavailable`).
  defp open_where_led(broker, range_id, node, attempts \\ 50) do
    case BrokerServer.open_stream({broker, node}, range_id) do
      {:ok, token, segment_id, _pid} ->
        :ok = BrokerServer.close_stream({broker, node}, token)
        {node, segment_id}

      {:moved, :elsewhere, [{_range, {_segment, {_repl, primary}}}]} ->
        open_where_led(broker, range_id, primary, attempts)

      {:error, _not_yet} when attempts > 0 ->
        Process.sleep(100)
        open_where_led(broker, range_id, node, attempts - 1)
    end
  end
end
