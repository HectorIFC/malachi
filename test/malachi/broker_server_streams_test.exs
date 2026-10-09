defmodule Malachi.BrokerServerStreamsTest do
  # The broker's half of producer streams (`Malachi.BrokerServer.open_stream/3` and friends): where a
  # stream opens, the move it is sent when its range moves on (a seal, a failover, a split), and the answer
  # an append gets, the produce's own reply with the share of the window the range's load leaves.
  use ExUnit.Case, async: true

  import Malachi.Test.TeardownHelper

  alias Malachi.BrokerServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record

  @moduletag :tmp_dir

  defp start(directory, opts \\ []) do
    {:ok, server} = BrokerServer.start_link(directory, opts)
    on_exit(fn -> stop_quietly(server) end)
    server
  end

  defp start_repl(directory, index) do
    name = :"bss_repl_#{System.unique_integer([:positive])}_#{index}"
    {:ok, _pid} = ReplicationServer.start_link(name: name, directory: Path.join(directory, "r#{index}"))
    name
  end

  defp with_topic(directory, opts \\ []) do
    server = start(directory, opts)
    {:ok, root} = BrokerServer.create_topic(server, "events", 4)
    {server, root}
  end

  # One append, waited for: the produce reply and the window scale.
  defp append(server, range_id, records) do
    reqids = BrokerServer.send_stream_produce(server, range_id, records, :label, :gen_server.reqids_new())
    {{:reply, reply}, :label, _reqids} = :gen_server.receive_response(reqids, 5_000, true)
    reply
  end

  defmodule HeldPrimary do
    @moduledoc false
    # A segment primary that answers nothing itself: each replicate and seal cast is handed to the test, which
    # answers it as the real one would, in the order the test needs.
    use GenServer

    def start(name, test), do: GenServer.start(__MODULE__, test, name: name)

    @impl true
    def init(test), do: {:ok, test}

    @impl true
    def handle_cast(message, test) do
      send(test, {:held, message})
      {:noreply, test}
    end
  end

  describe "open_stream/3" do
    test "a stream opens where the range's active segment is led, placing the first segment", %{tmp_dir: dir} do
      {server, root} = with_topic(dir)

      # the reply names the process whose index holds the stream
      assert {:ok, token, {^root, 0}, ^server} = BrokerServer.open_stream(server, root)
      assert is_reference(token)
      # a second stream on the same range shares the segment
      assert {:ok, _token, {^root, 0}, _broker} = BrokerServer.open_stream(server, root)
    end

    test "a range whose segment another node leads answers where it is", %{tmp_dir: dir} do
      remote = {Malachi.LogReplication, :remote@nowhere}
      {server, root} = with_topic(dir, brokers: [remote])

      assert {:moved, :elsewhere, [{^root, {{^root, 0}, ^remote}}]} = BrokerServer.open_stream(server, root)
    end

    test "a range the topic does not hold, or one a split retired", %{tmp_dir: dir} do
      {server, root} = with_topic(dir)
      assert BrokerServer.open_stream(server, {"events", 9}) == {:error, :no_such_range}

      {:ok, left, right} = BrokerServer.split_range(server, root)
      assert {:moved, :retired, [{^left, nil}, {^right, nil}]} = BrokerServer.open_stream(server, root)
    end
  end

  describe "moves" do
    test "a roll seals the stream's segment: it is moved to the range", %{tmp_dir: dir} do
      one_record = Record.encoded_size(Record.new("v0", key: "k"))
      {server, root} = with_topic(dir, segment_max_bytes: one_record)
      {:ok, token, _segment, _broker} = BrokerServer.open_stream(server, root)

      assert {{:ok, _placements}, _scale} = append(server, root, [Record.new("v0", key: "k")])
      assert_receive {:stream_moved, ^token, :sealed, [{^root, _next}]}, 2_000
    end

    test "a new primary for the segment moves the stream to it, as a seal does", %{tmp_dir: dir} do
      [a, b] = for index <- 1..2, do: start_repl(dir, index)
      {server, root} = with_topic(Path.join(dir, "broker"), brokers: [a, b], replication_factor: 2)
      {:ok, token, segment, _broker} = BrokerServer.open_stream(server, root)

      [primary, follower] =
        if :sys.get_state(server).broker.segments[root].replica_set == [a, b], do: [a, b], else: [b, a]

      :ok = BrokerServer.apply_heal(server, [{:set_segment_replicas, segment, [follower, primary]}])

      assert_receive {:stream_moved, ^token, :sealed, [{^root, {^segment, ^follower}}]}, 2_000
    end

    test "a split retires the range: it is moved to the children", %{tmp_dir: dir} do
      {server, root} = with_topic(dir)
      {:ok, token, _segment, _broker} = BrokerServer.open_stream(server, root)

      {:ok, left, right} = BrokerServer.split_range(server, root)
      assert_receive {:stream_moved, ^token, :retired, [{^left, nil}, {^right, nil}]}, 2_000
    end

    test "a closed stream, or one whose connection went away, is sent nothing", %{tmp_dir: dir} do
      {server, root} = with_topic(dir)
      {:ok, closed, _segment, _broker} = BrokerServer.open_stream(server, root)
      :ok = BrokerServer.close_stream(server, closed)
      :ok = BrokerServer.close_stream(server, closed)

      holder = spawn(fn -> receive(do: (:stop -> :ok)) end)
      {:ok, _gone, _segment, _broker} = BrokerServer.open_stream(server, root, holder)
      send(holder, :stop)
      ref = Process.monitor(holder)
      assert_receive {:DOWN, ^ref, :process, _pid, _reason}

      {:ok, _left, _right} = BrokerServer.split_range(server, root)
      refute_receive {:stream_moved, _token, _reason, _targets}, 200
      assert :sys.get_state(server).streams == %{}
    end
  end

  describe "send_stream_produce/5" do
    test "an append is the range's produce, answered with the share of the window its load leaves", %{tmp_dir: dir} do
      {server, root} = with_topic(dir)

      assert {{:ok, %{^root => {0, 1}}}, scale} =
               append(server, root, [Record.new("a", key: "k"), Record.new("b", key: "k")])

      # nothing else in flight on the range
      assert scale == 1.0
    end

    test "a key outside the range refuses the whole append", %{tmp_dir: dir} do
      {server, root} = with_topic(dir)
      {:ok, left, _right} = BrokerServer.split_range(server, root)
      outside = Enum.find(1..10_000, &(:erlang.phash2("k#{&1}", 16) >= 8))

      assert {{:error, :key_outside_range}, _scale} = append(server, left, [Record.new("v", key: "k#{outside}")])
    end

    test "with group commit an append is answered with the share its flush relieved of the parked load", %{tmp_dir: dir} do
      # Two appends park (the interval timer is far off); the second reaches the eager flush threshold, and
      # both are answered with the load that flush carried: 2 parked against a soft limit of 1 and a hard
      # one of 3.
      {server, root} =
        with_topic(dir,
          group_commit: true,
          group_commit_interval_ms: 60_000,
          group_commit_flush_max_records: 2,
          group_commit_max_inflight: 3
        )

      reqids =
        Enum.reduce(1..2, :gen_server.reqids_new(), fn index, reqids ->
          BrokerServer.send_stream_produce(server, root, [Record.new("v#{index}", key: "k")], index, reqids)
        end)

      replies =
        for _ <- 1..2 do
          {{:reply, reply}, _label, _reqids} = :gen_server.receive_response(reqids, 5_000, false)
          reply
        end

      assert [{{:ok, _}, 0.5}, {{:ok, _}, 0.5}] = replies
    end

    test "a dispatch refused behind its fence is counted in flight once, beside another still out", %{tmp_dir: dir} do
      primary = :"bss_held_#{System.unique_integer([:positive])}"
      {:ok, held} = HeldPrimary.start(primary, self())
      on_exit(fn -> Process.exit(held, :kill) end)
      one_record = Record.encoded_size(Record.new("v0", key: "k"))
      {server, root} = with_topic(dir, brokers: [primary], segment_max_bytes: one_record)

      # the first append fills the segment, and its answer sends the roll's fence, which is left unanswered
      send_append = fn records, label ->
        BrokerServer.send_stream_produce(server, root, records, label, :gen_server.reqids_new())
      end

      first = send_append.([Record.new("v0", key: "k")], :first)
      assert_receive {:held, {:replicate_async, _seg, _set, _base, _records, {^server, tag}, _ctx}}
      send(server, {:replicate_result, tag, {:ok, elem(tag, 1).last}})
      {{:reply, {{:ok, _}, _scale}}, :first, _} = :gen_server.receive_response(first, 5_000, true)
      assert_receive {:held, {:seal_async, _segment, base, {^server, fence}}}

      # two more go out behind that fence: the refused one waits for it, the other is still out
      refused = send_append.([Record.new("a", key: "k")], :refused)
      assert_receive {:held, {:replicate_async, _seg, _set, _base, _records, {^server, refused_tag}, _ctx}}
      out = send_append.([Record.new("b1", key: "k"), Record.new("b2", key: "k")], :out)
      assert_receive {:held, {:replicate_async, _seg, _set, _base, _records, {^server, out_tag}, _ctx}}
      assert :sys.get_state(server).range_inflight == %{root => 3}

      # the fence closes the segment after the first record
      sealed_at = base + 1
      send(server, {:replicate_result, refused_tag, {:error, {:sealed, sealed_at}}})
      send(server, {:seal_result, fence, {:ok, sealed_at, one_record}})

      # replanned into the successor and answered there
      assert_receive {:held, {:replicate_async, _seg, _set, _base, _records, {^server, replanned}, _ctx}}
      send(server, {:replicate_result, replanned, {:ok, elem(replanned, 1).last}})
      {{:reply, {{:ok, _}, _scale}}, :refused, _} = :gen_server.receive_response(refused, 5_000, true)

      # only the other append's two records are left in flight
      assert :sys.get_state(server).range_inflight == %{root => 2}
      send(server, {:replicate_result, out_tag, {:error, :no_quorum}})
      {{:reply, {{:error, :no_quorum}, _scale}}, :out, _} = :gen_server.receive_response(out, 5_000, true)
      assert :sys.get_state(server).range_inflight == %{}
    end

    test "a range's records in flight go back to none once its appends are answered", %{tmp_dir: dir} do
      {server, root} = with_topic(dir, stream_inflight_soft: 0, stream_inflight_hard: 1)

      for index <- 1..5, do: assert({{:ok, _}, _scale} = append(server, root, [Record.new("v#{index}", key: "k")]))

      assert :sys.get_state(server).range_inflight == %{}
    end
  end
end
