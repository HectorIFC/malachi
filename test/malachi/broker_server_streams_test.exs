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

  describe "consume streams" do
    alias Malachi.Broker
    alias Malachi.Broker.ReadView
    alias Malachi.BrokerServer.ConsumeIndex
    alias Malachi.Test.PollingHelper

    # The consume stream's wake for `token`, with its view.
    defp wake(token) do
      assert_receive {:consume_wake, ^token, %ReadView{} = view, _reporter}, 2_000
      view
    end

    defp produced(server, root, values) do
      for value <- values, do: assert({{:ok, _}, _scale} = append(server, root, [Record.new(value, key: "k")]))
      :ok
    end

    test "a stream opens from where its start resolves, and is handed a view of its range at once", %{tmp_dir: dir} do
      {server, root} = with_topic(dir)
      produced(server, root, ["a", "b"])

      assert {:ok, token, {0, 0}, ^server} = BrokerServer.open_consume(server, root, :earliest)
      view = wake(token)

      assert {:ok, [{{0, 0}, %{value: "a"}}, {{0, 1}, %{value: "b"}}], {0, 2}, []} =
               Broker.read_consume_positioned(view, root, {0, 0}, 10, &ReplicationServer.read/4)

      assert {:ok, _token, {0, 2}, _server} = BrokerServer.open_consume(server, root, :latest)
      assert BrokerServer.open_consume(server, root, {:position, {3, 0}}) == {:error, :invalid_position}
      assert BrokerServer.open_consume(server, {"events", 9}, :earliest) == {:error, :no_such_range}
    end

    test "an armed stream is woken once the range grows past what it read, and only then", %{tmp_dir: dir} do
      {server, root} = with_topic(dir)
      {:ok, token, _position, _server} = BrokerServer.open_consume(server, root, :earliest)
      _first = wake(token)

      :ok = BrokerServer.arm_consume(server, token, 0)
      refute_receive {:consume_wake, ^token, _view, _reporter}, 100

      produced(server, root, ["x"])
      view = wake(token)
      assert ReadView.fetch_end(view, root) == {:ok, 1}

      # a stream not armed again is not woken again
      produced(server, root, ["y"])
      refute_receive {:consume_wake, ^token, _view, _reporter}, 100

      # armed behind the range's end, it is woken at once
      :ok = BrokerServer.arm_consume(server, token, 1)
      assert ReadView.fetch_end(wake(token), root) == {:ok, 2}
    end

    test "a range whose active segment another node leads is answered where it is read", %{tmp_dir: dir} do
      remote = {Malachi.LogReplication, :remote@nowhere}
      {server, root} = with_topic(dir, brokers: [remote])
      # a producer's open places the segment, on the only broker there is
      {:moved, :elsewhere, _targets} = BrokerServer.open_stream(server, root)
      assert {:moved, :elsewhere, [{^root, {_segment, ^remote}}]} = BrokerServer.open_consume(server, root, :earliest)
      assert {:moved, :elsewhere, _targets} = BrokerServer.fetch_range(server, root, :earliest, 0)
    end

    test "reading never places a segment: a range with none is read here", %{tmp_dir: dir} do
      remote = {Malachi.LogReplication, :remote@nowhere}
      {server, root} = with_topic(dir, brokers: [remote])
      assert {:ok, _token, {0, 0}, _server} = BrokerServer.open_consume(server, root, :earliest)
      assert {:ok, {0, 0}, _view, _reporter} = BrokerServer.fetch_range(server, root, :earliest, 0)
      assert Malachi.Metadata.segments_of_range(BrokerServer.metadata(server), root) == []
    end

    test "a roll keeps the range's consume streams: they read the range, not one segment", %{tmp_dir: dir} do
      one_record = Record.encoded_size(Record.new("v0", key: "k"))
      {server, root} = with_topic(dir, segment_max_bytes: one_record)
      {:ok, token, _position, _server} = BrokerServer.open_consume(server, root, :earliest)
      _first = wake(token)
      :ok = BrokerServer.arm_consume(server, token, 0)

      # the append fills the segment, and its fence seals it: the range has no active segment until the next
      produced(server, root, ["v0"])
      assert_receive {:consume_wake, ^token, _view, _reporter}, 2_000
      refute_receive {:stream_moved, ^token, _reason, _targets}, 300
      assert ConsumeIndex.consumer?(:sys.get_state(server).consume, token)
    end

    test "a stream reads only up to what a quorum acknowledged, and a position past it is refused", %{tmp_dir: dir} do
      {server, root} = with_topic(dir)
      produced(server, root, ["a", "b"])
      broker = :sys.get_state(server).broker
      assert Broker.durable_end(broker, root) == 2
      assert ReadView.fetch_end(Broker.consume_view(broker, root), root) == {:ok, 2}

      assert {:ok, _token, {0, 2}, _server} = BrokerServer.open_consume(server, root, :latest)
      assert {:ok, _token, {0, 2}, _server} = BrokerServer.open_consume(server, root, {:position, {0, 2}})
      assert BrokerServer.open_consume(server, root, {:position, {0, 3}}) == {:error, :invalid_position}
    end

    test "a split moves an open stream to the children, and a new primary moves it as a seal does", %{tmp_dir: dir} do
      {server, root} = with_topic(dir)
      {:ok, token, _position, _server} = BrokerServer.open_consume(server, root, :earliest)
      {:ok, left, right} = BrokerServer.split_range(server, root)
      assert_receive {:stream_moved, ^token, :retired, [{^left, _}, {^right, _}]}, 2_000
      assert :sys.get_state(server).consume.consumers == %{}

      # a segment led here whose primary moves to another node: the stream follows it
      local = start_repl(dir, 1)
      remote = {Malachi.LogReplication, :remote@nowhere}
      {replicated, range} = with_topic(Path.join(dir, "replicated"), brokers: [local, remote], replication_factor: 2)
      # opening places the segment; it is led here when the local store comes first, so the test makes it so
      _ = BrokerServer.open_stream(replicated, range)
      segment = :sys.get_state(replicated).broker.segments[range].id
      :ok = BrokerServer.apply_heal(replicated, [{:set_segment_replicas, segment, [local, remote]}])
      {:ok, moved, _position, _server} = BrokerServer.open_consume(replicated, range, :earliest)

      :ok = BrokerServer.apply_heal(replicated, [{:set_segment_replicas, segment, [remote, local]}])
      assert_receive {:stream_moved, ^moved, :sealed, [{^range, {^segment, ^remote}}]}, 2_000
      assert :sys.get_state(replicated).consume.consumers == %{}
    end

    test "a stream whose connection goes away, or that is closed, is dropped", %{tmp_dir: dir} do
      {server, root} = with_topic(dir)
      holder = spawn(fn -> receive(do: (:stop -> :ok)) end)
      {:ok, _gone, _position, _server} = BrokerServer.open_consume(server, root, :earliest, holder)
      {:ok, closed, _position, _server} = BrokerServer.open_consume(server, root, :earliest)
      ref = Process.monitor(holder)
      send(holder, :stop)
      assert_receive {:DOWN, ^ref, :process, _pid, _reason}

      :ok = BrokerServer.close_stream(server, closed)
      :ok = BrokerServer.close_stream(server, closed)
      assert :sys.get_state(server).consume.consumers == %{}
    end

    test "a fetch answers at once when records are past its start, waits for them otherwise", %{tmp_dir: dir} do
      {server, root} = with_topic(dir)
      produced(server, root, ["a"])
      assert {:ok, {0, 0}, %ReadView{}, _reporter} = BrokerServer.fetch_range(server, root, :earliest, 5_000)

      waiting = Task.async(fn -> BrokerServer.fetch_range(server, root, :latest, 5_000) end)
      PollingHelper.wait_until!(fn -> map_size(:sys.get_state(server).consume.waiters) == 1 end)
      produced(server, root, ["b"])
      assert {:ok, {0, 1}, view, _reporter} = Task.await(waiting)
      assert ReadView.fetch_end(view, root) == {:ok, 2}

      # nothing lands: answered after its wait, with a view to read nothing from
      assert {:ok, {0, 2}, _view, _reporter} = BrokerServer.fetch_range(server, root, :latest, 100)
      assert :sys.get_state(server).consume.waiters == %{}
    end

    test "records reserved for a produce not yet acknowledged are not read, and their ack wakes the stream", %{
      tmp_dir: dir
    } do
      primary = :"bss_consume_held_#{System.unique_integer([:positive])}"
      {:ok, held} = HeldPrimary.start(primary, self())
      on_exit(fn -> Process.exit(held, :kill) end)
      {server, root} = with_topic(dir, brokers: [primary])

      # a segment led by the held primary, and a stream on it caught up at 0
      _ = BrokerServer.open_stream(server, root)
      {:ok, token, {0, 0}, _server} = BrokerServer.open_consume(server, root, :earliest)
      _first = wake(token)
      :ok = BrokerServer.arm_consume(server, token, 0)

      # a produce reserves offset 0 and is held before its quorum
      pending =
        BrokerServer.send_stream_produce(server, root, [Record.new("v", key: "k")], :p, :gen_server.reqids_new())

      assert_receive {:held, {:replicate_async, _seg, _set, _base, _records, {^server, tag}, _ctx}}
      broker = :sys.get_state(server).broker
      assert Broker.range_end(broker, root) == 1
      assert Broker.durable_end(broker, root) == 0
      assert ReadView.fetch_end(Broker.consume_view(broker, root), root) == {:ok, 0}
      assert {:ok, _token, {0, 0}, _server} = BrokerServer.open_consume(server, root, :latest)
      # a position at the reserved end is taken and waits; one past the range's end is refused
      assert {:ok, waiting, {0, 1}, _server} = BrokerServer.open_consume(server, root, {:position, {0, 1}})
      assert BrokerServer.open_consume(server, root, {:position, {0, 2}}) == {:error, :invalid_position}
      refute_receive {:consume_wake, ^token, _view, _reporter}, 100
      :ok = BrokerServer.close_stream(server, waiting)

      # the quorum acknowledges it: now it is durable, and the waiting stream is woken to read it
      send(server, {:replicate_result, tag, {:ok, 0}})
      {{:reply, {{:ok, _}, _scale}}, :p, _} = :gen_server.receive_response(pending, 5_000, true)
      assert ReadView.fetch_end(wake(token), root) == {:ok, 1}
    end

    test "the horizon moves only with produces answered as stored, and a seal sets it", %{tmp_dir: dir} do
      {server, root} = with_topic(dir)
      produced(server, root, ["a", "b"])
      broker = :sys.get_state(server).broker
      segment = broker.segments[root].id
      assert Broker.durable_end(broker, root) == 2

      # a recovery's seed (a primary's local end) and a dispatch's adopted end move the offsets, not the horizon
      seeded = Broker.seed_range_state(broker, root, 10, 0)
      assert Broker.range_end(seeded, root) == 10
      assert Broker.durable_end(seeded, root) == 2
      # a cursor read before a restart (past the horizon, within the recovered end) is still taken, and waits
      assert Broker.consume_start(seeded, root, {:position, {0, 8}}) == {:ok, {0, 8}}
      refute Broker.consume_ready?(seeded, root, {0, 8})
      assert Broker.consume_start(seeded, root, {:position, {0, 11}}) == {:error, :invalid_position}
      adopted = Broker.adopt_offsets(seeded, root, segment, 11)
      assert Broker.durable_end(adopted, root) == 2

      # a seal below a tail no quorum held sets it there: the next segment reuses those offsets
      sealed = broker |> Broker.mark_durable(%{root => {0, 4}}) |> Broker.forget_sealed(root, segment, 1)
      assert Broker.durable_end(sealed, root) == 1
      # and the next segment's offsets, reserved past it, are not durable until their own produces are answered
      reserved = Broker.seed_range_state(sealed, root, 6, 0)
      assert Broker.durable_end(reserved, root) == 1
      # and a position past the seal names nothing any more
      assert Broker.consume_start(sealed, root, {:position, {0, 2}}) == {:error, :invalid_position}
    end

    test "a cursor on sealed records is taken though this frontend's end is behind, and an unrecovered range is retried",
         %{
           tmp_dir: dir
         } do
      one_record = Record.encoded_size(Record.new("v0", key: "k"))
      {server, root} = with_topic(dir, segment_max_bytes: one_record)
      produced(server, root, ["v1", "v2"])

      PollingHelper.wait_until!(fn ->
        server
        |> BrokerServer.metadata()
        |> Malachi.Metadata.segments_of_range(root)
        |> Enum.any?(&(&1.state == :sealed))
      end)

      broker = :sys.get_state(server).broker
      # an end this frontend has not learned yet: the sealed segment still bounds what a cursor may name
      behind = %{broker | offsets: Map.delete(broker.offsets, root)}
      assert {:ok, {0, 1}} = Broker.consume_start(behind, root, {:position, {0, 1}})

      # a recovery that could not learn the end leaves the range to be retried, not a refusal
      {:ok, fresh} = BrokerServer.create_topic(server, "fresh", 4)
      unrecovered = Broker.seed_unrecovered_range(:sys.get_state(server).broker, fresh, 0)
      assert Broker.consume_start(unrecovered, fresh, {:position, {0, 0}}) == {:error, :metadata_unavailable}
    end

    test "with group commit, records count as durable once their flush answered them", %{tmp_dir: dir} do
      {server, root} =
        with_topic(dir,
          group_commit: true,
          group_commit_interval_ms: 60_000,
          group_commit_flush_max_records: 2,
          group_commit_max_inflight: 100
        )

      first = BrokerServer.send_stream_produce(server, root, [Record.new("v1", key: "k")], 1, :gen_server.reqids_new())
      PollingHelper.wait_until!(fn -> :sys.get_state(server).pending_records == 1 end)
      # buffered, not flushed: not durable, though its offset is reserved
      assert Broker.durable_end(:sys.get_state(server).broker, root) == 0

      second = BrokerServer.send_stream_produce(server, root, [Record.new("v2", key: "k")], 2, :gen_server.reqids_new())
      {{:reply, {{:ok, _}, _}}, 1, _} = :gen_server.receive_response(first, 5_000, true)
      {{:reply, {{:ok, _}, _}}, 2, _} = :gen_server.receive_response(second, 5_000, true)
      assert Broker.durable_end(:sys.get_state(server).broker, root) == 2
    end

    test "a fetch's timer that fires after a wake answered it changes nothing", %{tmp_dir: dir} do
      {server, root} = with_topic(dir)
      send(server, {:fetch_range_timeout, make_ref()})
      assert {:ok, _token, _position, ^server} = BrokerServer.open_consume(server, root, :earliest)
    end

    test "a fetch waiting on a range a split retires is answered where its records went", %{tmp_dir: dir} do
      {server, root} = with_topic(dir)
      waiting = Task.async(fn -> BrokerServer.fetch_range(server, root, :latest, 5_000) end)
      PollingHelper.wait_until!(fn -> map_size(:sys.get_state(server).consume.waiters) == 1 end)

      {:ok, left, right} = BrokerServer.split_range(server, root)
      assert {:moved, :retired, [{^left, _}, {^right, _}]} = Task.await(waiting, 2_000)
      assert :sys.get_state(server).consume.waiters == %{}
    end

    test "the backlog past a position counts the range's own records exactly and its ancestors whole", %{tmp_dir: dir} do
      {server, root} = with_topic(dir)
      produced(server, root, ["p0", "p1", "p2"])
      {:ok, left, _right} = BrokerServer.split_range(server, root)
      # two records of the child's own, keyed into its slice
      %{key_end: key_end, keyspace_size: size} =
        :sys.get_state(server).broker |> Broker.metadata() |> Malachi.Metadata.get_range(left)

      key = "k#{Enum.find(1..10_000, &(:erlang.phash2("k#{&1}", size) < key_end))}"
      for value <- ["c0", "c1"], do: assert({{:ok, _}, _scale} = append(server, left, [Record.new(value, key: key)]))
      broker = :sys.get_state(server).broker
      view = ReadView.new(broker, [left])

      # in the ancestor: what is left of it, counted whole, and every record of the child's own
      assert Broker.consume_backlog(view, left, {0, 1}) == 2 + 2
      assert Broker.consume_backlog(view, left, {1, 1}) == 1
      assert Broker.consume_backlog(view, left, {1, 2}) == 0
      assert Broker.consume_backlog(view, {"events", 99}, {0, 0}) == 0
      assert Broker.consume_ready?(broker, left, {0, 3})
      assert Broker.consume_ready?(broker, left, {1, 1})
      refute Broker.consume_ready?(broker, left, {1, 2})
      refute Broker.consume_ready?(broker, {"events", 99}, {0, 0})
    end
  end
end
