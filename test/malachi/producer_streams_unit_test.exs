defmodule Malachi.ProducerStreamsUnitTest do
  # `Malachi.ProducerStreams` against a broker stand-in that holds every append until the test releases it,
  # so the orders a real broker produces only by racing can be set up one step at a time: an answer after a
  # move, an answer out of order, a broker that goes down with appends in flight.
  use ExUnit.Case, async: true

  alias Malachi.Log.Record
  alias Malachi.ProducerStreams
  alias Malachi.Wire
  alias Malachi.Wire.Batch

  defmodule HeldBroker do
    @moduledoc false
    use GenServer

    def start(opts \\ []), do: GenServer.start(__MODULE__, nil, opts)
    # answers the held appends of `sequences`, in the order given, with `reply`
    def release(broker, sequences, reply), do: GenServer.call(broker, {:release, sequences, reply})

    @impl true
    def init(nil), do: {:ok, %{held: []}}

    @impl true
    def handle_call({:stream_produce, _range_id, records, _ctx}, from, state),
      do: {:noreply, %{state | held: state.held ++ [{hd(records).value, from}]}}

    def handle_call({:close_stream, _token}, _from, state), do: {:reply, :ok, state}

    def handle_call({:release, values, reply}, _from, state) do
      Enum.each(values, fn value -> GenServer.reply(:proplists.get_value(value, state.held), {reply, 1.0}) end)
      {:reply, :ok, %{state | held: Enum.reject(state.held, fn {value, _from} -> value in values end)}}
    end
  end

  setup do
    {:ok, broker} = HeldBroker.start()
    on_exit(fn -> if Process.alive?(broker), do: Process.exit(broker, :kill) end)
    token = make_ref()
    granted = %{appends: 8, bytes: 1_000_000}

    opened = %{
      corr: 7,
      broker: broker,
      broker_pid: broker,
      topic: "t",
      range_id: {"t", 0},
      token: token,
      granted: granted
    }

    {streams, id} = ProducerStreams.open(ProducerStreams.new(), opened)
    %{broker: broker, token: token, streams: streams, id: id}
  end

  defp batch(value), do: Batch.encode([{Record.new(value), false}], :none)

  defp append(streams, id, sequence) do
    {streams, frames} = ProducerStreams.append(streams, id, sequence, batch(Integer.to_string(sequence)), 1_000_000)
    {streams, Enum.map(frames, &push/1)}
  end

  defp push(frame) do
    {:ok, body, <<>>} = Wire.decode_frame(frame)
    {7, 0, payload} = Wire.decode_response(body)
    Wire.decode_push(payload)
  end

  # Feeds the connection's mailbox to the streams until `count` pushes came out.
  defp pushes(streams, count, acc \\ [])
  defp pushes(streams, 0, acc), do: {streams, Enum.reverse(acc)}

  defp pushes(streams, count, acc) do
    receive do
      message ->
        case ProducerStreams.handle_message(streams, message) do
          {streams, frames} -> pushes(streams, count - length(frames), Enum.reverse(Enum.map(frames, &push/1)) ++ acc)
          :no -> pushes(streams, count, acc)
        end
    after
      2_000 -> flunk("expected #{count} more pushes")
    end
  end

  test "an ack names the sequence below which every append is answered: 0 until the first one is", ctx do
    {streams, []} = append(ctx.streams, ctx.id, 0)
    {streams, []} = append(streams, ctx.id, 1)

    :ok = HeldBroker.release(ctx.broker, ["1"], {:error, :no_quorum})
    {streams, [{:append_ack, ack}]} = pushes(streams, 1)
    # 1 answered, 0 not: nothing below 0 is answered, and the failure waits for 0 to be named
    assert %{acked_sequence: 0, errors: []} = ack

    :ok = HeldBroker.release(ctx.broker, ["0"], {:ok, %{}})
    {_streams, [{:append_ack, ack}]} = pushes(streams, 1)
    assert %{acked_sequence: 2, errors: [%{sequence: 1, reason: "no_quorum"}]} = ack
  end

  test "after a move the stream takes no new append, and the appends in flight are still answered", ctx do
    {streams, []} = append(ctx.streams, ctx.id, 0)
    send(self(), {:stream_moved, ctx.token, :sealed, [{{"t", 0}, nil}]})

    {streams, [{:moved, moved}]} = pushes(streams, 1)
    assert moved.reason == "sealed"
    assert ProducerStreams.append(streams, ctx.id, 1, batch("1"), 1_000_000) == {:unknown, ctx.id}
    assert ProducerStreams.busy?(streams)

    :ok = HeldBroker.release(ctx.broker, ["0"], {:ok, %{}})
    {streams, [{:append_ack, ack}]} = pushes(streams, 1)
    assert ack.acked_sequence == 1
    # answered, so the stream is gone
    refute ProducerStreams.busy?(streams)
    assert ProducerStreams.topic(streams, ctx.id) == nil
  end

  test "a gap stops the stream, and the appends before it are still answered", ctx do
    {streams, []} = append(ctx.streams, ctx.id, 0)
    {streams, [{:append_ack, gap}]} = append(streams, ctx.id, 2)
    assert %{acked_sequence: 0, errors: [%{sequence: 2, reason: "sequence_gap"}]} = gap

    :ok = HeldBroker.release(ctx.broker, ["0"], {:ok, %{}})
    {_streams, [{:append_ack, ack}]} = pushes(streams, 1)
    assert ack.acked_sequence == 1
  end

  test "a broker that goes down moves every stream on it, and its appends in flight answer as failed", ctx do
    {streams, []} = append(ctx.streams, ctx.id, 0)
    Process.exit(ctx.broker, :kill)

    {_streams, pushed} = pushes(streams, 2)

    assert {:moved, %{reason: "restarted", targets: [%{range: 0, segment: nil}]}} =
             Enum.find(pushed, &match?({:moved, _}, &1))

    assert {:append_ack, %{acked_sequence: 1, errors: [%{sequence: 0}]}} =
             Enum.find(pushed, &match?({:append_ack, _}, &1))
  end

  test "a broker restarted under its name is watched anew: the old one's DOWN moves only its own streams" do
    name = :"held_broker_#{System.unique_integer([:positive])}"
    {:ok, old} = HeldBroker.start(name: name)
    granted = %{appends: 8, bytes: 1_000_000}

    opened = fn range, pid ->
      %{corr: 7, broker: name, broker_pid: pid, topic: "t", range_id: {"t", range}, token: make_ref(), granted: granted}
    end

    {streams, first} = ProducerStreams.open(ProducerStreams.new(), opened.(0, old))

    Process.exit(old, :kill)
    wait_for_unregistered(name)
    {:ok, new} = HeldBroker.start(name: name)
    on_exit(fn -> Process.exit(new, :kill) end)
    # opened on the new process before the old one's DOWN is read
    {streams, second} = ProducerStreams.open(streams, opened.(1, new))

    {streams, [{:moved, %{stream_id: ^first, reason: "restarted"}}]} = pushes(streams, 1)
    assert ProducerStreams.topic(streams, first) == nil
    assert ProducerStreams.topic(streams, second) == "t"
  end

  test "a stream whose broker restarted between its answer and the open being recorded is moved at once", ctx do
    # The broker that answered the open died, and another took its name before the connection recorded the
    # stream: the stream is watched on the process that answered, whose index held it, and moved.
    name = :"held_broker_#{System.unique_integer([:positive])}"
    {:ok, answered} = HeldBroker.start(name: name)
    Process.exit(answered, :kill)
    wait_for_unregistered(name)
    {:ok, successor} = HeldBroker.start(name: name)
    on_exit(fn -> Process.exit(successor, :kill) end)

    opened = %{
      corr: 7,
      broker: name,
      broker_pid: answered,
      topic: "t",
      range_id: {"t", 1},
      token: make_ref(),
      granted: %{appends: 1, bytes: 1}
    }

    {streams, id} = ProducerStreams.open(ctx.streams, opened)

    {streams, [{:moved, %{stream_id: ^id, reason: "restarted"}}]} = pushes(streams, 1)
    assert ProducerStreams.topic(streams, id) == nil
    # the stream on the live broker is untouched
    assert ProducerStreams.topic(streams, ctx.id) == "t"
  end

  test "an append on a stream the connection never opened is unknown", ctx do
    assert ProducerStreams.append(ctx.streams, ctx.id + 1, 0, batch("x"), 1_000_000) == {:unknown, ctx.id + 1}
  end

  test "messages that are not about its streams are not taken", ctx do
    # a move for a stream it does not hold, and an unrelated message, with nothing in flight and with an append out
    assert ProducerStreams.handle_message(ctx.streams, {:stream_moved, make_ref(), :sealed, []}) == :no
    assert ProducerStreams.handle_message(ctx.streams, :unrelated) == :no
    {streams, []} = append(ctx.streams, ctx.id, 0)
    assert ProducerStreams.handle_message(streams, :unrelated) == :no
  end

  test "a failure's reason is named by its atom, or its tag when it carries detail", ctx do
    {streams, []} = append(ctx.streams, ctx.id, 0)
    {streams, []} = append(streams, ctx.id, 1)
    {streams, []} = append(streams, ctx.id, 2)
    :ok = HeldBroker.release(ctx.broker, ["0"], {:error, {:sealed, 12}})
    :ok = HeldBroker.release(ctx.broker, ["1"], {:error, "odd"})
    :ok = HeldBroker.release(ctx.broker, ["2"], {:error, :no_quorum})

    {_streams, acks} = pushes(streams, 3)
    errors = Enum.flat_map(acks, fn {:append_ack, ack} -> ack.errors end)

    assert errors == [
             %{sequence: 0, reason: "sealed"},
             %{sequence: 1, reason: ~s("odd")},
             %{sequence: 2, reason: "no_quorum"}
           ]
  end

  test "a move names each target range with its active segment and primary, when it has one", ctx do
    targets = [{{"t", 3}, {{{"t", 3}, 7}, {:repl, :n1@host}}}, {{"t", 4}, nil}]
    send(self(), {:stream_moved, ctx.token, :retired, targets})

    {_streams, [{:moved, moved}]} = pushes(ctx.streams, 1)
    assert moved.reason == "retired"

    assert [%{range: 3, segment: %{segment: 7, primary: primary}}, %{range: 4, segment: nil}] = moved.targets
    assert primary == Malachi.Metadata.broker_ref_string({:repl, :n1@host})
  end

  defp wait_for_unregistered(name) do
    if Process.whereis(name), do: Process.sleep(5) && wait_for_unregistered(name), else: :ok
  end

  test "a stream that moved no longer holds the socket back when its window is full", ctx do
    one = %{
      corr: 7,
      broker: ctx.broker,
      broker_pid: ctx.broker,
      topic: "t",
      range_id: {"t", 1},
      token: make_ref(),
      granted: %{appends: 1, bytes: 1_000}
    }

    {streams, other} = ProducerStreams.open(ctx.streams, one)
    {streams, []} = ProducerStreams.append(streams, other, 0, batch("x"), 1_000_000)
    refute ProducerStreams.room?(streams)

    send(self(), {:stream_moved, one.token, :sealed, [{{"t", 1}, nil}]})
    {streams, [{:moved, _}]} = pushes(streams, 1)
    assert ProducerStreams.room?(streams)
  end
end
