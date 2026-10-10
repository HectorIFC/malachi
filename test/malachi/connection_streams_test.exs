defmodule Malachi.ConnectionStreamsTest do
  # `Malachi.ConnectionStreams` against a broker stand-in: one id space for both kinds of stream, a close by
  # either kind's id, a broker that goes down holding streams of both kinds, and the idle and room rules.
  use ExUnit.Case, async: true

  alias Malachi.ConnectionStreams
  alias Malachi.Wire

  defmodule StandIn do
    @moduledoc false
    use GenServer

    def start, do: GenServer.start(__MODULE__, nil)

    @impl true
    def init(nil), do: {:ok, nil}

    @impl true
    def handle_call({:close_stream, _token}, _from, state), do: {:reply, :ok, state}
  end

  setup do
    {:ok, broker} = StandIn.start()
    on_exit(fn -> if Process.alive?(broker), do: Process.exit(broker, :kill) end)

    producer = %{
      corr: 10,
      broker: broker,
      broker_pid: broker,
      topic: "t",
      range_id: {"t", 0},
      token: make_ref(),
      granted: %{appends: 4, bytes: 1_000}
    }

    consumer = %{
      corr: 20,
      broker: broker,
      broker_pid: broker,
      topic: "t",
      range_id: {"t", 0},
      token: make_ref(),
      position: {0, 0},
      window: 10,
      max: 10,
      max_bytes: 1_000,
      allowed: fn -> :ok end
    }

    %{broker: broker, producer: producer, consumer: consumer}
  end

  defp kinds(frames) do
    for frame <- frames do
      {:ok, body, <<>>} = Wire.decode_frame(frame)
      {corr, 0, payload} = Wire.decode_response(body)
      {corr, Wire.decode_push(payload)}
    end
  end

  test "both kinds share one id space, and a close finds either by its id", ctx do
    {streams, producer} = ConnectionStreams.open_producer(ConnectionStreams.new(), ctx.producer)
    {streams, consumer} = ConnectionStreams.open_consumer(streams, ctx.consumer)
    assert {producer, consumer} == {1, 2}

    assert {:ok, streams} = ConnectionStreams.close(streams, consumer)
    assert {:ok, streams} = ConnectionStreams.close(streams, producer)
    assert ConnectionStreams.close(streams, producer) == :unknown
    assert ConnectionStreams.close(streams, 99) == :unknown
  end

  test "a broker that goes down moves the streams of both kinds it served", ctx do
    {streams, _producer} = ConnectionStreams.open_producer(ConnectionStreams.new(), ctx.producer)
    {streams, _consumer} = ConnectionStreams.open_consumer(streams, ctx.consumer)
    Process.exit(ctx.broker, :kill)

    downs =
      for _ <- 1..2 do
        assert_receive {:DOWN, _ref, :process, _pid, _reason} = down
        down
      end

    {streams, frames} =
      Enum.reduce(downs, {streams, []}, fn down, {streams, frames} ->
        {streams, more} = ConnectionStreams.handle_message(streams, down)
        {streams, frames ++ more}
      end)

    assert [{10, {:moved, %{reason: "restarted"}}}, {20, {:moved, %{reason: "restarted"}}}] =
             Enum.sort_by(kinds(frames), &elem(&1, 0))

    refute ConnectionStreams.busy?(streams)
  end

  test "a message for neither kind is not taken, nor a down of a process it does not watch", ctx do
    {streams, _consumer} = ConnectionStreams.open_consumer(ConnectionStreams.new(), ctx.consumer)
    assert ConnectionStreams.handle_message(streams, :unrelated) == :no
    assert ConnectionStreams.handle_message(streams, {:DOWN, make_ref(), :process, self(), :normal}) == :no
  end

  test "a fetch whose broker goes down before it answers is answered as broker_down", ctx do
    streams =
      ConnectionStreams.fetch(ConnectionStreams.new(), ctx.broker, :never_answered, fn reply -> {:answered, reply} end)

    assert ConnectionStreams.busy?(streams)
    Process.exit(ctx.broker, :kill)

    assert_receive {:DOWN, _ref, :process, _pid, _reason} = down
    assert {streams, [{:answered, {:error, :broker_down}}]} = ConnectionStreams.handle_message(streams, down)
    refute ConnectionStreams.busy?(streams)
  end

  test "an open consume stream keeps the connection busy; consume streams never hold the socket", ctx do
    refute ConnectionStreams.busy?(ConnectionStreams.new())
    {streams, _consumer} = ConnectionStreams.open_consumer(ConnectionStreams.new(), ctx.consumer)
    assert ConnectionStreams.busy?(streams)
    assert ConnectionStreams.room?(streams)
  end
end
