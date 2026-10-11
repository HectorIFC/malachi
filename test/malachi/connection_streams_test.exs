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

  test "a job runs beside the connection and is answered with the frame it returns" do
    parent = self()

    job = fn ->
      send(parent, {:job, self()})
      receive(do: (:go -> "frame"))
    end

    streams = ConnectionStreams.run(ConnectionStreams.new(), job, "fallback")
    assert ConnectionStreams.busy?(streams)
    assert_receive {:job, pid}
    send(pid, :go)

    assert_receive {:DOWN, _ref, :process, ^pid, _reason} = down
    assert {streams, ["frame"]} = ConnectionStreams.handle_message(streams, down)
    refute ConnectionStreams.busy?(streams)
  end

  test "in-order jobs start one after the other, in the order they came, and a crash lets the next one go" do
    parent = self()

    job = fn n ->
      fn ->
        send(parent, {:started, n, self()})
        receive(do: (:go -> "frame #{n}"))
      end
    end

    streams =
      ConnectionStreams.new()
      |> ConnectionStreams.run_in_order(job.(1), "fallback 1")
      |> ConnectionStreams.run_in_order(job.(2), "fallback 2")
      |> ConnectionStreams.run_in_order(job.(3), "fallback 3")

    assert_receive {:started, 1, first}
    refute_receive {:started, _, _}, 100

    send(first, :go)
    assert_receive {:DOWN, _, :process, ^first, _} = down
    assert {streams, ["frame 1"]} = ConnectionStreams.handle_message(streams, down)

    assert_receive {:started, 2, second}
    refute_receive {:started, 3, _}, 100
    Process.exit(second, :kill)
    assert_receive {:DOWN, _, :process, ^second, _} = down
    assert {streams, ["fallback 2"]} = ConnectionStreams.handle_message(streams, down)

    assert_receive {:started, 3, third}
    send(third, :go)
    assert_receive {:DOWN, _, :process, ^third, _} = down
    assert {streams, ["frame 3"]} = ConnectionStreams.handle_message(streams, down)
    refute ConnectionStreams.busy?(streams)
  end

  test "a plain job that ends while an in-order one runs leaves the next in-order one waiting" do
    parent = self()

    held = fn tag ->
      fn ->
        send(parent, {tag, self()})
        receive(do: (:go -> "#{tag}"))
      end
    end

    streams =
      ConnectionStreams.new()
      |> ConnectionStreams.run_in_order(held.(:commit1), "")
      |> ConnectionStreams.run_in_order(held.(:commit2), "")
      |> ConnectionStreams.run(held.(:fetch), "")

    assert_receive {:commit1, commit1}
    assert_receive {:fetch, fetch}
    send(fetch, :go)
    assert_receive {:DOWN, _, :process, ^fetch, _} = down
    assert {streams, ["fetch"]} = ConnectionStreams.handle_message(streams, down)
    refute_receive {:commit2, _}, 100

    send(commit1, :go)
    assert_receive {:DOWN, _, :process, ^commit1, _} = down
    assert {_streams, ["commit1"]} = ConnectionStreams.handle_message(streams, down)
    assert_receive {:commit2, commit2}
    Process.exit(commit2, :kill)
  end

  test "handed off on close, the waiting in-order jobs run in order once the running one ends, past a crash" do
    parent = self()

    running = fn ->
      send(parent, {:running, self()})
      receive(do: (:go -> ""))
    end

    streams =
      ConnectionStreams.new()
      |> ConnectionStreams.run_in_order(running, "")
      |> ConnectionStreams.run_in_order(fn -> raise "boom" end, "")
      |> ConnectionStreams.run_in_order(fn -> send(parent, :third) && "" end, "")

    assert_receive {:running, pid}
    assert ConnectionStreams.hand_off(streams) == :ok
    refute_receive :third, 100

    send(pid, :go)
    assert_receive :third
    assert ConnectionStreams.hand_off(ConnectionStreams.new()) == :ok

    # an in-order job running with nothing behind it leaves nothing to hand off
    lone = ConnectionStreams.run_in_order(ConnectionStreams.new(), running, "")
    assert_receive {:running, lone_pid}
    assert ConnectionStreams.hand_off(lone) == :ok
    Process.exit(lone_pid, :kill)
  end

  test "a job queued with on_close: :drop is left out of the hand-off; the ones after it still run" do
    parent = self()

    running = fn ->
      send(parent, {:running, self()})
      receive(do: (:go -> ""))
    end

    streams =
      ConnectionStreams.new()
      |> ConnectionStreams.run_in_order(running, "")
      |> ConnectionStreams.run_in_order(fn -> send(parent, :join) && "" end, "", on_close: :drop)
      |> ConnectionStreams.run_in_order(fn -> send(parent, :leave) && "" end, "")

    assert_receive {:running, pid}
    :ok = ConnectionStreams.hand_off(streams)
    send(pid, :go)

    assert_receive :leave
    refute_received :join
  end

  test "a connection with as many jobs as it may hold, running or waiting, stops reading its socket" do
    parent = self()

    hold = fn ->
      send(parent, {:held, self()})
      receive(do: (:never -> ""))
    end

    max = ConnectionStreams.max_jobs()

    streams = Enum.reduce(1..(max - 2), ConnectionStreams.new(), fn _, s -> ConnectionStreams.run(s, hold, "") end)
    streams = ConnectionStreams.run_in_order(streams, hold, "")
    assert ConnectionStreams.room?(streams)

    # one more waits behind the running in-order job, and counts
    streams = ConnectionStreams.run_in_order(streams, hold, "")
    refute ConnectionStreams.room?(streams)

    # the jobs that started, and only those, end with the test
    for _ <- 1..(max - 1) do
      assert_receive {:held, pid}
      Process.exit(pid, :kill)
    end

    for {ref, _fallback} <- streams.jobs, do: Process.demonitor(ref, [:flush])
  end

  @tag capture_log: true
  test "a job that crashes is answered with its fallback" do
    streams = ConnectionStreams.run(ConnectionStreams.new(), fn -> raise "boom" end, "fallback")
    assert_receive {:DOWN, _ref, :process, _pid, _reason} = down, 1_000
    assert {streams, ["fallback"]} = ConnectionStreams.handle_message(streams, down)
    refute ConnectionStreams.busy?(streams)
  end

  test "an open consume stream keeps the connection busy; consume streams never hold the socket", ctx do
    refute ConnectionStreams.busy?(ConnectionStreams.new())
    {streams, _consumer} = ConnectionStreams.open_consumer(ConnectionStreams.new(), ctx.consumer)
    assert ConnectionStreams.busy?(streams)
    assert ConnectionStreams.room?(streams)
  end
end
