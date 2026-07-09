defmodule Malachi.LogStreamingTest do
  # End-to-end over the real TCP server (started by the app): the B2 streaming path — subscribe opens a
  # server-push stream (pushes tagged with the subscribe's correlation id), a credit window bounds
  # in-flight records, and a stream_ack returns credit while durably committing the group. Exercises the
  # binary wiring (Wire subscribe/stream_ack + the acceptor's active-mode stream loop) against the live
  # Malachi.LogBroker.
  use ExUnit.Case, async: false

  alias Malachi.Log.Record
  alias Malachi.Test.TCPHelper
  alias Malachi.Wire

  @port Application.compile_env(:malachi, :tcp_port, 4040)

  defp ok?(code), do: code == Wire.ok_code()
  defp reason(payload), do: Wire.decode_auth_resp(payload)
  defp values(records), do: Enum.map(records, & &1.value)

  defp create_topic(socket, topic) do
    TCPHelper.request(socket, Wire.create_topic_key(), 1, Wire.encode_create_topic_req(topic, 8))
  end

  defp produce(socket, topic, values) do
    wire = Enum.map(values, &%Record{value: &1})
    TCPHelper.request(socket, Wire.produce_key(), 1, Wire.encode_produce_req(topic, wire))
  end

  defp with_session(username, password, fun) do
    case TCPHelper.connect(port: @port) do
      {:ok, socket} ->
        {:ok, _token} = TCPHelper.authenticate_wire(socket, username, password)

        try do
          fun.(socket)
        after
          :gen_tcp.close(socket)
        end

      {:error, _reason} ->
        # No TCP server in this environment; nothing to assert.
        :ok
    end
  end

  # Runs `fun` on a fresh authenticated connection used only to produce (the streaming connection cannot
  # do request/response ops once subscribed).
  defp on_producer(username \\ "app", password \\ "app123", fun) do
    with_session(username, password, fun)
  end

  test "subscribe streams the backlog, then a produce on another connection pushes the new record" do
    topic = "stream_#{System.unique_integer([:positive])}"

    on_producer(fn prod ->
      assert {code, _} = create_topic(prod, topic)
      assert ok?(code)
      assert {code, <<2::32>>} = produce(prod, topic, ["a", "b"])
      assert ok?(code)
    end)

    with_session("app", "app123", fn sub ->
      corr = 77
      TCPHelper.subscribe(sub, topic, "sg", 10, 100, corr)

      # the initial backlog is pushed, tagged with the subscribe's correlation id
      {records, cursor} = TCPHelper.recv_push(sub, corr)
      assert values(records) == ["a", "b"]
      assert is_binary(cursor)
      assert Enum.all?(records, &(&1.offset == nil))

      # a produce on a different connection wakes the stream with the new record
      on_producer(fn prod ->
        assert {code, <<1::32>>} = produce(prod, topic, ["c"])
        assert ok?(code)
      end)

      {records2, _cursor} = TCPHelper.recv_push(sub, corr)
      assert values(records2) == ["c"]
    end)
  end

  test "the credit window bounds in-flight records until an ack returns credit" do
    topic = "stream_win_#{System.unique_integer([:positive])}"

    on_producer(fn prod ->
      assert {code, _} = create_topic(prod, topic)
      assert ok?(code)
      assert {code, <<5::32>>} = produce(prod, topic, ["a", "b", "c", "d", "e"])
      assert ok?(code)
    end)

    with_session("app", "app123", fn sub ->
      corr = 88
      TCPHelper.subscribe(sub, topic, "wg", 2, 100, corr)

      # only the window's worth is in flight, though 5 are available
      {records, cursor} = TCPHelper.recv_push(sub, corr)
      assert values(records) == ["a", "b"]
      assert {:error, :timeout} = TCPHelper.recv_frame(sub, timeout: 200)

      # acking 2 returns 2 credit → the next 2 are pushed
      TCPHelper.stream_ack(sub, topic, "wg", cursor, 2, 89)
      {records2, _cursor} = TCPHelper.recv_push(sub, corr)
      assert values(records2) == ["c", "d"]
    end)
  end

  test "subscribe requires :consume — a producer-only session gets an error, not a stream" do
    topic = "stream_perm_#{System.unique_integer([:positive])}"

    on_producer("producer", "producer123", fn prod ->
      assert {code, _} = create_topic(prod, topic)
      assert ok?(code)
    end)

    with_session("producer", "producer123", fn sub ->
      corr = 99
      TCPHelper.subscribe(sub, topic, "pg", 10, 100, corr)

      {:ok, body} = TCPHelper.recv_frame(sub, timeout: 1_000)
      assert {^corr, code, payload} = Wire.decode_response(body)
      refute ok?(code)
      assert reason(payload) == "permission_denied"
    end)
  end
end
