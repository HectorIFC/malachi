defmodule Malachi.Loadtest.ConnActiveTest do
  # A connection read by messages (`Conn.activate/1` and `Conn.message/2`), as the routing producer reads
  # several at once, over a loopback socket whose far end the test writes to directly.
  use ExUnit.Case, async: true

  alias Malachi.Loadtest.Conn
  alias Malachi.Wire

  setup do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, packet: 0, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    {:ok, conn} = Conn.connect(host: "127.0.0.1", port: port)
    {:ok, far} = :gen_tcp.accept(listen)
    :ok = Conn.activate(conn)
    %{conn: conn, far: far}
  end

  defp next_message(socket) do
    receive do
      {_tag, ^socket, _data} = message -> message
      {_tag, ^socket} = message -> message
    after
      2_000 -> flunk("no socket message")
    end
  end

  test "a frame cut across two messages comes out whole, once both have come", %{conn: conn, far: far} do
    frame = Wire.encode_ok(5, "hello")
    {head, tail} = String.split_at(frame, 6)

    :ok = :gen_tcp.send(far, head)
    assert {:frames, [], conn} = Conn.message(conn, next_message(conn.socket))

    :ok = Conn.activate(conn)
    :ok = :gen_tcp.send(far, tail)
    assert {:frames, [body], conn} = Conn.message(conn, next_message(conn.socket))
    assert Wire.decode_response(body) == {5, 0, "hello"}
    assert conn.buffer == ""
  end

  test "several frames in one message come out in order, and a partial one waits", %{conn: conn, far: far} do
    third = Wire.encode_ok(3, "c")
    :ok = :gen_tcp.send(far, Wire.encode_ok(1, "a") <> Wire.encode_ok(2, "b") <> binary_part(third, 0, 3))

    assert {:frames, [a, b], conn} = Conn.message(conn, next_message(conn.socket))
    assert Enum.map([a, b], &Wire.decode_response/1) == [{1, 0, "a"}, {2, 0, "b"}]
    assert conn.buffer == binary_part(third, 0, 3)
  end

  test "the far end closing is the connection gone", %{conn: conn, far: far} do
    :ok = :gen_tcp.close(far)
    assert Conn.message(conn, next_message(conn.socket)) == {:closed, :closed}
  end

  test "an error on the socket is the connection gone, with its reason", %{conn: conn} do
    assert Conn.message(conn, {:tcp_error, conn.socket, :econnreset}) == {:closed, :econnreset}
    assert Conn.message(conn, {:ssl_error, conn.socket, :closed}) == {:closed, :closed}
  end

  test "a message about another socket, or anything else, is not this connection's", %{conn: conn} do
    other = make_ref()

    assert Conn.message(conn, {:tcp, other, "x"}) == :no
    assert Conn.message(conn, {:tcp_closed, other}) == :no
    assert Conn.message(conn, :other) == :no
  end

  test "a connection that has not been activated again delivers nothing more", %{conn: conn, far: far} do
    :ok = :gen_tcp.send(far, Wire.encode_ok(1, "a"))
    assert {:frames, [_], _conn} = Conn.message(conn, next_message(conn.socket))

    :ok = :gen_tcp.send(far, Wire.encode_ok(2, "b"))
    socket = conn.socket
    refute_receive {:tcp, ^socket, _}, 100

    :ok = Conn.activate(conn)
    assert_receive {:tcp, ^socket, _}, 2_000
  end
end
