defmodule Malachi.Loadtest.RouterTest do
  # The route cache the load generator and the chaos checker route by: pure, but for the bootstrap, which
  # is asked of a wire listener here (the real server always answers `cluster_state`, so a node too old to
  # know it is played by one that refuses the key).
  use ExUnit.Case, async: true

  alias Malachi.Keyspace
  alias Malachi.Loadtest.Conn
  alias Malachi.Loadtest.Router
  alias Malachi.Wire

  @bootstrap {"10.0.0.1", 4040}

  defp cluster(brokers, enabled \\ true),
    do: Router.put_cluster_state(Router.new(@bootstrap), %{brokers: brokers, streams_enabled: enabled})

  defp broker(id, host, port), do: %{id: id, host: host, port: port, status: :alive}

  defp routes(router, ranges, version \\ "v1"),
    do: Router.put_routes(router, %{topic: "t", version: version, keyspace_bits: 4, ranges: ranges})

  defp range(seq, key_start, key_end, primary, state \\ :active) do
    segment = if primary, do: %{segment: 0, primary: primary}, else: nil
    %{range: seq, key_start: key_start, key_end: key_end, state: state, segment: segment}
  end

  # A key whose position in a keyspace of `size` falls in `[low, high)`.
  defp key_within(low, high, size \\ 16) do
    Enum.find_value(0..10_000, fn i ->
      key = "k#{i}"
      if Keyspace.within?(Keyspace.position_of(key, size), low, high), do: key
    end)
  end

  describe "range_of/3" do
    test "the active range whose block holds the key's position, as the server hashes it" do
      router = cluster([broker("a@h", "h", 1)]) |> routes([range(1, 0, 8, "a@h"), range(2, 8, 16, "a@h")])

      assert Router.range_of(router, "t", key_within(0, 8)) == {:ok, 1}
      assert Router.range_of(router, "t", key_within(8, 16)) == {:ok, 2}
    end

    test "a nil key routes by its hash too, as the server's keyspace does" do
      router = cluster([]) |> routes([range(0, 0, 16, nil)])
      assert Router.range_of(router, "t", nil) == {:ok, 0}
    end

    test "sealed ranges route nothing: a position only a sealed range covers has no range" do
      router = cluster([]) |> routes([range(0, 0, 16, nil, :sealed), range(1, 0, 8, nil)])

      assert Router.range_of(router, "t", key_within(0, 8)) == {:ok, 1}
      assert Router.range_of(router, "t", key_within(8, 16)) == :no_range
    end

    test "a topic whose routes were never read, or were forgotten, is unknown" do
      router = cluster([]) |> routes([range(0, 0, 16, nil)])

      assert Router.range_of(router, "other", "k") == :unknown_topic
      assert Router.range_of(Router.forget(router, "t"), "t", "k") == :unknown_topic
    end
  end

  describe "address_of/3" do
    test "the primary's advertised address" do
      router = cluster([broker("a@h", "10.0.0.2", 5050)]) |> routes([range(0, 0, 16, "a@h")])
      assert Router.address_of(router, "t", 0) == {:ok, {"10.0.0.2", 5050}}
    end

    test "a range with no active segment yet is opened through the bootstrap node" do
      router = cluster([broker("a@h", "10.0.0.2", 5050)]) |> routes([range(0, 0, 16, nil)])
      assert Router.address_of(router, "t", 0) == {:ok, @bootstrap}
    end

    test "a primary that advertises nothing is the bootstrap node only when it is the one broker" do
      single = cluster([broker("a@h", nil, 0)]) |> routes([range(0, 0, 16, "a@h")])
      assert Router.address_of(single, "t", 0) == {:ok, @bootstrap}

      several = cluster([broker("a@h", nil, 0), broker("b@h", "10.0.0.3", 4040)]) |> routes([range(0, 0, 16, "a@h")])
      assert Router.address_of(several, "t", 0) == {:error, :unadvertised}
    end

    test "a host without a port, or a primary the cluster state does not list, is not reachable" do
      no_port =
        cluster([broker("a@h", "10.0.0.2", 0), broker("b@h", "10.0.0.3", 1)]) |> routes([range(0, 0, 16, "a@h")])

      assert Router.address_of(no_port, "t", 0) == {:error, :unadvertised}

      unlisted = cluster([broker("b@h", "10.0.0.3", 1)]) |> routes([range(0, 0, 16, "gone@h")])
      assert Router.address_of(unlisted, "t", 0) == {:error, :unadvertised}
    end

    test "a range the routes do not hold (retired by a split), and a topic not read, say so" do
      router = cluster([]) |> routes([range(1, 0, 16, nil)])

      assert Router.address_of(router, "t", 0) == {:error, :unknown_range}
      assert Router.address_of(router, "other", 0) == :unknown_topic
    end
  end

  test "the routes keep the version a stream is opened against, and the keyspace size" do
    router = cluster([], false) |> routes([range(0, 0, 16, nil)], "v9")

    assert Router.version(router, "t") == "v9"
    assert Router.version(router, "other") == nil
    assert %{size: 16} = Router.routes(router, "t")
    refute Router.streams_enabled?(router)
  end

  describe "bootstrap/2" do
    # A wire listener that answers every request with `answer` (a function of the correlation id).
    defp listener(answer) do
      {:ok, listen} = :gen_tcp.listen(0, [:binary, packet: 0, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listen)

      spawn_link(fn ->
        {:ok, socket} = :gen_tcp.accept(listen)
        serve(socket, answer, "")
      end)

      {:ok, conn} = Conn.connect(host: "127.0.0.1", port: port)
      conn
    end

    defp serve(socket, answer, buffer) do
      case Wire.decode_frame(buffer) do
        {:ok, <<_key::16, corr::32, _payload::binary>>, rest} ->
          :ok = :gen_tcp.send(socket, answer.(corr))
          serve(socket, answer, rest)

        :incomplete ->
          case :gen_tcp.recv(socket, 0) do
            {:ok, data} -> serve(socket, answer, buffer <> data)
            {:error, _closed} -> :ok
          end
      end
    end

    test "a node that refuses cluster_state is one the client speaks the legacy keys to" do
      conn = listener(&Wire.encode_error(&1, :unknown_api_key))
      assert {:legacy, :no_cluster_state, conn} = Router.bootstrap(Router.new(@bootstrap), conn)
      Conn.close(conn)
    end

    test "a refresh against such a node is an error naming it, not a legacy answer" do
      conn = listener(&Wire.encode_error(&1, :unknown_api_key))
      assert {:error, "no_cluster_state", conn} = Router.refresh(Router.new(@bootstrap), conn, "t")
      Conn.close(conn)
    end

    test "a connection that drops is the transport error" do
      {:ok, listen} = :gen_tcp.listen(0, [:binary, packet: 0, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listen)
      {:ok, conn} = Conn.connect(host: "127.0.0.1", port: port)
      {:ok, server_side} = :gen_tcp.accept(listen)
      :ok = :gen_tcp.close(server_side)

      assert {:error, _closed} = Router.bootstrap(Router.new(@bootstrap), conn)
      assert {:error, _closed} = Router.refresh(Router.new(@bootstrap), conn, "t")
    end
  end
end
