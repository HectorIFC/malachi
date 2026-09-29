defmodule Malachi.Test.SubscribeResetStub do
  @moduledoc """
  A wire server that loses a stream worker's connection at a chosen moment, so both load generators can be
  shown to count it as a dropped connection and resubscribe (issue #218).

  Every request is answered ok except a subscribe, which gets nothing: the stream stays silent until the
  deadline, and no record or error blurs the counts under test. Connection 1 is always the setup connection
  (auth and create_topic). The modes decide what happens to the workers after it.

  ## Before the subscribe: `:reconnect`, `:give_up` and `:stall_reconnect`

  The first worker finds its socket already reset when it sends the subscribe. A plain close does not do
  it: after a FIN the first send still answers `:ok`, because the kernel buffer takes the bytes, and the
  recv after it takes the path a drop mid-stream takes. The send only fails once the client has received a
  reset. The order below makes that certain without a sleep, provided the generator opens its connections
  one at a time (`:bounded` with a concurrency of 1), runs two of them, and prepopulates nothing
  (`prepopulate: 0`, `--prepopulate 0`): the Node generator prepopulates on a connection of its own, which
  would then be connection 2 and take the reset meant for the first worker:

    1. connection 2 is the first worker, which authenticates and waits at the start barrier;
    2. when connection 3 (the second worker) arrives, connection 2 has already read its auth reply, and it
       is reset (`linger: {true, 0}` then close) before connection 3's auth is answered. The barrier lifts
       only after that answer, so the first worker's subscribe goes out on a reset socket.

  After the reset, `:reconnect` serves every later connection, so the worker reconnects and resubscribes;
  `:give_up` closes the listener too, so every reconnect is refused until the generator's retry cap;
  `:stall_reconnect` accepts later connections and never answers them, so a reconnect hangs in its
  authentication until the generator stops waiting; `:refuse_reconnect_auth` refuses the authentication
  of every later connection but keeps it open, so a client that walks away from a failed reconnect without
  closing it leaves a live socket behind.

  ## After the subscribe: `{:mid_stream, :close}` and `{:mid_stream, :reset}`

  The first worker's connection (connection 2) is lost the moment its subscribe arrives, with a FIN
  (`:close`) or a reset (`:reset`), so the failure reaches the client on a live subscription rather than
  on the send. Later connections are served. One connection is enough.
  """

  alias Malachi.Wire

  @type mode ::
          :reconnect | :give_up | :stall_reconnect | :refuse_reconnect_auth | {:mid_stream, :close | :reset}

  @doc """
  Runs `fun` with the port of a stub in `mode` and returns its result. The stub, and every socket it holds,
  is gone when this returns.
  """
  @spec with_stub(mode(), (:inet.port_number() -> result)) :: result when result: term()
  def with_stub(mode, fun) do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, packet: 4, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    owner = spawn(fn -> accept(listen, mode, 1, nil) end)
    :ok = :gen_tcp.controlling_process(listen, owner)

    try do
      fun.(port)
    after
      # The owner holds every accepted socket, so killing it closes them all with the listener.
      Process.exit(owner, :kill)
    end
  end

  defp accept(listen, mode, n, first_worker) do
    case :gen_tcp.accept(listen) do
      {:ok, sock} ->
        if n == 3 and not mid_stream?(mode), do: reset_first_worker(first_worker, listen, mode)
        start_serving(sock, mode, n)
        accept(listen, mode, n + 1, if(n == 2, do: sock, else: first_worker))

      {:error, _closed} ->
        # Keep the accepted sockets open until with_stub/2 kills this process.
        Process.sleep(:infinity)
    end
  end

  defp mid_stream?({:mid_stream, _how}), do: true
  defp mid_stream?(_mode), do: false

  defp reset_first_worker(sock, listen, mode) do
    lose(sock, :reset)
    if mode == :give_up, do: :gen_tcp.close(listen)
  end

  # A stalled connection is accepted and held, never read, so its authentication gets no answer.
  defp start_serving(_sock, :stall_reconnect, n) when n > 3, do: :ok
  defp start_serving(sock, :refuse_reconnect_auth, n) when n > 3, do: spawn(fn -> refuse_auth(sock) end)
  defp start_serving(sock, {:mid_stream, how}, 2), do: spawn(fn -> serve(sock, {:lose_on_subscribe, how}) end)
  defp start_serving(sock, _mode, _n), do: spawn(fn -> serve(sock, :keep) end)

  # A passive socket may be read, closed or reset by any process, so the owner keeps it while this loop
  # answers it.
  defp serve(sock, on_subscribe) do
    case :gen_tcp.recv(sock, 0) do
      {:ok, <<api_key::16, corr::32, _payload::binary>>} ->
        cond do
          api_key == Wire.subscribe_key() and on_subscribe != :keep ->
            {:lose_on_subscribe, how} = on_subscribe
            lose(sock, how)

          api_key == Wire.subscribe_key() ->
            serve(sock, on_subscribe)

          # The Node client decodes the token an auth answers with, as the wire's present-string.
          api_key == Wire.auth_key() ->
            :gen_tcp.send(sock, <<corr::32, Wire.ok_code()::16, 1, 3::32, "tok">>)
            serve(sock, on_subscribe)

          true ->
            :gen_tcp.send(sock, <<corr::32, Wire.ok_code()::16>>)
            serve(sock, on_subscribe)
        end

      {:error, _closed} ->
        :ok
    end
  end

  # Answers every request with an error frame (the reason as the wire's present-string) and never closes.
  defp refuse_auth(sock) do
    case :gen_tcp.recv(sock, 0) do
      {:ok, <<_api_key::16, corr::32, _payload::binary>>} ->
        reason = "invalid_credentials"
        :gen_tcp.send(sock, <<corr::32, Wire.error_code()::16, 1, byte_size(reason)::32, reason::binary>>)
        refuse_auth(sock)

      {:error, _closed} ->
        :ok
    end
  end

  defp lose(sock, :close), do: :gen_tcp.close(sock)

  defp lose(sock, :reset) do
    :ok = :inet.setopts(sock, linger: {true, 0})
    :gen_tcp.close(sock)
  end
end
