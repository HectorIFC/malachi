defmodule Malachi.TCPAcceptorPoolTest do
  use ExUnit.Case, async: false

  alias Malachi.TCPAcceptorPool
  alias Malachi.Test.CertFixtures

  # Every pool started here is registered under its own name, so it runs beside the application's pool
  # (which holds the module name) and its bound port is recorded under that name, never over the
  # application's.
  defp pool_name, do: :"tcp_acceptor_pool_test_#{System.unique_integer([:positive])}"

  defp start_pool(port) do
    name = pool_name()
    {start_supervised({TCPAcceptorPool, {port, name: name}}, id: name), name}
  end

  defp acceptor_ports(pool) do
    for {{:acceptor, _i} = id, _pid, :worker, _modules} <- Supervisor.which_children(pool) do
      {:ok, %{start: {Malachi.TCPAcceptor, :start_link, [{port, _opts, _id, _transport}]}}} =
        :supervisor.get_childspec(pool, id)

      port
    end
  end

  describe "start_link/1" do
    test "fails with an out of range port" do
      assert {{:error, _reason}, _name} = start_pool(99_999_999)
    end

    test "fails with a negative port" do
      assert {{:error, _reason}, _name} = start_pool(-1)
    end

    test "fails when the port is held by a socket that does not share it" do
      # No `reuseport` on the holder: the pool's own listeners set it, and on Linux two sockets of one
      # user that BOTH set it may bind the same port, which is exactly what this must not be mistaken for.
      {:ok, holder} = :gen_tcp.listen(0, [:binary, active: false])
      on_exit(fn -> :gen_tcp.close(holder) end)
      {:ok, held} = :inet.port(holder)

      assert {{:error, {:eaddrinuse, _child}}, name} = start_pool(held)
      assert TCPAcceptorPool.port(name) == nil
    end

    test "the application's pool is registered under the module name" do
      assert {:error, {:already_started, _pid}} = TCPAcceptorPool.start_link(0)
    end
  end

  describe "port 0" do
    test "binds a port the operating system picks and records it" do
      {{:ok, _pool}, name} = start_pool(0)

      port = TCPAcceptorPool.port(name)
      assert is_integer(port) and port > 0
      assert {:ok, client} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1_000)
      :gen_tcp.close(client)
    end

    test "hands every acceptor the bound port, never 0" do
      {{:ok, pool}, name} = start_pool(0)

      ports = acceptor_ports(pool)
      assert length(ports) == System.schedulers_online()
      assert Enum.uniq(ports) == [TCPAcceptorPool.port(name)]
    end

    test "an acceptor that restarts comes back on the same port" do
      {{:ok, pool}, name} = start_pool(0)
      bound = TCPAcceptorPool.port(name)

      [{id, pid, :worker, _modules} | _rest] = Supervisor.which_children(pool)
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}

      restarted =
        Enum.find_value(1..50, fn _attempt ->
          case List.keyfind(Supervisor.which_children(pool), id, 0) do
            {^id, new_pid, :worker, _modules} when is_pid(new_pid) and new_pid != pid ->
              new_pid

            _other ->
              Process.sleep(20)
              nil
          end
        end)

      assert is_pid(restarted)
      %{socket: socket} = :sys.get_state(restarted)
      assert :inet.port(socket) == {:ok, bound}
    end

    test "over TLS reads the bound port from the ssl socket" do
      dir = Path.join(System.tmp_dir!(), "malachi_pool_tls_#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm_rf!(dir) end)
      %{certfile: certfile, keyfile: keyfile} = CertFixtures.server_cert!(dir)

      for {key, value} <- [enable_tls: true, tls_certfile: certfile, tls_keyfile: keyfile] do
        previous = Application.fetch_env(:malachi, key)
        Application.put_env(:malachi, key, value)

        on_exit(fn ->
          case previous do
            {:ok, old} -> Application.put_env(:malachi, key, old)
            :error -> Application.delete_env(:malachi, key)
          end
        end)
      end

      {{:ok, pool}, name} = start_pool(0)
      port = TCPAcceptorPool.port(name)
      assert is_integer(port) and port > 0
      assert Enum.uniq(acceptor_ports(pool)) == [port]

      client_opts = [:binary, active: false, verify: :verify_none, versions: [:"tlsv1.3", :"tlsv1.2"]]
      assert {:ok, client} = :ssl.connect(~c"127.0.0.1", port, client_opts, 5_000)
      :ssl.close(client)
    end
  end

  describe "port/1" do
    test "is nil for a pool that never started" do
      assert TCPAcceptorPool.port(pool_name()) == nil
    end

    test "the application's pool bound a real port while the test config asks for 0" do
      assert Application.get_env(:malachi, :tcp_port) == 0
      assert TCPAcceptorPool.port() > 0
    end
  end

  describe "configuration" do
    test "reads tcp_buffer_size from config" do
      buffer_size = Application.get_env(:malachi, :tcp_buffer_size, 32_768)
      assert is_integer(buffer_size)
      assert buffer_size > 0
    end

    test "reads tcp_backlog from config" do
      backlog = Application.get_env(:malachi, :tcp_backlog, 4096)
      assert is_integer(backlog)
      assert backlog > 0
    end

    test "reads tcp_send_timeout from config" do
      send_timeout = Application.get_env(:malachi, :tcp_send_timeout, 30_000)
      assert is_integer(send_timeout)
      assert send_timeout > 0
    end

    test "reads enable_tls from config" do
      enable_tls = Application.get_env(:malachi, :enable_tls, false)
      assert is_boolean(enable_tls)
    end
  end

  describe "supervisor behavior" do
    test "TCPAcceptorPool supervisor is running" do
      assert Process.whereis(Malachi.TCPAcceptorPool) != nil
    end

    test "has one acceptor per online scheduler" do
      assert length(Supervisor.which_children(Malachi.TCPAcceptorPool)) == System.schedulers_online()
    end
  end
end
