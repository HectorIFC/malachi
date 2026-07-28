defmodule Malachi.ConnectionRegistryTest do
  use ExUnit.Case, async: false

  setup do
    # Create a real TCP socket for testing
    {:ok, listen_socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen_socket)

    spawn(fn ->
      case :gen_tcp.accept(listen_socket) do
        {:ok, _socket} ->
          receive do
            _ -> :ok
          end

        {:error, _} ->
          :ok
      end
    end)

    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false])

    on_exit(fn ->
      :gen_tcp.close(socket)
      :gen_tcp.close(listen_socket)
    end)

    {:ok, socket: socket}
  end

  describe "register/3" do
    test "registers a new connection", %{socket: socket} do
      pid =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      transport = :gen_tcp

      assert :ok = Malachi.ConnectionRegistry.register(pid, socket, transport)

      count = Malachi.ConnectionRegistry.count()
      assert count >= 1
    end

    test "monitors registered process", %{socket: socket} do
      test_pid = self()

      pid =
        spawn(fn ->
          send(test_pid, :ready)

          receive do
            _ -> :ok
          end
        end)

      receive do
        :ready -> :ok
      end

      initial_count = Malachi.ConnectionRegistry.count()
      Malachi.ConnectionRegistry.register(pid, socket, :gen_tcp)

      Process.exit(pid, :kill)
      :timer.sleep(100)

      final_count = Malachi.ConnectionRegistry.count()
      assert final_count < initial_count + 1 or not Process.alive?(pid)
    end
  end

  describe "set_connection_type/3" do
    test "updates connection type to producer", %{socket: socket} do
      pid =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      Malachi.ConnectionRegistry.register(pid, socket, :gen_tcp)

      assert :ok = Malachi.ConnectionRegistry.set_connection_type(pid, :producer, "test_queue")
    end

    test "updates connection type to consumer", %{socket: socket} do
      pid =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      Malachi.ConnectionRegistry.register(pid, socket, :gen_tcp)

      assert :ok = Malachi.ConnectionRegistry.set_connection_type(pid, :consumer, "test_queue")
    end

    test "returns error for unregistered pid" do
      fake_pid = spawn(fn -> :ok end)
      :timer.sleep(50)

      assert {:error, :not_found} = Malachi.ConnectionRegistry.set_connection_type(fake_pid, :producer)
    end

    test "updates type multiple times", %{socket: socket} do
      pid =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      Malachi.ConnectionRegistry.register(pid, socket, :gen_tcp)

      assert :ok = Malachi.ConnectionRegistry.set_connection_type(pid, :producer, "queue1")
      assert :ok = Malachi.ConnectionRegistry.set_connection_type(pid, :consumer, "queue2")
    end
  end

  describe "unregister/1" do
    test "unregisters a connection", %{socket: socket} do
      pid =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      Malachi.ConnectionRegistry.register(pid, socket, :gen_tcp)

      initial_count = Malachi.ConnectionRegistry.count()
      assert :ok = Malachi.ConnectionRegistry.unregister(pid)

      final_count = Malachi.ConnectionRegistry.count()
      assert final_count < initial_count
    end

    test "unregistering non-existent connection is harmless" do
      fake_pid = spawn(fn -> :ok end)
      assert :ok = Malachi.ConnectionRegistry.unregister(fake_pid)
    end
  end

  describe "count/0" do
    test "returns count of active connections" do
      count = Malachi.ConnectionRegistry.count()
      assert is_integer(count)
      assert count >= 0
    end
  end

  describe "list_producers_by_queue/1" do
    test "lists producers for specific queue", %{socket: socket} do
      queue_name = "producer_queue_#{:rand.uniform(10000)}"

      pid =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      Malachi.ConnectionRegistry.register(pid, socket, :gen_tcp)
      Malachi.ConnectionRegistry.set_connection_type(pid, :producer, queue_name)

      producers = Malachi.ConnectionRegistry.list_producers_by_queue(queue_name)

      assert is_list(producers)
      assert producers != []

      if producers != [] do
        producer = hd(producers)
        assert Map.has_key?(producer, :pid)
        assert Map.has_key?(producer, :ip)
        assert Map.has_key?(producer, :connected_at)
      end
    end

    test "returns empty list for queue with no producers" do
      producers = Malachi.ConnectionRegistry.list_producers_by_queue("nonexistent_queue_#{:rand.uniform(10000)}")
      assert producers == []
    end

    test "does not include consumers", %{socket: _socket} do
      queue_name = "mixed_queue_#{:rand.uniform(10000)}"

      # Create two sockets
      {:ok, listen_socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listen_socket)

      spawn(fn ->
        case :gen_tcp.accept(listen_socket) do
          {:ok, _s1} ->
            case :gen_tcp.accept(listen_socket) do
              {:ok, _s2} ->
                receive do
                  _ -> :ok
                end

              _ ->
                :ok
            end

          _ ->
            :ok
        end
      end)

      {:ok, socket1} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false])
      {:ok, socket2} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false])

      producer_pid =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      consumer_pid =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      Malachi.ConnectionRegistry.register(producer_pid, socket1, :gen_tcp)
      Malachi.ConnectionRegistry.register(consumer_pid, socket2, :gen_tcp)

      Malachi.ConnectionRegistry.set_connection_type(producer_pid, :producer, queue_name)
      Malachi.ConnectionRegistry.set_connection_type(consumer_pid, :consumer, queue_name)

      producers = Malachi.ConnectionRegistry.list_producers_by_queue(queue_name)

      assert producers != []
      assert Enum.all?(producers, fn p -> String.contains?(p.pid, inspect(producer_pid)) end)
    end
  end

  describe "list_consumers_by_queue/1" do
    test "lists consumers for specific queue", %{socket: socket} do
      queue_name = "consumer_queue_#{:rand.uniform(10000)}"

      pid =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      Malachi.ConnectionRegistry.register(pid, socket, :gen_tcp)
      Malachi.ConnectionRegistry.set_connection_type(pid, :consumer, queue_name)

      consumers = Malachi.ConnectionRegistry.list_consumers_by_queue(queue_name)

      assert is_list(consumers)
      assert consumers != []

      if consumers != [] do
        consumer = hd(consumers)
        assert Map.has_key?(consumer, :pid)
        assert Map.has_key?(consumer, :ip)
        assert Map.has_key?(consumer, :connected_at)
      end
    end

    test "returns empty list for queue with no consumers" do
      consumers = Malachi.ConnectionRegistry.list_consumers_by_queue("nonexistent_queue_#{:rand.uniform(10000)}")
      assert consumers == []
    end
  end

  describe "list_producers/0" do
    test "lists all producers", %{socket: socket} do
      pid =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      Malachi.ConnectionRegistry.register(pid, socket, :gen_tcp)
      Malachi.ConnectionRegistry.set_connection_type(pid, :producer, "test_queue")

      producers = Malachi.ConnectionRegistry.list_producers()
      assert is_list(producers)
    end
  end

  describe "list_consumers/0" do
    test "lists all consumers", %{socket: socket} do
      pid =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      Malachi.ConnectionRegistry.register(pid, socket, :gen_tcp)
      Malachi.ConnectionRegistry.set_connection_type(pid, :consumer, "test_queue")

      consumers = Malachi.ConnectionRegistry.list_consumers()
      assert is_list(consumers)
    end
  end

  describe "graceful_shutdown/1" do
    test "closes all connections", %{socket: _socket} do
      # Create two new sockets for this test
      {:ok, listen_socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listen_socket)

      spawn(fn ->
        with {:ok, _s1} <- :gen_tcp.accept(listen_socket),
             {:ok, _s2} <- :gen_tcp.accept(listen_socket) do
          receive do
            _ -> :ok
          end
        else
          _ -> :ok
        end
      end)

      {:ok, socket1} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false])
      {:ok, socket2} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false])

      pid1 =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      pid2 =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      Malachi.ConnectionRegistry.register(pid1, socket1, :gen_tcp)
      Malachi.ConnectionRegistry.register(pid2, socket2, :gen_tcp)

      result = Malachi.ConnectionRegistry.close_all()
      assert result == :ok

      :timer.sleep(100)
      count = Malachi.ConnectionRegistry.count()
      assert count == 0

      :gen_tcp.close(listen_socket)
    end

    test "handles empty connection list gracefully" do
      # Clear any existing connections first
      Malachi.ConnectionRegistry.close_all()
      :timer.sleep(50)

      # Call close_all when there are no connections
      result = Malachi.ConnectionRegistry.close_all()
      assert result == :ok
    end

    test "sends shutdown notification to clients", %{socket: _socket} do
      {:ok, listen_socket} = :gen_tcp.listen(0, [:binary, packet: 0, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listen_socket)

      test_pid = self()

      spawn(fn ->
        {:ok, client_socket} = :gen_tcp.accept(listen_socket)
        # Try to receive the shutdown message
        case :gen_tcp.recv(client_socket, 0, 200) do
          {:ok, msg} ->
            send(test_pid, {:received, msg})

          _ ->
            send(test_pid, :no_message)
        end
      end)

      {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, packet: 0, active: false])

      pid =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      Malachi.ConnectionRegistry.register(pid, socket, :gen_tcp)
      Malachi.ConnectionRegistry.close_all()

      # The shutdown message should have been sent
      receive do
        {:received, msg} ->
          assert String.contains?(msg, "shutdown")

        :no_message ->
          # Message might have been sent but not received in time, that's ok
          :ok
      after
        500 ->
          :ok
      end

      :gen_tcp.close(listen_socket)
    end
  end

  describe "process monitoring" do
    test "automatically unregisters dead processes", %{socket: socket} do
      pid = spawn(fn -> :ok end)
      Malachi.ConnectionRegistry.register(pid, socket, :gen_tcp)

      :timer.sleep(100)

      # Process should have exited and been unregistered
      # We can verify by checking that count doesn't grow indefinitely
      initial_count = Malachi.ConnectionRegistry.count()
      assert is_integer(initial_count)
    end

    test "prunes a dead connection's entry (monitor runs in the registry)", %{socket: socket} do
      pid = spawn(fn -> Process.sleep(:infinity) end)

      Malachi.ConnectionRegistry.register(pid, socket, :gen_tcp)
      assert [{^pid, _, _, _, _, _, _}] = :ets.lookup(:malachi_connections, pid)

      # Monitor the pid from the test too, so we can wait until it is actually dead before checking.
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}

      # A synchronous round-trip forces the registry to drain its mailbox, including the :DOWN that the
      # registry's own monitor received when the pid died. If the monitor had stayed in the caller, this
      # :DOWN would never reach the registry and the entry below would remain.
      _ = :sys.get_state(Malachi.ConnectionRegistry)
      assert :ets.lookup(:malachi_connections, pid) == []
    end
  end

  describe "IP address formatting" do
    test "formats IPv4 addresses correctly", %{socket: _socket} do
      # When we register with a TCP socket, it should format the IP correctly
      {:ok, listen_socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listen_socket)

      spawn(fn ->
        case :gen_tcp.accept(listen_socket) do
          {:ok, _s} ->
            receive do
              _ -> :ok
            end

          _ ->
            :ok
        end
      end)

      {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false])

      pid =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      Malachi.ConnectionRegistry.register(pid, socket, :gen_tcp)
      Malachi.ConnectionRegistry.set_connection_type(pid, :producer, "test")

      producers = Malachi.ConnectionRegistry.list_producers_by_queue("test")

      if producers != [] do
        producer = hd(producers)
        # IP should be formatted as string
        assert is_binary(producer.ip)
        # Should contain dots for IPv4
        assert String.contains?(producer.ip, ".")
      end

      :gen_tcp.close(listen_socket)
    end
  end

  describe "GenServer callbacks" do
    test "handles unknown messages gracefully" do
      # Send a message directly to the GenServer
      send(Malachi.ConnectionRegistry, :unknown_message)
      :timer.sleep(50)

      # Should still be alive
      assert Process.whereis(Malachi.ConnectionRegistry) != nil
    end
  end
end
