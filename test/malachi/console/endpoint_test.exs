defmodule Malachi.Console.EndpointTest do
  # Not async: cases rewrite application env the endpoint reads at start, and the rate limit case
  # compares whole ETS tables before and after.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Malachi.Console.Assets
  alias Malachi.Console.Endpoint
  alias Malachi.Console.HeaderDeadline
  alias Malachi.Test.ConsoleFixture
  alias Malachi.Test.DashboardHelper
  alias Malachi.Test.HttpClient

  @keys [
    :dashboard_max_header_count,
    :dashboard_max_header_line_size,
    :dashboard_max_header_size,
    :dashboard_recv_timeout_ms,
    :console_num_acceptors,
    :console_max_connections,
    :console_static_dir
  ]

  setup do
    originals = Map.new(@keys, &{&1, Application.fetch_env(:malachi, &1)})

    on_exit(fn ->
      Enum.each(originals, fn
        {key, {:ok, value}} -> Application.put_env(:malachi, key, value)
        {key, :error} -> Application.delete_env(:malachi, key)
      end)
    end)

    %{dir: ConsoleFixture.bundle!()}
  end

  defp start!(dir), do: ConsoleFixture.start_endpoint!(:"console_#{System.unique_integer([:positive])}", dir)

  defp declared_length(response), do: response |> HttpClient.header("content-length") |> String.to_integer()

  describe "keep alive" do
    test "two responses on one socket, each exactly as long as it declares", %{dir: dir} do
      {:ok, socket} = dir |> start!() |> HttpClient.connect()

      assert {:ok, first} = HttpClient.request(socket, "GET", "/assets/app-3f2aB9x1.js")
      assert {:ok, second} = HttpClient.request(socket, "GET", "/")

      assert first.status == 200 and second.status == 200
      assert byte_size(first.body) == declared_length(first)
      assert byte_size(second.body) == declared_length(second)
      assert first.body == ConsoleFixture.script_body()
      assert second.body == ConsoleFixture.index_body()
      refute HttpClient.closed?(socket, 100)
    end

    test "two pipelined requests in one write get two framed responses in order", %{dir: dir} do
      {:ok, socket} = dir |> start!() |> HttpClient.connect()

      :ok =
        HttpClient.send_raw(socket, [
          HttpClient.encode("GET", "/favicon.ico"),
          HttpClient.encode("GET", "/assets/app-3f2aB9x1.js")
        ])

      assert {:ok, first, rest} = HttpClient.recv(socket)
      assert {:ok, second, ""} = HttpClient.recv(socket, rest)
      assert first.body == "ICO"
      assert second.body == ConsoleFixture.script_body()
    end

    test "a HEAD response declares the length and sends no body, and the socket stays usable", %{dir: dir} do
      {:ok, socket} = dir |> start!() |> HttpClient.connect()

      assert {:ok, head} = HttpClient.request(socket, "HEAD", "/assets/app-3f2aB9x1.js")
      assert head.body == ""
      assert declared_length(head) == byte_size(ConsoleFixture.script_body())

      assert {:ok, get} = HttpClient.request(socket, "GET", "/favicon.ico")
      assert get.body == "ICO"
    end

    test "Connection: close is honored", %{dir: dir} do
      {:ok, socket} = dir |> start!() |> HttpClient.connect()

      assert {:ok, %{status: 200}} = HttpClient.request(socket, "GET", "/", [{"Connection", "close"}])
      assert HttpClient.closed?(socket)
    end

    test "an idle connection is closed after the read deadline", %{dir: dir} do
      Application.put_env(:malachi, :dashboard_recv_timeout_ms, 200)
      {:ok, socket} = dir |> start!() |> HttpClient.connect()

      assert {:ok, %{status: 200}} = HttpClient.request(socket, "GET", "/")
      assert HttpClient.closed?(socket, 2_000)
    end

    test "a request whose headers never finish is dropped at the deadline", %{dir: dir} do
      Application.put_env(:malachi, :dashboard_recv_timeout_ms, 200)
      {:ok, socket} = dir |> start!() |> HttpClient.connect()

      :ok = HttpClient.send_raw(socket, "GET / HTTP/1.1\r\nHost: localhost\r\nX-Slow: 1\r\n")

      # Closed unanswered at the deadline, as the dashboard does.
      assert :gen_tcp.recv(socket, 0, 2_000) == {:error, :closed}
    end
  end

  describe "header deadline" do
    test "a response slower than the deadline is not cut off: the deadline covers headers only", %{dir: dir} do
      Application.put_env(:malachi, :dashboard_recv_timeout_ms, 200)
      big = :binary.copy("x", 8 * 1024 * 1024)
      File.write!(Path.join(dir, "assets/big-1a2bC4d5.js"), big)
      {:ok, socket} = dir |> start!() |> HttpClient.connect()

      :ok = HttpClient.send_raw(socket, HttpClient.encode("GET", "/assets/big-1a2bC4d5.js"))
      # A reader slower than the deadline: the server blocks on a full socket buffer meanwhile.
      Process.sleep(600)

      assert {:ok, %{status: 200, body: body}, ""} = HttpClient.recv(socket, "", timeout: 5_000)
      assert byte_size(body) == byte_size(big)
    end

    test "a connection's watchdog is registered while it lives and gone after it closes", %{dir: dir} do
      {:ok, socket} = dir |> start!() |> HttpClient.connect()
      assert {:ok, %{status: 200}} = HttpClient.request(socket, "GET", "/")

      assert [{connection, watchdog}] = connections_of(socket)

      assert Process.alive?(watchdog)
      ref = Process.monitor(watchdog)
      :gen_tcp.close(socket)

      assert_receive {:DOWN, ^ref, :process, ^watchdog, _}, 2_000
      refute Process.alive?(connection)
    end

    test "an idle kept alive connection's process stops at the deadline, not at the read timeout", %{dir: dir} do
      # Deadline 1 s, Bandit's read timeout 2 s: the stop reason says which of the two ended it.
      Application.put_env(:malachi, :dashboard_recv_timeout_ms, 1_000)
      {:ok, socket} = dir |> start!() |> HttpClient.connect()
      assert {:ok, %{status: 200}} = HttpClient.request(socket, "GET", "/")

      [{connection, _watchdog}] = connections_of(socket)
      ref = Process.monitor(connection)

      assert_receive {:DOWN, ^ref, :process, ^connection, reason}, 3_000
      assert reason == {:shutdown, :header_deadline}
    end

    test "the deadline starts again with every request, not once per connection", %{dir: dir} do
      # Deadline 600 ms: requests at about 0, 400 and 800 ms. A deadline fixed when the connection
      # opened would close it at 600 ms, before the third; one that restarts after each response
      # leaves 400 ms of slack every time.
      Application.put_env(:malachi, :dashboard_recv_timeout_ms, 600)
      {:ok, socket} = dir |> start!() |> HttpClient.connect()

      for step <- 1..3 do
        if step > 1, do: Process.sleep(400)
        assert {:ok, %{status: 200}} = HttpClient.request(socket, "GET", "/favicon.ico"), "request #{step}"
      end
    end

    test "an arm while armed starts the deadline again", %{dir: dir} do
      Application.put_env(:malachi, :dashboard_recv_timeout_ms, 600)
      {:ok, socket} = dir |> start!() |> HttpClient.connect()
      opened = System.monotonic_time(:millisecond)
      [{_connection, watchdog}] = await_connection(socket)

      # Armed since the connection opened: moving the deadline to about 1,000 ms keeps it open at 800.
      sleep_until(opened + 400)
      send(watchdog, :arm)
      sleep_until(opened + 800)
      refute HttpClient.closed?(socket, 0)
      assert HttpClient.closed?(socket, 2_000)
    end

    test "a disarm while disarmed is taken and leaves the connection open", %{dir: dir} do
      Application.put_env(:malachi, :dashboard_recv_timeout_ms, 300)
      {:ok, socket} = dir |> start!() |> HttpClient.connect()
      opened = System.monotonic_time(:millisecond)
      [{_connection, watchdog}] = await_connection(socket)

      send(watchdog, :disarm)
      send(watchdog, :disarm)
      sleep_until(opened + 450)

      assert Process.info(watchdog, :message_queue_len) == {:message_queue_len, 0}
      refute HttpClient.closed?(socket, 0)
    end

    test "arm and disarm outside a console connection do nothing" do
      assert HeaderDeadline.disarm() == :ok
      assert HeaderDeadline.arm() == :ok
    end
  end

  # connections_of/1 once the server has registered the connection, which happens after the client's
  # connect returns: polled every 10 ms for up to 2 s rather than guessed with a fixed sleep.
  defp await_connection(socket, attempts \\ 200) do
    case connections_of(socket) do
      [] when attempts > 0 ->
        Process.sleep(10)
        await_connection(socket, attempts - 1)

      found ->
        found
    end
  end

  defp sleep_until(deadline), do: Process.sleep(max(deadline - System.monotonic_time(:millisecond), 0))

  # The {connection, watchdog} pairs registered for the server side of our client `socket`.
  defp connections_of(socket) do
    HeaderDeadline.registry()
    |> Registry.select([{{:"$1", :"$2", :"$3"}, [], [{{:"$1", :"$3"}}]}])
    |> Enum.filter(fn {pid, _} -> Process.alive?(pid) and connected_to?(pid, socket) end)
  end

  # Whether the connection process `pid` serves the peer of our client `socket`.
  defp connected_to?(pid, socket) do
    {:ok, local} = :inet.sockname(socket)

    pid
    |> Process.info(:links)
    |> elem(1)
    |> Enum.any?(fn
      port when is_port(port) -> :inet.peername(port) == {:ok, local}
      _ -> false
    end)
  end

  describe "connection bound" do
    test "a connection past the bound waits until one closes", %{dir: dir} do
      Application.put_env(:malachi, :console_num_acceptors, 1)
      Application.put_env(:malachi, :console_max_connections, 1)
      port = start!(dir)

      {:ok, first} = HttpClient.connect(port)
      assert {:ok, %{status: 200}} = HttpClient.request(first, "GET", "/")

      {:ok, second} = HttpClient.connect(port)
      :ok = HttpClient.send_raw(second, HttpClient.encode("GET", "/favicon.ico"))
      assert {:error, {:timeout, ""}} = HttpClient.recv(second, "", timeout: 300)

      :gen_tcp.close(first)
      assert {:ok, %{status: 200, body: "ICO"}, ""} = HttpClient.recv(second, "", timeout: 3_000)
    end
  end

  describe "bandit_options/2" do
    test "maps the shared header limits and the read deadline" do
      Application.put_env(:malachi, :dashboard_max_header_count, 7)
      Application.put_env(:malachi, :dashboard_max_header_line_size, 700)
      Application.put_env(:malachi, :dashboard_max_header_size, 1_400)
      Application.put_env(:malachi, :dashboard_recv_timeout_ms, 1_234)

      opts = Endpoint.bandit_options(0, :absent)

      assert opts[:http_1_options] == [max_request_line_length: 700, max_header_length: 700, max_header_count: 7]
      assert opts[:thousand_island_options][:read_timeout] == 2_468
      assert opts[:thousand_island_options][:handler_module] == HeaderDeadline
      assert opts[:http_2_options] == [enabled: false]
      assert {Malachi.Console.Router, %{manifest: :absent, max_header_bytes: 1_400}} = opts[:plug]
      assert opts[:scheme] == :http
      assert opts[:startup_log] == false
      assert opts[:http_options] == [compress: false, log_protocol_errors: false]
    end

    test "splits the connection total across acceptors, rounding up" do
      assert Endpoint.bandit_options(0, :absent)[:thousand_island_options][:num_acceptors] == 10
      assert Endpoint.bandit_options(0, :absent)[:thousand_island_options][:num_connections] == 103

      Application.put_env(:malachi, :console_num_acceptors, 4)
      Application.put_env(:malachi, :console_max_connections, 10)
      assert Endpoint.bandit_options(0, :absent)[:thousand_island_options][:num_connections] == 3
    end

    test "a connection setting that is not a positive integer falls back and says so" do
      Application.put_env(:malachi, :console_num_acceptors, 0)
      Application.put_env(:malachi, :console_max_connections, "many")

      log =
        capture_log(fn ->
          opts = Endpoint.bandit_options(0, :absent)[:thousand_island_options]
          assert opts[:num_acceptors] == 10
          assert opts[:num_connections] == 103
        end)

      assert log =~ "console_num_acceptors"
      assert log =~ "console_max_connections"
    end
  end

  describe "starting" do
    test "records the port it bound and logs it", %{dir: dir} do
      name = :"console_#{System.unique_integer([:positive])}"
      log = capture_log(fn -> ConsoleFixture.start_endpoint!(name, dir) end)
      port = Endpoint.port(name)

      assert is_integer(port) and port > 0
      assert log =~ "console serving at http://localhost:#{port}"
    end

    test "a port that cannot be opened is :ignore, logged, and its supervisor carries on", %{dir: dir} do
      {:ok, taken} = :gen_tcp.listen(0, [])
      {:ok, port} = :inet.port(taken)
      on_exit(fn -> :gen_tcp.close(taken) end)
      name = :"console_#{System.unique_integer([:positive])}"

      # Under a supervisor, as in the application: the failed listener's exit reaches a process that
      # traps exits, and the child is recorded as ignored instead of failing the supervisor's start.
      log =
        capture_log(fn ->
          assert {:ok, :undefined} = start_supervised({Endpoint, {port, name: name, static_dir: dir}})
        end)

      assert log =~ "Console could not listen on port #{port}"
      assert log =~ "eaddrinuse"
      assert Endpoint.port(name) == nil

      # The supervisor is still there and still starts children.
      assert {:ok, _} = start_supervised({Agent, fn -> :ok end})
    end

    test "the application runs one endpoint on its own ephemeral port" do
      assert is_integer(Endpoint.port()) and Endpoint.port() > 0
      refute Endpoint.port() == Malachi.Dashboard.port()
      assert %{status: 503} = HttpClient.get(Endpoint.port(), "/")
    end

    test "the bundle directory defaults to priv/static/console" do
      Application.delete_env(:malachi, :console_static_dir)
      assert Assets.static_dir() == Application.app_dir(:malachi, "priv/static/console")
    end

    @tag skip: ConsoleFixture.unreadable_skip()
    test "a bundle file that cannot be read does not stop the endpoint from starting", %{dir: dir} do
      ConsoleFixture.unreadable!(dir, "favicon.ico")

      capture_log(fn -> send(self(), {:port, start!(dir)}) end)
      assert_received {:port, port}

      assert %{status: 200} = HttpClient.get(port, "/")
      assert %{status: 404} = HttpClient.get(port, "/favicon.ico")
    end

    test "the console switch decides whether there is a child at all" do
      assert Malachi.Application.console_children(true, 4042) == [
               {Registry, keys: :unique, name: HeaderDeadline.registry()},
               {Endpoint, 4042}
             ]

      assert Malachi.Application.console_children(false, 4042) == []
    end

    test "child specs are distinct per name" do
      assert Endpoint.child_spec(4042).id == {Endpoint, Endpoint}
      assert Endpoint.child_spec({0, name: :other}).id == {Endpoint, :other}
      assert Endpoint.child_spec(4042).type == :supervisor
    end
  end

  describe "sessions and rate limits" do
    test "a request carrying a valid session cookie spends no bucket and creates no session", %{dir: dir} do
      port = start!(dir)
      {:ok, token} = DashboardHelper.login("admin", "admin123")

      buckets = :ets.tab2list(:malachi_rate_limits)
      sessions = :ets.info(:malachi_sessions, :size)

      for path <- ["/", "/assets/app-3f2aB9x1.js", "/missing.png"] do
        HttpClient.get(port, path, [{"Cookie", "malachi_token=#{token}"}, {"Authorization", "Bearer #{token}"}])
      end

      assert :ets.tab2list(:malachi_rate_limits) == buckets
      assert :ets.info(:malachi_sessions, :size) == sessions
    end
  end
end
