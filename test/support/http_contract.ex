defmodule Malachi.Test.HttpContract do
  @moduledoc """
  The transport guarantees every HTTP listener on the node keeps, written once and run against each.

  `use Malachi.Test.HttpContract, target: :dashboard` runs them against the legacy dashboard
  (`Malachi.Dashboard`, the application's instance), and `target: :console` against the console
  endpoint (`Malachi.Console.Endpoint`, a fresh instance over a fixture bundle). Where the two differ
  by design, each side's case asserts its own behavior, so a difference cannot appear or disappear
  unnoticed.

  This suite is the parity gate for removing the dashboard: every route that moves to the console
  adds its case here, and the dashboard goes when the console passes on its own.
  """

  import ExUnit.Assertions, only: [assert: 1, assert: 2]

  alias Malachi.Dashboard
  alias Malachi.Test.ConsoleFixture
  alias Malachi.Test.HttpClient

  defmacro __using__(opts) do
    target = Keyword.fetch!(opts, :target)

    quote do
      # Not async: the slow request case lowers the read deadline in application env.
      use ExUnit.Case, async: false

      alias Malachi.Test.HttpClient
      alias Malachi.Test.HttpContract

      @target unquote(target)

      setup do
        original = Application.fetch_env(:malachi, :dashboard_recv_timeout_ms)

        on_exit(fn ->
          case original do
            {:ok, value} -> Application.put_env(:malachi, :dashboard_recv_timeout_ms, value)
            :error -> Application.delete_env(:malachi, :dashboard_recv_timeout_ms)
          end
        end)

        HttpContract.target(@target)
      end

      test "a request within every header limit is served", %{port: port, path: path} do
        assert %{status: 200} = HttpClient.get(port, path, HttpContract.header_lines(49))
      end

      test "one header past the count is 431", %{port: port, path: path} do
        # 50 extra lines plus Host is 51.
        response = HttpClient.get(port, path, HttpContract.header_lines(50))
        assert response.status == 431

        case @target do
          # The dashboard counts the lines itself and answers with its problem body.
          :dashboard ->
            assert Jason.decode!(response.body) == %{"type" => "errors.http.header_fields_too_large", "status" => 431}

          # On the console Bandit refuses while parsing, before any plug runs: a bare 431, no body
          # and none of the security headers. Bandit offers no hook for that answer.
          :console ->
            HttpContract.assert_bare(response)
        end
      end

      test "headers past the total byte budget are 431 even when each line fits", %{port: port, path: path} do
        # Five values of 7,000 bytes: every line is under 10,000, the sum is over 32,768.
        headers = for i <- 1..5, do: {"x-big#{i}", String.duplicate("a", 7_000)}
        {:ok, socket} = HttpClient.connect(port)
        :ok = HttpClient.send_raw(socket, HttpClient.encode("GET", path, headers))

        assert {:ok, %{status: 431, body: body}, _} = HttpClient.recv(socket)
        assert Jason.decode!(body) == %{"type" => "errors.http.header_fields_too_large", "status" => 431}
      end

      test "Content-Length is exactly the bytes of the body", %{port: port, path: path} do
        response = HttpClient.get(port, path)
        assert String.to_integer(HttpClient.header(response, "content-length")) == byte_size(response.body)
      end

      test "the base security headers are on the response", %{port: port, path: path} do
        response = HttpClient.get(port, path)

        for {name, value} <- [
              {"x-content-type-options", "nosniff"},
              {"x-frame-options", "DENY"},
              {"referrer-policy", "no-referrer"}
            ] do
          assert HttpClient.header(response, name) == value, name
        end

        assert HttpClient.header(response, "content-security-policy")
        assert HttpClient.header(response, "permissions-policy") =~ "camera=()"
      end

      test "a request whose headers never finish is dropped at the read deadline", ctx do
        socket = HttpContract.connect_with_deadline(ctx, 200)
        :ok = HttpClient.send_raw(socket, "GET #{ctx.path} HTTP/1.1\r\nHost: localhost\r\n")

        # Both close without a word.
        HttpContract.assert_closed_unanswered(socket)
      end

      test "headers trickled one byte at a time are cut off at the read deadline", ctx do
        socket = HttpContract.connect_with_deadline(ctx, 300)
        :ok = HttpClient.send_raw(socket, "GET #{ctx.path} HTTP/1.1\r\nHost: localhost\r\nX-Slow: ")

        HttpContract.assert_trickle_cut_off(
          socket,
          "still open after 3 s of trickled headers against a 300 ms deadline"
        )
      end

      test "the next request on a kept alive connection gets the deadline again", ctx do
        socket = HttpContract.connect_with_deadline(ctx, 300)
        assert {:ok, %{status: 200}} = HttpClient.request(socket, "GET", ctx.path)

        # The dashboard has already closed; the console must cut the second request's headers off.
        _ = HttpClient.send_raw(socket, "GET #{ctx.path} HTTP/1.1\r\nHost: localhost\r\nX-Slow: ")

        HttpContract.assert_trickle_cut_off(socket, "the second request's headers trickled for 3 s with no deadline")
      end

      test "a partial request pipelined behind a full one gets the deadline too", ctx do
        socket = HttpContract.connect_with_deadline(ctx, 300)

        :ok =
          HttpClient.send_raw(socket, [
            HttpClient.encode("GET", ctx.path),
            "GET #{ctx.path} HTTP/1.1\r\nHost: localhost\r\nX-Slow: "
          ])

        assert {:ok, %{status: 200}, _rest} = HttpClient.recv(socket)

        HttpContract.assert_trickle_cut_off(socket, "the pipelined request's headers trickled for 3 s with no deadline")
      end

      test "a request line past the line limit", %{port: port} do
        {:ok, socket} = HttpClient.connect(port)
        :ok = HttpClient.send_raw(socket, "GET /#{String.duplicate("a", 10_001)} HTTP/1.1\r\nHost: localhost\r\n\r\n")

        case @target do
          # Bandit: 414, no body, none of the security headers.
          :console ->
            assert {:ok, %{status: 414} = response, _} = HttpClient.recv(socket)
            HttpContract.assert_bare(response)

          # The socket driver refuses the line and closes; nothing is sent.
          :dashboard ->
            HttpContract.assert_closed_unanswered(socket)
        end
      end

      test "a header line past the line limit", %{port: port, path: path} do
        {:ok, socket} = HttpClient.connect(port)
        :ok = HttpClient.send_raw(socket, HttpClient.encode("GET", path, [{"x-long", String.duplicate("a", 10_001)}]))

        case @target do
          # Bandit: 431, no body, none of the security headers.
          :console ->
            assert {:ok, %{status: 431} = response, _} = HttpClient.recv(socket)
            HttpContract.assert_bare(response)

          # The socket driver refuses the line and closes; nothing is sent.
          :dashboard ->
            HttpContract.assert_closed_unanswered(socket)
        end
      end
    end
  end

  @doc false
  # Bandit's answer to a request it refuses while parsing: no body, and none of the security headers,
  # since no plug ran.
  def assert_bare(response) do
    assert(response.body == "")
    assert(HttpClient.header(response, "x-frame-options") == nil)
  end

  @doc false
  # The dashboard's answer to a line its socket driver refuses, or a deadline it enforces: none.
  def assert_closed_unanswered(socket) do
    assert(:gen_tcp.recv(socket, 0, 2_000) == {:error, :closed})
  end

  @doc false
  # A client connection to a fresh instance of the target whose read deadline is `deadline_ms`.
  def connect_with_deadline(ctx, deadline_ms) do
    Application.put_env(:malachi, :dashboard_recv_timeout_ms, deadline_ms)
    {:ok, socket} = HttpClient.connect(ctx.restart.())
    socket
  end

  @doc false
  # Headers trickled into `socket` are cut off well inside 1.5 s, against deadlines of a few hundred ms.
  def assert_trickle_cut_off(socket, message) do
    closed_after = trickle_until_closed(socket)
    assert(closed_after, message)
    assert(closed_after < 1_500)
  end

  @doc false
  def header_lines(n), do: for(i <- 1..n, do: {"x-h#{i}", "v"})

  @doc false
  # One byte every 100 ms until the peer closes: each read is well inside the deadline, the header
  # block never is. Returns how long it took, or nil if still open after 3 s.
  def trickle_until_closed(socket) do
    started = System.monotonic_time(:millisecond)

    Enum.find_value(1..30, fn _ ->
      Process.sleep(100)

      case HttpClient.send_raw(socket, "a") do
        :ok -> if HttpClient.closed?(socket, 0), do: System.monotonic_time(:millisecond) - started
        {:error, _} -> System.monotonic_time(:millisecond) - started
      end
    end)
  end

  @doc false
  def target(:dashboard) do
    %{port: Dashboard.port(), path: "/health", restart: &Dashboard.port/0}
  end

  def target(:console) do
    dir = ConsoleFixture.bundle!()

    start = fn ->
      ConsoleFixture.start_endpoint!(:"contract_#{System.unique_integer([:positive])}", dir)
    end

    %{port: start.(), path: "/", restart: start}
  end
end
