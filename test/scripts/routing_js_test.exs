defmodule RoutingJsTest do
  # The Node routing client (scripts/lib/router.js, links.js) and the stream plumbing of client.js: its own
  # cases, the broker it opens a range at against the one Malachi.Loadtest.Router picks, and a live run
  # against the test server, where a producer stream's pushes and refusals must reach the stream.
  use ExUnit.Case, async: false

  alias Malachi.Loadtest.Router
  alias Malachi.TCPAcceptorPool
  alias Malachi.Test.StreamsOn
  alias Malachi.Test.TCPHelper
  alias Malachi.Wire

  @selftest Path.expand("../../scripts/lib/routing.selftest.js", __DIR__)

  setup_all do
    # A missing runtime fails loudly instead of skipping: a skipped client test reads as a passing one.
    %{node: System.find_executable("node") || flunk("node is required; install Node 22.15+ (zstd in zlib)")}
  end

  test "the routing cases that need no server", ctx do
    assert {output, 0} = System.cmd(ctx.node, [@selftest], stderr_to_stdout: true)
    assert output =~ "passed 18 routing cases"
  end

  test "a range opens at the broker the Elixir router picks, for every cluster and range", ctx do
    statuses = [:alive, :alive, :suspect, :dead]

    clusters =
      for size <- 1..6, seed <- 1..4 do
        for i <- 1..size do
          status = Enum.at(statuses, rem(i * seed, length(statuses)))
          %{id: "n#{i}@host#{rem(i * 7 + seed, 10)}", host: "10.0.#{seed}.#{i}", port: 4000 + i, status: status}
        end
      end

    ranges = for n <- 0..15, do: %{range: n, key_start: n, key_end: n + 1, state: :active, segment: nil}
    routes = for topic <- ["orders", "t", "a/b"], do: %{topic: topic, version: 3, keyspace_bits: 4, ranges: ranges}
    queries = for %{topic: topic} <- routes, n <- 0..15, do: [topic, n]

    for brokers <- clusters do
      state = %{version: 1, streams_enabled: true, brokers: brokers, vnodes: []}

      router =
        Enum.reduce(routes, Router.put_cluster_state(Router.new({"boot", 4040}), state), &Router.put_routes(&2, &1))

      expected = for [topic, n] <- queries, do: elixir_answer(Router.address_of(router, topic, n))

      input = %{
        bootstrap: %{host: "boot", port: 4040},
        cluster_state: json(state),
        routes: json(routes),
        queries: queries
      }

      assert {answers, 0} = System.cmd(ctx.node, [@selftest, "--addresses", Jason.encode!(input)])
      assert Jason.decode!(answers) == expected, "cluster #{inspect(brokers)}"
    end
  end

  test "against the server: routes, one connection per broker, and a stream's pushes and refusals", ctx do
    :ok = StreamsOn.setup(%{})
    topic = "routing_js_#{System.unique_integer([:positive])}"
    {:ok, socket} = TCPHelper.connect()
    {:ok, _token} = TCPHelper.authenticate_wire(socket, "admin", "admin123")
    {0, _} = TCPHelper.request(socket, Wire.create_topic_key(), 2, Wire.encode_create_topic_req(topic, 4))
    :gen_tcp.close(socket)

    env = [
      {"MALACHI_HOST", "127.0.0.1"},
      {"MALACHI_PORT", Integer.to_string(TCPAcceptorPool.port())},
      {"MALACHI_USER", "admin"},
      {"MALACHI_PASS", "admin123"}
    ]

    assert {output, 0} = System.cmd(ctx.node, [@selftest, "--live", topic], env: env, stderr_to_stdout: true)
    seen = Jason.decode!(output)

    assert seen["routed"] == %{"ok" => true}
    assert seen["ranges"] == [0]
    assert seen["connected"] == %{"ok" => true}
    assert seen["sharedConnection"]

    # the append acknowledged, then the out of order one named at its sequence (ending the stream) ...
    assert [%{"kind" => "append_ack", "acked_sequence" => 1, "errors" => []}, gap] = seen["pushes"]
    assert %{"kind" => "append_ack", "errors" => [%{"sequence" => 5, "reason" => "sequence_gap"}]} = gap
    # ... and the append after it refused by an error frame under the stream's correlation id
    assert seen["refusals"] == ["unknown_stream"]
    # a missing topic backs off: the second ask answers the refusal without reading the routes again
    assert seen["missing"] == [%{"error" => "no_such_topic"}, %{"error" => "no_such_topic"}]
    assert seen["missingRefreshes"] == 1
    # a metadata connection that went away is replaced
    assert seen["afterControlLost"] == %{"ok" => true}
    assert seen["stats"]["refreshes"] >= 1
    assert seen["stats"]["connect_failures"] == 0
  end

  defp elixir_answer({:ok, {host, port}}), do: %{"address" => %{"host" => host, "port" => port}}
  defp elixir_answer({:error, reason}), do: %{"error" => Atom.to_string(reason)}

  # The decoded answers as JSON, as the Node codecs decode them: atoms become strings.
  defp json(term), do: term |> Jason.encode!() |> Jason.decode!()
end
