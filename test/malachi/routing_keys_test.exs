defmodule Malachi.RoutingKeysTest do
  # The routing keys through the protocol boundary (`Malachi.TCPProtocol.process_frame/4`), against the
  # running application: `cluster_state` (24) always answers, and every key from `topic_routes` (25) to
  # `commit_offsets` (34) waits for the `producer_streams` cluster flag.
  #
  # async: false. The flag is the application's own cache (`Malachi.Test.ClusterFlagsPause` takes it over),
  # and the topic routing reads the topology the consumer-group router publishes, which a sharded broker
  # started by another test can leave behind: it is cleared for each test and put back afterwards.
  use ExUnit.Case, async: false

  alias Malachi.Cluster.ClusterFlagsCache
  alias Malachi.Log.Record
  alias Malachi.Routing
  alias Malachi.TCPAcceptorPool
  alias Malachi.TCPProtocol
  alias Malachi.Test.AppChildren
  alias Malachi.Test.ClusterFlagsPause
  alias Malachi.Test.EchoTransport
  alias Malachi.Wire

  @admin %{username: "routing_keys_admin", permissions: [:produce, :consume, :admin]}
  # mirrors Malachi.Consumer.CoordinatorRouter's private @topology_key
  @router_topology {Malachi.Consumer.CoordinatorRouter, :topology}

  setup do
    :ok = ClusterFlagsPause.pause()
    ClusterFlagsCache.put([])

    topology = :persistent_term.get(@router_topology, nil)
    :persistent_term.erase(@router_topology)
    acl_strict = Application.get_env(:malachi, :acl_strict)

    on_exit(fn ->
      if topology,
        do: :persistent_term.put(@router_topology, topology),
        else: :persistent_term.erase(@router_topology)

      if acl_strict == nil,
        do: Application.delete_env(:malachi, :acl_strict),
        else: Application.put_env(:malachi, :acl_strict, acl_strict)
    end)

    Application.put_env(:malachi, :acl_strict, false)
    :ok
  end

  defp streams_on, do: ClusterFlagsCache.put([Routing.flag()])

  defp process(api_key, payload, session \\ @admin, wait \\ 2_000) do
    {:ok, body, <<>>} = api_key |> Wire.encode_request(7, payload) |> Wire.decode_frame()
    :ok = TCPProtocol.process_frame(self(), body, session, EchoTransport)

    receive do
      {:frame, frame} ->
        {:ok, frame_body, <<>>} = Wire.decode_frame(frame)
        {7, code, response} = Wire.decode_response(frame_body)
        if code == Wire.ok_code(), do: {:ok, response}, else: {:error, Wire.decode_error_reason(response)}
    after
      wait -> flunk("no response frame")
    end
  end

  defp new_topic do
    topic = "routing_#{System.unique_integer([:positive])}"
    assert {:ok, _} = process(Wire.create_topic_key(), Wire.encode_create_topic_req(topic, 6))
    topic
  end

  defp routes(topic, session \\ @admin) do
    with {:ok, payload} <- process(Wire.topic_routes_key(), Wire.encode_topic_routes_req(topic), session),
         do: {:ok, Wire.decode_topic_routes_resp(payload)}
  end

  describe "cluster_state" do
    test "answers with the flag off, naming this node at the address its listener took" do
      assert {:ok, payload} = process(Wire.cluster_state_key(), Wire.encode_cluster_state_req())
      state = Wire.decode_cluster_state_resp(payload)

      assert state.streams_enabled == false
      assert state.vnodes != []
      me = Enum.find(state.brokers, &(&1.id == Atom.to_string(node())))
      assert %{status: :alive, port: port, host: host} = me
      assert port == TCPAcceptorPool.port()
      assert is_binary(host)
      assert {:ok, ^state} = Routing.read_cluster_state()
    end

    test "says when the stream keys are on, and its version moves with that" do
      {:ok, off} = process(Wire.cluster_state_key(), <<>>)
      streams_on()
      {:ok, on} = process(Wire.cluster_state_key(), <<>>)

      assert Wire.decode_cluster_state_resp(on).streams_enabled
      refute Wire.decode_cluster_state_resp(on).version == Wire.decode_cluster_state_resp(off).version
    end

    test "a membership server that is not running (between a crash and its restart) makes the answer unavailable" do
      # This node has a control plane, so no membership server is a gap, not a node that is its own cluster.
      :ok = AppChildren.suspend(Malachi.LogMembership)

      assert process(Wire.cluster_state_key(), <<>>) == {:error, "unavailable"}
    end

    test "a membership server that does not answer in time makes the answer unavailable, not this node alone" do
      pid = Process.whereis(Malachi.LogMembership)
      :ok = :sys.suspend(pid)
      on_exit(fn -> :sys.resume(pid) end)

      assert process(Wire.cluster_state_key(), <<>>, @admin, 5_000) == {:error, "unavailable"}
      :sys.resume(pid)
    end

    test "a payload with anything in it is malformed" do
      assert process(Wire.cluster_state_key(), <<0>>) == {:error, "malformed_request"}
    end
  end

  describe "cluster_state with no control plane (a single node measured in memory)" do
    setup do
      previous = Application.fetch_env(:malachi, :log_cluster)
      Application.put_env(:malachi, :log_cluster, nil)

      on_exit(fn ->
        case previous do
          {:ok, value} -> Application.put_env(:malachi, :log_cluster, value)
          :error -> Application.delete_env(:malachi, :log_cluster)
        end
      end)
    end

    test "the node makes no peers of MALACHI_LOG_NODES, so it needs no address to advertise" do
      for key <- [:log_nodes, :advertised_host] do
        previous = Application.fetch_env(:malachi, key)

        on_exit(fn ->
          case previous do
            {:ok, value} -> Application.put_env(:malachi, key, value)
            :error -> Application.delete_env(:malachi, key)
          end
        end)
      end

      Application.put_env(:malachi, :log_nodes, [node(), :other@nowhere])
      Application.delete_env(:malachi, :advertised_host)

      assert {:ok, %{host: host}} = Malachi.Application.advertised_address()
      assert is_binary(host)
    end

    test "the node is its own whole cluster: itself and one vnode at token 0" do
      assert {:ok, payload} = process(Wire.cluster_state_key(), <<>>)
      assert %{brokers: [%{id: id, status: :alive}], vnodes: [0]} = Wire.decode_cluster_state_resp(payload)
      assert id == Atom.to_string(node())
    end
  end

  describe "with producer_streams off" do
    test "every key from topic_routes to commit_offsets is unsupported" do
      for key <- Wire.topic_routes_key()..Wire.commit_offsets_key() do
        assert process(key, <<>>) == {:error, "unsupported"}, "key #{key}"
      end
    end
  end

  describe "topic_routes with producer_streams on" do
    setup do
      streams_on()
      :ok
    end

    test "a new topic is one range over its whole keyspace with no segment until its first record" do
      topic = new_topic()

      assert {:ok, %{topic: ^topic, keyspace_bits: bits, ranges: [range]} = before} = routes(topic)
      assert range == %{range: 0, key_start: 0, key_end: 2 ** bits, state: :active, segment: nil}

      payload = topic |> Wire.encode_produce_req([Record.new("v", key: "k")]) |> IO.iodata_to_binary()
      assert {:ok, _count} = process(Wire.produce_key(), payload)

      assert {:ok, %{ranges: [%{segment: %{segment: _seq, primary: primary}}]} = after_produce} = routes(topic)

      assert primary == Atom.to_string(node())
      refute after_produce.version == before.version
      assert {:ok, ^after_produce} = Routing.read_topic_routes(topic)
    end

    test "a topic that does not exist is no_such_topic" do
      assert routes("never_created_#{System.unique_integer([:positive])}") == {:error, "no_such_topic"}
    end

    test "a producer or a consumer may read a topic's routes, a session with neither may not" do
      topic = new_topic()

      for permissions <- [[:produce], [:consume]] do
        assert {:ok, _routes} = routes(topic, %{username: "routing_keys_user", permissions: permissions})
      end

      assert routes(topic, %{username: "routing_keys_nobody", permissions: []}) == {:error, "permission_denied"}
    end

    test "under strict ACLs a per-topic grant is what lets a user read the routes" do
      topic = new_topic()
      user = "routing_keys_acl_#{System.unique_integer([:positive])}"
      session = %{username: user, permissions: [:consume]}
      Application.put_env(:malachi, :acl_strict, true)

      assert routes(topic, session) == {:error, "permission_denied"}
      :ok = Malachi.Auth.grant_acl(user, :consume, topic)
      assert {:ok, _routes} = routes(topic, session)
    end

    test "the stream keys no server answers yet are still unknown" do
      for key <- Wire.open_consume_key()..Wire.commit_offsets_key() do
        assert process(key, <<>>) == {:error, "unknown_api_key"}, "key #{key}"
      end
    end
  end
end
