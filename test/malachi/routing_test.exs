defmodule Malachi.RoutingTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.Advertised
  alias Malachi.Cluster.HashRing
  alias Malachi.Metadata
  alias Malachi.Routing
  alias Malachi.Wire

  @a {Malachi.LogReplication, :malachi@a}
  @b {Malachi.LogReplication, :malachi@b}

  defp apply!(state, command) do
    {state, reply} = Metadata.apply(state, command)
    assert reply == :ok or elem(reply, 0) == :ok, "#{inspect(command)}: #{inspect(reply)}"
    state
  end

  # A topic of 16 positions whose root range has an active segment led by node a.
  defp topic(name \\ "orders") do
    {state, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, name, 4})
    {apply!(state, {:register_segment, root, {root, 0}, [@a, @b], 0}), root}
  end

  defp routes!(state, topic \\ "orders") do
    {:ok, routes} = Routing.topic_routes(state, topic)
    routes
  end

  describe "topic_routes/2" do
    test "a range with an active segment routes to that segment's primary" do
      {state, _root} = topic()

      assert %{topic: "orders", keyspace_bits: 4, version: version, ranges: [range]} = routes!(state)

      assert range == %{
               range: 0,
               key_start: 0,
               key_end: 16,
               state: :active,
               segment: %{segment: 0, primary: "malachi@a"}
             }

      assert is_integer(version) and version >= 0 and version < 2 ** 64
    end

    test "after a split the parent is sealed and the children have no segment until their first append" do
      {state, root} = topic()
      state = state |> apply!({:seal_segment, {root, 0}, 10, 100, 1}) |> apply!({:split_range, root})

      assert [parent, left, right] = routes!(state).ranges
      assert parent == %{range: 0, key_start: 0, key_end: 16, state: :sealed, segment: nil}
      assert left == %{range: 1, key_start: 0, key_end: 8, state: :active, segment: nil}
      assert right == %{range: 2, key_start: 8, key_end: 16, state: :active, segment: nil}
    end

    test "the newest active segment is the one routed to" do
      {state, root} = topic()

      state =
        state
        |> apply!({:seal_segment, {root, 0}, 10, 100, 1})
        |> apply!({:register_segment, root, {root, 1}, [@b], 10})

      assert [%{segment: %{segment: 1, primary: "malachi@b"}}] = routes!(state).ranges
    end

    test "a segment whose id is not the broker's {range, seq} shape routes nowhere" do
      {state, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "orders", 4})

      assert [%{segment: nil}] =
               state |> apply!({:register_segment, root, "seg1", [@a], 0}) |> routes!() |> Map.fetch!(:ranges)
    end

    test "a segment with an empty replica set has no primary, so the range routes nowhere" do
      {state, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "orders", 4})

      assert [%{segment: nil}] =
               state |> apply!({:register_segment, root, {root, 0}, [], 0}) |> routes!() |> Map.fetch!(:ranges)
    end

    test "the version is the leading 64 bits of a SHA-256 of the wire answer at version 0" do
      {state, _root} = topic()
      routes = routes!(state)
      <<expected::64, _rest::binary>> = :crypto.hash(:sha256, Wire.encode_topic_routes_resp(%{routes | version: 0}))

      assert routes.version == expected

      state = Routing.cluster_state([{:malachi@a, :alive, %{}}], [0], false)
      <<expected::64, _rest::binary>> = :crypto.hash(:sha256, Wire.encode_cluster_state_resp(%{state | version: 0}))
      assert state.version == expected
    end

    test "the version is the same for the same routes and changes when any part of them does" do
      {state, root} = topic()
      version = routes!(state).version

      assert routes!(state).version == version
      # same routes in another topic's company: unchanged
      {other, _} = Metadata.apply(state, {:create_topic, "other", 2})
      assert routes!(other).version == version

      changed = [
        apply!(state, {:set_segment_replicas, {root, 0}, [@b, @a]}),
        state |> apply!({:seal_segment, {root, 0}, 10, 100, 1}),
        state
        |> apply!({:seal_segment, {root, 0}, 10, 100, 1})
        |> apply!({:register_segment, root, {root, 1}, [@a], 10})
      ]

      versions = Enum.map(changed, &routes!(&1).version)
      assert Enum.uniq([version | versions]) == [version | versions]
    end

    test "a topic the vnode does not hold is no_such_topic" do
      {state, _root} = topic()
      assert Routing.topic_routes(state, "missing") == {:error, :no_such_topic}
    end

    test "keyspace bits come back from the stored size at both ends of the range" do
      for bits <- [1, 32] do
        {state, {:ok, _root}} = Metadata.apply(Metadata.new(), {:create_topic, "t#{bits}", bits})
        assert %{keyspace_bits: ^bits, ranges: [%{key_end: key_end}]} = routes!(state, "t#{bits}")
        assert key_end == 2 ** bits
      end
    end
  end

  describe "tokens/1" do
    test "an unsharded control plane is one vnode at token 0, a ring is its tokens, an unread ring is unavailable" do
      assert Routing.tokens({:ok, []}) == {:ok, [0]}
      assert Routing.tokens({:ok, [{:vn_0, 0, [:a]}, {:vn_1, 2_147_483_648, [:b]}]}) == {:ok, [0, 2_147_483_648]}
      assert Routing.tokens(:error) == {:error, :unavailable}
    end
  end

  describe "metadata_source/3" do
    test "a sharded control plane reads the vnode that owns the topic" do
      {:ok, ring} = HashRing.add_vnode(HashRing.new(), :vn_0, 0)
      topology = %{ring: ring, servers: %{vn_0: {:vn_0, :malachi@a}}}

      assert Routing.metadata_source("orders", topology, :malachi_log) == {:ok, {:vn_0, :malachi@a}}
    end

    test "a vnode missing from the server map, or an empty ring, is unavailable" do
      {:ok, ring} = HashRing.add_vnode(HashRing.new(), :vn_0, 0)
      assert Routing.metadata_source("orders", %{ring: ring, servers: %{}}, nil) == {:error, :unavailable}

      assert Routing.metadata_source("orders", %{ring: HashRing.new(), servers: %{}}, nil) ==
               {:error, :unavailable}
    end

    test "a single control plane is read through this node's member, and no cluster means the broker's memory" do
      assert Routing.metadata_source("orders", nil, :malachi_log) == {:ok, {:malachi_log, node()}}
      assert Routing.metadata_source("orders", nil, nil) == :in_memory
    end
  end

  describe "cluster_state/3" do
    defp member(name, status, address), do: {name, status, if(address, do: Advertised.put(%{}, address), else: %{})}

    test "brokers are ordered by id, with the address each advertises and its status" do
      members = [
        member(:malachi@b, :suspect, %{host: "b.svc", port: 4040}),
        member(:malachi@a, :alive, %{host: "a.svc", port: 5050})
      ]

      assert %{brokers: brokers, vnodes: [7, 100], streams_enabled: false} =
               Routing.cluster_state(members, [100, 7], false)

      assert brokers == [
               %{id: "malachi@a", host: "a.svc", port: 5050, status: :alive},
               %{id: "malachi@b", host: "b.svc", port: 4040, status: :suspect}
             ]
    end

    test "a member that advertises no address has a nil host and port 0" do
      assert %{brokers: [%{host: nil, port: 0, status: :dead}]} =
               Routing.cluster_state([member(:malachi@old, :dead, nil)], [0], true)
    end

    test "the version is the same for the same state and changes when a broker, a vnode or the flag does" do
      members = [member(:malachi@a, :alive, %{host: "a.svc", port: 4040})]
      %{version: version} = Routing.cluster_state(members, [0], false)

      assert Routing.cluster_state(Enum.reverse(members), [0], false).version == version

      others = [
        Routing.cluster_state([member(:malachi@a, :suspect, %{host: "a.svc", port: 4040})], [0], false),
        Routing.cluster_state([member(:malachi@a, :alive, %{host: "a.svc", port: 4041})], [0], false),
        Routing.cluster_state(members, [0, 1], false),
        Routing.cluster_state(members, [0], true)
      ]

      versions = Enum.map(others, & &1.version)
      assert Enum.uniq([version | versions]) == [version | versions]
    end
  end
end
