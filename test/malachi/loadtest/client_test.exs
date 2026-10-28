defmodule Malachi.Loadtest.ClientTest do
  # The client the Docker release checks run through `bin/malachi rpc`, against the test server: what it
  # writes it reads back whole, across a split too, and a group member that commits what it read resumes
  # past it.
  use ExUnit.Case, async: false

  alias Malachi.BrokerServer
  alias Malachi.DataPlaneRouter
  alias Malachi.Loadtest.Client
  alias Malachi.Log.Record
  alias Malachi.TCPAcceptorPool
  alias Malachi.Test.StreamsOn
  alias Malachi.Test.StreamStub
  alias Malachi.Wire

  setup do
    :ok = StreamsOn.setup(%{})
    {:ok, client} = Client.connect(opts("admin", "admin123"))
    on_exit(fn -> Client.close(client) end)
    %{client: client, topic: "client_#{System.unique_integer([:positive])}"}
  end

  defp opts(user, pass), do: [host: "127.0.0.1", port: TCPAcceptorPool.port(), user: user, pass: pass]
  defp records(values), do: Enum.map(values, &Record.new(&1, key: &1))

  # The ports (sockets) this process owns now.
  defp own_ports, do: Enum.filter(Port.list(), &(Port.info(&1, :connected) == {:connected, self()}))

  test "credentials the node refuses are the connect's error, and its connection is closed" do
    before = own_ports()
    assert {:error, _reason} = Client.connect(opts("admin", "not-the-password"))
    assert own_ports() == before
  end

  test "a topic is created once: a second create is the node's refusal", %{client: client, topic: topic} do
    assert Client.create_topic(client, topic, 4) == :ok
    assert Client.create_topic(client, topic, 4) == {:error, "already_exists"}
  end

  test "what is produced is acknowledged, and read back whole", %{client: client, topic: topic} do
    :ok = Client.create_topic(client, topic, 4)
    values = for i <- 1..50, do: "v#{i}"
    assert Client.produce(client, topic, records(values), 5_000) == {:ok, 50}

    assert {:ok, %{values: read}} = Client.read(client, topic)
    assert Enum.sort(read) == Enum.sort(values)
  end

  test "a topic split while it holds records is read back whole, each record once", %{client: client, topic: topic} do
    :ok = Client.create_topic(client, topic, 4)
    before = for i <- 1..20, do: "b#{i}"
    {:ok, 20} = Client.produce(client, topic, records(before), 5_000)
    {:ok, _left, _right} = BrokerServer.split_range(DataPlaneRouter.shard_for(topic), {topic, 0})
    later = for i <- 1..20, do: "a#{i}"
    {:ok, 20} = Client.produce(client, topic, records(later), 5_000)

    {:ok, %{values: read, positions: positions}} = Client.read(client, topic)
    assert Enum.sort(read) == Enum.sort(before ++ later)
    assert map_size(positions) == 2
  end

  test "an append the node refuses is the produce's error, by reason", %{client: client} do
    assert {:error, %{acked: 0, failed: failed}} = Client.produce(client, "never_created", records(["x"]), 2_000)
    assert map_size(failed) == 1
  end

  test "a member that commits what it read resumes past it, and leaves", %{client: client, topic: topic} do
    :ok = Client.create_topic(client, topic, 4)
    {:ok, 5} = Client.produce(client, topic, records(for(i <- 1..5, do: "g#{i}")), 5_000)

    assert {:ok, %{generation: generation, ranges: [0]}} = Client.join(client, topic, "rg", "m1")
    {:ok, %{values: first, positions: positions}} = Client.read(client, topic, start_from: {:committed, "rg"})
    assert length(first) == 5
    assert Client.commit(client, topic, "rg", "m1", generation, positions) == :ok

    assert {:ok, %{values: []}} = Client.read(client, topic, start_from: {:committed, "rg"})
    assert Client.leave(client, topic, "rg", "m1") == :ok
  end

  test "two members of a group each read their own share, commit it, and between them read every record once",
       %{client: client, topic: topic} do
    :ok = Client.create_topic(client, topic, 4)
    values = for i <- 1..30, do: "m#{i}"
    {:ok, 30} = Client.produce(client, topic, records(values), 5_000)
    {:ok, _left, _right} = BrokerServer.split_range(DataPlaneRouter.shard_for(topic), {topic, 0})

    # a second member whose name hashes one child to each, so the two split the topic
    {:ok, %{ranges: [_, _]}} = Client.join(client, topic, "pair", "a")
    second = Enum.find(for(i <- 1..100, do: "b#{i}"), &one_each?(client, topic, &1))
    {:ok, %{generation: generation, ranges: [theirs]}} = Client.join(client, topic, "pair", second)
    {:ok, %{generation: ^generation, ranges: [mine]}} = Client.join(client, topic, "pair", "a")

    reads =
      for {member, ranges} <- [{"a", [mine]}, {second, [theirs]}] do
        {:ok, %{values: read, positions: positions}} =
          Client.read(client, topic, start_from: {:committed, "pair"}, ranges: ranges)

        assert Map.keys(positions) == ranges, "a member reads, and commits, its own share only"
        assert Client.commit(client, topic, "pair", member, generation, positions) == :ok
        read
      end

    assert Enum.all?(reads, &(&1 != []))
    assert reads |> List.flatten() |> Enum.sort() == Enum.sort(values)
  end

  test "a member the group handed no range reads nothing", %{client: client, topic: topic} do
    :ok = Client.create_topic(client, topic, 4)
    {:ok, 1} = Client.produce(client, topic, records(["x"]), 5_000)
    assert Client.read(client, topic, ranges: []) == {:ok, %{values: [], positions: %{}}}
  end

  test "a member whose assigned range a split retired is told its assignment is stale, not read nothing",
       %{client: client, topic: topic} do
    :ok = Client.create_topic(client, topic, 4)
    {:ok, 5} = Client.produce(client, topic, records(for(i <- 1..5, do: "r#{i}")), 5_000)
    {:ok, _left, _right} = BrokerServer.split_range(DataPlaneRouter.shard_for(topic), {topic, 0})

    assert Client.read(client, topic, ranges: [0]) == {:error, :stale_assignment}
  end

  test "a commit the group refuses is the commit's error", %{client: client, topic: topic} do
    :ok = Client.create_topic(client, topic, 4)
    assert {:error, "unknown_member"} = Client.commit(client, topic, "rg", "ghost", 0, %{0 => {0, 0}})
  end

  test "a read the node refuses is the read's error", %{topic: topic, client: admin} do
    :ok = Client.create_topic(admin, topic, 4)
    user = "client_noread_#{System.unique_integer([:positive])}"
    :ok = Malachi.Auth.add_user(user, "secret-pass-123", [:produce])
    on_exit(fn -> Malachi.Auth.remove_user(user) end)

    {:ok, client} = Client.connect(opts(user, "secret-pass-123"))
    on_exit(fn -> Client.close(client) end)
    assert {:error, "permission_denied"} = Client.read(client, topic)
  end

  test "a read that cannot finish in its time is a timeout", %{client: client, topic: topic} do
    :ok = Client.create_topic(client, topic, 4)
    {:ok, 1} = Client.produce(client, topic, records(["t"]), 5_000)
    assert Client.read(client, topic, timeout_ms: 0) == {:error, :timeout}
  end

  # Whether `name`, joining the topic's group "probe" beside "a", is handed one of its two ranges.
  defp one_each?(client, topic, name) do
    {:ok, _} = Client.join(client, topic, "probe_#{name}", "a")
    {:ok, %{ranges: ranges}} = Client.join(client, topic, "probe_#{name}", name)
    length(ranges) == 1
  end

  describe "against a broker a test steers" do
    defp stub_client(stub) do
      {:ok, client} = Client.connect(host: "127.0.0.1", port: stub.port, user: "u", pass: "p")
      on_exit(fn -> Client.close(client) end)
      client
    end

    # The answer to `api_key` put back to the stub's own after `ms`, while the client keeps asking.
    defp restore_after(stub, api_key, ms) do
      spawn(fn ->
        Process.sleep(ms)
        StreamStub.set_override(stub, api_key, :default)
      end)
    end

    defp steered(overrides) do
      stub = StreamStub.start(overrides)
      on_exit(fn -> StreamStub.stop(stub) end)
      stub
    end

    test "a node that answers no cluster state, or drops the connection there, is the connect's error" do
      refusing = steered(%{Wire.cluster_state_key() => {:error, "unknown_api_key"}})
      assert {:error, _reason} = Client.connect(host: "127.0.0.1", port: refusing.port, user: "u", pass: "p")

      closing = steered(%{Wire.cluster_state_key() => :close})
      before = own_ports()
      assert {:error, _reason} = Client.connect(host: "127.0.0.1", port: closing.port, user: "u", pass: "p")
      assert own_ports() == before, "the connection a failed cluster state read used is closed"
    end

    test "a read waits out a dropped connection, a redirect, routes held back and a broker that takes no connection" do
      for {api_key, failing, before} <- [
            {Wire.fetch_range_key(), :close, fn _stub -> :ok end},
            {Wire.fetch_range_key(), {:error, "stale_routes"}, fn _stub -> :ok end},
            {Wire.topic_routes_key(), {:error, "metadata_unavailable"}, fn _stub -> :ok end}
          ] do
        stub = steered(%{})
        client = stub_client(stub)
        before.(stub)
        StreamStub.set_override(stub, api_key, failing)
        restore_after(stub, api_key, 300)
        assert {:ok, %{values: []}} = Client.read(client, "t", timeout_ms: 5_000), inspect(failing)
        assert StreamStub.fetched(stub) >= 1
      end
    end

    test "a read waits out a primary it cannot connect to, the routes read again" do
      stub = steered(%{})
      client = stub_client(stub)
      # the primary's address is one nobody listens on, until the stub advertises its own again
      {:ok, dead} = :gen_tcp.listen(0, [])
      {:ok, dead_port} = :inet.port(dead)
      :gen_tcp.close(dead)
      StreamStub.set_advertised_port(stub, dead_port)
      StreamStub.add_broker(stub, "other@127.0.0.1", dead_port)

      spawn(fn ->
        Process.sleep(300)
        StreamStub.set_advertised_port(stub, stub.port)
      end)

      assert {:ok, %{values: []}} = Client.read(client, "t", timeout_ms: 5_000)
    end

    test "a group call whose connection drops is that call's error" do
      stub = steered(%{Wire.join_group_key() => :close})
      client = stub_client(stub)
      assert {:error, _reason} = Client.join(client, "t", "g", "m")
    end
  end
end
