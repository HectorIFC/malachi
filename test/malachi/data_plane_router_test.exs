defmodule Malachi.DataPlaneRouterTest do
  # async: false because the routing tests toggle the shared :data_shards app env.
  use ExUnit.Case, async: false

  import Malachi.Test.TeardownHelper

  alias Malachi.BrokerServer
  alias Malachi.DataPlaneRouter
  alias Malachi.Log.Record

  setup do
    original = Application.get_env(:malachi, :data_shards)

    on_exit(fn ->
      case original do
        nil -> Application.delete_env(:malachi, :data_shards)
        value -> Application.put_env(:malachi, :data_shards, value)
      end
    end)

    :ok
  end

  defp set_shards(n), do: Application.put_env(:malachi, :data_shards, n)

  describe "single shard (default, the no-op)" do
    test "shard_count is 1 and every topic routes to Malachi.LogBroker" do
      set_shards(1)
      assert DataPlaneRouter.shard_count() == 1

      for topic <- ["a", "orders", "x/y", "", "topic_#{System.unique_integer([:positive])}"] do
        assert DataPlaneRouter.shard_for(topic) == Malachi.LogBroker
      end
    end

    test "shards/1 is the single historical child at the base dir" do
      set_shards(1)
      assert DataPlaneRouter.shards("/data") == [{Malachi.LogBroker, "/data"}]
    end

    test "a missing config still means one shard" do
      Application.delete_env(:malachi, :data_shards)
      assert DataPlaneRouter.shard_count() == 1
      assert DataPlaneRouter.shard_for("anything") == Malachi.LogBroker
    end

    test "a zero or negative count is clamped to one" do
      set_shards(0)
      assert DataPlaneRouter.shard_count() == 1
      set_shards(-3)
      assert DataPlaneRouter.shard_count() == 1
    end
  end

  describe "multiple shards" do
    test "shard 0 keeps the base name; others are suffixed" do
      assert DataPlaneRouter.shard_name(0) == Malachi.LogBroker
      assert Atom.to_string(DataPlaneRouter.shard_name(1)) == "Elixir.Malachi.LogBroker.1"
      assert Atom.to_string(DataPlaneRouter.shard_name(3)) == "Elixir.Malachi.LogBroker.3"
    end

    test "shard_for is deterministic: the same topic always maps to the same shard" do
      set_shards(4)

      for topic <- ["orders", "clicks", "payments", "a", "b"] do
        assert DataPlaneRouter.shard_for(topic) == DataPlaneRouter.shard_for(topic)
      end
    end

    test "topics spread across more than one shard, and every result is a valid shard" do
      set_shards(4)
      valid = MapSet.new(for i <- 0..3, do: DataPlaneRouter.shard_name(i))
      shards = for i <- 1..200, do: DataPlaneRouter.shard_for("topic_#{i}")

      assert length(Enum.uniq(shards)) > 1, "hashing should use more than one shard"
      assert Enum.all?(shards, &MapSet.member?(valid, &1))
    end

    test "shards/1 gives N distinct names and N distinct isolated dirs" do
      set_shards(4)
      shards = DataPlaneRouter.shards("/data")
      names = Enum.map(shards, &elem(&1, 0))
      dirs = Enum.map(shards, &elem(&1, 1))

      assert length(shards) == 4
      assert Enum.uniq(names) == names, "shard names must be distinct"
      assert Enum.uniq(dirs) == dirs, "shard dirs must be distinct"
      assert hd(names) == Malachi.LogBroker
      assert Enum.all?(dirs, &String.starts_with?(&1, "/data/shard_"))
    end
  end

  describe "shard independence" do
    # The spike rests on shards sharing nothing but disk and schedulers. Two independent in-memory brokers
    # (unique names, own dirs) must not leak state: the same topic name produced to each stays isolated.
    test "two independent brokers do not share topic state" do
      a = start_independent_broker()
      b = start_independent_broker()

      {:ok, _} = BrokerServer.create_topic(a, "shared", 8)
      {:ok, _} = BrokerServer.create_topic(b, "shared", 8)
      {:ok, _} = BrokerServer.produce(a, "shared", [Record.new("only-a", key: "k")])

      assert consume_values(a, "shared") == ["only-a"]
      assert consume_values(b, "shared") == [], "broker b must not see broker a's records"
    end
  end

  defp start_independent_broker do
    tag = System.unique_integer([:positive])
    dir = Path.join(System.tmp_dir!(), "dpr_test_#{tag}")
    File.rm_rf!(dir)
    {:ok, broker} = BrokerServer.start_link(dir, name: :"dpr_broker_#{tag}")

    on_exit(fn ->
      stop_quietly(broker)
      File.rm_rf!(dir)
    end)

    broker
  end

  defp consume_values(broker, topic) do
    {records, _next} = BrokerServer.consume(broker, topic, %{}, 1000, 0)
    Enum.map(records, & &1.value)
  end
end
