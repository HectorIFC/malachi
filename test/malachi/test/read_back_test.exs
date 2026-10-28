defmodule Malachi.Test.ReadBackTest do
  # ReadBack is how the in-process tests check what a broker holds, so a record it drops or counts twice
  # would pass a test that should fail: across a split, each record is read once, by the range that holds
  # its key now, though both children's histories hold the parent's records.
  use ExUnit.Case, async: false

  alias Malachi.BrokerServer
  alias Malachi.Log.Record
  alias Malachi.Test.ReadBack
  alias Malachi.Test.TmpDir

  setup do
    dir = TmpDir.path("read_back")
    {:ok, broker} = BrokerServer.start_link(dir, name: :"read_back_#{System.unique_integer([:positive])}")

    on_exit(fn ->
      if Process.alive?(broker), do: GenServer.stop(broker)
      File.rm_rf!(dir)
    end)

    {:ok, _} = BrokerServer.create_topic(broker, "t", 4)
    %{broker: broker}
  end

  test "reads every record of a topic once, in the order its range holds them", %{broker: broker} do
    values = for i <- 1..30, do: "v#{i}"
    {:ok, _} = BrokerServer.produce(broker, "t", Enum.map(values, &Record.new(&1, key: "k")))
    assert ReadBack.values(broker, "t") == values
  end

  test "across a split, each record once, by the range that holds its key", %{broker: broker} do
    before = for i <- 1..40, do: "b#{i}"
    {:ok, _} = BrokerServer.produce(broker, "t", Enum.map(before, &Record.new(&1, key: &1)))
    {:ok, _left, _right} = BrokerServer.split_range(broker, {"t", 0})
    later = for i <- 1..40, do: "a#{i}"
    for value <- later, do: {:ok, _} = BrokerServer.produce(broker, "t", [Record.new(value, key: value)])

    read = ReadBack.values(broker, "t")
    assert Enum.sort(read) == Enum.sort(before ++ later)
    assert length(read) == length(Enum.uniq(read))
  end

  test "a topic with nothing written reads as nothing", %{broker: broker} do
    assert ReadBack.values(broker, "t") == []
  end
end
