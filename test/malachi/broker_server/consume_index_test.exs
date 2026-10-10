defmodule Malachi.BrokerServer.ConsumeIndexTest do
  use ExUnit.Case, async: true

  alias Malachi.BrokerServer.ConsumeIndex

  @a {"t", 0}
  @b {"t", 1}

  defp waiter(range_id, position),
    do: %{from: {self(), make_ref()}, range_id: range_id, position: position, timer: make_ref()}

  test "a consume stream is indexed by its range, and dropped from it" do
    token = make_ref()
    index = ConsumeIndex.put_consumer(ConsumeIndex.new(), token, self(), @a)

    assert ConsumeIndex.consumer?(index, token)
    assert {:ok, %{range_id: @a, armed: -1}} = ConsumeIndex.fetch_consumer(index, token)
    assert ConsumeIndex.readers?(index, @a)
    refute ConsumeIndex.readers?(index, @b)
    assert ConsumeIndex.ranges(index) == [@a]

    index = ConsumeIndex.drop_consumer(index, token)
    refute ConsumeIndex.consumer?(index, token)
    refute ConsumeIndex.readers?(index, @a)
    assert ConsumeIndex.fetch_consumer(index, token) == :error
    # dropping what it does not hold changes nothing
    assert ConsumeIndex.drop_consumer(index, token) == index
  end

  test "an armed stream is ready once the durable end passes what it read, and is disarmed then" do
    token = make_ref()
    index = ConsumeIndex.put_consumer(ConsumeIndex.new(), token, self(), @a) |> ConsumeIndex.arm(token, 5)

    assert {[], [], ^index} = ConsumeIndex.take_ready(index, @a, 5, fn _ -> true end)
    assert {[{^token, %{armed: 5}}], [], index} = ConsumeIndex.take_ready(index, @a, 6, fn _ -> true end)
    assert {:ok, %{armed: nil}} = ConsumeIndex.fetch_consumer(index, token)

    # a stream that is reading is not woken again until it arms
    assert {[], [], _index} = ConsumeIndex.take_ready(index, @a, 100, fn _ -> true end)
  end

  test "waiters the check accepts are taken out; the others wait on" do
    {ready, waiting} = {make_ref(), make_ref()}

    index =
      ConsumeIndex.new()
      |> ConsumeIndex.put_waiter(ready, waiter(@a, {0, 1}))
      |> ConsumeIndex.put_waiter(waiting, waiter(@a, {0, 9}))

    assert {[], [{^ready, _}], index} = ConsumeIndex.take_ready(index, @a, 5, &(&1.position < {0, 5}))
    assert index.waiters |> Map.keys() == [waiting]
    assert {_waiter, index} = ConsumeIndex.pop_waiter(index, waiting)
    assert ConsumeIndex.pop_waiter(index, waiting) == :error
    refute ConsumeIndex.readers?(index, @a)
  end

  test "readers of the ranges a check rejects are taken out, of both kinds; the others stay" do
    {gone, kept, gone_waiter, kept_waiter} = {make_ref(), make_ref(), make_ref(), make_ref()}

    index =
      ConsumeIndex.new()
      |> ConsumeIndex.put_consumer(gone, self(), @a)
      |> ConsumeIndex.put_consumer(kept, self(), @b)
      |> ConsumeIndex.put_waiter(gone_waiter, waiter(@a, {0, 0}))
      |> ConsumeIndex.put_waiter(kept_waiter, waiter(@b, {0, 0}))

    assert {[{^gone, _}], [{^gone_waiter, _}], index} = ConsumeIndex.take_unless(index, &(&1 == @b))
    assert Map.keys(index.consumers) == [kept]
    assert Map.keys(index.waiters) == [kept_waiter]
    assert ConsumeIndex.ranges(index) == [@b]
  end
end
