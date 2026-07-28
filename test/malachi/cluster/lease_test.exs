defmodule Malachi.Cluster.LeaseTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.Lease

  @duration 10_000

  # acquire_or_renew helper returning {state, reply}
  defp acquire(state, candidate, now, duration \\ @duration) do
    Lease.apply(state, {:acquire_or_renew, candidate, duration}, now)
  end

  test "acquiring a free lease grants it and bumps the fence from 0 to 1" do
    {state, reply} = acquire(Lease.new(), :a, 1_000)

    assert reply == {:ok, 1}
    assert Lease.holder(state) == :a
  end

  test "renewal by the same holder keeps the fence and extends the term" do
    {state, {:ok, 1}} = acquire(Lease.new(), :a, 1_000)
    {state, reply} = acquire(state, :a, 5_000)

    assert reply == {:ok, 1}, "renewal keeps the fence"
    assert Lease.holder(state) == :a
    # the term was extended: at 1_000 + duration it is now valid (renewed at 5_000)
    {_state, refused} = acquire(state, :b, 1_000 + @duration)
    assert refused == {:error, {:held, :a}}
  end

  test "another candidate is refused while the lease is still valid" do
    {state, {:ok, 1}} = acquire(Lease.new(), :a, 1_000)
    {state2, reply} = acquire(state, :b, 1_000 + @duration - 1)

    assert reply == {:error, {:held, :a}}
    assert state2 == state, "a refused acquire does not change state"
  end

  test "another candidate acquires once the lease has expired, bumping the fence" do
    {state, {:ok, 1}} = acquire(Lease.new(), :a, 1_000)
    {state, reply} = acquire(state, :b, 1_000 + @duration)

    assert reply == {:ok, 2}, "a new holder advances the fence"
    assert Lease.holder(state) == :b
  end

  test "expiry is exactly now >= renew_at + duration (boundary)" do
    {state, {:ok, 1}} = acquire(Lease.new(), :a, 1_000)

    # one ms before the deadline: still held
    {_state, refused} = acquire(state, :b, 1_000 + @duration - 1)
    assert refused == {:error, {:held, :a}}

    # exactly at the deadline: expired, so :b takes it
    {_state, granted} = acquire(state, :b, 1_000 + @duration)
    assert granted == {:ok, 2}
  end

  test "the same holder renewing after its own expiry keeps its fence" do
    {state, {:ok, 1}} = acquire(Lease.new(), :a, 1_000)
    # :a lets it lapse, then renews itself: no one stole it, so the token is unchanged
    {state, reply} = acquire(state, :a, 1_000 + @duration + 5_000)

    assert reply == {:ok, 1}
    assert Lease.holder(state) == :a
  end

  test "release by the holder at the current fence frees the lease" do
    {state, {:ok, 1}} = acquire(Lease.new(), :a, 1_000)
    {state, reply} = Lease.apply(state, {:release, :a, 1}, 2_000)

    assert reply == :ok
    assert Lease.holder(state) == nil
  end

  test "release by a non-holder or with a stale fence is a benign no-op" do
    {held, {:ok, 1}} = acquire(Lease.new(), :a, 1_000)

    # wrong holder
    {state1, reply1} = Lease.apply(held, {:release, :b, 1}, 2_000)
    assert reply1 == :ok
    assert Lease.holder(state1) == :a

    # right holder, stale fence
    {state2, reply2} = Lease.apply(held, {:release, :a, 0}, 2_000)
    assert reply2 == :ok
    assert Lease.holder(state2) == :a
  end

  test "after release, the next acquire advances the fence again" do
    {state, {:ok, 1}} = acquire(Lease.new(), :a, 1_000)
    {state, :ok} = Lease.apply(state, {:release, :a, 1}, 2_000)
    {state, reply} = acquire(state, :b, 3_000)

    assert reply == {:ok, 2}
    assert Lease.holder(state) == :b
  end
end
