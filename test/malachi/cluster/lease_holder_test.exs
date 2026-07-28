defmodule Malachi.Cluster.LeaseHolderTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.LeaseHolder

  # Drives the holder deterministically: `renew` returns whatever the renew_agent currently holds, the
  # clock reads the clock_agent, and the callbacks report to the test. retry_period is long so the timer
  # never fires mid-test; tick_now/1 drives each election tick.
  defp start_holder(renew_agent, clock_agent, opts \\ []) do
    test_pid = self()

    base = [
      renew: fn -> Agent.get(renew_agent, & &1) end,
      release: fn fence -> send(test_pid, {:released, fence}) end,
      on_acquired: fn fence -> send(test_pid, {:acquired, fence}) end,
      on_lost: fn -> send(test_pid, :lost) end,
      clock: fn -> Agent.get(clock_agent, & &1) end,
      retry_period_ms: 60_000,
      renew_deadline_ms: 10_000
    ]

    {:ok, holder} = LeaseHolder.start_link(Keyword.merge(base, opts))
    holder
  end

  test "a follower that acquires the lease becomes leader and calls on_acquired with the fence" do
    {:ok, renew} = Agent.start_link(fn -> {:ok, 1} end)
    {:ok, clock} = Agent.start_link(fn -> 0 end)
    holder = start_holder(renew, clock)

    assert LeaseHolder.tick_now(holder) == {:leader, 1}
    assert_received {:acquired, 1}
  end

  test "a follower that cannot acquire the lease stays a follower" do
    {:ok, renew} = Agent.start_link(fn -> {:error, :held} end)
    {:ok, clock} = Agent.start_link(fn -> 0 end)
    holder = start_holder(renew, clock)

    assert LeaseHolder.tick_now(holder) == {:follower, nil}
    refute_received {:acquired, _}
  end

  test "leader?/1 reports the current role without forcing a tick" do
    {:ok, renew} = Agent.start_link(fn -> {:error, :held} end)
    {:ok, clock} = Agent.start_link(fn -> 0 end)
    holder = start_holder(renew, clock)

    # a plain read: still a follower (no tick has run), and it did not try to acquire
    refute LeaseHolder.leader?(holder)
    refute_received {:acquired, _}

    Agent.update(renew, fn _ -> {:ok, 1} end)
    assert LeaseHolder.tick_now(holder) == {:leader, 1}
    assert LeaseHolder.leader?(holder)
  end

  test "a leader that renews stays leader without calling on_acquired again" do
    {:ok, renew} = Agent.start_link(fn -> {:ok, 1} end)
    {:ok, clock} = Agent.start_link(fn -> 0 end)
    holder = start_holder(renew, clock)
    assert LeaseHolder.tick_now(holder) == {:leader, 1}
    assert_received {:acquired, 1}

    Agent.update(clock, fn _ -> 3_000 end)
    assert LeaseHolder.tick_now(holder) == {:leader, 1}
    refute_received {:acquired, _}
  end

  test "a leader that cannot reach the lease keeps leadership until the renew deadline, then drops" do
    {:ok, renew} = Agent.start_link(fn -> {:ok, 1} end)
    {:ok, clock} = Agent.start_link(fn -> 0 end)
    holder = start_holder(renew, clock)
    assert LeaseHolder.tick_now(holder) == {:leader, 1}
    assert_received {:acquired, 1}

    # the lease becomes unreachable; last successful renewal was at t=0, deadline is 10_000
    Agent.update(renew, fn _ -> {:error, :unavailable} end)

    Agent.update(clock, fn _ -> 9_999 end)
    assert LeaseHolder.tick_now(holder) == {:leader, 1}
    refute_received :lost

    Agent.update(clock, fn _ -> 10_000 end)
    assert LeaseHolder.tick_now(holder) == {:follower, nil}
    assert_received :lost
  end

  test "a non-held renew error (e.g. a timeout) is treated as unreachable, not a crash" do
    {:ok, renew} = Agent.start_link(fn -> {:ok, 1} end)
    {:ok, clock} = Agent.start_link(fn -> 0 end)
    holder = start_holder(renew, clock)
    assert LeaseHolder.tick_now(holder) == {:leader, 1}

    Agent.update(renew, fn _ -> {:error, :timeout} end)

    # within the deadline it keeps trying (not a CaseClauseError crash)
    Agent.update(clock, fn _ -> 5_000 end)
    assert LeaseHolder.tick_now(holder) == {:leader, 1}
    # past the deadline it drops, like any unreachable lease
    Agent.update(clock, fn _ -> 10_000 end)
    assert LeaseHolder.tick_now(holder) == {:follower, nil}
    assert_received :lost
  end

  test "a leader told the lease is held by another drops leadership immediately" do
    {:ok, renew} = Agent.start_link(fn -> {:ok, 1} end)
    {:ok, clock} = Agent.start_link(fn -> 0 end)
    holder = start_holder(renew, clock)
    assert LeaseHolder.tick_now(holder) == {:leader, 1}

    Agent.update(renew, fn _ -> {:error, :held} end)
    assert LeaseHolder.tick_now(holder) == {:follower, nil}
    assert_received :lost
  end

  test "a fence change while leader signals lost then re-acquired under the new token" do
    {:ok, renew} = Agent.start_link(fn -> {:ok, 1} end)
    {:ok, clock} = Agent.start_link(fn -> 0 end)
    holder = start_holder(renew, clock)
    assert LeaseHolder.tick_now(holder) == {:leader, 1}
    assert_received {:acquired, 1}

    # a leadership gap: the lease came back to us with a higher fence
    Agent.update(renew, fn _ -> {:ok, 3} end)
    assert LeaseHolder.tick_now(holder) == {:leader, 3}
    assert_received :lost
    assert_received {:acquired, 3}
  end

  test "on normal shutdown a leader releases the lease at its fence" do
    {:ok, renew} = Agent.start_link(fn -> {:ok, 1} end)
    {:ok, clock} = Agent.start_link(fn -> 0 end)
    holder = start_holder(renew, clock)
    assert LeaseHolder.tick_now(holder) == {:leader, 1}

    :ok = GenServer.stop(holder)
    assert_received {:released, 1}
  end

  test "a follower does not release anything on shutdown" do
    {:ok, renew} = Agent.start_link(fn -> {:error, :held} end)
    {:ok, clock} = Agent.start_link(fn -> 0 end)
    holder = start_holder(renew, clock)
    assert LeaseHolder.tick_now(holder) == {:follower, nil}

    :ok = GenServer.stop(holder)
    refute_received {:released, _}
  end
end
