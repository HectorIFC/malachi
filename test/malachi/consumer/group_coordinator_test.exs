defmodule Malachi.Consumer.GroupCoordinatorTest do
  use ExUnit.Case, async: true

  alias Malachi.Consumer.Assignment
  alias Malachi.Consumer.GroupCoordinator

  # Start a coordinator with a controllable clock and range set; the auto-timer is pushed far out so each
  # test drives reconcile explicitly via reconcile_now/1.
  defp start(opts \\ []) do
    {:ok, clock} = Agent.start_link(fn -> 0 end)
    {:ok, ranges} = Agent.start_link(fn -> Keyword.get(opts, :ranges, [:r0, :r1, :r2, :r3]) end)

    base = [
      clock: fn -> Agent.get(clock, & &1) end,
      ranges_fun: fn _topic -> Agent.get(ranges, & &1) end,
      session_ms: Keyword.get(opts, :session_ms, 100),
      tick_ms: 3_600_000
    ]

    child = base ++ Keyword.take(opts, [:owns_fun, :open_fun])
    coord = start_supervised!({GroupCoordinator, child})

    %{coord: coord, clock: clock, ranges: ranges}
  end

  defp set_clock(clock, t), do: Agent.update(clock, fn _ -> t end)
  defp set_ranges(ranges, rs), do: Agent.update(ranges, fn _ -> rs end)

  test "a lone member is assigned all ranges at generation 1" do
    %{coord: c} = start(ranges: [:r0, :r1])
    assert {:ok, %{generation: 1, ranges: ranges}} = GroupCoordinator.join_group(c, "g", "t", :m1)
    assert Enum.sort(ranges) == [:r0, :r1]
  end

  test "two members partition the ranges (disjoint, complete) and the generation advances" do
    %{coord: c} = start(ranges: [:r0, :r1, :r2, :r3])
    {:ok, %{generation: g1, ranges: _}} = GroupCoordinator.join_group(c, "g", "t", :m1)
    {:ok, %{generation: g2, ranges: m2}} = GroupCoordinator.join_group(c, "g", "t", :m2)
    assert g2 > g1

    {:ok, %{generation: ^g2, ranges: m1}} = GroupCoordinator.heartbeat_group(c, "g", "t", :m1)
    assert Enum.sort(m1 ++ m2) == [:r0, :r1, :r2, :r3]
    assert m1 -- m2 == m1
  end

  test "leave gives the ranges back to the survivor and advances the generation" do
    %{coord: c} = start(ranges: [:r0, :r1])
    {:ok, %{generation: _, ranges: _}} = GroupCoordinator.join_group(c, "g", "t", :m1)
    {:ok, %{generation: g2, ranges: _}} = GroupCoordinator.join_group(c, "g", "t", :m2)

    :ok = GroupCoordinator.leave(c, "g", "t", :m2)

    {:ok, %{generation: g3, ranges: ranges}} = GroupCoordinator.heartbeat_group(c, "g", "t", :m1)
    assert g3 > g2
    assert Enum.sort(ranges) == [:r0, :r1]
  end

  test "a member silent past the session window is evicted on reconcile and must rejoin" do
    %{coord: c, clock: clock} = start(ranges: [:r0, :r1], session_ms: 100)
    GroupCoordinator.join_group(c, "g", "t", :m1)
    GroupCoordinator.join_group(c, "g", "t", :m2)

    set_clock(clock, 50)
    GroupCoordinator.heartbeat_group(c, "g", "t", :m1)

    # cutoff = 120 - 100 = 20: m2 (hb 0) is evicted, m1 (hb 50) survives
    set_clock(clock, 120)
    :ok = GroupCoordinator.reconcile_now(c)

    assert {:error, :unknown_member} = GroupCoordinator.heartbeat_group(c, "g", "t", :m2)
    {:ok, %{generation: _, ranges: ranges}} = GroupCoordinator.heartbeat_group(c, "g", "t", :m1)
    assert Enum.sort(ranges) == [:r0, :r1]
  end

  test "a group with no members is dropped" do
    %{coord: c} = start(ranges: [:r0])
    GroupCoordinator.join_group(c, "g", "t", :m1)
    :ok = GroupCoordinator.leave(c, "g", "t", :m1)
    assert {:error, :unknown_member} = GroupCoordinator.heartbeat_group(c, "g", "t", :m1)
  end

  test "a change in the topic's ranges rebalances on reconcile" do
    %{coord: c, ranges: ranges} = start(ranges: [:r0])
    {:ok, %{generation: g1, ranges: [:r0]}} = GroupCoordinator.join_group(c, "g", "t", :m1)

    set_ranges(ranges, [:r0, :r1, :r2])
    :ok = GroupCoordinator.reconcile_now(c)

    {:ok, %{generation: g2, ranges: rs}} = GroupCoordinator.heartbeat_group(c, "g", "t", :m1)
    assert g2 > g1
    assert Enum.sort(rs) == [:r0, :r1, :r2]
  end

  test "reconcile is idempotent: no membership or range change leaves the generation put" do
    %{coord: c} = start(ranges: [:r0])
    {:ok, %{generation: g1, ranges: _}} = GroupCoordinator.join_group(c, "g", "t", :m1)

    :ok = GroupCoordinator.reconcile_now(c)

    {:ok, %{generation: g2, ranges: _}} = GroupCoordinator.heartbeat_group(c, "g", "t", :m1)
    assert g2 == g1
  end

  test "heartbeat for an unknown group or member is rejected" do
    %{coord: c} = start()
    assert {:error, :unknown_member} = GroupCoordinator.heartbeat_group(c, "nope", "t", :m1)
  end

  test "poll registers a new member (rebalance) and only heartbeats a known one (no rebalance)" do
    %{coord: c} = start(ranges: [:r0, :r1])

    {:ok, g1, ranges} = GroupCoordinator.poll(c, "g", "t", :m1)
    assert Enum.sort(ranges) == [:r0, :r1]

    {:ok, g2, _} = GroupCoordinator.poll(c, "g", "t", :m1)
    assert g2 == g1

    {:ok, g3, _} = GroupCoordinator.poll(c, "g", "t", :m2)
    assert g3 > g1
  end

  test "poll re-registers a member that had been evicted" do
    %{coord: c, clock: clock} = start(ranges: [:r0], session_ms: 100)
    GroupCoordinator.poll(c, "g", "t", :m1)

    set_clock(clock, 200)
    :ok = GroupCoordinator.reconcile_now(c)
    assert {:error, :unknown_member} = GroupCoordinator.heartbeat_group(c, "g", "t", :m1)

    {:ok, _, ranges} = GroupCoordinator.poll(c, "g", "t", :m1)
    assert Enum.sort(ranges) == [:r0]
  end

  describe "ownership guard (owns_fun)" do
    test "poll and join are rejected with :not_owner when this node does not own the topic" do
      %{coord: c} = start(ranges: [:r0, :r1], owns_fun: fn _topic -> false end)
      assert {:error, :not_owner} = GroupCoordinator.poll(c, "g", "t", :m1)
      assert {:error, :not_owner} = GroupCoordinator.join_group(c, "g", "t", :m1)
    end

    test "a rejected poll registers no member (no phantom assignment)" do
      %{coord: c} = start(ranges: [:r0, :r1], owns_fun: fn _topic -> false end)
      assert {:error, :not_owner} = GroupCoordinator.poll(c, "g", "t", :m1)
      # the group must not exist: the assignment finds no member rather than a stale registration
      assert GroupCoordinator.assignment(c, "g", "t", :m1) == {:error, :unknown_member}
    end

    test "owns_fun is consulted per topic, so a node can own some topics and reject others" do
      %{coord: c} = start(ranges: [:r0, :r1], owns_fun: fn topic -> topic == "mine" end)
      assert {:ok, 1, ranges} = GroupCoordinator.poll(c, "g", "mine", :m1)
      assert Enum.sort(ranges) == [:r0, :r1]
      assert {:error, :not_owner} = GroupCoordinator.poll(c, "g", "theirs", :m1)
    end
  end

  # An ownership guard that can be flipped mid-test, as a leadership move flips it.
  defp owns_agent do
    {:ok, owns} = Agent.start_link(fn -> true end)
    {owns, fn _topic -> Agent.get(owns, & &1) end}
  end

  # A control plane that opens epochs 1, 2, ... and counts how often it was asked.
  defp epochs do
    {:ok, opened} = Agent.start_link(fn -> 0 end)
    {opened, fn _group, _topic -> {:ok, Agent.get_and_update(opened, &{&1 + 1, &1 + 1})} end}
  end

  defp gen(epoch, count), do: Bitwise.bsl(epoch, 32) + count

  describe "the group keys' answers" do
    test "join_group and heartbeat_group carry the session and the generation above the group's epoch" do
      %{coord: c} = start(ranges: [:r0, :r1], session_ms: 250, open_fun: fn "g", "t" -> {:ok, 7} end)
      gen1 = gen(7, 1)
      gen2 = gen(7, 2)

      assert {:ok, %{generation: ^gen1, session_ms: 250, ranges: [_, _]}} =
               GroupCoordinator.join_group(c, "g", "t", :m1)

      assert {:ok, %{generation: ^gen2, ranges: [mine]}} = GroupCoordinator.join_group(c, "g", "t", :m2)
      assert {:ok, %{generation: ^gen2, ranges: [theirs]}} = GroupCoordinator.heartbeat_group(c, "g", "t", :m1)
      assert Enum.sort([mine, theirs]) == [:r0, :r1]
      assert GroupCoordinator.heartbeat_group(c, "g", "t", :ghost) == {:error, :unknown_member}
    end

    test "the epoch is opened once per group, by the group keys only" do
      {opened, open_fun} = epochs()
      %{coord: c} = start(ranges: [:r0], open_fun: open_fun)

      # the legacy entry points never ask, and hand out the bare count
      assert {:ok, 1, [:r0]} = GroupCoordinator.join(c, "legacy", "t", :m1)
      assert Agent.get(opened, & &1) == 0

      # a group the legacy path made gets its epoch on its first group key
      gen1 = gen(1, 1)
      assert {:ok, %{generation: ^gen1}} = GroupCoordinator.heartbeat_group(c, "legacy", "t", :m1)
      gen2 = gen(2, 1)
      assert {:ok, %{generation: ^gen2}} = GroupCoordinator.join_group(c, "g", "t", :m1)
      assert {:ok, %{generation: ^gen2}} = GroupCoordinator.heartbeat_group(c, "g", "t", :m1)
      assert :ok = GroupCoordinator.validate_commit(c, "g", "t", :m1, gen2, [:r0])
      assert Agent.get(opened, & &1) == 2
    end

    test "a control plane that refuses, does not answer or answers nonsense is the caller's error, asked again" do
      {:ok, replies} = Agent.start_link(fn -> [{:error, :unsupported}, :exit, :garbage, {:ok, 4}] end)

      open_fun = fn _group, _topic ->
        case Agent.get_and_update(replies, fn [reply | rest] -> {reply, rest} end) do
          :exit -> exit(:timeout)
          reply -> reply
        end
      end

      %{coord: c} = start(ranges: [:r0], open_fun: open_fun)
      assert GroupCoordinator.join_group(c, "g", "t", :m1) == {:error, :unsupported}
      assert GroupCoordinator.join_group(c, "g", "t", :m1) == {:error, :timeout}
      assert GroupCoordinator.join_group(c, "g", "t", :m1) == {:error, {:open_group, :garbage}}
      gen = gen(4, 1)
      assert {:ok, %{generation: ^gen}} = GroupCoordinator.join_group(c, "g", "t", :m1)
    end

    test "an opening runs beside the coordinator: other groups are served meanwhile, and one opening serves every call waiting" do
      parent = self()
      {:ok, opened} = Agent.start_link(fn -> 0 end)

      open_fun = fn _group, _topic ->
        Agent.update(opened, &(&1 + 1))
        send(parent, {:opening, self()})
        receive(do: (:go -> {:ok, 5}))
      end

      %{coord: c} = start(ranges: [:r0, :r1], open_fun: open_fun)
      first = Task.async(fn -> GroupCoordinator.join_group(c, "slow", "t", :m1) end)
      assert_receive {:opening, opener}
      second = Task.async(fn -> GroupCoordinator.join_group(c, "slow", "t", :m2) end)

      # the coordinator answers another group while the opening is under way
      assert {:ok, 1, _} = GroupCoordinator.join(c, "other", "t", :m1)
      refute Task.yield(first, 50)

      send(opener, :go)
      gen1 = gen(5, 1)
      gen2 = gen(5, 2)
      assert {:ok, %{generation: ^gen1}} = Task.await(first)
      assert {:ok, %{generation: ^gen2}} = Task.await(second)
      assert Agent.get(opened, & &1) == 1
    end

    test "an opening past its deadline answers timeout, takes no late epoch, and the next call opens again" do
      {:ok, calls} = Agent.start_link(fn -> 0 end)

      open_fun = fn _group, _topic ->
        case Agent.get_and_update(calls, &{&1 + 1, &1 + 1}) do
          1 -> receive(do: (:never -> {:ok, 1}))
          n -> {:ok, n}
        end
      end

      {:ok, c} =
        GroupCoordinator.start_link(ranges_fun: fn _topic -> [:r0] end, open_fun: open_fun, open_deadline_ms: 50)

      assert GroupCoordinator.join_group(c, "g", "t", :m1) == {:error, :timeout}
      # nothing changed: the member was not registered
      assert GroupCoordinator.assignment(c, "g", "t", :m1) == {:error, :unknown_member}

      gen = gen(2, 1)
      assert {:ok, %{generation: ^gen}} = GroupCoordinator.join_group(c, "g", "t", :m1)
      GenServer.stop(c)
    end

    test "a refused join_group registers no one, so the group's members keep their ranges" do
      %{coord: c} = start(ranges: [:r0, :r1], open_fun: fn _group, _topic -> {:error, :unsupported} end)
      assert {:ok, 1, [:r0, :r1]} = GroupCoordinator.join(c, "g", "t", :old)

      for new <- [:n1, :n2, :n3] do
        assert GroupCoordinator.join_group(c, "g", "t", new) == {:error, :unsupported}
        assert GroupCoordinator.assignment(c, "g", "t", new) == {:error, :unknown_member}
      end

      assert {:ok, 1, [:r0, :r1]} = GroupCoordinator.assignment(c, "g", "t", :old)
    end

    test "a member the coordinator does not know opens nothing" do
      {opened, open_fun} = epochs()
      %{coord: c} = start(ranges: [:r0], open_fun: open_fun)
      {:ok, 1, _} = GroupCoordinator.join(c, "legacy", "t", :m1)

      assert GroupCoordinator.heartbeat_group(c, "legacy", "t", :ghost) == {:error, :unknown_member}
      assert GroupCoordinator.validate_commit(c, "legacy", "t", :ghost, 1, []) == {:error, :unknown_member}
      assert GroupCoordinator.heartbeat_group(c, "none", "t", :ghost) == {:error, :unknown_member}
      assert Agent.get(opened, & &1) == 0
    end

    test "a dropped epoch is opened again above it, and only the epoch held is dropped" do
      {_opened, open_fun} = epochs()
      %{coord: c} = start(ranges: [:r0], open_fun: open_fun)
      {:ok, %{generation: first}} = GroupCoordinator.join_group(c, "g", "t", :m1)

      :ok = GroupCoordinator.drop_epoch(c, "g", "t", 7)
      assert {:ok, %{generation: ^first}} = GroupCoordinator.heartbeat_group(c, "g", "t", :m1)

      :ok = GroupCoordinator.drop_epoch(c, "g", "t", 1)
      second = gen(2, 1)
      assert {:ok, %{generation: ^second}} = GroupCoordinator.heartbeat_group(c, "g", "t", :m1)
      assert GroupCoordinator.validate_commit(c, "g", "t", :m1, first, [:r0]) == {:error, :stale_generation}
      assert GroupCoordinator.validate_commit(c, "g", "t", :m1, second, [:r0]) == :ok
    end

    test "ownership is asked again for the calls that waited for an opening" do
      parent = self()
      {owns, owns_fun} = owns_agent()

      open_fun = fn _group, _topic ->
        send(parent, {:opening, self()})
        receive(do: (:go -> {:ok, 1}))
      end

      %{coord: c} = start(ranges: [:r0], owns_fun: owns_fun, open_fun: open_fun)
      waiting = Task.async(fn -> GroupCoordinator.join_group(c, "g", "t", :m1) end)
      assert_receive {:opening, opener}

      # leadership moves while the opening is under way
      Agent.update(owns, fn _ -> false end)
      send(opener, :go)

      assert Task.await(waiting) == {:error, :not_owner}
      assert GroupCoordinator.assignment(c, "g", "t", :m1) == {:error, :unknown_member}
    end

    test "a group whose last member is evicted drops its epoch with it" do
      {_opened, open_fun} = epochs()
      %{coord: c, clock: clock} = start(ranges: [:r0], session_ms: 100, open_fun: open_fun)
      {:ok, _} = GroupCoordinator.join_group(c, "g", "t", :m1)

      set_clock(clock, 500)
      :ok = GroupCoordinator.reconcile_now(c)

      gen = gen(2, 1)
      assert {:ok, %{generation: ^gen}} = GroupCoordinator.join_group(c, "g", "t", :m1)
    end

    test "a group that loses its last member drops its epoch with it" do
      {_opened, open_fun} = epochs()
      %{coord: c} = start(ranges: [:r0], open_fun: open_fun)
      {:ok, _} = GroupCoordinator.join_group(c, "g", "t", :m1)
      :ok = GroupCoordinator.leave(c, "g", "t", :m1)

      gen = gen(2, 1)
      assert {:ok, %{generation: ^gen}} = GroupCoordinator.join_group(c, "g", "t", :m1)
    end

    test "a coordinator that starts again opens the next epoch, so a generation from before is stale" do
      {_opened, open_fun} = epochs()
      opts = [ranges_fun: fn _topic -> [:r0] end, tick_ms: 3_600_000, open_fun: open_fun]

      {:ok, before} = GroupCoordinator.start_link(opts)
      {:ok, %{generation: old}} = GroupCoordinator.join_group(before, "g", "t", :m1)
      GenServer.stop(before)

      {:ok, again} = GroupCoordinator.start_link(opts)
      {:ok, %{generation: new}} = GroupCoordinator.join_group(again, "g", "t", :m1)
      # the count starts over at 1 in both, so only the epoch tells them apart, and it only grows
      assert Bitwise.band(old, 0xFFFFFFFF) == Bitwise.band(new, 0xFFFFFFFF)
      assert new > old
      assert GroupCoordinator.validate_commit(again, "g", "t", :m1, old, [:r0]) == {:error, :stale_generation}
      GenServer.stop(again)
    end
  end

  describe "validate_commit" do
    test "a member of the current generation may commit its own ranges, and none at all" do
      %{coord: c} = start(ranges: [:r0, :r1])
      {:ok, %{generation: generation, ranges: ranges}} = GroupCoordinator.join_group(c, "g", "t", :m1)
      assert GroupCoordinator.validate_commit(c, "g", "t", :m1, generation, ranges) == :ok
      assert GroupCoordinator.validate_commit(c, "g", "t", :m1, generation, []) == :ok
    end

    test "a rebalance that leaves a member's range with it does not fence the member's older generation" do
      # :m2 takes no range of [:r0] (HRW), yet its joining bumps the generation
      %{coord: c} = start(ranges: [:r0])
      {:ok, %{generation: old, ranges: [:r0]}} = GroupCoordinator.join_group(c, "g", "t", :m1)
      idle = Enum.find(for(i <- 2..50, do: :"m#{i}"), &(Assignment.assign([:r0], [:m1, &1])[&1] == []))
      {:ok, %{generation: new, ranges: []}} = GroupCoordinator.join_group(c, "g", "t", idle)
      assert new > old

      assert GroupCoordinator.validate_commit(c, "g", "t", :m1, old, [:r0]) == :ok
      assert GroupCoordinator.validate_commit(c, "g", "t", :m1, new, [:r0]) == :ok
    end

    test "a range handed to the member after the generation it commits at is stale" do
      %{coord: c, ranges: ranges} = start(ranges: [:r0])
      {:ok, %{generation: old, ranges: [:r0]}} = GroupCoordinator.join_group(c, "g", "t", :m1)

      # a new range appears and goes to :m1 at the next generation
      set_ranges(ranges, [:r0, :r1])
      :ok = GroupCoordinator.reconcile_now(c)
      {:ok, %{generation: new, ranges: [:r0, :r1]}} = GroupCoordinator.heartbeat_group(c, "g", "t", :m1)

      assert GroupCoordinator.validate_commit(c, "g", "t", :m1, old, [:r1]) == {:error, :stale_generation}
      assert GroupCoordinator.validate_commit(c, "g", "t", :m1, old, [:r0]) == :ok
      assert GroupCoordinator.validate_commit(c, "g", "t", :m1, new, [:r0, :r1]) == :ok
    end

    test "a range that left the member and came back counts from its return" do
      %{coord: c} = start(ranges: [:r0, :r1])
      {:ok, %{generation: first}} = GroupCoordinator.join_group(c, "g", "t", :m1)
      {:ok, %{ranges: [taken]}} = GroupCoordinator.join_group(c, "g", "t", :m2)
      :ok = GroupCoordinator.leave(c, "g", "t", :m2)
      {:ok, %{generation: back}} = GroupCoordinator.heartbeat_group(c, "g", "t", :m1)

      assert GroupCoordinator.validate_commit(c, "g", "t", :m1, first, [taken]) == {:error, :stale_generation}
      assert GroupCoordinator.validate_commit(c, "g", "t", :m1, back, [taken]) == :ok
    end

    test "a generation past the group's count, or of another epoch, is stale" do
      %{coord: c} = start(ranges: [:r0], open_fun: fn _group, _topic -> {:ok, 3} end)
      {:ok, %{generation: generation}} = GroupCoordinator.join_group(c, "g", "t", :m1)

      assert GroupCoordinator.validate_commit(c, "g", "t", :m1, generation + 1, [:r0]) == {:error, :stale_generation}
      assert GroupCoordinator.validate_commit(c, "g", "t", :m1, gen(2, 1), [:r0]) == {:error, :stale_generation}
      assert GroupCoordinator.validate_commit(c, "g", "t", :m1, gen(4, 1), [:r0]) == {:error, :stale_generation}
      assert GroupCoordinator.validate_commit(c, "g", "t", :m1, generation, [:r0]) == :ok
    end

    test "a range that is not the member's refuses the whole commit, also one the topic does not have" do
      %{coord: c} = start(ranges: [:r0, :r1])
      {:ok, _} = GroupCoordinator.join_group(c, "g", "t", :m1)
      {:ok, %{generation: generation, ranges: [theirs]}} = GroupCoordinator.join_group(c, "g", "t", :m2)
      {:ok, %{ranges: [mine]}} = GroupCoordinator.heartbeat_group(c, "g", "t", :m1)

      assert GroupCoordinator.validate_commit(c, "g", "t", :m1, generation, [mine, theirs]) == {:error, :not_assigned}
      assert GroupCoordinator.validate_commit(c, "g", "t", :m1, generation, [:r9]) == {:error, :not_assigned}
    end

    test "a member unknown to the group, or a group that does not exist, cannot commit" do
      %{coord: c} = start(ranges: [:r0])
      assert GroupCoordinator.validate_commit(c, "none", "t", :m1, 1, []) == {:error, :unknown_member}
      {:ok, %{generation: generation}} = GroupCoordinator.join_group(c, "g", "t", :m1)
      assert GroupCoordinator.validate_commit(c, "g", "t", :ghost, generation, []) == {:error, :unknown_member}
    end

    test "a coordinator that no longer owns the topic refuses commits and heartbeats, as it refuses joins" do
      {owns, owns_fun} = owns_agent()
      %{coord: c} = start(ranges: [:r0], owns_fun: owns_fun)
      {:ok, %{generation: generation}} = GroupCoordinator.join_group(c, "g", "t", :m1)

      Agent.update(owns, fn _ -> false end)
      assert GroupCoordinator.validate_commit(c, "g", "t", :m1, generation, [:r0]) == {:error, :not_owner}
      assert GroupCoordinator.heartbeat_group(c, "g", "t", :m1) == {:error, :not_owner}
      assert GroupCoordinator.heartbeat(c, "g", "t", :m1) == {:error, :not_owner}
      assert GroupCoordinator.join_group(c, "g", "t", :m1) == {:error, :not_owner}

      Agent.update(owns, fn _ -> true end)
      assert GroupCoordinator.validate_commit(c, "g", "t", :m1, generation, [:r0]) == :ok
    end

    test "a commit keeps a member alive as a heartbeat does; a refused one does not" do
      %{coord: c, clock: clock} = start(ranges: [:r0, :r1], session_ms: 100)
      {:ok, _} = GroupCoordinator.join_group(c, "g", "t", :m1)
      {:ok, %{generation: generation, ranges: [theirs]}} = GroupCoordinator.join_group(c, "g", "t", :m2)
      {:ok, %{ranges: [mine]}} = GroupCoordinator.heartbeat_group(c, "g", "t", :m1)

      set_clock(clock, 80)
      assert GroupCoordinator.validate_commit(c, "g", "t", :m1, generation, [mine]) == :ok
      # m2 names m1's range, which refreshes nothing
      assert GroupCoordinator.validate_commit(c, "g", "t", :m2, generation, [theirs, mine]) == {:error, :not_assigned}

      set_clock(clock, 150)
      :ok = GroupCoordinator.reconcile_now(c)
      assert {:ok, _, _} = GroupCoordinator.assignment(c, "g", "t", :m1)
      assert GroupCoordinator.assignment(c, "g", "t", :m2) == {:error, :unknown_member}
    end
  end

  describe "the reconcile tick" do
    test "is a third of the session, at most 10 s, unless given" do
      opts = [ranges_fun: fn _topic -> [] end]

      tick = fn extra ->
        {:ok, c} = GroupCoordinator.start_link(opts ++ extra)
        t = :sys.get_state(c).tick_ms
        GenServer.stop(c)
        t
      end

      assert tick.(session_ms: 300) == 100
      assert tick.(session_ms: 2) == 1
      assert tick.(session_ms: 60_000) == 10_000
      assert tick.([]) == 10_000
      assert tick.(session_ms: 300, tick_ms: 7) == 7
    end
  end
end
