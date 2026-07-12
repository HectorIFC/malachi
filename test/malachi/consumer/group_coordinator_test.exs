defmodule Malachi.Consumer.GroupCoordinatorTest do
  use ExUnit.Case, async: true

  alias Malachi.Consumer.GroupCoordinator

  # Start a coordinator with a controllable clock and range set; the auto-timer is pushed far out so each
  # test drives reconcile explicitly via reconcile_now/1.
  defp start(opts \\ []) do
    {:ok, clock} = Agent.start_link(fn -> 0 end)
    {:ok, ranges} = Agent.start_link(fn -> Keyword.get(opts, :ranges, [:r0, :r1, :r2, :r3]) end)

    coord =
      start_supervised!(
        {GroupCoordinator,
         clock: fn -> Agent.get(clock, & &1) end,
         ranges_fun: fn _topic -> Agent.get(ranges, & &1) end,
         session_ms: Keyword.get(opts, :session_ms, 100),
         tick_ms: 3_600_000}
      )

    %{coord: coord, clock: clock, ranges: ranges}
  end

  defp set_clock(clock, t), do: Agent.update(clock, fn _ -> t end)
  defp set_ranges(ranges, rs), do: Agent.update(ranges, fn _ -> rs end)

  test "a lone member is assigned all ranges at generation 1" do
    %{coord: c} = start(ranges: [:r0, :r1])
    assert {:ok, 1, ranges} = GroupCoordinator.join(c, "g", "t", :m1)
    assert Enum.sort(ranges) == [:r0, :r1]
  end

  test "two members partition the ranges (disjoint, complete) and the generation advances" do
    %{coord: c} = start(ranges: [:r0, :r1, :r2, :r3])
    {:ok, g1, _} = GroupCoordinator.join(c, "g", "t", :m1)
    {:ok, g2, m2} = GroupCoordinator.join(c, "g", "t", :m2)
    assert g2 > g1

    {:ok, ^g2, m1} = GroupCoordinator.heartbeat(c, "g", "t", :m1)
    assert Enum.sort(m1 ++ m2) == [:r0, :r1, :r2, :r3]
    assert m1 -- m2 == m1
  end

  test "leave gives the ranges back to the survivor and advances the generation" do
    %{coord: c} = start(ranges: [:r0, :r1])
    {:ok, _, _} = GroupCoordinator.join(c, "g", "t", :m1)
    {:ok, g2, _} = GroupCoordinator.join(c, "g", "t", :m2)

    :ok = GroupCoordinator.leave(c, "g", "t", :m2)

    {:ok, g3, ranges} = GroupCoordinator.heartbeat(c, "g", "t", :m1)
    assert g3 > g2
    assert Enum.sort(ranges) == [:r0, :r1]
  end

  test "a member silent past the session window is evicted on reconcile and must rejoin" do
    %{coord: c, clock: clock} = start(ranges: [:r0, :r1], session_ms: 100)
    GroupCoordinator.join(c, "g", "t", :m1)
    GroupCoordinator.join(c, "g", "t", :m2)

    set_clock(clock, 50)
    GroupCoordinator.heartbeat(c, "g", "t", :m1)

    # cutoff = 120 - 100 = 20: m2 (hb 0) is evicted, m1 (hb 50) survives
    set_clock(clock, 120)
    :ok = GroupCoordinator.reconcile_now(c)

    assert {:error, :unknown_member} = GroupCoordinator.heartbeat(c, "g", "t", :m2)
    {:ok, _, ranges} = GroupCoordinator.heartbeat(c, "g", "t", :m1)
    assert Enum.sort(ranges) == [:r0, :r1]
  end

  test "a group with no members is dropped" do
    %{coord: c} = start(ranges: [:r0])
    GroupCoordinator.join(c, "g", "t", :m1)
    :ok = GroupCoordinator.leave(c, "g", "t", :m1)
    assert {:error, :unknown_member} = GroupCoordinator.assignment(c, "g", "t", :m1)
  end

  test "a change in the topic's ranges rebalances on reconcile" do
    %{coord: c, ranges: ranges} = start(ranges: [:r0])
    {:ok, g1, [:r0]} = GroupCoordinator.join(c, "g", "t", :m1)

    set_ranges(ranges, [:r0, :r1, :r2])
    :ok = GroupCoordinator.reconcile_now(c)

    {:ok, g2, rs} = GroupCoordinator.heartbeat(c, "g", "t", :m1)
    assert g2 > g1
    assert Enum.sort(rs) == [:r0, :r1, :r2]
  end

  test "reconcile is idempotent: no membership or range change leaves the generation put" do
    %{coord: c} = start(ranges: [:r0])
    {:ok, g1, _} = GroupCoordinator.join(c, "g", "t", :m1)

    :ok = GroupCoordinator.reconcile_now(c)

    {:ok, g2, _} = GroupCoordinator.heartbeat(c, "g", "t", :m1)
    assert g2 == g1
  end

  test "heartbeat for an unknown group or member is rejected" do
    %{coord: c} = start()
    assert {:error, :unknown_member} = GroupCoordinator.heartbeat(c, "nope", "t", :m1)
  end
end
