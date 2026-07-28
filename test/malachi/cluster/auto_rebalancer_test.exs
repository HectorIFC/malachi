defmodule Malachi.Cluster.AutoRebalancerTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.AutoRebalancer

  # A long interval effectively disables the auto timer; each test drives one reconcile via reconcile_now.
  defp start(opts) do
    test = self()

    defaults = [
      plan_fun: fn -> [] end,
      commit_fun: fn ->
        send(test, :committed)
        {:ok, [:vnode1]}
      end,
      leader?: fn -> true end,
      interval: 3_600_000,
      stabilization: 3,
      on_result: fn result -> send(test, {:result, result}) end
    ]

    start_supervised!({AutoRebalancer, Keyword.merge(defaults, opts)})
  end

  test "commits after the plan is stable for `stabilization` ticks" do
    {:ok, plan} = Agent.start_link(fn -> [:plan_a] end)
    ar = start(plan_fun: fn -> Agent.get(plan, & &1) end, stabilization: 3)

    assert {[:plan_a], 1} = AutoRebalancer.reconcile_now(ar)
    refute_received :committed
    assert {[:plan_a], 2} = AutoRebalancer.reconcile_now(ar)
    refute_received :committed

    # third consecutive identical plan → commit, then reset
    assert {[], 0} = AutoRebalancer.reconcile_now(ar)
    assert_received :committed
    assert_received {:result, {:ok, [:vnode1]}}
  end

  test "a changed plan restarts the stabilization window" do
    {:ok, plan} = Agent.start_link(fn -> [:plan_a] end)
    ar = start(plan_fun: fn -> Agent.get(plan, & &1) end, stabilization: 3)

    assert {[:plan_a], 1} = AutoRebalancer.reconcile_now(ar)
    assert {[:plan_a], 2} = AutoRebalancer.reconcile_now(ar)

    # a membership flap changes the plan before it stabilizes
    Agent.update(plan, fn _ -> [:plan_b] end)
    assert {[:plan_b], 1} = AutoRebalancer.reconcile_now(ar)
    refute_received :committed
  end

  test "an empty plan never commits and keeps the counter at zero" do
    ar = start(plan_fun: fn -> [] end, stabilization: 1)

    assert {[], 0} = AutoRebalancer.reconcile_now(ar)
    assert {[], 0} = AutoRebalancer.reconcile_now(ar)
    refute_received :committed
  end

  test "does not commit when this node is not the leader" do
    ar = start(plan_fun: fn -> [:plan_a] end, leader?: fn -> false end, stabilization: 1)

    assert {[], 0} = AutoRebalancer.reconcile_now(ar)
    refute_received :committed
  end

  test "stabilization 1 commits on the first non-empty plan" do
    ar = start(plan_fun: fn -> [:plan_a] end, stabilization: 1)

    assert {[], 0} = AutoRebalancer.reconcile_now(ar)
    assert_received :committed
  end

  test "stabilization below 1 is clamped to 1 (does not defeat flap protection into an every-tick commit)" do
    ar = start(plan_fun: fn -> [:plan_a] end, stabilization: 0)

    assert {[], 0} = AutoRebalancer.reconcile_now(ar)
    assert_received :committed
  end

  test "losing leadership mid-window resets the counter, and regaining it restarts fresh" do
    {:ok, leader} = Agent.start_link(fn -> true end)
    ar = start(plan_fun: fn -> [:plan_a] end, leader?: fn -> Agent.get(leader, & &1) end, stabilization: 3)

    assert {[:plan_a], 1} = AutoRebalancer.reconcile_now(ar)

    Agent.update(leader, fn _ -> false end)
    assert {[], 0} = AutoRebalancer.reconcile_now(ar)

    Agent.update(leader, fn _ -> true end)
    assert {[:plan_a], 1} = AutoRebalancer.reconcile_now(ar)
    refute_received :committed
  end

  test "a partial-failure commit result is passed to on_result" do
    ar =
      start(
        plan_fun: fn -> [:plan_a] end,
        stabilization: 1,
        commit_fun: fn -> {:error, {[:vnode1], {:vnode2, :cluster_change_not_permitted}}} end
      )

    AutoRebalancer.reconcile_now(ar)
    assert_received {:result, {:error, {[:vnode1], {:vnode2, :cluster_change_not_permitted}}}}
  end
end
