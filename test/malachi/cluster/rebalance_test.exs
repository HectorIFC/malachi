defmodule Malachi.Cluster.RebalanceTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.Rebalance

  # A recording seam: each call appends {op, vnode_id, node} to the agent and returns the programmed
  # result for that node (default :ok). `fails` maps a node to {:error, reason}.
  defp recorder(agent, op, fails \\ %{}) do
    fn vnode_id, node ->
      Agent.update(agent, &[{op, vnode_id, node} | &1])
      Map.get(fails, node, :ok)
    end
  end

  defp calls(agent), do: agent |> Agent.get(& &1) |> Enum.reverse()

  describe "apply_change/3" do
    test "adds every joining member before removing any leaving one" do
      {:ok, log} = Agent.start_link(fn -> [] end)
      change = %{vnode_id: :vn_0, add: [:d@h, :e@h], remove: [:a@h, :b@h]}

      assert Rebalance.apply_change(change, recorder(log, :add), recorder(log, :remove)) == :ok

      assert calls(log) == [
               {:add, :vn_0, :d@h},
               {:add, :vn_0, :e@h},
               {:remove, :vn_0, :a@h},
               {:remove, :vn_0, :b@h}
             ]
    end

    test "a failing add stops the change before any remove (protecting quorum)" do
      {:ok, log} = Agent.start_link(fn -> [] end)
      change = %{vnode_id: :vn_0, add: [:d@h], remove: [:a@h]}
      add = recorder(log, :add, %{d@h: {:error, :boom}})

      assert Rebalance.apply_change(change, add, recorder(log, :remove)) ==
               {:error, {:add, :vn_0, :d@h, :boom}}

      # the remove was never attempted
      assert calls(log) == [{:add, :vn_0, :d@h}]
    end

    test "a failing remove is reported after the adds succeeded" do
      {:ok, log} = Agent.start_link(fn -> [] end)
      change = %{vnode_id: :vn_0, add: [:d@h], remove: [:a@h]}
      remove = recorder(log, :remove, %{a@h: {:error, :nope}})

      assert Rebalance.apply_change(change, recorder(log, :add), remove) ==
               {:error, {:remove, :vn_0, :a@h, :nope}}

      assert calls(log) == [{:add, :vn_0, :d@h}, {:remove, :vn_0, :a@h}]
    end

    test "idempotent seams (already-present add / already-gone remove return :ok) yield :ok" do
      {:ok, log} = Agent.start_link(fn -> [] end)
      change = %{vnode_id: :vn_0, add: [:d@h], remove: [:a@h]}
      assert Rebalance.apply_change(change, recorder(log, :add), recorder(log, :remove)) == :ok
    end
  end

  describe "apply_plan/4" do
    test "applies every change and returns the applied vnode ids in order" do
      {:ok, log} = Agent.start_link(fn -> [] end)

      plan = [
        %{vnode_id: :vn_0, add: [:d@h], remove: [:a@h]},
        %{vnode_id: :vn_1, add: [:e@h], remove: [:b@h]}
      ]

      assert Rebalance.apply_plan(plan, recorder(log, :add), recorder(log, :remove)) ==
               {:ok, [:vn_0, :vn_1]}
    end

    test "fail-fast: stops at the first failing change, returning what was applied" do
      {:ok, log} = Agent.start_link(fn -> [] end)

      plan = [
        %{vnode_id: :vn_0, add: [:d@h], remove: [:a@h]},
        %{vnode_id: :vn_1, add: [:e@h], remove: [:b@h]},
        %{vnode_id: :vn_2, add: [:f@h], remove: [:c@h]}
      ]

      add = recorder(log, :add, %{e@h: {:error, :boom}})

      assert Rebalance.apply_plan(plan, add, recorder(log, :remove)) ==
               {:error, {[:vn_0], {:add, :vn_1, :e@h, :boom}}}

      # vn_2 was never touched
      refute Enum.any?(calls(log), fn {_op, vnode_id, _node} -> vnode_id == :vn_2 end)
    end

    test "stops when leadership is lost mid-commit, before touching the next change" do
      {:ok, log} = Agent.start_link(fn -> [] end)
      # leader on the first check, not on the second
      {:ok, leader_calls} = Agent.start_link(fn -> 0 end)

      leader? = fn ->
        n = Agent.get_and_update(leader_calls, &{&1, &1 + 1})
        n < 1
      end

      plan = [
        %{vnode_id: :vn_0, add: [:d@h], remove: [:a@h]},
        %{vnode_id: :vn_1, add: [:e@h], remove: [:b@h]}
      ]

      assert Rebalance.apply_plan(plan, recorder(log, :add), recorder(log, :remove), leader?) ==
               {:error, {[:vn_0], :lost_leadership}}

      # vn_1 was never touched once leadership was lost
      refute Enum.any?(calls(log), fn {_op, vnode_id, _node} -> vnode_id == :vn_1 end)
    end
  end
end
