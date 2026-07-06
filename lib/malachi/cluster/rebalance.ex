defmodule Malachi.Cluster.Rebalance do
  @moduledoc """
  Executes a rebalancing plan (from `Malachi.Application.rebalance_plan/2`) against the vnodes' ra
  clusters (R3). For each change it **adds the joining members before removing the leaving ones**
  (add-before-remove, so a vnode never drops below quorum mid-move) through injected `add_member` /
  `remove_member` seams — so the executor is testable without ra, and R3-b supplies the real ra ops.

  It is **idempotent**: the seams must treat an already-present add / already-gone remove as `:ok`, so an
  interrupted commit can be re-run. It is **fail-fast**: within a change the first failing add stops it
  (the removes are not attempted, protecting quorum); across a plan the first failing change stops the
  run, returning what was applied so a later commit resumes. Between changes it re-checks the `leader?`
  seam and stops if leadership was lost mid-commit (the lease holder dropped the lease).
  """

  @type change :: %{vnode_id: atom(), add: [node()], remove: [node()]}
  @type member_op :: (atom(), node() -> :ok | {:error, term()})
  @type failure :: {:add | :remove, atom(), node(), term()} | :lost_leadership

  @doc """
  Applies one change: adds every `add` member, then removes every `remove` member (never the reverse),
  stopping at the first failure. Returns `:ok` or `{:error, {step, vnode_id, node, reason}}`.
  """
  @spec apply_change(change(), member_op(), member_op()) :: :ok | {:error, failure()}
  def apply_change(%{vnode_id: vnode_id, add: add, remove: remove}, add_member, remove_member) do
    with :ok <- each(add, :add, vnode_id, fn node -> add_member.(vnode_id, node) end) do
      each(remove, :remove, vnode_id, fn node -> remove_member.(vnode_id, node) end)
    end
  end

  @doc """
  Applies a whole plan one change at a time, fail-fast. Before each change it checks `leader?` (default
  always) and stops if leadership was lost. Returns `{:ok, applied_vnode_ids}` or
  `{:error, {applied_vnode_ids, failure}}`; idempotent, so re-running resumes.
  """
  @spec apply_plan([change()], member_op(), member_op(), (-> boolean())) ::
          {:ok, [atom()]} | {:error, {[atom()], failure()}}
  def apply_plan(plan, add_member, remove_member, leader? \\ fn -> true end) do
    do_apply_plan(plan, add_member, remove_member, leader?, [])
  end

  defp do_apply_plan([], _add_member, _remove_member, _leader?, applied) do
    {:ok, Enum.reverse(applied)}
  end

  defp do_apply_plan([change | rest], add_member, remove_member, leader?, applied) do
    if leader?.() do
      case apply_change(change, add_member, remove_member) do
        :ok ->
          do_apply_plan(rest, add_member, remove_member, leader?, [change.vnode_id | applied])

        {:error, failure} ->
          {:error, {Enum.reverse(applied), failure}}
      end
    else
      {:error, {Enum.reverse(applied), :lost_leadership}}
    end
  end

  # Applies `op` to each node in order, halting at the first error (fail-fast).
  defp each(nodes, step, vnode_id, op) do
    Enum.reduce_while(nodes, :ok, fn node, :ok ->
      case op.(node) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {step, vnode_id, node, reason}}}
      end
    end)
  end
end
