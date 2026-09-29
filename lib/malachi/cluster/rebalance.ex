defmodule Malachi.Cluster.Rebalance do
  @moduledoc """
  Executes a rebalancing plan (from `Malachi.Application.rebalance_plan/2`) against the vnodes' ra
  clusters. For each change it **adds the joining members before removing the leaving ones**
  (add-before-remove, so a vnode never drops below quorum mid-move) through injected `add_member` /
  `remove_member` seams, so the executor is testable without ra, and R3-b supplies the real ra ops.

  It is **idempotent**: the seams must treat an already-present add / already-gone remove as `:ok`, so an
  interrupted commit can be re-run. It is **fail-fast**: within a change the first failing add stops it
  (the removes are not attempted, protecting quorum); across a plan the first failing change stops the
  run, returning what was applied so a later commit resumes. Between changes it re-checks the `leader?`
  seam and stops if leadership was lost mid-commit (the lease holder dropped the lease).
  """

  alias Malachi.Cluster.MetadataMachine

  @system :default
  @machine {:module, MetadataMachine, %{}}

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

  @doc """
  Adds `new_node` to vnode `vnode_id`'s ra cluster (the real `add_member` seam `apply_plan/4` uses in
  production): starts the ra server on `new_node` (via `:erpc`, as a member of the existing cluster) then
  adds it to the consensus, routing the change through `current_members` (any of them reaches the
  leader). **Idempotent**. An already-running server or already-present member counts as `:ok`. On
  success ra replicates the vnode's state (log/snapshot) to the new member automatically.
  """
  @spec ra_add_member(atom(), node(), [node()]) :: :ok | {:error, term()}
  def ra_add_member(vnode_id, new_node, current_members) do
    server_ids = Enum.map(current_members, &{vnode_id, &1})

    # announce the member to the cluster first (it need not be running yet), then start its server, which
    # the leader then replicates the vnode's log/snapshot to (the order ra's add_member doc prescribes)
    with :ok <- join_consensus(server_ids, {vnode_id, new_node}) do
      start_member(vnode_id, new_node, server_ids)
    end
  end

  @doc """
  Removes `leaving_node` from vnode `vnode_id`'s ra cluster: removes it from the consensus (routing
  through `current_members`), and once the group has **committed** that, deletes its server.
  **Idempotent**. A non-member counts as `:ok`. Within one call, a leave that took longer to commit than
  ra's call timeout is heard as a non-member, and that member is stopped rather than deleted (see the
  comment on `not_member` below). A leave still uncommitted when the retries run out answers an error and
  touches nothing; the planner then never lists that member again, since it plans from the group's live
  membership, so it stays registered on its node, which only resumes it when its group cannot answer.

  Deleted, not stopped: a stopped server stays registered on its node, and a node that comes back resumes
  its registered vnode members (`Malachi.Application.resume_local_vnodes/3`). Once the group has let the
  member go, its log describes a membership the group no longer has, and deleting it is what keeps a
  restart of that node from bringing it back.

  Only after the commit (`:ra.leave_and_delete_server/3`, which waits for consensus), never on the
  append that `:ra.remove_member` answers on: until the leave commits, the leaving member may hold the
  only other copy of an entry the group acknowledged, with the member the rebalance just added still
  catching up, and deleting its log then would leave that entry on the leader's disk alone.
  """
  @spec ra_remove_member(atom(), node(), [node()], keyword()) :: :ok | {:error, term()}
  def ra_remove_member(vnode_id, leaving_node, current_members, opts \\ []) do
    leave = Keyword.get(opts, :leave, &:ra.leave_and_delete_server/3)
    stop = Keyword.get(opts, :stop, &:ra.stop_server/2)
    server_ids = Enum.map(current_members, &{vnode_id, &1})

    change_membership(fn -> leave_then_delete(leave, stop, server_ids, {vnode_id, leaving_node}) end)
  end

  # Once the leave has committed, ra deletes the member through rpcs to its node, and a node that is down, or
  # goes down in between, fails them: that is the ordinary case of replacing a dead node's replica. The leave
  # itself never answers a badrpc nor exits, so each of these can only mean the group has already let the
  # member go, and the removal stands. What stays behind is a member registered on its node that its group
  # no longer counts. `Malachi.Application.resume_local_vnodes/3` asks the group before resuming it, so it
  # stays down whenever the group has a leader that answers within that tick's poll. When none does (the
  # whole group restarting, or an election under way), it is resumed as a follower whose log is behind the
  # members that removed it: it cannot win an election against them, and the group does not count it.
  # Reporting it as a failure would stop the rest of the plan at the first dead node, and let the delete's
  # exit take the rebalancing coordinator down with it.
  #
  # `not_member` is how this call hears about its own leave when that leave took longer to commit than ra's
  # call timeout: ra resends it, the leader refuses the resend while the change is pending, and once it has
  # committed answers that `target` is not a member. ra then deleted nothing, and the member keeps running.
  # It is stopped here, not deleted: `not_member` comes from whichever server believes it leads, and ra has
  # no check-quorum, so an old leader cut off from its group answers it from an old membership, about a
  # member the group has since added. Stopping is safe either way, since a member its group counts is
  # resumed by its own node (`Malachi.Application.resume_local_vnodes/3`); deleting on that answer would
  # destroy a live member's log.
  defp leave_then_delete(leave, stop, server_ids, target) do
    case leave.(@system, server_ids, target) do
      {:error, :not_member} -> stop_removed(stop, target)
      other -> after_committed_leave(other)
    end
  catch
    :exit, _reason -> :ok
  end

  defp stop_removed(stop, target) do
    _ = stop.(@system, target)
    :ok
  catch
    :exit, _reason -> :ok
  end

  defp after_committed_leave({:badrpc, _reason}), do: :ok
  defp after_committed_leave({:error, {:badrpc, _reason}}), do: :ok
  defp after_committed_leave({:error, reason}) when reason in [:system_not_started, :not_found], do: :ok
  defp after_committed_leave(other), do: other

  defp start_member(vnode_id, new_node, server_ids) do
    case safe_erpc(new_node, :ra, :start_server, [@system, vnode_id, {vnode_id, new_node}, @machine, server_ids]) do
      :ok -> :ok
      {:error, reason} -> if already_started?(reason), do: :ok, else: {:error, reason}
    end
  end

  # ra wraps an already-running server as {:already_started, pid}, possibly nested inside a supervisor
  # start failure; either shape means the member's server is up, which is what we want (idempotent).
  defp already_started?({:already_started, _pid}), do: true
  defp already_started?({:shutdown, {:failed_to_start_child, _id, reason}}), do: already_started?(reason)
  defp already_started?(_other), do: false

  defp join_consensus(server_ids, new_server) do
    change_membership(fn -> :ra.add_member(server_ids, new_server) end)
  end

  # Runs a membership change (add/remove), treating already-done as :ok (idempotent) and retrying while
  # a prior change is still settling (ra allows one membership change at a time, so add-then-remove on the
  # same vnode - or a repeated op - would otherwise get :cluster_change_not_permitted).
  defp change_membership(op, remaining_ms \\ 5_000) do
    case op.() do
      {:ok, _reply, _leader} ->
        :ok

      # `:ra.leave_and_delete_server/3` answers bare atoms: the leave committed and the server is gone,
      # or the leave did not commit in time (and nothing was deleted).
      :ok ->
        :ok

      :timeout ->
        {:error, :timeout}

      {:error, benign} when benign in [:already_member, :not_member] ->
        :ok

      {:error, :cluster_change_not_permitted} when remaining_ms > 0 ->
        Process.sleep(100)
        change_membership(op, remaining_ms - 100)

      {:error, reason} ->
        {:error, reason}

      {:timeout, _server} ->
        {:error, :timeout}
    end
  end

  # :erpc.call raises on rpc/node errors; normalize to {:error, _} so callers see a uniform result.
  defp safe_erpc(node, module, fun, args) do
    :erpc.call(node, module, fun, args)
  catch
    kind, reason -> {:error, {kind, reason}}
  end
end
