defmodule Malachi.Cluster.RaResume do
  @moduledoc """
  Resume-first lifecycle for ra members: restart the local server if it ever existed on this node,
  and only fall back to forming/joining when the name was **never started here**.

  Order matters for durability. `:ra.start_cluster` (and `:ra.start_server`) REGISTER a fresh,
  empty uid for the server name before they can fail on an already-formed cluster, which orphans
  the persisted Raft log under the old uid; a subsequent restart then resurrects an AMNESIAC
  member over the empty uid. One amnesiac is masked (the leader replicates state back into it),
  but two rebooting nodes can form an empty-log quorum, elect an empty leader, and truncate the
  surviving member's real state: the storage-chaos harness caught the whole control plane wiped
  this way, with every health check green throughout. Resuming first means a registered uid is
  never overwritten, so a member always comes back with its history.
  """

  @doc """
  Restarts `server_id` in `system` if this node has ever started it; calls `never_started_fun`
  (which should form or join the cluster) only when the name is unknown to this node. Any other
  restart failure (a registered member that cannot come back: corrupt dir, bad system state) is
  returned as `{:error, reason}` rather than falling through to formation, because re-forming over
  a registered member is exactly the amnesia path described in the moduledoc.

  Returns `:ok` when resumed (or already running), otherwise `never_started_fun`'s result or the
  restart error.
  """
  @spec resume_or(atom(), :ra.server_id(), (-> result)) :: :ok | result | {:error, term()}
        when result: term()
  def resume_or(system, server_id, never_started_fun) do
    case :ra.restart_server(system, server_id) do
      :ok -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, reason} when reason in [:name_not_registered, :not_found] -> never_started_fun.()
      {:error, reason} -> {:error, reason}
    end
  end
end
