defmodule Malachi.Retention.Expirer do
  @moduledoc """
  Retention's only production delete path: drop one expired segment from the control plane, then delete
  its stored bytes on the replicas, **and only then**.

  ## Why the order and the condition carry the whole module

  The control plane is the authority on which segments exist, so its answer decides whether the bytes
  under a segment are still owed to a reader. `Malachi.Metadata` refuses a delete while the topic is
  migrating between vnodes, refuses one for a segment that is still the write head, and can answer
  neither way when Raft times out. Deleting the files regardless leaves a segment the control plane
  still lists with no copy anywhere: `Malachi.Cluster.SelfHealing` measures every replica short of the
  recorded `byte_size`, finds no intact copy to repair from, and a consumer positioned inside that
  segment reads `:eof` until some later sweep finally gets `:ok`. The rule lives in
  `Malachi.Cluster.Retention.delete_replicas?/1`, which is pure and says why for each answer.

  ## What it reports, and what it deliberately does not

  Three outcomes, three channels, chosen so a cluster in trouble does not drown its own log:

    * **A refusal** is counted, not logged. It is expected and self-correcting, and a vnode split moving
      a busy topic refuses every one of its sealed segments on every sweep, which would be a line per
      segment per minute for as long as the migration runs. `malachi_retention_expire_failures_total`
      already carries it, labeled by the reply.
    * **A replica that did not answer** leaves a directory on its disk that no later sweep can name,
      because a segment gone from the control plane never comes back from
      `Malachi.Cluster.Retention.expired/3`. That is counted as
      `malachi_retention_orphan_directories_left_total`, the other half of the sweeper's reclaim count:
      the two together say whether `Malachi.Retention.OrphanSweeper` is keeping up, and the first still
      moves when the sweeper is turned off.
    * **A control-plane call that did not answer at all** is logged, because it means the broker on this
      node is not serving, which is a fact about the node rather than about the segment.

  ## Seams

  `:delete_command` and `:delete_replica` exist so the refusal and the unreachable replica can be
  exercised without a wedged cluster. Their defaults are the real ones.
  """

  require Logger

  alias Malachi.BrokerServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.Retention
  alias Malachi.I18n
  alias Malachi.Metadata
  alias Malachi.Telemetry

  @doc """
  Expires `segment` through `broker`, answering what the control plane answered.

  The answer is the caller's telemetry (`Malachi.Cluster.RetentionCoordinator` turns it into a label),
  so it travels back unchanged whatever this function did with the replicas.

  ## Options

    * `:delete_command` - `(segment_id -> reply)`, how the segment is dropped from the control plane
      (default `Malachi.BrokerServer.delete_segment/2` against `broker`);
    * `:delete_replica` - `(replica, segment_id -> :ok | {:error, term()})`, how one replica's stored
      bytes are removed (default `Malachi.Cluster.ReplicationServer.delete/2`).
  """
  @spec expire(Metadata.segment_meta(), GenServer.server(), keyword()) :: term()
  def expire(segment, broker \\ Malachi.LogBroker, opts \\ []) do
    delete_command = Keyword.get(opts, :delete_command, &BrokerServer.delete_segment(broker, &1))
    delete_replica = Keyword.get(opts, :delete_replica, &ReplicationServer.delete/2)

    reply = drop(delete_command, segment)

    if Retention.delete_replicas?(reply), do: delete_replicas(segment, delete_replica)

    reply
  end

  # `BrokerServer.delete_segment/2` is a plain `GenServer.call/2`, so a broker that is restarting or
  # wedged exits the caller after the default timeout instead of answering. The caller is the retention
  # coordinator mid-sweep: letting the exit through costs the rest of that sweep, and the answer this
  # module branches on never arrives. Caught here into an answer that reads as "no", which is the side
  # that keeps the bytes.
  defp drop(delete_command, segment) do
    delete_command.(segment.id)
  catch
    :exit, reason ->
      Logger.warning(I18n.t(:retention_expire_call_failed, segment: inspect(segment.id), reason: inspect(reason)))

      {:error, :call_failed}
  end

  defp delete_replicas(segment, delete_replica) do
    Enum.each(segment.replica_set, fn replica ->
      case delete_replica.(replica, segment.id) do
        :ok -> :ok
        {:error, _reason} -> Telemetry.retention_orphan_left(elem(segment.range_id, 0))
      end
    end)
  end
end
