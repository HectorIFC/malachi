defmodule Malachi.Cluster.ClusterFlagsServer do
  @moduledoc """
  A thin named facade over `Malachi.Cluster.RaCluster` for the cluster's **feature flags**
  (`Malachi.Cluster.ClusterFlagsMachine`): start the dedicated Raft cluster, switch a flag on once
  every node can handle it, and read which flags are on.

  The store is formed over the statically configured node set in **every** deployment mode, single node
  included, the same way the user, lockout and ACL stores are. A one-node deployment still has to be
  able to commit to a format-changing feature, and the ring store, which only exists on a clustered
  node, therefore could not host this.

  ## The two barriers, and why both are needed

  `enable/5` refuses before it writes anything unless every configured node is alive and advertises the
  capability the flag names (`Malachi.Cluster.Capabilities`). Independently, `{:enable_flag, flag}` was
  introduced at machine version 2, so `ra` refuses it identically on every member until the last one
  runs a build that supports 2.

  They are not redundant. The machine version says every member of **this Raft group** runs new enough
  code, which during a rolling upgrade is only the nodes already upgraded, because a member on the old
  build has no `ClusterFlagsMachine` to start at all. The capability check says every node the operator
  configured, upgraded or not, can handle the feature. The first protects the Raft log; the second
  protects the data plane, where there is no log to refuse anything.

  ## Why the check runs here and not in `apply/3`

  It reads the SWIM membership view, which is node-local and changes with time. Reading it inside the
  state machine would make `apply/3` non-deterministic and diverge the replicas, which is the whole
  failure this store exists to prevent. So the check is an admission check in the caller, and the
  command itself is unconditional. That is safe because the flag is idempotent and one-way: two
  operators racing converge, and a check that was passed a moment ago cannot become false, since a node
  only ever gains capabilities by being upgraded.
  """

  require Logger

  alias Malachi.Cluster.Capabilities
  alias Malachi.Cluster.ClusterFlags
  alias Malachi.Cluster.ClusterFlagsMachine
  alias Malachi.Cluster.RaCluster
  alias Malachi.I18n

  @type cluster_name :: atom()
  @type server_id :: {cluster_name(), node()}

  @typedoc "How a read reaches the state: through consensus, or from the local replica."
  @type mode :: :consistent | :local

  @doc """
  Starts the flag store's Raft cluster named `cluster_name` across `nodes` (default the local node) and
  returns a `server_id` addressing a real member (the local node when it is one, else the first).
  """
  @spec start(cluster_name(), [node()]) :: {:ok, server_id()} | {:error, term()}
  def start(cluster_name, nodes \\ [node()]) do
    RaCluster.start(ClusterFlagsMachine, cluster_name, nodes)
  end

  @doc """
  Keeps this node joined to the flag store, so a staggered boot converges to a fully-replicated store.
  Idempotent and best-effort; meant to be called periodically until the node has joined.

  During a rolling upgrade the nodes still on the old build have no machine module to start, so they
  simply are not members yet. That is why the capability check in `enable/5` counts the configured node
  set rather than this store's members.
  """
  @spec reconcile(cluster_name(), [node()]) :: :ok
  def reconcile(cluster_name, nodes), do: RaCluster.reconcile(ClusterFlagsMachine, cluster_name, nodes)

  @doc """
  Switches the flag named `name` on, once every node in `nodes` advertises the capability it names.

  Answers `{:error, :unknown_flag}` for a name this build does not know, `{:error, {:unsupported,
  nodes}}` naming the nodes that do not advertise it (refusing tells the operator **who** to upgrade,
  not merely no), or `{:error, reason}` when the store cannot be written.

  `reads` is the membership view as `node -> {status, attributes}`, and `known` the registry the name
  is resolved against, both injected so the decision is testable without a cluster.
  """
  @spec enable(server_id(), String.t(), [node()], Capabilities.reads(), [Capabilities.capability()]) ::
          :ok | {:error, term()}
  def enable(server_id, name, nodes, reads, known \\ Capabilities.known()) do
    with {:ok, flag} <- Capabilities.resolve(name, known),
         :ok <- admit(flag, nodes, reads) do
      submit(server_id, flag)
    end
  end

  @doc """
  Reads the flags, through consensus (`:consistent`) or from the local replica (`:local`).

  A node's first read is consistent, because the answer decides whether it may serve at all and a
  replica that has not caught up would answer that no flag is on. Every read after that is local: the
  local cache is allowed to lag, since a flag only ever goes on and a node that has not noticed one yet
  simply keeps to the old behaviour, which every node still understands.
  """
  @spec read(server_id(), mode()) :: {:ok, ClusterFlags.t()} | {:error, term()}
  def read(server_id, :consistent) do
    case RaCluster.query(server_id) do
      {:ok, %ClusterFlags{} = flags} -> {:ok, flags}
      {:error, reason} -> {:error, reason}
    end
  end

  def read(server_id, :local), do: RaCluster.local_query(server_id, & &1)

  @doc "Stops and deletes the flag store's Raft cluster, removing its on-disk state."
  @spec delete(server_id() | cluster_name()) :: :ok | {:error, term()}
  def delete(target), do: RaCluster.delete(target)

  # The admission check, with the refusal logged on the node that was asked: the operator sees it
  # through the CLI, and the cluster's own log keeps the record of who was missing and when.
  defp admit(flag, nodes, reads) do
    case Capabilities.supported_by_all(nodes, flag, reads) do
      :ok ->
        :ok

      {:error, {:unsupported, missing}} = refusal ->
        Logger.warning(
          I18n.t(:cluster_flag_enable_refused, flag: flag, nodes: Enum.map_join(missing, ", ", &to_string/1))
        )

        refusal
    end
  end

  defp submit(server_id, flag) do
    case RaCluster.command(server_id, {:enable_flag, flag}) do
      {:ok, :ok} ->
        Logger.info(I18n.t(:cluster_flag_enabled, flag: flag))
        :ok

      {:ok, {:error, reason}} ->
        {:error, reason}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
