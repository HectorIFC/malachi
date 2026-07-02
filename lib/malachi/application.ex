defmodule Malachi.Application do
  @moduledoc """
  Main application supervisor for Malachi.

  Coordinates all core services including:
  - Queue management and partitioning
  - TCP/TLS server for client connections
  - Metrics collection and monitoring
  - Authentication and authorization
  - Web dashboard
  - Message acknowledgment tracking
  """
  use Application
  require Logger
  alias Malachi.Auth.ConfigValidator
  alias Malachi.BrokerServer
  alias Malachi.Cluster.HealCoordinator
  alias Malachi.Cluster.MembershipServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.RetentionCoordinator
  alias Malachi.I18n
  alias Malachi.TLSValidator

  def start(_type, _args) do
    # Validate authentication configuration before starting
    # This prevents insecure deployments in production
    ConfigValidator.validate!(Application.get_env(:malachi, :config_env, :prod))

    # Validate TLS configuration
    # In production: raises on invalid config (fail fast)
    # In dev/test: logs warnings only
    TLSValidator.validate!(Application.get_env(:malachi, :config_env, :prod))

    port = Application.get_env(:malachi, :tcp_port, 4040)
    dashboard_port = Application.get_env(:malachi, :dashboard_port, 4041)

    available_schedulers = System.schedulers_online()
    configured_schedulers = Application.get_env(:malachi, :schedulers, available_schedulers)
    schedulers_to_use = min(configured_schedulers, available_schedulers)

    :erlang.system_flag(:schedulers_online, schedulers_to_use)

    children =
      [
        {Registry, keys: :unique, name: Malachi.QueueRegistry, partitions: System.schedulers_online()},
        {Registry, keys: :unique, name: Malachi.ChannelRegistry},
        {DynamicSupervisor, name: Malachi.QueueSupervisor, strategy: :one_for_one, max_children: 100_000},
        {DynamicSupervisor, name: Malachi.ChannelSupervisor, strategy: :one_for_one, max_children: 100_000},
        # Increase max_children for Task.Supervisor to allow large parallel broadcasts
        {Task.Supervisor, name: Malachi.TaskSupervisor, max_children: 200_000},
        Malachi.PartitionManager,
        Malachi.QueueConfig,
        Malachi.Metrics,
        # Audit logging (must start early for security event tracking)
        Malachi.AuditLog,
        # Resource monitors (must start after AuditLog for alert logging)
        Malachi.AtomMonitor,
        Malachi.MemoryMonitor,
        # Account lockout manager (must start before Auth)
        Malachi.Auth.LockoutManager,
        # Input validation with ETS cache (must start before Queue/Channel creation)
        Malachi.Validator,
        Malachi.RateLimiter,
        Malachi.ConnectionLimiter,
        # User persistence (must start before Auth to load persisted users into ETS)
        Malachi.Auth.UserStore,
        Malachi.Auth,
        Malachi.AckManager,
        Malachi.ConnectionRegistry
        # NorthGuard log stack (reachable by clients via the log protocol actions). Single-node/
        # in-memory by default; with :log_cluster configured the control plane is replicated over `ra`
        # (HA metadata) and the data plane is replicated across nodes (see `log_children/0`).
      ] ++
        log_children() ++
        [
          {Malachi.TCPAcceptorPool, port},
          {Malachi.Dashboard, dashboard_port}
        ]

    opts = [strategy: :one_for_one, name: Malachi.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp log_data_dir do
    Application.get_env(:malachi, :log_data_dir, Path.join(System.tmp_dir!(), "malachi_log"))
  end

  # The log stack's supervised children. Single-node (no :log_cluster): just the BrokerServer, which
  # owns a local ReplicationServer. Clustered: start `ra`, plus a named ReplicationServer (this node's
  # data-plane broker) and the BrokerServer wired to every node's ReplicationServer with a replication
  # factor. The ReplicationServer must precede the BrokerServer (the latter references it).
  defp log_children do
    cluster = Application.get_env(:malachi, :log_cluster)

    nodes =
      case Application.get_env(:malachi, :log_nodes, []) do
        [] -> [node()]
        configured -> configured
      end

    log_stack =
      if cluster do
        start_ra!()
        # Order matters (one_for_one starts in order): membership feeds live_brokers; replication must
        # precede the broker that references it; the healer references the broker + membership.
        [membership_child(nodes), replication_child(), log_broker_child(cluster, nodes), healer_child()]
      else
        [log_broker_child(nil, nodes)]
      end

    # Retention runs whenever a policy is configured (it matters single-node too); it references the
    # broker, so it comes last.
    log_stack ++ retention_children()
  end

  defp retention_children do
    if retention_configured?(), do: [retention_child()], else: []
  end

  defp retention_child do
    %{
      id: Malachi.LogRetention,
      start:
        {RetentionCoordinator, :start_link,
         [
           [
             name: Malachi.LogRetention,
             metadata_source: fn -> BrokerServer.metadata(Malachi.LogBroker) end,
             expire_segment: &expire_segment/1,
             policy: retention_policy(),
             interval: Application.get_env(:malachi, :retention_interval_ms, 60_000)
           ]
         ]}
    }
  end

  # Removes an expired segment from the control plane, then deletes its stored data on each replica.
  # Best-effort: the control-plane drop is idempotent and the storage delete tolerates a missing
  # segment, so a replica that is momentarily unreachable just leaves harmless files to be retried.
  defp expire_segment(segment) do
    _ = BrokerServer.delete_segment(Malachi.LogBroker, segment.id)
    Enum.each(segment.replica_set, fn broker -> ReplicationServer.delete(broker, segment.id) end)
  end

  @doc "The configured retention policy (`:max_age_ms` / `:max_bytes`; `nil` = that rule is off)."
  @spec retention_policy() :: Malachi.Cluster.Retention.policy()
  def retention_policy do
    %{
      max_age_ms: Application.get_env(:malachi, :retention_max_age_ms),
      max_bytes: Application.get_env(:malachi, :retention_max_bytes)
    }
  end

  defp retention_configured?, do: retention_policy() |> Map.values() |> Enum.any?(&(&1 != nil))

  defp membership_child(nodes) do
    opts = [
      name: Malachi.LogMembership,
      self_ref: {Malachi.LogMembership, node()},
      peers: membership_seeds(nodes),
      attributes: parse_attributes(Application.get_env(:malachi, :log_attributes))
    ]

    %{id: Malachi.LogMembership, start: {MembershipServer, :start_link, [opts]}}
  end

  @doc ~S"""
  Parses this node's broker attributes from a `"key=value,key2=value2"` string into a `%{}` of string
  k/v (`nil`/`""` → `%{}`). Entries without exactly one `=` are ignored; keys and values are trimmed.
  """
  @spec parse_attributes(String.t() | nil) :: %{optional(String.t()) => String.t()}
  def parse_attributes(raw) when raw in [nil, ""], do: %{}

  def parse_attributes(raw) do
    raw
    |> String.split(",", trim: true)
    |> Enum.flat_map(fn entry ->
      case String.split(entry, "=", parts: 2) do
        [key, value] -> [{String.trim(key), String.trim(value)}]
        _no_equals -> []
      end
    end)
    |> Map.new()
  end

  defp replication_child do
    %{
      id: Malachi.LogReplication,
      start:
        {Malachi.Cluster.ReplicationServer, :start_link, [[name: Malachi.LogReplication, directory: log_data_dir()]]}
    }
  end

  # Closes the loop broker dies -> membership marks it gone -> its segments are re-replicated and any
  # active-segment primary it held is promoted. Uses the live broker set and control plane by name.
  defp healer_child do
    %{
      id: Malachi.LogHealer,
      start:
        {HealCoordinator, :start_link,
         [
           [
             name: Malachi.LogHealer,
             live_brokers: &live_brokers/0,
             metadata_source: fn -> BrokerServer.metadata(Malachi.LogBroker) end,
             apply_command: fn command -> BrokerServer.apply_heal(Malachi.LogBroker, [command]) end,
             replication_factor: replication_factor()
           ]
         ]}
    }
  end

  defp log_broker_child(cluster, nodes) do
    opts = [name: Malachi.LogBroker] ++ metadata_opts(cluster, nodes) ++ data_plane_opts(cluster, nodes)
    %{id: Malachi.LogBroker, start: {Malachi.BrokerServer, :start_link, [log_data_dir(), opts]}}
  end

  # Single ra cluster by default; with `:log_vnodes` > 1 (and a clustered control plane) the metadata
  # is sharded across that many vnodes, each its own ra cluster routed by topic, and each replicated
  # over the same `nodes` for HA per vnode (D-b).
  defp metadata_opts(cluster, nodes) do
    case Application.get_env(:malachi, :log_vnodes, 1) do
      count when is_integer(count) and count > 1 and not is_nil(cluster) ->
        [metadata_vnodes: sharded_vnodes(cluster, count), metadata_nodes: nodes]

      _one_or_unclustered ->
        metadata_cluster_opts(cluster, nodes)
    end
  end

  @doc """
  The metadata options passed to `Malachi.BrokerServer` for the given control-plane `cluster` and
  `nodes`: none (single-node in-memory) when `cluster` is `nil`, otherwise the `ra`-backed cluster.
  Pure — the `ra` runtime is started separately (see `log_children/0`).
  """
  @spec metadata_cluster_opts(atom() | nil, [node()]) :: keyword()
  def metadata_cluster_opts(nil, _nodes), do: []
  def metadata_cluster_opts(cluster, nodes), do: [metadata_cluster: cluster, metadata_nodes: nodes]

  @doc """
  The `count` vnodes (`{cluster_name, token}`) of a sharded control plane, named from `base` and
  spread evenly over the 32-bit ring. Each vnode is its own ra cluster (D-b-1: single-node per vnode;
  HA per vnode comes later). Pure.
  """
  @spec sharded_vnodes(atom(), pos_integer()) :: [{atom(), non_neg_integer()}]
  def sharded_vnodes(base, count) when is_integer(count) and count > 0 do
    ring_size = Integer.pow(2, 32)
    for index <- 0..(count - 1), do: {:"#{base}_vn_#{index}", div(index * ring_size, count)}
  end

  @doc """
  The data-plane options for `Malachi.BrokerServer`: none when `cluster` is `nil` (the BrokerServer
  owns a single local ReplicationServer, `replication_factor` 1); otherwise it places segment
  replicas across every node's named ReplicationServer with the configured replication factor.
  """
  @spec data_plane_opts(atom() | nil, [node()]) :: keyword()
  def data_plane_opts(nil, _nodes), do: []

  def data_plane_opts(_cluster, nodes) do
    # :brokers is the static full set (initial placement); :live_brokers narrows new placements to the
    # currently-alive nodes as membership converges/changes (an empty result is ignored by the broker).
    # :spread_by + :broker_attributes make placement rack/DC-aware from the attributes gossiped in C2.
    [
      brokers: broker_refs(nodes),
      replication_factor: replication_factor(),
      live_brokers: &live_brokers/0,
      spread_by: Application.get_env(:malachi, :log_spread_by),
      broker_attributes: &broker_attributes/0
    ]
  end

  @doc "The ReplicationServer references (one per node) that segment replicas are placed across."
  @spec broker_refs([node()]) :: [{module(), node()}]
  def broker_refs(nodes), do: for(n <- nodes, do: {Malachi.LogReplication, n})

  @doc "The membership seeds this node joins with — the other nodes' membership servers (not self)."
  @spec membership_seeds([node()]) :: [{module(), node()}]
  def membership_seeds(nodes), do: for(n <- nodes, n != node(), do: {Malachi.LogMembership, n})

  @doc "Maps membership members (`{name, node}`) to their nodes' ReplicationServer references."
  @spec live_replication_refs([{term(), node()}]) :: [{module(), node()}]
  def live_replication_refs(members), do: for({_name, node} <- members, do: {Malachi.LogReplication, node})

  # The currently-alive broker set, derived from SWIM membership (fed to the broker + healer).
  defp live_brokers do
    Malachi.LogMembership |> MembershipServer.alive_members() |> live_replication_refs()
  end

  # Each alive member's ReplicationServer ref mapped to that member's attributes (both keyed by node),
  # so placement can spread replicas over an attribute (e.g. rack). Absent attrs => an empty map.
  defp broker_attributes do
    members = MembershipServer.alive_members(Malachi.LogMembership)
    broker_attributes_for(members, &MembershipServer.attributes(Malachi.LogMembership, &1))
  end

  @doc "Maps membership `members` to `%{ReplicationServer ref => attributes}`, keyed by node."
  @spec broker_attributes_for([{term(), node()}], (term() -> map())) :: %{{module(), node()} => map()}
  def broker_attributes_for(members, attributes_of) do
    Map.new(members, fn {_name, node} = member -> {{Malachi.LogReplication, node}, attributes_of.(member)} end)
  end

  defp replication_factor, do: Application.get_env(:malachi, :log_replication_factor, 3)

  defp start_ra! do
    {:ok, _apps} = Application.ensure_all_started(:ra)
    ra_data_dir = Application.get_env(:malachi, :ra_data_dir, Path.join(System.tmp_dir!(), "malachi_ra"))
    # Idempotent: a restart of the same data dir returns an error we can ignore (ra is already in).
    _ = :ra.start_in(String.to_charlist(ra_data_dir))
    :ok
  end

  @doc """
  Called before the application stops.
  Performs graceful shutdown of all client connections.
  """
  def prep_stop(_state) do
    Logger.info(I18n.t(:graceful_shutdown))

    try do
      Malachi.ConnectionRegistry.close_all()
    rescue
      ArgumentError -> :ok
    catch
      _, _ -> :ok
    end

    :ok
  end
end
