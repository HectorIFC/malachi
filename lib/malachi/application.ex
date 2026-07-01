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

    if cluster do
      start_ra!()
      [replication_child(), log_broker_child(cluster, nodes)]
    else
      [log_broker_child(nil, nodes)]
    end
  end

  defp replication_child do
    %{
      id: Malachi.LogReplication,
      start:
        {Malachi.Cluster.ReplicationServer, :start_link, [[name: Malachi.LogReplication, directory: log_data_dir()]]}
    }
  end

  defp log_broker_child(cluster, nodes) do
    opts = [name: Malachi.LogBroker] ++ metadata_cluster_opts(cluster, nodes) ++ data_plane_opts(cluster, nodes)
    %{id: Malachi.LogBroker, start: {Malachi.BrokerServer, :start_link, [log_data_dir(), opts]}}
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
  The data-plane options for `Malachi.BrokerServer`: none when `cluster` is `nil` (the BrokerServer
  owns a single local ReplicationServer, `replication_factor` 1); otherwise it places segment
  replicas across every node's named ReplicationServer with the configured replication factor.
  """
  @spec data_plane_opts(atom() | nil, [node()]) :: keyword()
  def data_plane_opts(nil, _nodes), do: []

  def data_plane_opts(_cluster, nodes) do
    [brokers: broker_refs(nodes), replication_factor: replication_factor()]
  end

  @doc "The ReplicationServer references (one per node) that segment replicas are placed across."
  @spec broker_refs([node()]) :: [{module(), node()}]
  def broker_refs(nodes), do: for(n <- nodes, do: {Malachi.LogReplication, n})

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
