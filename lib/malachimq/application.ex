defmodule MalachiMQ.Application do
  @moduledoc """
  Main application supervisor for MalachiMQ.

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
  alias MalachiMQ.Auth.ConfigValidator
  alias MalachiMQ.I18n
  alias MalachiMQ.TLSValidator

  def start(_type, _args) do
    # Validate authentication configuration before starting
    # This prevents insecure deployments in production
    ConfigValidator.validate!(Application.get_env(:malachimq, :config_env, :prod))

    # Validate TLS configuration
    # In production: raises on invalid config (fail fast)
    # In dev/test: logs warnings only
    TLSValidator.validate!(Application.get_env(:malachimq, :config_env, :prod))

    port = Application.get_env(:malachimq, :tcp_port, 4040)
    dashboard_port = Application.get_env(:malachimq, :dashboard_port, 4041)

    available_schedulers = System.schedulers_online()
    configured_schedulers = Application.get_env(:malachimq, :schedulers, available_schedulers)
    schedulers_to_use = min(configured_schedulers, available_schedulers)

    :erlang.system_flag(:schedulers_online, schedulers_to_use)

    children = [
      {Registry, keys: :unique, name: MalachiMQ.QueueRegistry, partitions: System.schedulers_online()},
      {Registry, keys: :unique, name: MalachiMQ.ChannelRegistry},
      {DynamicSupervisor, name: MalachiMQ.QueueSupervisor, strategy: :one_for_one, max_children: 100_000},
      {DynamicSupervisor, name: MalachiMQ.ChannelSupervisor, strategy: :one_for_one, max_children: 100_000},
      # Increase max_children for Task.Supervisor to allow large parallel broadcasts
      {Task.Supervisor, name: MalachiMQ.TaskSupervisor, max_children: 200_000},
      MalachiMQ.PartitionManager,
      MalachiMQ.QueueConfig,
      MalachiMQ.Metrics,
      # Audit logging (must start early for security event tracking)
      MalachiMQ.AuditLog,
      # Resource monitors (must start after AuditLog for alert logging)
      MalachiMQ.AtomMonitor,
      MalachiMQ.MemoryMonitor,
      # Account lockout manager (must start before Auth)
      MalachiMQ.Auth.LockoutManager,
      # Input validation with ETS cache (must start before Queue/Channel creation)
      MalachiMQ.Validator,
      MalachiMQ.RateLimiter,
      MalachiMQ.ConnectionLimiter,
      # User persistence (must start before Auth to load persisted users into ETS)
      MalachiMQ.Auth.UserStore,
      MalachiMQ.Auth,
      MalachiMQ.AckManager,
      MalachiMQ.ConnectionRegistry,
      {MalachiMQ.TCPAcceptorPool, port},
      {MalachiMQ.Dashboard, dashboard_port}
    ]

    opts = [strategy: :one_for_one, name: MalachiMQ.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @doc """
  Called before the application stops.
  Performs graceful shutdown of all client connections.
  """
  def prep_stop(_state) do
    Logger.info(I18n.t(:graceful_shutdown))

    try do
      MalachiMQ.ConnectionRegistry.close_all()
    rescue
      ArgumentError -> :ok
    catch
      _, _ -> :ok
    end

    :ok
  end
end
