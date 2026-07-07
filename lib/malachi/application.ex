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
  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.Placement
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.RetentionCoordinator
  alias Malachi.Cluster.VnodeCoordinatorManager
  alias Malachi.I18n
  alias Malachi.Metadata
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
        # precede the broker that references it.
        [membership_child(nodes), replication_child(), log_broker_child(cluster, nodes)]
      else
        [log_broker_child(nil, nodes)]
      end

    # Coordinators reference the broker (and, when sharded, the vnodes' ra clusters), so they come last.
    log_stack ++ coordinator_children(cluster, vnode_placement(cluster, nodes))
  end

  # The retention/heal coordinators. Sharded control plane (1C-b-ii): a DynamicSupervisor plus a manager
  # that runs one coordinator pair per vnode this node leads. Otherwise (single-node, or a single-vnode
  # cluster): the node-wide coordinators of 1C-a — heal whenever clustered, retention when a policy is set.
  defp coordinator_children(cluster, nil) do
    heal = if cluster, do: [healer_child()], else: []
    heal ++ retention_children(cluster)
  end

  defp coordinator_children(_cluster, vnodes) do
    [vnode_coordinator_tree_spec(vnodes)]
  end

  defp retention_children(cluster) do
    if retention_configured?(), do: [retention_child(cluster)], else: []
  end

  defp retention_child(cluster) do
    opts =
      [name: Malachi.LogRetention] ++
        retention_opts(fn -> BrokerServer.metadata(Malachi.LogBroker) end, coordinator_leader?(cluster))

    %{id: Malachi.LogRetention, start: {RetentionCoordinator, :start_link, [opts]}}
  end

  # The retention coordinator opts shared by the node-wide (1C-a) and per-vnode (1C-b) coordinators; only
  # the metadata source (global merge vs one vnode) and the leader gate differ.
  defp retention_opts(metadata_source, leader?) do
    [
      metadata_source: metadata_source,
      expire_segment: &expire_segment/1,
      policy: retention_policy(),
      interval: Application.get_env(:malachi, :retention_interval_ms, 60_000),
      leader?: leader?
    ]
  end

  # The heal coordinator opts shared by the node-wide (1C-a) and per-vnode (1C-b) coordinators; only the
  # metadata source and the leader gate differ.
  defp heal_opts(metadata_source, leader?) do
    [
      live_brokers: &live_brokers/0,
      metadata_source: metadata_source,
      apply_command: fn command -> BrokerServer.apply_heal(Malachi.LogBroker, [command]) end,
      replication_factor: replication_factor(),
      leader?: leader?
    ]
  end

  # The leader gate for a cluster coordinator: only the membership leader acts (1C). A single node (no
  # `:log_cluster`, so no membership server) always acts.
  defp coordinator_leader?(nil), do: fn -> true end
  defp coordinator_leader?(_cluster), do: membership_leader(Malachi.LogMembership)

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
  def parse_attributes(raw), do: parse_kv(raw, & &1)

  @doc ~S"""
  Parses a `"node1=rack_a,node2=rack_b"` cluster topology string into `%{node => value}` (node atom →
  attribute string, e.g. rack/zone) — the static per-node placement attribute used by `place_vnodes/4`
  for rack-aware vnode placement (A1). `nil`/`""` => `%{}`. Entries without exactly one `=` are ignored;
  node and value are trimmed. Node names come from a trusted operator (deploy config), so
  `String.to_atom` is fine here. Being static config identical on every node keeps placement deterministic.
  """
  @spec parse_topology(String.t() | nil) :: %{optional(node()) => String.t()}
  def parse_topology(raw), do: parse_kv(raw, &String.to_atom/1)

  # Parses a `"k1=v1,k2=v2"` string into a `%{key => value}` map, applying `key_fun` to each trimmed key
  # (identity for attributes, `String.to_atom` for node names). Entries without exactly one `=` are
  # dropped; keys and values are trimmed. `nil`/`""` => `%{}`.
  defp parse_kv(raw, _key_fun) when raw in [nil, ""], do: %{}

  defp parse_kv(raw, key_fun) do
    raw
    |> String.split(",", trim: true)
    |> Enum.flat_map(fn entry ->
      case String.split(entry, "=", parts: 2) do
        [key, value] -> [{key_fun.(String.trim(key)), String.trim(value)}]
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
    opts =
      [name: Malachi.LogHealer] ++
        heal_opts(
          fn -> BrokerServer.metadata(Malachi.LogBroker) end,
          membership_leader(Malachi.LogMembership)
        )

    %{id: Malachi.LogHealer, start: {HealCoordinator, :start_link, [opts]}}
  end

  @vnode_coordinator_supervisor Malachi.LogVnodeCoordinatorSupervisor

  # Groups the dynamic supervisor and its manager under a one_for_all supervisor: if the manager crashes
  # and restarts, the dynamic supervisor (and thus every running coordinator) restarts with it, so the
  # manager never rebuilds its set on top of orphaned coordinators. The dynamic supervisor starts first
  # (the manager references it by name on its first reconcile).
  defp vnode_coordinator_tree_spec(vnodes) do
    %{
      id: Malachi.LogVnodeCoordinatorTree,
      type: :supervisor,
      start:
        {Supervisor, :start_link,
         [
           [vnode_coordinator_supervisor_spec(), vnode_coordinator_manager_spec(vnodes)],
           [strategy: :one_for_all]
         ]}
    }
  end

  # The dynamic supervisor under which the manager starts each led vnode's coordinator pair (1C-b-ii).
  defp vnode_coordinator_supervisor_spec do
    {DynamicSupervisor, name: @vnode_coordinator_supervisor, strategy: :one_for_one}
  end

  # The manager that reconciles the running coordinators against the vnodes this node leads. It is
  # level-triggered (polls Raft leadership via `leading_vnodes/3`), so it tolerates leadership flaps.
  defp vnode_coordinator_manager_spec(vnodes) do
    %{
      id: Malachi.LogVnodeCoordinatorManager,
      start:
        {VnodeCoordinatorManager, :start_link,
         [
           [
             name: Malachi.LogVnodeCoordinatorManager,
             leading: fn -> leading_vnodes(vnodes, node(), &MetadataServer.leader?/1) end,
             spawn: &start_vnode_coordinators/1,
             stop: &stop_vnode_coordinators/1,
             interval: Application.get_env(:malachi, :vnode_reconcile_interval_ms, 5_000)
           ]
         ]}
    }
  end

  # Starts a vnode's coordinators under a per-vnode supervisor (so a coordinator that crashes is
  # restarted without the manager losing its handle), returning that supervisor's pid — the handle the
  # manager stops the vnode by. Heal always; retention only when a policy is configured.
  defp start_vnode_coordinators(vnode_id) do
    children =
      [heal_vnode_child(vnode_id)] ++
        if retention_configured?(), do: [retention_vnode_child(vnode_id)], else: []

    spec = %{
      id: {Malachi.LogVnodeCoordinators, vnode_id},
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]},
      type: :supervisor,
      restart: :temporary
    }

    {:ok, pid} = DynamicSupervisor.start_child(@vnode_coordinator_supervisor, spec)
    pid
  end

  defp stop_vnode_coordinators(pid) do
    _ = DynamicSupervisor.terminate_child(@vnode_coordinator_supervisor, pid)
    :ok
  end

  # A single vnode's retention coordinator: reads only that vnode's Metadata and acts only while this
  # node leads its ra group (defense-in-depth on top of the manager). Reuses expire_segment/1 (routed).
  defp retention_vnode_child(vnode_id) do
    opts = retention_opts(vnode_metadata_source(vnode_id), vnode_leader_gate(vnode_id))
    %{id: Malachi.LogRetention, start: {RetentionCoordinator, :start_link, [opts]}}
  end

  # A single vnode's heal coordinator (per-vnode analogue of healer_child/0).
  defp heal_vnode_child(vnode_id) do
    opts = heal_opts(vnode_metadata_source(vnode_id), vnode_leader_gate(vnode_id))
    %{id: Malachi.LogHealer, start: {HealCoordinator, :start_link, [opts]}}
  end

  # The per-vnode leader gate: true only while this node leads the vnode's ra group.
  defp vnode_leader_gate(vnode_id), do: fn -> MetadataServer.leader?({vnode_id, node()}) end

  defp log_broker_child(cluster, nodes) do
    opts = [name: Malachi.LogBroker] ++ metadata_opts(cluster, nodes) ++ data_plane_opts(cluster, nodes)
    %{id: Malachi.LogBroker, start: {Malachi.BrokerServer, :start_link, [log_data_dir(), opts]}}
  end

  # Single ra cluster by default; with `:log_vnodes` > 1 (and a clustered control plane) the metadata is
  # sharded across that many vnodes, each its own ra cluster placed on a subset of `nodes` (rendezvous,
  # `:log_vnode_replication_factor` members), routed by topic. A single deterministic seed node
  # bootstraps them; the others only route (D-c-1c).
  defp metadata_opts(cluster, nodes) do
    case vnode_placement(cluster, nodes) do
      nil ->
        metadata_cluster_opts(cluster, nodes)

      vnodes ->
        [metadata_vnodes: vnodes, bootstrap_orchestrator: membership_leader(Malachi.LogMembership)]
    end
  end

  # The sharded-control-plane vnode placement (`[{vnode_id, token, nodes}]`), or `nil` when the control
  # plane is single-node/unclustered or configured with a single vnode. Shared by `metadata_opts/2` (to
  # bootstrap the vnodes) and `coordinator_children/2` (to run a coordinator pair per led vnode, 1C-b-ii).
  defp vnode_placement(cluster, nodes) do
    case Application.get_env(:malachi, :log_vnodes, 1) do
      count when is_integer(count) and count > 1 and not is_nil(cluster) ->
        rf = Application.get_env(:malachi, :log_vnode_replication_factor, 3)
        place_vnodes(sharded_vnodes(cluster, count), nodes, rf, vnode_place_opts())

      _one_or_unclustered ->
        nil
    end
  end

  # Placement options for the vnodes: when a spread attribute (`:log_spread_by`) and a static topology
  # (`:log_topology`, `node => value`) are configured, spread each vnode's replicas across distinct
  # values of that attribute (A1, rack/zone aware). Otherwise plain HRW. The topology is static config
  # (identical on every node), so the placement is deterministic across the cluster.
  defp vnode_place_opts do
    spread_by = Application.get_env(:malachi, :log_spread_by)
    topology = parse_topology(Application.get_env(:malachi, :log_topology))

    if spread_by && topology != %{} do
      node_attributes = Map.new(topology, fn {node, value} -> {node, %{spread_by => value}} end)
      [spread: {spread_by, node_attributes}]
    else
      []
    end
  end

  @doc """
  The bootstrap-orchestrator policy for the sharded control plane: a `(-> boolean())` that is true only
  on the deterministic seed node (the lowest-sorted of `nodes`), so exactly one node starts each vnode's
  ra cluster and the rest only route to it. Pure. D-c-1d swaps this for a membership-leader policy with
  fencing (the mature, k8s-style approach).
  """
  @spec static_seed([node()]) :: (-> boolean())
  def static_seed(nodes) do
    seed = nodes |> Enum.sort() |> List.first()
    fn -> node() == seed end
  end

  @doc """
  The bootstrap-orchestrator policy used at runtime (D-c-1d): a `(-> boolean())` true only on the
  lowest-sorted **live** member (per SWIM membership on `membership_server`), so the role **fails over**
  when the current leader dies — unlike `static_seed/1`, which is fixed to the lowest configured node.
  Conservative: if membership is unavailable it returns false (never risking two orchestrators); a
  transient double-leader is still fenced by the ra cluster name at bootstrap.
  """
  @spec membership_leader(GenServer.server()) :: (-> boolean())
  def membership_leader(membership_server) do
    fn -> node() == live_leader(membership_server) end
  end

  # The lowest-sorted live member's node, or nil when membership is unavailable/empty (→ not leader).
  # alive_members/1 returns the members sorted, so the head is the lowest node.
  defp live_leader(membership_server) do
    case safe_alive_members(membership_server) do
      [{_name, leader_node} | _] -> leader_node
      _empty_or_unavailable -> nil
    end
  end

  defp safe_alive_members(membership_server) do
    MembershipServer.alive_members(membership_server)
  catch
    _kind, _reason -> []
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
  Assigns each vnode (`{vnode_id, token}`) the nodes its ra cluster lives on: the `replication_factor`
  nodes chosen from `nodes` by rendezvous hashing (the same HRW that places segment replicas), so
  vnodes spread across the cluster and a node join/leave moves the fewest vnodes. Returns
  `{vnode_id, token, nodes}`. The effective replica count is `min(replication_factor, length(nodes))`.

  `place_opts` is forwarded to `Malachi.Cluster.Placement.place/4`; pass
  `[spread: {attribute_key, node_attributes}]` to keep a vnode's replicas in **distinct racks/zones**
  (topology-aware placement, so losing a whole rack does not take a majority of a vnode's replicas).
  Pure and deterministic — every node computes the same placement from the same static topology.
  Used by the sharded control plane so vnode leaders land on different nodes (D-c-1).
  """
  @spec place_vnodes([{atom(), non_neg_integer()}], [node()], pos_integer(), keyword()) ::
          [{atom(), non_neg_integer(), [node()]}]
  def place_vnodes(vnodes, nodes, replication_factor, place_opts \\ []) do
    Enum.map(vnodes, fn {vnode_id, token} ->
      {:ok, chosen} = Placement.place(vnode_id, nodes, replication_factor, place_opts)
      {vnode_id, token, chosen}
    end)
  end

  @doc """
  The **desired** vnode placement for a given set of (live) `nodes`: the `count` logical vnodes of
  control plane `base` (`sharded_vnodes/2`, node-independent) placed over `nodes` by rendezvous hashing
  (`place_vnodes/4`). Deterministic and, crucially for rebalancing, **minimal-movement** — because the
  vnode ids/tokens are fixed and HRW is stable, adding or removing a node re-places only the vnodes that
  must move (a vnode changes only if it adopts a joined node, or held a left one), leaving the rest put.

  This is the target that the rebalancing plan (R2) diffs the *current* placement against: recomputing
  it over the live membership (vs the static `:log_nodes`) is how the ring follows nodes joining/leaving.
  `place_opts` is forwarded for rack/zone spread (A1). Pure — the caller supplies `nodes` (e.g. the live
  members), so a coordinated recompute stays deterministic across the cluster.
  """
  @spec desired_placement(atom(), pos_integer(), [node()], pos_integer(), keyword()) ::
          [{atom(), non_neg_integer(), [node()]}]
  def desired_placement(base, count, nodes, replication_factor, place_opts \\ []) do
    base |> sharded_vnodes(count) |> place_vnodes(nodes, replication_factor, place_opts)
  end

  @doc """
  The **rebalancing plan** (R2): the per-vnode membership changes to move from the `current` placement to
  the `desired` one (from `desired_placement/5`), *staged* — it computes what to change without applying
  anything. For each vnode whose node set differs it yields `%{vnode_id:, add:, remove:}`, where `add`
  are the nodes to join that vnode's ra cluster and `remove` the ones to drop; vnodes that already match
  are omitted (so an empty plan means "nothing to do"). R3 executes each change **add-before-remove**, so
  a vnode never drops below its replica count mid-move (when the replication factor is unchanged, `add`
  and `remove` are equal-sized).

  Assumes `current` and `desired` cover the **same vnode ids** (rebalancing follows membership; changing
  the vnode count is re-sharding, out of scope). Deterministic — the plan follows `desired`'s order. Pure.
  """
  @spec rebalance_plan([{atom(), non_neg_integer(), [node()]}], [{atom(), non_neg_integer(), [node()]}]) ::
          [%{vnode_id: atom(), add: [node()], remove: [node()]}]
  def rebalance_plan(current, desired) do
    current_nodes_by_id = Map.new(current, fn {vnode_id, _token, nodes} -> {vnode_id, nodes} end)

    desired
    |> Enum.map(fn {vnode_id, _token, desired_nodes} ->
      current_nodes = Map.get(current_nodes_by_id, vnode_id, [])
      %{vnode_id: vnode_id, add: desired_nodes -- current_nodes, remove: current_nodes -- desired_nodes}
    end)
    |> Enum.reject(fn %{add: add, remove: remove} -> add == [] and remove == [] end)
  end

  @doc """
  The **current** placement read from the vnodes' live ra memberships: for each `{vnode_id, token}` in
  `vnode_configs`, `members_of.(vnode_id)` returns `{:ok, nodes}` (its current member nodes) or
  `{:error, _}`. A vnode whose membership cannot be read is **omitted** — conservative: we never plan a
  change for a vnode we cannot currently see. Returns `[{vnode_id, token, nodes}]`. Pure given the seam.
  """
  @spec readable_placement([{atom(), non_neg_integer()}], (atom() -> {:ok, [node()]} | {:error, term()})) ::
          [{atom(), non_neg_integer(), [node()]}]
  def readable_placement(vnode_configs, members_of) do
    for {vnode_id, token} <- vnode_configs, {:ok, nodes} <- [members_of.(vnode_id)] do
      {vnode_id, token, nodes}
    end
  end

  @doc """
  The rebalancing plan for the **live** cluster (R3-b-i): diffs the current placement — read from the
  vnodes' ra memberships via `members_of` (`readable_placement/2`) — against the placement **desired**
  over `alive_nodes` (the live members), for the **readable** vnodes only, so an unreachable vnode is
  neither planned for nor recomputed. `rf`/`place_opts` are the replication factor and spread options.
  Reads the world through the `members_of` seam (real: `:ra.members`); pure otherwise. The result feeds
  `Malachi.Cluster.Rebalance.apply_plan/4` (the commit), run under the lease.
  """
  @spec live_rebalance_plan(
          [{atom(), non_neg_integer()}],
          (atom() -> {:ok, [node()]} | {:error, term()}),
          [node()],
          pos_integer(),
          keyword()
        ) :: [%{vnode_id: atom(), add: [node()], remove: [node()]}]
  def live_rebalance_plan(vnode_configs, members_of, alive_nodes, rf, place_opts \\ []) do
    current = readable_placement(vnode_configs, members_of)
    readable_configs = Enum.map(current, fn {vnode_id, token, _nodes} -> {vnode_id, token} end)
    desired = place_vnodes(readable_configs, alive_nodes, rf, place_opts)
    rebalance_plan(current, desired)
  end

  @doc """
  The vnode ids `this_node` both **hosts** (its placement includes `this_node`) and currently **leads**
  (its Raft group's leader is the local server `{vnode_id, this_node}`), given the placement `vnodes`
  (`[{vnode_id, token, nodes}]`, as stored in the bootstrap) and a `leader?` predicate over a local
  server id (defaults to `MetadataServer.leader?/1`). This is where 1C-b runs each vnode's
  retention/healing coordinators, so every vnode is managed by exactly one node — the one leading its
  Raft group — distributing the control-plane work the NorthGuard-faithful way (vs 1C-a's single
  membership leader). Pure given `leader?`; deterministic given the current leadership.
  """
  @spec leading_vnodes([{atom(), non_neg_integer(), [node()]}], node(), (MetadataServer.server_id() ->
                                                                           boolean())) :: [atom()]
  def leading_vnodes(vnodes, this_node \\ node(), leader? \\ &MetadataServer.leader?/1) do
    for {vnode_id, _token, nodes} <- vnodes, this_node in nodes, leader?.({vnode_id, this_node}) do
      vnode_id
    end
  end

  @doc """
  A `metadata_source` bound to a single vnode: reads this node's view of the vnode's replicated
  `Metadata` via a consistent query to its ra cluster (`{vnode_id, node()}`). Tolerant — an unreachable
  or not-yet-formed vnode yields empty `Metadata` (no work) instead of crashing the coordinator. Used by
  1C-b so each vnode's retention/heal coordinator sees only that vnode's shard, versus 1C-a's global
  merge routed through the single membership leader.
  """
  @spec vnode_metadata_source(atom()) :: (-> Metadata.t())
  def vnode_metadata_source(vnode_id) do
    server_id = {vnode_id, node()}

    fn ->
      case MetadataServer.query(server_id, & &1) do
        {:ok, metadata} -> metadata
        {:error, _unformed_or_unreachable} -> Metadata.new()
      end
    end
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
