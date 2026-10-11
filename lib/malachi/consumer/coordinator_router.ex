defmodule Malachi.Consumer.CoordinatorRouter do
  @moduledoc """
  Routes a topic's consumer-group coordination to a single owning node, so every member of a group:
  connected to any broker: reaches the **same** `GroupCoordinator`. The invariant "each range under
  exactly one member" needs one coordination authority per topic; with a local coordinator per node,
  members on different nodes would see divergent assignments.

  Mirrors NorthGuard's request routing: a broker consults its local view of the sharded metadata (the
  `HashRing` over vnodes) to find the vnode that owns the topic, then forwards to the node currently
  **leading** that vnode's Raft cluster (the owner co-locates with the topic's metadata, so the
  coordinator's `active_range_ids/2` resolves against the local broker).

  A cluster whose metadata is one Raft group (no vnodes) routes every topic to that group's leader
  (`put_leader/1`), so it too has one coordination authority while that leader is known, where NorthGuard
  has one per shard. While it is not (an election, a node cut off in a minority), routing falls back to
  the local coordinator like every unresolved case, and the group fence in the control plane still orders
  the checkpoints the group keys commit.
  Single-node / in-memory has no replicated control plane → no topology → everything is `:local`, so the
  behavior is exactly the pre-routing one. The routing decision is a pure function (`location/4`) with
  the topology and live leadership passed in; `resolve/2` is the thin runtime wrapper that reads the
  boot-published topology (`:persistent_term`) and the current Raft leader.
  """

  alias Malachi.Cluster.HashRing

  @topology_key {__MODULE__, :topology}

  @type topology :: %{ring: HashRing.t(), servers: %{term() => term()}} | %{leader_of: term()}
  @type location :: :local | {:remote, node()}

  # -- pure core (topology + leadership passed in; fully unit-testable without a cluster) --

  @doc """
  Decides where a topic's coordination lives. Returns `:local` when this node owns the topic's vnode:
  and, **fail-safe**, whenever there is no topology, the ring can't route, the vnode is unknown, or its
  leader can't be resolved (a resolution gap degrades to the local coordinator rather than dropping the
  request). Returns `{:remote, node}` when another node leads the owning vnode. `leader_fn` maps a
  vnode's server id to the node currently leading it (or `nil` if unknown).
  """
  @spec location(term(), topology() | nil, node(), (term() -> node() | nil)) :: location()
  def location(topic, topology, this_node, leader_fn) do
    with {:ok, _vnode_id, server_id} <- route(topic, topology),
         owner when is_atom(owner) and not is_nil(owner) <- leader_fn.(server_id) do
      if owner == this_node, do: :local, else: {:remote, owner}
    else
      _unresolved -> :local
    end
  end

  @doc "Turns a `location/4` result into a coordinator `GenServer.server()` ref (`{name, node}` if remote)."
  @spec ref(location(), atom()) :: GenServer.server()
  def ref(:local, name), do: name
  def ref({:remote, remote_node}, name), do: {name, remote_node}

  @doc """
  The per-vnode coordinator name derived from a `base_name` and a `vnode_id`. In the sharded control plane
  each led vnode runs its own `GroupCoordinator` (one per vnode, on the leader: the NorthGuard model), so
  routing and the boot wiring must agree on this name.
  """
  @spec coordinator_name(atom(), term()) :: atom()
  def coordinator_name(base_name, vnode_id), do: Module.concat(base_name, to_string(vnode_id))

  # -- runtime resolution (impure: reads persistent_term + live Raft leadership) --

  @doc """
  Resolves the coordinator ref for `topic`. Single-node / in-memory (no topology): the bare `base_name`.
  One Raft group for the whole metadata (`put_leader/1`): `base_name` on its leader, `{base_name, leader}`
  elsewhere.
  Sharded: the topic's vnode owns a **per-vnode** coordinator on its leader, `coordinator_name/2` locally
  when this node leads it, or `{coordinator_name/2, owner_node}` to forward. Falls back to `base_name` when
  the owning vnode or its leader can't be resolved (a transient failover state; the caller re-resolves).
  """
  @spec resolve(atom(), term()) :: GenServer.server()
  def resolve(base_name, topic) do
    with {:ok, vnode_id, server_id} <- route(topic, topology()),
         owner when is_atom(owner) and not is_nil(owner) <- leader_node(server_id) do
      name = if vnode_id == nil, do: base_name, else: coordinator_name(base_name, vnode_id)
      if owner == node(), do: name, else: {name, owner}
    else
      _unresolved -> base_name
    end
  end

  @doc """
  Whether **this** node owns `topic`'s coordination (it leads the topic's vnode, or there is no sharded
  topology). The owning coordinator uses this to reject work routed to it by a stale leadership view.
  """
  @spec owns?(term()) :: boolean()
  def owns?(topic), do: location(topic, topology(), node(), &leader_node/1) == :local

  # Routes a topic to its owning vnode and that vnode's ra server id, or `:error` when there is no
  # topology, the ring can't route, or the vnode is unknown to the server map. One Raft group for the whole
  # metadata owns every topic, under no vnode id.
  @spec route(term(), topology() | nil) :: {:ok, term(), term()} | :error
  defp route(_topic, nil), do: :error
  defp route(_topic, %{leader_of: server_id}), do: {:ok, nil, server_id}

  defp route(topic, %{ring: ring, servers: servers}) do
    with {:ok, vnode_id} <- HashRing.route(ring, topic),
         {:ok, server_id} <- Map.fetch(servers, vnode_id) do
      {:ok, vnode_id, server_id}
    else
      _unroutable -> :error
    end
  end

  @doc """
  Publishes the static routing topology (the ring + vnode→server-id map) for `resolve/2`. Called once at
  boot when the sharded control plane is established; unset means single-node / in-memory.
  """
  @spec put_topology(HashRing.t(), %{term() => term()}) :: :ok
  def put_topology(ring, servers) do
    :persistent_term.put(@topology_key, %{ring: ring, servers: servers})
  end

  @doc """
  Publishes the routing of a control plane that is one Raft group (`server_id`): every topic's coordination
  lives on the node leading it, under the node-wide coordinator's name. Called when the broker joins that
  group; unset means single-node / in-memory.
  """
  @spec put_leader(term()) :: :ok
  def put_leader(server_id), do: :persistent_term.put(@topology_key, %{leader_of: server_id})

  @doc "The current routing topology, or `nil` in single-node / in-memory mode (no replicated control plane)."
  @spec topology() :: topology() | nil
  def topology, do: :persistent_term.get(@topology_key, nil)

  # How long asking another node for a cluster's leader may take: this is read on the coordinator's own
  # loop for every group request, where ra's default 5 s, which an election or a minority member takes in
  # full, would hold every other group.
  @leader_ask_timeout_ms 200

  # ra server ids are `{name, node}`; the node leading the vnode's Raft cluster coordinates its topics. A
  # cluster this node is a member of has its leader in ra's local table, read with no round trip; another
  # (a vnode placed elsewhere) is asked, briefly.
  defp leader_node({name, _node} = server_id) do
    case :ra_leaderboard.lookup_leader(name) do
      {_name, owner_node} -> owner_node
      :undefined -> ask_leader(server_id)
    end
  end

  defp ask_leader(server_id) do
    case :ra.members(server_id, @leader_ask_timeout_ms) do
      {:ok, _members, {_name, owner_node}} -> owner_node
      _not_ready -> nil
    end
  end
end
