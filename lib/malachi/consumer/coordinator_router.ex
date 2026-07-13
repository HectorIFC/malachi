defmodule Malachi.Consumer.CoordinatorRouter do
  @moduledoc """
  Routes a topic's consumer-group coordination to a single owning node, so every member of a group —
  connected to any broker — reaches the **same** `GroupCoordinator`. The invariant "each range under
  exactly one member" needs one coordination authority per topic; with a local coordinator per node,
  members on different nodes would see divergent assignments.

  Mirrors NorthGuard's request routing: a broker consults its local view of the sharded metadata (the
  `HashRing` over vnodes) to find the vnode that owns the topic, then forwards to the node currently
  **leading** that vnode's Raft cluster (the owner co-locates with the topic's metadata, so the
  coordinator's `active_range_ids/2` resolves against the local broker).

  Single-node / in-memory has no sharded control plane → no topology → everything is `:local`, so the
  behaviour is exactly the pre-routing one. The routing decision is a pure function (`location/4`) with
  the topology and live leadership passed in; `resolve/2` is the thin runtime wrapper that reads the
  boot-published topology (`:persistent_term`) and the current Raft leader.
  """

  alias Malachi.Cluster.HashRing

  @topology_key {__MODULE__, :topology}

  @type topology :: %{ring: HashRing.t(), servers: %{term() => term()}}
  @type location :: :local | {:remote, node()}

  # -- pure core (topology + leadership passed in; fully unit-testable without a cluster) --

  @doc """
  Decides where a topic's coordination lives. Returns `:local` when this node owns the topic's vnode —
  and, **fail-safe**, whenever there is no topology, the ring can't route, the vnode is unknown, or its
  leader can't be resolved (a resolution gap degrades to the local coordinator rather than dropping the
  request). Returns `{:remote, node}` when another node leads the owning vnode. `leader_fn` maps a
  vnode's server id to the node currently leading it (or `nil` if unknown).
  """
  @spec location(term(), topology() | nil, node(), (term() -> node() | nil)) :: location()
  def location(_topic, nil, _this_node, _leader_fn), do: :local

  def location(topic, %{ring: ring, servers: servers}, this_node, leader_fn) do
    with {:ok, vnode_id} <- HashRing.route(ring, topic),
         {:ok, server_id} <- Map.fetch(servers, vnode_id),
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

  # -- runtime resolution (impure: reads persistent_term + live Raft leadership) --

  @doc """
  Resolves the coordinator ref for `topic`: the local `name` in single-node / when this node owns the
  topic, or `{name, owner_node}` to forward to the node leading the owning vnode.
  """
  @spec resolve(atom(), term()) :: GenServer.server()
  def resolve(name, topic) do
    topic
    |> location(topology(), node(), &leader_node/1)
    |> ref(name)
  end

  @doc """
  Publishes the static routing topology (the ring + vnode→server-id map) for `resolve/2`. Called once at
  boot when the sharded control plane is established; unset means single-node / in-memory.
  """
  @spec put_topology(HashRing.t(), %{term() => term()}) :: :ok
  def put_topology(ring, servers) do
    :persistent_term.put(@topology_key, %{ring: ring, servers: servers})
  end

  @doc "The current routing topology, or `nil` in single-node / in-memory mode (no sharded control plane)."
  @spec topology() :: topology() | nil
  def topology, do: :persistent_term.get(@topology_key, nil)

  # ra server ids are `{name, node}`; the node leading the vnode's Raft cluster coordinates its topics.
  defp leader_node(server_id) do
    case :ra.members(server_id) do
      {:ok, _members, {_name, owner_node}} -> owner_node
      _not_ready -> nil
    end
  end
end
