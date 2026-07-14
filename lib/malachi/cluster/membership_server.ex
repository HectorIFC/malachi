defmodule Malachi.Cluster.MembershipServer do
  @moduledoc """
  Runs SWIM-style membership live: a `GenServer`, one per broker, that drives the pure
  `Malachi.Cluster.Membership` view with a failure detector and gossip dissemination.

  Each **protocol period** it pings a random alive peer; if no `ack` arrives within `ack_timeout`,
  it does not suspect immediately — it asks `indirect_fanout` random other peers to ping the target
  on its behalf (**indirect ping**), and only marks the peer `:suspect` if no ack (direct or
  relayed) arrives within `indirect_timeout`. This cuts false positives from a transiently lost
  direct path. If the suspicion is not refuted within `suspicion_timeout`, it confirms the peer
  `:dead`. Every message **piggybacks** the sender's view (a list of `{member, status,
  incarnation}` updates), so peers learn of joins, suspicions, deaths and refutations by
  anti-entropy and the views **converge**. A node that is wrongly suspected learns of it from the
  ack to its own next ping and refutes by bumping its incarnation.

  Peers are reachable by `GenServer` references (a registered name locally, a `{name, node}` tuple
  across nodes), so the same code runs in-process for tests and over distributed Erlang. Sends are
  fire-and-forget: pinging a dead peer simply yields no ack, which is exactly how failure is
  detected.

  On startup a node **joins** by sending each seed (its `:peers`) a `{:join, ...}`; the seed adds
  the joiner as `:alive` and replies with its full view, so the joiner learns the whole cluster at
  once instead of waiting for gossip to converge. Join is best-effort — gossip is the safety net if
  a seed is unreachable. (A node that restarts after being declared dead would rejoin at
  incarnation 0, which an existing `:dead` entry outranks; durable/higher rejoin incarnations are a
  later concern.)

  The same gossip also **piggybacks the cluster's versioned routing topology** (a
  `Malachi.Cluster.RingTopology`): every message carries it alongside the view updates, and peers keep the
  higher version (last-version-wins), so a vnode split's ring change disseminates and converges the same
  way membership does — NorthGuard's *"minimal global state"* spread over the SWIM dissemination path.

  Scope: direct ping, indirect ping, suspicion, gossip (membership + topology), and a join handshake.
  """

  use GenServer

  alias Malachi.Cluster.Membership
  alias Malachi.Cluster.RingTopology

  @default_protocol_period 1_000
  @default_ack_timeout 500
  @default_suspicion_timeout 3_000
  @default_indirect_fanout 3

  @doc """
  Starts a membership server.

  ## Options
    * `:name` (optional) - the local registered name to start the GenServer under.
    * `:self_ref` (optional) - the reference this member is **known by to peers** and gossips as its
      own identity. Across nodes this must be a node-qualified `{name, node()}` (not a bare local
      name, which would resolve to a different server on each node). Defaults to `:name`, then the pid.
    * `:peers` - seed peer references, learned as `:alive`.
    * `:attributes` - this member's own attributes (opaque k/v, e.g. `%{rack: "a"}`); gossiped to peers.
    * `:protocol_period` / `:ack_timeout` / `:suspicion_timeout` - detector timings in ms.
    * `:indirect_timeout` - ms to wait for an indirect (relayed) ack (default `ack_timeout`).
    * `:indirect_fanout` - number of peers asked to relay a ping (default 3).
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, gen_server_opts(opts))

  @doc "Like `start_link/1` but not linked to the caller (e.g. to start on a remote node)."
  @spec start(keyword()) :: GenServer.on_start()
  def start(opts), do: GenServer.start(__MODULE__, opts, gen_server_opts(opts))

  defp gen_server_opts(opts) do
    case Keyword.fetch(opts, :name) do
      {:ok, name} -> [name: name]
      :error -> []
    end
  end

  @doc "The sorted list of currently alive members (the live broker set)."
  @spec alive_members(GenServer.server()) :: [Membership.member()]
  def alive_members(server), do: GenServer.call(server, :alive_members)

  @doc "The current `Malachi.Cluster.Membership` view (for inspection/tests)."
  @spec view(GenServer.server()) :: Membership.t()
  def view(server), do: GenServer.call(server, :view)

  @doc """
  Sets this node's own `attributes`, raising its incarnation so the change propagates and wins.
  Gossip disseminates it on the next protocol period (no proactive push, like every other update).
  """
  @spec set_attributes(GenServer.server(), Membership.attributes()) :: :ok
  def set_attributes(server, attributes), do: GenServer.call(server, {:set_attributes, attributes})

  @doc "The attributes known for `member` (`%{}` if unknown or none set)."
  @spec attributes(GenServer.server(), Membership.member()) :: Membership.attributes()
  def attributes(server, member), do: GenServer.call(server, {:attributes, member})

  @doc """
  Sets this node's view of the cluster `topology` (a `Malachi.Cluster.RingTopology`). Gossip disseminates
  it on the next protocol period and every node converges on the highest version (last-version-wins), so
  the rebalancing leader publishes a split's new ring here and the cluster adopts it.
  """
  @spec set_topology(GenServer.server(), RingTopology.t()) :: :ok
  def set_topology(server, %RingTopology{} = topology), do: GenServer.call(server, {:set_topology, topology})

  @doc "The current cluster routing topology, or `nil` if none has been set/received yet."
  @spec topology(GenServer.server()) :: RingTopology.t() | nil
  def topology(server), do: GenServer.call(server, :topology)

  # --- server ---

  @impl true
  def init(opts) do
    # Identity gossiped to peers: prefer an explicit node-qualified :self_ref, else the local :name,
    # else the pid. The local :name still registers the process (see gen_server_opts/1).
    self_ref = Keyword.get(opts, :self_ref) || Keyword.get(opts, :name) || self()
    ack_timeout = Keyword.get(opts, :ack_timeout, @default_ack_timeout)

    membership_opts = [peers: Keyword.get(opts, :peers, []), attributes: Keyword.get(opts, :attributes, %{})]

    state = %{
      view: Membership.new(self_ref, membership_opts),
      self: self_ref,
      # the cluster's versioned routing topology (a `Malachi.Cluster.RingTopology`), piggybacked on gossip
      # so a vnode split's ring change converges everywhere; `nil` until one is set/received.
      topology: Keyword.get(opts, :topology),
      # fired with the new topology when this node adopts a **higher version** (a real ring change), so it
      # applies the new ring locally (e.g. the CoordinatorRouter). Must be fast and not raise (runs inline
      # in the server). Default: no-op.
      on_topology: Keyword.get(opts, :on_topology, fn _topology -> :ok end),
      protocol_period: Keyword.get(opts, :protocol_period, @default_protocol_period),
      ack_timeout: ack_timeout,
      indirect_timeout: Keyword.get(opts, :indirect_timeout, ack_timeout),
      indirect_fanout: Keyword.get(opts, :indirect_fanout, @default_indirect_fanout),
      suspicion_timeout: Keyword.get(opts, :suspicion_timeout, @default_suspicion_timeout),
      awaiting: MapSet.new()
    }

    schedule_period(state)
    send_joins(state)
    {:ok, state}
  end

  @impl true
  def handle_call(:alive_members, _from, state), do: {:reply, Membership.alive_members(state.view), state}
  def handle_call(:view, _from, state), do: {:reply, state.view, state}

  def handle_call({:attributes, member}, _from, state) do
    {:reply, Membership.attributes(state.view, member), state}
  end

  def handle_call({:set_attributes, attributes}, _from, state) do
    # Update our own attributes locally (raising our incarnation); gossip carries it onward.
    {view, _effect} = Membership.set_attributes(state.view, attributes)
    {:reply, :ok, %{state | view: view}}
  end

  def handle_call(:topology, _from, state), do: {:reply, state.topology, state}

  def handle_call({:set_topology, topology}, _from, state) do
    # Adopt the higher version (in case gossip already carried a newer one); gossip carries ours onward.
    {:reply, :ok, adopt_topology(state, topology)}
  end

  @impl true
  def handle_cast({:ping, from, updates}, state) do
    state = merge_updates(state, updates)
    cast(from, {:ack, state.self, gossip_payload(state)})
    {:noreply, state}
  end

  def handle_cast({:ack, from, updates}, state) do
    state = merge_updates(state, updates)
    {:noreply, %{state | awaiting: MapSet.delete(state.awaiting, from)}}
  end

  # A peer asks us to probe `target` on behalf of `requester` (indirect ping).
  def handle_cast({:ping_req, target, requester, updates}, state) do
    state = merge_updates(state, updates)
    cast(target, {:ping_relay, state.self, requester, gossip_payload(state)})
    {:noreply, state}
  end

  # We are the target of a relayed probe; ack back to the relay so it can forward to the requester.
  def handle_cast({:ping_relay, relay, requester, updates}, state) do
    state = merge_updates(state, updates)
    cast(relay, {:ack_relay, state.self, requester, gossip_payload(state)})
    {:noreply, state}
  end

  # The relay heard back from the target; forward an ack to the requester as if from the target.
  def handle_cast({:ack_relay, target, requester, updates}, state) do
    state = merge_updates(state, updates)
    cast(requester, {:ack, target, gossip_payload(state)})
    {:noreply, state}
  end

  # A node is joining via us as a seed: record it alive and reply with our full view. Its own
  # attributes arrive in `updates` (merged above); this is just the belt-and-suspenders alive record.
  def handle_cast({:join, joiner, updates}, state) do
    state = merge_updates(state, updates)
    {view, _effect} = Membership.apply_update(state.view, {joiner, :alive, 0, %{}})
    state = %{state | view: view}
    cast(joiner, {:join_ok, state.self, gossip_payload(state)})
    {:noreply, state}
  end

  # A seed answered our join with its full view of the cluster.
  def handle_cast({:join_ok, _seed, updates}, state) do
    {:noreply, merge_updates(state, updates)}
  end

  @impl true
  def handle_info(:protocol_period, state) do
    state = ping_random_peer(state)
    schedule_period(state)
    {:noreply, state}
  end

  def handle_info({:ack_timeout, target}, state) do
    # Direct ping went unanswered: try indirect ping (keep awaiting) before suspecting.
    if MapSet.member?(state.awaiting, target) do
      {:noreply, start_indirect(state, target)}
    else
      {:noreply, state}
    end
  end

  def handle_info({:indirect_timeout, target}, state) do
    # Neither the direct nor any relayed ack arrived: now suspect.
    if MapSet.member?(state.awaiting, target) do
      state = %{state | awaiting: MapSet.delete(state.awaiting, target)}
      {:noreply, suspect(state, target)}
    else
      {:noreply, state}
    end
  end

  def handle_info({:suspicion_timeout, target, incarnation}, state) do
    still_suspect? =
      Membership.status(state.view, target) == :suspect and
        Membership.incarnation(state.view, target) == incarnation

    if still_suspect? do
      {view, _effect} = Membership.confirm(state.view, target)
      {:noreply, %{state | view: view}}
    else
      {:noreply, state}
    end
  end

  # --- internals ---

  defp ping_random_peer(state) do
    case alive_peers(state) do
      [] ->
        state

      peers ->
        target = Enum.random(peers)
        cast(target, {:ping, state.self, gossip_payload(state)})
        Process.send_after(self(), {:ack_timeout, target}, state.ack_timeout)
        %{state | awaiting: MapSet.put(state.awaiting, target)}
    end
  end

  defp start_indirect(state, target) do
    relays =
      state.view
      |> Membership.alive_members()
      |> Kernel.--([state.self, target])
      |> Enum.take_random(state.indirect_fanout)

    Enum.each(relays, fn relay ->
      cast(relay, {:ping_req, target, state.self, gossip_payload(state)})
    end)

    # Even with no relays available, schedule the timeout so an unreachable target is still suspected.
    Process.send_after(self(), {:indirect_timeout, target}, state.indirect_timeout)
    state
  end

  defp suspect(state, target) do
    case Membership.suspect(state.view, target) do
      {view, {:applied, _update}} ->
        incarnation = Membership.incarnation(view, target)
        Process.send_after(self(), {:suspicion_timeout, target, incarnation}, state.suspicion_timeout)
        %{state | view: view}

      {view, _effect} ->
        %{state | view: view}
    end
  end

  # Outbound gossip payload piggybacked on every message: our view updates plus our current topology.
  defp gossip_payload(state), do: {Membership.updates(state.view), state.topology}

  # Ingest a peer's gossip (the value bound in each message handler): merge its view updates and its
  # topology observation. Tolerates an older peer that gossips a bare updates list (pre-topology) — its
  # view still merges and the topology is left as-is.
  defp merge_updates(state, {updates, topology}) do
    {view, _effects} = Membership.merge(state.view, updates)
    adopt_topology(%{state | view: view}, topology)
  end

  defp merge_updates(state, updates) when is_list(updates) do
    {view, _effects} = Membership.merge(state.view, updates)
    %{state | view: view}
  end

  # Adopt `remote` if it is a newer topology than ours (CRDT last-version-wins), firing `on_topology` on
  # an actual **advance** (a version increase) so this node applies the new ring locally. A same-or-older
  # version is a no-op — no adoption, no hook.
  defp adopt_topology(state, remote) do
    merged = merge_topology(state.topology, remote)
    if topology_advanced?(state.topology, merged), do: state.on_topology.(merged)
    %{state | topology: merged}
  end

  defp topology_advanced?(nil, %RingTopology{}), do: true
  defp topology_advanced?(%RingTopology{version: old}, %RingTopology{version: new}), do: new > old
  defp topology_advanced?(_local, nil), do: false

  # Converge two topology observations (last-version-wins), tolerating either side being absent (nil).
  defp merge_topology(nil, remote), do: remote
  defp merge_topology(local, nil), do: local
  defp merge_topology(local, remote), do: RingTopology.merge(local, remote)

  # Best-effort join: ask each seed (the initially known peers) for its view of the cluster.
  defp send_joins(state) do
    Enum.each(alive_peers(state), fn seed ->
      cast(seed, {:join, state.self, gossip_payload(state)})
    end)
  end

  defp alive_peers(state), do: Membership.alive_members(state.view) -- [state.self]

  defp schedule_period(state), do: Process.send_after(self(), :protocol_period, state.protocol_period)

  # Fire-and-forget, and never crash this server if the destination is gone (an unregistered name
  # would otherwise raise): a missing peer just produces no ack, which the detector handles.
  defp cast(ref, message) do
    GenServer.cast(ref, message)
  catch
    _kind, _reason -> :ok
  end
end
