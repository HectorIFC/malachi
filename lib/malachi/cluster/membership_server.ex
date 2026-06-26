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

  Scope: direct ping, indirect ping, suspicion, gossip, and a join handshake.
  """

  use GenServer

  alias Malachi.Cluster.Membership

  @default_protocol_period 1_000
  @default_ack_timeout 500
  @default_suspicion_timeout 3_000
  @default_indirect_fanout 3

  @doc """
  Starts a membership server.

  ## Options
    * `:name` (optional) - the reference this server is known by; its pid when omitted.
    * `:peers` - seed peer references, learned as `:alive`.
    * `:protocol_period` / `:ack_timeout` / `:suspicion_timeout` - detector timings in ms.
    * `:indirect_timeout` - ms to wait for an indirect (relayed) ack (default `ack_timeout`).
    * `:indirect_fanout` - number of peers asked to relay a ping (default 3).
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    gen_server_opts =
      case Keyword.fetch(opts, :name) do
        {:ok, name} -> [name: name]
        :error -> []
      end

    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @doc "The sorted list of currently alive members (the live broker set)."
  @spec alive_members(GenServer.server()) :: [Membership.member()]
  def alive_members(server), do: GenServer.call(server, :alive_members)

  @doc "The current `Malachi.Cluster.Membership` view (for inspection/tests)."
  @spec view(GenServer.server()) :: Membership.t()
  def view(server), do: GenServer.call(server, :view)

  # --- server ---

  @impl true
  def init(opts) do
    self_ref = Keyword.get(opts, :name) || self()
    ack_timeout = Keyword.get(opts, :ack_timeout, @default_ack_timeout)

    state = %{
      view: Membership.new(self_ref, peers: Keyword.get(opts, :peers, [])),
      self: self_ref,
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

  @impl true
  def handle_cast({:ping, from, updates}, state) do
    state = merge_updates(state, updates)
    cast(from, {:ack, state.self, Membership.updates(state.view)})
    {:noreply, state}
  end

  def handle_cast({:ack, from, updates}, state) do
    state = merge_updates(state, updates)
    {:noreply, %{state | awaiting: MapSet.delete(state.awaiting, from)}}
  end

  # A peer asks us to probe `target` on behalf of `requester` (indirect ping).
  def handle_cast({:ping_req, target, requester, updates}, state) do
    state = merge_updates(state, updates)
    cast(target, {:ping_relay, state.self, requester, Membership.updates(state.view)})
    {:noreply, state}
  end

  # We are the target of a relayed probe; ack back to the relay so it can forward to the requester.
  def handle_cast({:ping_relay, relay, requester, updates}, state) do
    state = merge_updates(state, updates)
    cast(relay, {:ack_relay, state.self, requester, Membership.updates(state.view)})
    {:noreply, state}
  end

  # The relay heard back from the target; forward an ack to the requester as if from the target.
  def handle_cast({:ack_relay, target, requester, updates}, state) do
    state = merge_updates(state, updates)
    cast(requester, {:ack, target, Membership.updates(state.view)})
    {:noreply, state}
  end

  # A node is joining via us as a seed: record it alive and reply with our full view.
  def handle_cast({:join, joiner, updates}, state) do
    state = merge_updates(state, updates)
    {view, _effect} = Membership.apply_update(state.view, {joiner, :alive, 0})
    state = %{state | view: view}
    cast(joiner, {:join_ok, state.self, Membership.updates(state.view)})
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
        cast(target, {:ping, state.self, Membership.updates(state.view)})
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
      cast(relay, {:ping_req, target, state.self, Membership.updates(state.view)})
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

  defp merge_updates(state, updates) do
    {view, _effects} = Membership.merge(state.view, updates)
    %{state | view: view}
  end

  # Best-effort join: ask each seed (the initially known peers) for its view of the cluster.
  defp send_joins(state) do
    Enum.each(alive_peers(state), fn seed ->
      cast(seed, {:join, state.self, Membership.updates(state.view)})
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
