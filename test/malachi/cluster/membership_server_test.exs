defmodule Malachi.Cluster.MembershipServerTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.Membership
  alias Malachi.Cluster.MembershipServer
  alias Malachi.Cluster.RingTopology
  alias Malachi.Test.UnknownMessages

  # Tight timings so the detector converges quickly in tests.
  @timings [protocol_period: 15, ack_timeout: 15, suspicion_timeout: 90]

  # A minimal one-vnode topology at a specific version (content is fixed, so equal versions are `==`).
  defp topology_at(version) do
    {:ok, ring} = HashRing.add_vnode(HashRing.new(), :v0, 0)
    %RingTopology{version: version, ring: ring, placements: %{v0: [node()]}}
  end

  defp start_node(name, peers) do
    start_supervised!({MembershipServer, [name: name, peers: peers] ++ @timings}, id: name)
    name
  end

  defp eventually(check, remaining_ms \\ 3_000) do
    cond do
      check.() -> true
      remaining_ms <= 0 -> false
      true -> Process.sleep(15) && eventually(check, remaining_ms - 15)
    end
  end

  test "self_ref is the member's gossiped identity, distinct from the local registered name" do
    name = :"msr_#{System.unique_integer([:positive])}"
    self_ref = {:msr_id, name}

    start_supervised!({MembershipServer, [name: name, self_ref: self_ref, peers: []] ++ @timings}, id: name)

    # the server registers under `name` (callable), but its own identity in the view is `self_ref`
    assert MembershipServer.alive_members(name) == [self_ref]
  end

  test "gossip spreads partial seed knowledge until every node knows every node" do
    suffix = System.unique_integer([:positive])
    a = :"ms_a_#{suffix}"
    b = :"ms_b_#{suffix}"
    c = :"ms_c_#{suffix}"

    # a knows only b; c knows only b; b bridges them
    start_node(a, [b])
    start_node(b, [a, c])
    start_node(c, [b])

    full = Enum.sort([a, b, c])
    assert eventually(fn -> Enum.all?([a, b, c], &(MembershipServer.alive_members(&1) == full)) end)
  end

  test "a stopped node is detected and marked dead across the cluster" do
    suffix = System.unique_integer([:positive])
    a = :"ms_a_#{suffix}"
    b = :"ms_b_#{suffix}"
    c = :"ms_c_#{suffix}"

    start_node(a, [b, c])
    start_node(b, [a, c])
    start_node(c, [a, b])

    # let them all see each other first
    assert eventually(fn -> MembershipServer.alive_members(a) == Enum.sort([a, b, c]) end)

    :ok = stop_supervised!(c)

    # the survivors converge on c being dead (suspect → dead after the suspicion timeout)
    assert eventually(fn ->
             Membership.status(MembershipServer.view(a), c) == :dead and
               Membership.status(MembershipServer.view(b), c) == :dead
           end)

    assert MembershipServer.alive_members(a) == Enum.sort([a, b])
    assert MembershipServer.alive_members(b) == Enum.sort([a, b])
  end

  test "an indirect ping relays the target's ack back to the requester" do
    suffix = System.unique_integer([:positive])
    relay = :"ms_relay_#{suffix}"
    target = :"ms_target_#{suffix}"

    start_node(relay, [])
    start_node(target, [])

    # ask the relay to probe the target on our behalf (as the failure detector would)
    GenServer.cast(relay, {:ping_req, target, self(), []})

    # the relay probes the target, the target acks the relay, and the relay forwards the ack to us
    assert_receive {:"$gen_cast", {:ack, ^target, _updates}}, 1_000
  end

  test "a join records the joiner and replies with the seed's full view" do
    suffix = System.unique_integer([:positive])
    seed = :"ms_seed_#{suffix}"
    other = :"ms_other_#{suffix}"

    start_node(seed, [other])

    # join as if we were a new node
    GenServer.cast(seed, {:join, self(), []})

    # the reply piggybacks the seed's gossip payload: {view updates, topology}
    assert_receive {:"$gen_cast", {:join_ok, ^seed, {updates, _topology}}}, 1_000
    # the seed shares its cluster view (itself and the member it knew)
    members = for {member, _status, _inc, _attrs} <- updates, do: member
    assert seed in members and other in members

    # and the seed now knows us as alive
    assert Membership.status(MembershipServer.view(seed), self()) == :alive
  end

  test "a new node learns the whole cluster by joining a single seed" do
    suffix = System.unique_integer([:positive])
    a = :"ms_a_#{suffix}"
    b = :"ms_b_#{suffix}"
    c = :"ms_c_#{suffix}"

    start_node(a, [b])
    start_node(b, [a])
    assert eventually(fn -> MembershipServer.alive_members(a) == Enum.sort([a, b]) end)

    # c is seeded with only a, yet should learn b (and a should learn c)
    start_node(c, [a])

    full = Enum.sort([a, b, c])
    assert eventually(fn -> MembershipServer.alive_members(c) == full end)
    assert eventually(fn -> MembershipServer.alive_members(a) == full end)
  end

  test "a node refutes a false suspicion about itself" do
    suffix = System.unique_integer([:positive])
    a = :"ms_a_#{suffix}"
    b = :"ms_b_#{suffix}"

    start_node(a, [b])
    start_node(b, [a])

    # inject gossip (as if from b) that wrongly suspects a at its current incarnation
    GenServer.cast(a, {:ping, b, [{a, :suspect, 0, %{}}]})

    # a stays alive and bumps its incarnation to refute
    assert eventually(fn ->
             view = MembershipServer.view(a)
             Membership.status(view, a) == :alive and Membership.incarnation(view, a) >= 1
           end)
  end

  describe "the incarnation reservation" do
    test "starts at the reserved incarnation rather than at the first-boot default" do
      a = :"msinc_#{System.unique_integer([:positive])}"
      start_supervised!({MembershipServer, [name: a, peers: [], incarnation: 40] ++ @timings}, id: a)

      assert Membership.incarnation(MembershipServer.view(a), a) == 40
    end

    test "reserves on every process start, so a restart resumes above the last one" do
      # The defect this closes: a child spec is built once and reused, so a reservation made while the
      # spec was built would seed a restarted server at the number the first one started from, below
      # whatever it had raised itself to.
      {:ok, calls} = Agent.start_link(fn -> 0 end)
      on_exit(fn -> if Process.alive?(calls), do: Agent.stop(calls) end)
      reserve = fn -> {:ok, %{start: Agent.get_and_update(calls, &{&1 * 10 + 1, &1 + 1}), ceiling: 9_999}} end

      a = :"msinc_#{System.unique_integer([:positive])}"
      start_supervised!({MembershipServer, [name: a, peers: [], reserve: reserve] ++ @timings}, id: a)

      assert Membership.incarnation(MembershipServer.view(a), a) == 1
      assert Agent.get(calls, & &1) == 1

      stop_supervised!(a)
      start_supervised!({MembershipServer, [name: a, peers: [], reserve: reserve] ++ @timings}, id: a)

      assert Membership.incarnation(MembershipServer.view(a), a) == 11
      assert Agent.get(calls, & &1) == 2
    end

    test "a reservation that fails stops the server from starting" do
      a = :"msinc_#{System.unique_integer([:positive])}"
      opts = [name: a, peers: [], reserve: fn -> {:error, :enospc} end] ++ @timings

      assert {:error, {{%RuntimeError{}, _stack}, _child}} = start_supervised({MembershipServer, opts}, id: a)
    end

    test "asks for a new ceiling once it has used the block, and adopts the answer" do
      # The block is what keeps the durable write off the failure detector's path: the node raises its
      # own incarnation this many times before it touches a disk again.
      parent = self()
      a = :"msinc_#{System.unique_integer([:positive])}"

      on_ceiling = fn incarnation ->
        send(parent, {:ceiling_asked, incarnation})
        {:ok, incarnation + 10}
      end

      opts = [name: a, peers: [], incarnation: 1, ceiling: 3, on_ceiling: on_ceiling] ++ @timings
      start_supervised!({MembershipServer, opts}, id: a)

      :ok = MembershipServer.set_attributes(a, %{rack: "1"})
      refute_received {:ceiling_asked, _incarnation}

      :ok = MembershipServer.set_attributes(a, %{rack: "2"})
      assert_received {:ceiling_asked, 3}

      # The new ceiling was adopted, so the next rise does not ask again.
      :ok = MembershipServer.set_attributes(a, %{rack: "3"})
      refute_received {:ceiling_asked, _incarnation}
    end

    test "a lost ceiling is latched: it stops once and stops announcing itself" do
      # System.stop/0 is asynchronous, so this server keeps handling messages for the whole shutdown
      # window. Without the latch every ping, ack and join would see the same incarnation above the same
      # stale ceiling and repeat the write, the log and the stop.
      parent = self()
      a = :"msinc_#{System.unique_integer([:positive])}"

      opts =
        [
          name: a,
          peers: [],
          incarnation: 1,
          ceiling: 2,
          on_ceiling: fn i ->
            send(parent, {:asked, i})
            {:error, :enospc}
          end,
          stop_fun: fn -> send(parent, :stopping) end
        ] ++ @timings

      start_supervised!({MembershipServer, opts}, id: a)

      capture_log(fn ->
        :ok = MembershipServer.set_attributes(a, %{rack: "x"})
        # Whatever a peer sends afterwards must not restart the storm.
        for _ <- 1..5, do: GenServer.cast(a, {:ping, :nobody, {[], nil}})
        _ = MembershipServer.view(a)
      end)

      assert_received :stopping
      refute_received :stopping
      assert_received {:asked, 2}
      refute_received {:asked, _again}
    end

    test "a node that has lost its ceiling leaves itself out of its own gossip" do
      # Its incarnation may already be past the last ceiling on disk, and a refutation in the shutdown
      # window can raise it further. Announcing that would leave peers remembering a number this node
      # cannot resume above. Read straight off the wire: pose as a peer, ping it, and look at the view it
      # piggybacks on the ack.
      a = :"msgossip_#{System.unique_integer([:positive])}"

      opts =
        [
          name: a,
          peers: [],
          incarnation: 1,
          ceiling: 2,
          on_ceiling: fn _i -> {:error, :enospc} end,
          stop_fun: fn -> :ok end
        ] ++ @timings

      start_supervised!({MembershipServer, opts}, id: a)

      # Through the real path: a peer's gossip, not a local copy of the view. Applying an update to what
      # `view/1` hands back changes nothing in the server, and a test that did that would leave the other
      # half of the contract below unchecked.
      GenServer.cast(a, {:ping, :nobody, {[{:peer, :alive, 7, %{}}], nil}})
      assert eventually(fn -> Membership.status(MembershipServer.view(a), :peer) == :alive end)

      assert gossiped_members(a) == Enum.sort([a, :peer]),
             "expected it to announce itself and the peer while healthy"

      capture_log(fn -> :ok = MembershipServer.set_attributes(a, %{rack: "x"}) end)

      # Itself out, and only itself: what it knows about everyone else is not this node's to withhold.
      assert gossiped_members(a) == [:peer], "a node that lost its ceiling stopped gossiping the cluster"
    end

    test "stops the node when a new ceiling cannot be written" do
      # Carrying on would gossip incarnations above the last one on disk, and the next restart would
      # resume below what peers remember, where nothing corrects it. Stopping is recoverable: the node
      # comes back and reserves a block it can trust.
      parent = self()
      a = :"msinc_#{System.unique_integer([:positive])}"

      opts =
        [
          name: a,
          peers: [],
          incarnation: 1,
          ceiling: 2,
          on_ceiling: fn _i -> {:error, :enospc} end,
          stop_fun: fn -> send(parent, :stopping) end
        ] ++ @timings

      start_supervised!({MembershipServer, opts}, id: a)

      log = capture_log(fn -> :ok = MembershipServer.set_attributes(a, %{rack: "x"}) end)

      assert_received :stopping
      assert log =~ "incarnation ceiling"
    end

    test "a peer claiming a higher incarnation for us changes nothing" do
      # We own our own number and resume it above our past from disk, so a peer's copy of it tells us
      # nothing. Adopting it would hand a peer the ability to drive this node's incarnation upward from
      # outside, and each step past the reserved block costs a durable write inline in this server.
      parent = self()
      a = :"msinc_#{System.unique_integer([:positive])}"
      opts = [name: a, peers: [], incarnation: 2, ceiling: 3, on_ceiling: fn i -> send(parent, {:asked, i}) end]
      start_supervised!({MembershipServer, opts ++ @timings}, id: a)

      GenServer.cast(a, {:ping, :nobody, [{a, :alive, 9_999, %{rack: "theirs"}}]})
      assert MembershipServer.attributes(a, a) == %{}

      assert Membership.incarnation(MembershipServer.view(a), a) == 2
      refute_received {:asked, _incarnation}
    end

    test "a server started without a reservation never asks for a ceiling" do
      parent = self()
      a = :"msinc_#{System.unique_integer([:positive])}"
      opts = [name: a, peers: [], on_ceiling: fn i -> send(parent, {:asked, i}) end] ++ @timings
      start_supervised!({MembershipServer, opts}, id: a)

      for rack <- ["a", "b", "c"], do: :ok = MembershipServer.set_attributes(a, %{rack: rack})

      refute_received {:asked, _incarnation}
    end
  end

  describe "attributes" do
    test "a node's own attributes are set at start and readable" do
      a = :"msattr_#{System.unique_integer([:positive])}"
      start_supervised!({MembershipServer, [name: a, peers: [], attributes: %{rack: "a"}] ++ @timings}, id: a)

      assert MembershipServer.attributes(a, a) == %{rack: "a"}
    end

    test "set_attributes updates own attributes and gossip propagates them to peers" do
      suffix = System.unique_integer([:positive])
      a = :"msattr_a_#{suffix}"
      b = :"msattr_b_#{suffix}"

      start_node(a, [b])
      start_node(b, [a])

      # both learn of each other first
      full = Enum.sort([a, b])
      assert eventually(fn -> MembershipServer.alive_members(b) == full end)

      :ok = MembershipServer.set_attributes(a, %{rack: "x"})

      # b learns a's attributes through gossip (and a knows its own immediately)
      assert MembershipServer.attributes(a, a) == %{rack: "x"}
      assert eventually(fn -> MembershipServer.attributes(b, a) == %{rack: "x"} end)
    end
  end

  describe "topology dissemination" do
    test "set_topology on one node propagates to peers by gossip" do
      suffix = System.unique_integer([:positive])
      a = :"mstopo_a_#{suffix}"
      b = :"mstopo_b_#{suffix}"

      start_node(a, [b])
      start_node(b, [a])
      assert eventually(fn -> MembershipServer.alive_members(b) == Enum.sort([a, b]) end)

      # a fresh cluster carries no topology
      assert MembershipServer.topology(a) == nil
      assert MembershipServer.topology(b) == nil

      topo = topology_at(1)
      :ok = MembershipServer.set_topology(a, topo)

      assert MembershipServer.topology(a) == topo
      assert eventually(fn -> MembershipServer.topology(b) == topo end)
    end

    test "the highest version wins on every node, whoever set it (last-version-wins)" do
      suffix = System.unique_integer([:positive])
      a = :"mstopo_a_#{suffix}"
      b = :"mstopo_b_#{suffix}"

      start_node(a, [b])
      start_node(b, [a])
      assert eventually(fn -> MembershipServer.alive_members(b) == Enum.sort([a, b]) end)

      :ok = MembershipServer.set_topology(a, topology_at(1))
      :ok = MembershipServer.set_topology(b, topology_at(2))

      # both converge on version 2 (b's), regardless of the order gossip carried them
      assert eventually(fn -> MembershipServer.topology(a) == topology_at(2) end)
      assert eventually(fn -> MembershipServer.topology(b) == topology_at(2) end)
    end
  end

  describe "topology adoption hook (on_topology)" do
    test "fires with the new topology on a version advance, not on a same-or-lower version" do
      test = self()
      a = :"mshook_#{System.unique_integer([:positive])}"

      start_supervised!(
        {MembershipServer, [name: a, peers: [], on_topology: fn t -> send(test, {:adopted, t}) end] ++ @timings},
        id: a
      )

      :ok = MembershipServer.set_topology(a, topology_at(1))
      assert_receive {:adopted, %RingTopology{version: 1}}

      # a higher version advances → fires again
      :ok = MembershipServer.set_topology(a, topology_at(2))
      assert_receive {:adopted, %RingTopology{version: 2}}

      # a same-or-lower version is a no-op → the hook does not fire
      :ok = MembershipServer.set_topology(a, topology_at(1))
      refute_receive {:adopted, _}, 100
    end

    test "fires on a node that learns a higher-version topology by gossip" do
      test = self()
      suffix = System.unique_integer([:positive])
      a = :"mshook_a_#{suffix}"
      b = :"mshook_b_#{suffix}"

      start_node(a, [b])

      start_supervised!(
        {MembershipServer, [name: b, peers: [a], on_topology: fn t -> send(test, {:b_adopted, t}) end] ++ @timings},
        id: b
      )

      assert eventually(fn -> MembershipServer.alive_members(a) == Enum.sort([a, b]) end)

      :ok = MembershipServer.set_topology(a, topology_at(1))
      # b adopts a's topology through gossip and its hook fires
      assert_receive {:b_adopted, %RingTopology{version: 1}}, 2_000
    end
  end

  # Every cast here is a SWIM message from another node, so a newer member's new message is the first
  # thing an older one meets during a rolling upgrade.
  test "an unknown SWIM message, info message or call is counted and survived" do
    name = start_node(:"ms_unknown_#{System.unique_integer([:positive])}", [])

    UnknownMessages.assert_survives_unknown(name, :membership, fn ->
      assert MembershipServer.alive_members(name) == [name]
    end)
  end

  # The members a server names in the view it piggybacks on an ack, read by posing as a peer that pings it.
  defp gossiped_members(server) do
    probe = self()
    GenServer.cast(server, {:ping, probe, {[], nil}})

    receive do
      {:"$gen_cast", {:ack, _from, {updates, _topology}}} -> updates |> Enum.map(&elem(&1, 0)) |> Enum.sort()
    after
      1_000 -> flunk("#{server} never acked the ping")
    end
  end
end
