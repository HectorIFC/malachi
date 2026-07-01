defmodule Malachi.Cluster.MembershipServerTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.Membership
  alias Malachi.Cluster.MembershipServer

  # Tight timings so the detector converges quickly in tests.
  @timings [protocol_period: 15, ack_timeout: 15, suspicion_timeout: 90]

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

    assert_receive {:"$gen_cast", {:join_ok, ^seed, updates}}, 1_000
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
end
