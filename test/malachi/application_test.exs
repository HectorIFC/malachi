defmodule Malachi.ApplicationTest do
  use ExUnit.Case, async: true

  alias Malachi.Application, as: App
  alias Malachi.Cluster.Capabilities
  alias Malachi.Cluster.MemberIncarnation

  describe "metadata_cluster_opts/2" do
    test "no cluster configured yields no metadata options (single-node in-memory default)" do
      assert App.metadata_cluster_opts(nil, [:"a@127.0.0.1"]) == []
    end

    test "a configured cluster yields the ra-backed control-plane options" do
      opts = App.metadata_cluster_opts(:log_meta, [:"a@127.0.0.1", :"b@127.0.0.1"])

      assert Keyword.fetch!(opts, :metadata_cluster) == :log_meta
      assert Keyword.fetch!(opts, :metadata_nodes) == [:"a@127.0.0.1", :"b@127.0.0.1"]
    end
  end

  describe "sharded_vnodes/2" do
    test "names vnodes from the base and spreads distinct tokens evenly over the 32-bit ring" do
      vnodes = App.sharded_vnodes(:log_meta, 4)
      ring_size = Integer.pow(2, 32)

      assert vnodes == [
               {:log_meta_vn_0, 0},
               {:log_meta_vn_1, div(ring_size, 4)},
               {:log_meta_vn_2, div(ring_size, 2)},
               {:log_meta_vn_3, div(3 * ring_size, 4)}
             ]

      # tokens are distinct and in range (the ring rejects duplicates / out-of-range placement)
      tokens = Enum.map(vnodes, &elem(&1, 1))
      assert length(Enum.uniq(tokens)) == 4
      assert Enum.all?(tokens, &(&1 >= 0 and &1 < ring_size))
    end

    test "a single vnode sits at token 0" do
      assert App.sharded_vnodes(:log_meta, 1) == [{:log_meta_vn_0, 0}]
    end
  end

  describe "place_vnodes/3" do
    test "assigns each vnode replication_factor nodes by HRW, carrying the token" do
      vnodes = App.sharded_vnodes(:log_meta, 4)
      nodes = [:a@h, :b@h, :c@h, :d@h, :e@h]

      placed = App.place_vnodes(vnodes, nodes, 3)

      # every vnode keeps its {id, token} and gains exactly 3 distinct nodes drawn from the set
      assert length(placed) == 4

      for {{vnode_id, token}, {p_id, p_token, chosen}} <- Enum.zip(vnodes, placed) do
        assert {p_id, p_token} == {vnode_id, token}
        assert length(chosen) == 3
        assert length(Enum.uniq(chosen)) == 3
        assert Enum.all?(chosen, &(&1 in nodes))
      end

      # HRW spreads the vnodes' primaries rather than piling them on one node
      primaries = Enum.map(placed, fn {_id, _token, [primary | _]} -> primary end)
      assert length(Enum.uniq(primaries)) > 1, "expected vnode primaries to spread across nodes"
    end

    test "is deterministic: the same inputs yield the same placement" do
      vnodes = App.sharded_vnodes(:log_meta, 3)
      nodes = [:a@h, :b@h, :c@h]
      assert App.place_vnodes(vnodes, nodes, 2) == App.place_vnodes(vnodes, nodes, 2)
    end

    test "clamps the replica count to the number of nodes" do
      vnodes = App.sharded_vnodes(:log_meta, 2)
      nodes = [:a@h, :b@h]

      for {_id, _token, chosen} <- App.place_vnodes(vnodes, nodes, 3) do
        assert Enum.sort(chosen) == [:a@h, :b@h]
      end
    end

    test "with a topology, each vnode's replicas land in distinct racks" do
      vnodes = App.sharded_vnodes(:log_meta, 6)
      nodes = [:a1@h, :a2@h, :b1@h, :b2@h]

      attrs = %{
        :a1@h => %{"rack" => "a"},
        :a2@h => %{"rack" => "a"},
        :b1@h => %{"rack" => "b"},
        :b2@h => %{"rack" => "b"}
      }

      placed = App.place_vnodes(vnodes, nodes, 2, spread: {"rack", attrs})

      # every vnode's 2 replicas span both racks: a whole rack can fail without losing a majority
      for {_id, _token, chosen} <- placed do
        racks = Enum.map(chosen, fn n -> attrs[n]["rack"] end)
        assert Enum.sort(racks) == ["a", "b"]
      end

      # still deterministic
      assert App.place_vnodes(vnodes, nodes, 2, spread: {"rack", attrs}) == placed
    end

    test "with :max_skew, balances vnode replicas across nodes" do
      vnodes = App.sharded_vnodes(:log_meta, 9)
      nodes = [:a@h, :b@h, :c@h]

      placed = App.place_vnodes(vnodes, nodes, 1, max_skew: 1)

      assert length(placed) == 9
      for {_id, _token, chosen} <- placed, do: assert(length(chosen) == 1)

      # evenly spread (3/3/3) instead of HRW's lopsided split, and each vnode keeps its {id, token}
      loads = placed |> Enum.flat_map(fn {_id, _token, ns} -> ns end) |> Enum.frequencies() |> Map.values()
      assert Enum.max(loads) - Enum.min(loads) <= 1

      for {{vnode_id, token}, {p_id, p_token, _ns}} <- Enum.zip(vnodes, placed) do
        assert {p_id, p_token} == {vnode_id, token}
      end
    end
  end

  describe "desired_placement/5" do
    test "places every vnode over the given live nodes, deterministically" do
      nodes = [:a@h, :b@h, :c@h]
      placement = App.desired_placement(:log_meta, 6, nodes, 2)

      assert placement == App.desired_placement(:log_meta, 6, nodes, 2)
      assert length(placement) == 6

      for {_id, _token, chosen} <- placement do
        assert length(chosen) == 2
        assert Enum.all?(chosen, &(&1 in nodes))
      end
    end

    test "adding a node re-places only vnodes that adopt it (minimal movement)" do
      before = App.desired_placement(:log_meta, 12, [:a@h, :b@h, :c@h], 2)
      after_join = App.desired_placement(:log_meta, 12, [:a@h, :b@h, :c@h, :d@h], 2)

      # a vnode's replica set changes only if it took on the new node; everything else stays put
      for {{id, _t, ns_before}, {id, _t2, ns_after}} <- Enum.zip(before, after_join),
          ns_after != ns_before do
        assert :d@h in ns_after
      end

      # and the join is not a no-op: some vnode actually adopts the new node
      assert Enum.any?(after_join, fn {_id, _t, ns} -> :d@h in ns end)
    end

    test "removing a node re-places only vnodes that held it (minimal movement)" do
      before = App.desired_placement(:log_meta, 12, [:a@h, :b@h, :c@h, :d@h], 2)
      after_leave = App.desired_placement(:log_meta, 12, [:a@h, :b@h, :c@h], 2)

      for {{id, _t, ns_before}, {id, _t2, ns_after}} <- Enum.zip(before, after_leave),
          ns_after != ns_before do
        assert :d@h in ns_before
      end

      # the departed node no longer appears anywhere
      refute Enum.any?(after_leave, fn {_id, _t, ns} -> :d@h in ns end)
    end

    test "clamps replicas to the number of live nodes" do
      for {_id, _token, chosen} <- App.desired_placement(:log_meta, 4, [:a@h, :b@h], 3) do
        assert Enum.sort(chosen) == [:a@h, :b@h]
      end
    end
  end

  describe "rebalance_plan/2" do
    test "is empty when the current placement already matches the desired one" do
      placement = [{:vn_0, 0, [:a@h, :b@h]}, {:vn_1, 1, [:b@h, :c@h]}]
      assert App.rebalance_plan(placement, placement) == []
    end

    test "yields add/remove per changed vnode and omits unchanged ones" do
      current = [{:vn_0, 0, [:a@h, :b@h]}, {:vn_1, 1, [:b@h, :c@h]}]
      desired = [{:vn_0, 0, [:a@h, :d@h]}, {:vn_1, 1, [:b@h, :c@h]}]

      assert App.rebalance_plan(current, desired) ==
               [%{vnode_id: :vn_0, add: [:d@h], remove: [:b@h]}]
    end

    test "a join keeps every vnode's replica count constant (add and remove balance, so add-before-remove never drops below quorum)" do
      current = App.desired_placement(:log_meta, 12, [:a@h, :b@h, :c@h], 2)
      desired = App.desired_placement(:log_meta, 12, [:a@h, :b@h, :c@h, :d@h], 2)
      plan = App.rebalance_plan(current, desired)

      refute plan == []
      for %{add: add, remove: remove} <- plan, do: assert(length(add) == length(remove))
    end

    test "a leave plans to add the same node it removes for exactly the vnodes that held the departed node" do
      current = App.desired_placement(:log_meta, 12, [:a@h, :b@h, :c@h, :d@h], 2)
      desired = App.desired_placement(:log_meta, 12, [:a@h, :b@h, :c@h], 2)
      plan = App.rebalance_plan(current, desired)

      # every planned vnode is one that held the departed node
      for %{remove: remove} <- plan, do: assert(:d@h in remove)
      # and no planned change re-adds the departed node
      for %{add: add} <- plan, do: refute(:d@h in add)

      changed_ids = Enum.map(plan, & &1.vnode_id)
      assert changed_ids == Enum.uniq(changed_ids)
    end
  end

  describe "readable_placement/2" do
    test "reads each vnode's current members, omitting ones that cannot be read" do
      configs = [{:vn_0, 0}, {:vn_1, 1}, {:vn_2, 2}]

      members_of = fn
        :vn_0 -> {:ok, [:a@h, :b@h]}
        :vn_1 -> {:error, :noproc}
        :vn_2 -> {:ok, [:b@h, :c@h]}
      end

      # vn_1 is unreachable, so it is left out of the current placement
      assert App.readable_placement(configs, members_of) ==
               [{:vn_0, 0, [:a@h, :b@h]}, {:vn_2, 2, [:b@h, :c@h]}]
    end
  end

  describe "live_rebalance_plan/5" do
    test "plans the moves from current ra memberships to the placement desired over live members" do
      configs = App.sharded_vnodes(:log_meta, 12)
      # current: every vnode lives on {a,b,c}; desired: recomputed over {a,b,c,d} (d joined)
      current = App.desired_placement(:log_meta, 12, [:a@h, :b@h, :c@h], 2)
      members_of = fn id -> {:ok, current |> Enum.find(&(elem(&1, 0) == id)) |> elem(2)} end

      plan = App.live_rebalance_plan(configs, members_of, [:a@h, :b@h, :c@h, :d@h], 2)

      # equivalent to diffing the current against the desired-over-live directly
      desired = App.desired_placement(:log_meta, 12, [:a@h, :b@h, :c@h, :d@h], 2)
      assert plan == App.rebalance_plan(current, desired)
      refute plan == []
    end

    test "is empty when the live membership already matches the current placement" do
      configs = App.sharded_vnodes(:log_meta, 6)
      current = App.desired_placement(:log_meta, 6, [:a@h, :b@h, :c@h], 2)
      members_of = fn id -> {:ok, current |> Enum.find(&(elem(&1, 0) == id)) |> elem(2)} end

      assert App.live_rebalance_plan(configs, members_of, [:a@h, :b@h, :c@h], 2) == []
    end

    test "never plans for an unreadable vnode (it is excluded from both current and desired)" do
      configs = App.sharded_vnodes(:log_meta, 6)
      # vn 3 is unreadable; every other vnode currently lives on {a,b}
      members_of = fn
        :log_meta_vn_3 -> {:error, :noproc}
        _other -> {:ok, [:a@h, :b@h]}
      end

      plan = App.live_rebalance_plan(configs, members_of, [:a@h, :b@h, :c@h, :d@h], 2)

      refute Enum.any?(plan, &(&1.vnode_id == :log_meta_vn_3))
    end
  end

  describe "try_members/2" do
    test "returns the members of the first node that answers, skipping ones that cannot be reached" do
      members_fun = fn
        :a@h -> :error
        :b@h -> {:ok, [:b@h, :c@h]}
        :c@h -> {:ok, [:should_not_reach]}
      end

      assert App.try_members([:a@h, :b@h, :c@h], members_fun) == {:ok, [:b@h, :c@h]}
    end

    test "returns {:error, :unreachable} when no node answers" do
      assert App.try_members([:a@h, :b@h], fn _node -> :error end) == {:error, :unreachable}
    end
  end

  describe "leading_vnodes/3" do
    test "returns the vnodes this node both hosts and leads, preserving order" do
      this = :n1@h

      vnodes = [
        {:vn_a, 0, [:n1@h, :n2@h]},
        {:vn_b, 1, [:n2@h, :n3@h]},
        {:vn_c, 2, [:n1@h, :n3@h]},
        {:vn_d, 3, [:n1@h]}
      ]

      # this node leads its local server for vn_a and vn_d only
      leader? = fn server_id -> server_id in [{:vn_a, this}, {:vn_d, this}] end

      assert App.leading_vnodes(vnodes, this, leader?) == [:vn_a, :vn_d]
    end

    test "excludes a vnode this node hosts but does not lead" do
      this = :n1@h
      assert App.leading_vnodes([{:vn_a, 0, [:n1@h, :n2@h]}], this, fn _ -> false end) == []
    end

    test "never even queries leadership for a vnode this node does not host" do
      this = :n1@h
      leader? = fn _ -> flunk("must not query leadership for a non-hosted vnode") end
      assert App.leading_vnodes([{:vn_b, 0, [:n2@h, :n3@h]}], this, leader?) == []
    end
  end

  describe "store_reconciler_child/5" do
    test "a clustered store self-joins and watches its machine version" do
      test_pid = self()

      spec =
        App.store_reconciler_child(:my_store_reconciler, Malachi.Auth.UserMachine, :my_store, [:a@h, :b@h], fn ->
          send(test_pid, :reconciled)
        end)

      assert %{id: :my_store_reconciler, start: {Malachi.Cluster.LeaseReconciler, :start_link, [opts]}} = spec
      assert opts[:version_check] == {Malachi.Auth.UserMachine, {:my_store, node()}}
      opts[:reconcile].()
      assert_received :reconciled
    end

    test "a single-node store keeps the watcher and drops the self-join" do
      test_pid = self()

      spec =
        App.store_reconciler_child(:solo_reconciler, Malachi.Auth.UserMachine, :solo_store, [node()], fn ->
          send(test_pid, :reconciled)
        end)

      assert %{start: {Malachi.Cluster.LeaseReconciler, :start_link, [opts]}} = spec
      # The one-member group still reaches a machine version, so the watch stays; there is nobody to join.
      assert opts[:version_check] == {Malachi.Auth.UserMachine, {:solo_store, node()}}
      assert opts[:reconcile].() == :ok
      refute_received :reconciled
    end
  end

  describe "cluster_flags_child/1" do
    test "watches the flag store's machine version on the local member" do
      spec = App.cluster_flags_child([node()])

      assert spec.id == Malachi.LogClusterFlagsReconciler
      assert %{start: {Malachi.Cluster.LeaseReconciler, :start_link, [opts]}} = spec
      assert opts[:version_check] == {Malachi.Cluster.ClusterFlagsMachine, {Malachi.LogClusterFlags, node()}}
      assert opts[:name] == Malachi.LogClusterFlagsReconciler
    end

    test "its tick runs the local flag pass, and tolerates a store it cannot read" do
      # The gate exists only because this child carries it. A store that is not formed must leave the
      # node running: treating "I could not ask" as "no flag is on" is the mistake the ring boot exists
      # to avoid, and here it would let a node serve past a flag it cannot honour.
      spec = App.cluster_flags_child([node()])
      assert %{start: {Malachi.Cluster.LeaseReconciler, :start_link, [opts]}} = spec

      assert opts[:reconcile].() == :ok
    end
  end

  describe "incarnation_opts/1" do
    @describetag :tmp_dir

    test "hands the membership server a reservation function, not reserved values", %{tmp_dir: dir} do
      # A function, because a child spec is built once and reused on every supervisor restart. Reserved
      # values would seed a restarted server at the number the first one started from.
      opts = App.incarnation_opts(dir)

      assert is_function(opts[:reserve], 0)
      assert is_function(opts[:on_ceiling], 1)
      # Seeded from the clock, so the numbers are whatever today is; what the child spec owes the server
      # is a whole block above wherever it starts (`Malachi.Cluster.MemberIncarnation`).
      assert {:ok, %{start: start, ceiling: ceiling}} = opts[:reserve].()
      assert ceiling == start - 1 + MemberIncarnation.block()
    end

    test "every call resumes above the block the previous one took", %{tmp_dir: dir} do
      reserve = App.incarnation_opts(dir)[:reserve]

      assert {:ok, first} = reserve.()
      assert {:ok, second} = reserve.()

      # This is what a supervisor restart of the membership server gets, and why it is a function: the
      # second process starts above everything the first could have announced.
      assert second.start > first.ceiling
    end

    test "raises when no reservation can be made, which stops the node", %{tmp_dir: dir} do
      # Starting anyway would announce a number below what peers remember, and nothing corrects that: a
      # live node is never suspected, so it never refutes, so the peers keep the old record and the old
      # attributes the capability check reads.
      File.write!(MemberIncarnation.path(dir), "not a number")
      reserve = App.incarnation_opts(dir)[:reserve]

      assert_raise RuntimeError, ~r/could not reserve this node's incarnation/, fn -> reserve.() end
    end
  end

  describe "membership_attributes/0" do
    test "gossips this build's capability set alongside the operator's own attributes" do
      attributes = App.membership_attributes()

      assert attributes[Capabilities.key()] == Capabilities.advertised()
    end

    test "is the operator's configured attributes put through the capability merge" do
      # The wiring, read rather than driven: :log_attributes is VM-wide application env and this module
      # is async, so setting it would race every other test and every application process that reads it.
      assert App.membership_attributes() ==
               Capabilities.attributes(App.parse_attributes(Application.get_env(:malachi, :log_attributes)))
    end

    test "the operator's attributes survive the merge" do
      attributes = Capabilities.attributes(App.parse_attributes("rack=a,dc=eu"))

      assert attributes["rack"] == "a"
      assert attributes["dc"] == "eu"
      assert attributes[Capabilities.key()] == Capabilities.advertised()
    end
  end

  describe "the operator's flag entry points" do
    test "cluster_flags/0 reads the store this node hosts" do
      assert {:ok, %{known: known, enabled: enabled}} = App.cluster_flags()
      assert known == Capabilities.known()
      assert is_list(enabled)
    end

    test "an unknown flag is refused before the membership view is even read" do
      assert App.enable_cluster_flag("no_such_flag") == {:error, :unknown_flag}
    end

    test "a flag this node itself does not advertise cannot be switched on" do
      # The local node is always one of the configured nodes, and the membership view reports what this
      # build really advertises. So the loop is closed: nothing can enable a flag this node would then
      # refuse to serve. The registry is empty here, so the name is injected; the advertisement is not.
      flag = :"app_flag_#{System.unique_integer([:positive])}"

      assert App.enable_cluster_flag(to_string(flag), known: [flag], nodes: [node()]) ==
               {:error, {:unsupported, [node()]}}

      assert {:ok, %{enabled: enabled}} = App.cluster_flags()
      refute flag in enabled
    end

    test "refuses and names a configured node the membership view does not vouch for" do
      flag = :"app_flag_#{System.unique_integer([:positive])}"

      assert App.enable_cluster_flag(to_string(flag),
               known: [flag],
               nodes: [node(), :absent@nowhere],
               reads: fn node ->
                 if node == node(), do: {:alive, Capabilities.attributes(%{}, [flag])}, else: {nil, %{}}
               end
             ) == {:error, {:unsupported, [:absent@nowhere]}}
    end
  end

  describe "membership_reads/0" do
    test "without a membership server it answers for the local node only" do
      # An unclustered deployment runs no membership server. Answering for a remote node would be a
      # guess, and a guess in this direction switches a flag on over a node nobody has heard from.
      reads = App.membership_reads()

      assert {:alive, attributes} = reads.(node())
      assert attributes[Capabilities.key()] == Capabilities.advertised()
      assert reads.(:somewhere@else) == {nil, %{}}
    end
  end

  describe "metadata_version_watcher_children/2" do
    test "an unsharded control plane gets a watcher for its single metadata group" do
      assert [spec] = App.metadata_version_watcher_children(:log_cluster, nil)
      assert %{start: {Malachi.Cluster.LeaseReconciler, :start_link, [opts]}} = spec
      assert opts[:version_check] == {Malachi.Cluster.MetadataMachine, {:log_cluster, node()}}
    end

    test "a sharded control plane gets none: the vnode coordinator manager watches its members" do
      assert App.metadata_version_watcher_children(:log_cluster, [{:vn_a, 0, [node()]}]) == []
    end

    test "an unclustered node gets none: it has no metadata group in ra" do
      assert App.metadata_version_watcher_children(nil, nil) == []
    end
  end

  describe "local_vnode_servers/2" do
    test "lists every metadata vnode this node hosts, led or not, as a machine and local server id" do
      vnodes = [{:vn_a, 0, [:n1@h, :n2@h]}, {:vn_b, 1, [:n2@h, :n3@h]}, {:vn_c, 2, [:n3@h, :n1@h]}]

      assert App.local_vnode_servers(vnodes, :n1@h) == [
               {Malachi.Cluster.MetadataMachine, {:vn_a, :n1@h}},
               {Malachi.Cluster.MetadataMachine, {:vn_c, :n1@h}}
             ]

      assert App.local_vnode_servers(vnodes, :n9@h) == []
    end
  end

  describe "static_seed/1" do
    test "self is the orchestrator only when it is the lowest-sorted node" do
      higher = :zzzz_higher@h
      lower = :"0000_lower@h"

      assert App.static_seed([node()]).()
      assert App.static_seed([node(), higher]).()
      refute App.static_seed([lower, node()]).()
      # order-independent: still the same seed regardless of input order
      refute App.static_seed([higher, lower, node()]).()
    end
  end

  describe "membership_leader/1" do
    alias Malachi.Test.AliveMembersStub

    test "not the leader when membership is unavailable" do
      # a server that does not exist / does not answer → conservative false (never risk two orchestrators)
      refute App.membership_leader(:no_such_membership_server).()
    end

    test "leader iff this node is the lowest-sorted live member" do
      # alive_members is sorted, so the head is the lowest node
      {:ok, self_lowest} =
        AliveMembersStub.start_link([{Malachi.LogMembership, node()}, {Malachi.LogMembership, :zzzz@h}])

      assert App.membership_leader(self_lowest).()

      {:ok, other_lower} =
        AliveMembersStub.start_link([{Malachi.LogMembership, :"0000@h"}, {Malachi.LogMembership, node()}])

      refute App.membership_leader(other_lower).()

      {:ok, empty} = AliveMembersStub.start_link([])
      refute App.membership_leader(empty).()
    end
  end

  describe "broker_refs/1" do
    test "one named ReplicationServer reference per node" do
      assert App.broker_refs([:"a@127.0.0.1", :"b@127.0.0.1"]) ==
               [{Malachi.LogReplication, :"a@127.0.0.1"}, {Malachi.LogReplication, :"b@127.0.0.1"}]
    end
  end

  describe "data_plane_opts/2" do
    test "no cluster configured yields no data-plane options (local single replica default)" do
      assert App.data_plane_opts(nil, [:"a@127.0.0.1"]) == []
    end

    test "a configured cluster places replicas across every node's ReplicationServer" do
      opts = App.data_plane_opts(:log_meta, [:"a@127.0.0.1", :"b@127.0.0.1"])

      assert Keyword.fetch!(opts, :brokers) ==
               [{Malachi.LogReplication, :"a@127.0.0.1"}, {Malachi.LogReplication, :"b@127.0.0.1"}]

      assert is_integer(Keyword.fetch!(opts, :replication_factor))
      # live_brokers narrows placement to the alive set as membership converges
      assert is_function(Keyword.fetch!(opts, :live_brokers), 0)
    end
  end

  describe "membership_seeds/1" do
    test "seeds with the other nodes' membership servers, excluding self" do
      others = [:"a@127.0.0.1", :"b@127.0.0.1"]

      assert App.membership_seeds([node() | others]) ==
               [{Malachi.LogMembership, :"a@127.0.0.1"}, {Malachi.LogMembership, :"b@127.0.0.1"}]
    end
  end

  describe "live_replication_refs/1" do
    test "maps membership members to their nodes' ReplicationServer references" do
      members = [{Malachi.LogMembership, :"a@127.0.0.1"}, {Malachi.LogMembership, :"b@127.0.0.1"}]

      assert App.live_replication_refs(members) ==
               [{Malachi.LogReplication, :"a@127.0.0.1"}, {Malachi.LogReplication, :"b@127.0.0.1"}]
    end
  end

  describe "broker_attributes_for/2" do
    test "maps members to their ReplicationServer refs with each member's attributes (by node)" do
      members = [{Malachi.LogMembership, :"a@127.0.0.1"}, {Malachi.LogMembership, :"b@127.0.0.1"}]
      attributes_of = fn {_name, node} -> %{"rack" => to_string(node)} end

      assert App.broker_attributes_for(members, attributes_of) == %{
               {Malachi.LogReplication, :"a@127.0.0.1"} => %{"rack" => "a@127.0.0.1"},
               {Malachi.LogReplication, :"b@127.0.0.1"} => %{"rack" => "b@127.0.0.1"}
             }
    end
  end

  describe "parse_attributes/1" do
    test "absent or empty yields no attributes" do
      assert App.parse_attributes(nil) == %{}
      assert App.parse_attributes("") == %{}
    end

    test "parses key=value pairs, trimming whitespace" do
      assert App.parse_attributes("rack=a,dc=east") == %{"rack" => "a", "dc" => "east"}
      assert App.parse_attributes(" rack = a , dc = east ") == %{"rack" => "a", "dc" => "east"}
    end

    test "ignores entries without an = and keeps a value that contains =" do
      assert App.parse_attributes("rack=a,bogus,url=http://x=y") == %{"rack" => "a", "url" => "http://x=y"}
    end
  end

  describe "parse_topology/1" do
    test "absent or empty yields no topology" do
      assert App.parse_topology(nil) == %{}
      assert App.parse_topology("") == %{}
    end

    test "parses node=value pairs into a node => value map, trimming" do
      assert App.parse_topology("n1@h=a,n2@h=b") == %{:n1@h => "a", :n2@h => "b"}
      assert App.parse_topology(" n1@h = a , n2@h = b ") == %{:n1@h => "a", :n2@h => "b"}
    end

    test "ignores entries without an =" do
      assert App.parse_topology("n1@h=a,bogus,n2@h=b") == %{:n1@h => "a", :n2@h => "b"}
    end
  end
end
