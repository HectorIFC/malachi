defmodule Malachi.Cluster.ReplicatedDSRSMTest do
  # async: false — ra is global/stateful.
  use ExUnit.Case, async: false

  alias Malachi.Cluster.MetadataServer
  alias Malachi.Cluster.ReplicatedDSRSM
  alias Malachi.Metadata

  setup_all do
    data_dir = Path.join(System.tmp_dir!(), "malachi_ra_rdsrsm_#{System.unique_integer([:positive])}")
    File.rm_rf!(data_dir)
    _ = :ra.start_in(String.to_charlist(data_dir))
    on_exit(fn -> File.rm_rf!(data_dir) end)
    :ok
  end

  # Two vnodes at fixed tokens, with unique names per test (ra clusters are global).
  defp start_cluster do
    suffix = System.unique_integer([:positive])
    a = :"rd_a_#{suffix}"
    b = :"rd_b_#{suffix}"
    {:ok, state} = ReplicatedDSRSM.new(ring_bits: 4) |> ReplicatedDSRSM.add_vnode(a, 4)
    {:ok, state} = ReplicatedDSRSM.add_vnode(state, b, 12)
    on_exit(fn -> ReplicatedDSRSM.delete(state) end)
    {state, a, b}
  end

  test "routes commands to the owning vnode's Raft cluster and serves consistent queries" do
    {state, _a, _b} = start_cluster()

    assert {:ok, root_id} = ReplicatedDSRSM.command(state, "events", {:create_topic, "events", 4})

    assert {:ok, %{name: "events", keyspace_size: 16, state: :active}} =
             ReplicatedDSRSM.query(state, "events", &Metadata.get_topic(&1, "events"))

    assert {:ok, left_id, right_id} = ReplicatedDSRSM.command(state, "events", {:split_range, root_id})

    assert {:ok, active} =
             ReplicatedDSRSM.query(state, "events", &Metadata.active_ranges_of_topic(&1, "events"))

    assert Enum.sort(Enum.map(active, & &1.id)) == Enum.sort([left_id, right_id])
  end

  test "shards topics across vnodes (each lives only in the cluster it routes to)" do
    {state, _a, _b} = start_cluster()

    names = for index <- 0..9, do: "topic-#{index}"
    for name <- names, do: assert({:ok, _root} = ReplicatedDSRSM.command(state, name, {:create_topic, name, 4}))

    # every topic is retrievable from the vnode it routes to, and absent from the other
    for name <- names do
      {:ok, owner} = ReplicatedDSRSM.vnode_for(state, name)
      assert {:ok, %{name: ^name}} = ReplicatedDSRSM.query(state, name, &Metadata.get_topic(&1, name))

      for other <- ReplicatedDSRSM.vnode_ids(state) -- [owner] do
        # querying the non-owning vnode directly must not find the topic
        {:ok, server_id} = Map.fetch(state.vnodes, other)
        assert {:ok, nil} = MetadataServer.query(server_id, &Metadata.get_topic(&1, name))
      end
    end

    # and at least one topic landed on each vnode (real sharding)
    owners = Enum.map(names, fn name -> elem(ReplicatedDSRSM.vnode_for(state, name), 1) end)
    assert length(Enum.uniq(owners)) >= 2
  end

  test "rejected commands return the machine error reply; empty ring reports no vnode" do
    {state, _a, _b} = start_cluster()
    {:ok, _root} = ReplicatedDSRSM.command(state, "events", {:create_topic, "events", 4})
    assert {:error, :already_exists} = ReplicatedDSRSM.command(state, "events", {:create_topic, "events", 4})

    empty = ReplicatedDSRSM.new(ring_bits: 4)
    assert {:error, :no_vnode} = ReplicatedDSRSM.command(empty, "events", {:create_topic, "events", 4})
    assert {:error, :no_vnode} = ReplicatedDSRSM.query(empty, "events", &Metadata.get_topic(&1, "events"))
  end
end
