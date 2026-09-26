defmodule Malachi.Cluster.UnexpectedMessageMultinodeTest do
  # A newer node sending a message shape an older one does not know, across real BEAM nodes (issue #187):
  # the receiving servers count and drop it, and replication and membership between the two nodes keep
  # working. This is the first thing an older node meets during a rolling upgrade.
  #
  # async: false and tagged, like every test that starts peer nodes.
  use ExUnit.Case, async: false

  @moduletag :multinode

  alias Malachi.Cluster.MembershipServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record
  alias Malachi.Test.Distribution
  alias Malachi.Test.UnknownMessages

  # Every peer registers its servers under the same names: a ref is `{name, node}`, so the node alone
  # tells them apart, as in a real cluster.
  @replication :unexpected_multinode_repl
  @membership :unexpected_multinode_membership
  @event [:malachi, :process, :unexpected_message]

  setup_all do
    Distribution.ensure_started()
  end

  # A peer node without the Malachi application, running only what the test needs: logging, telemetry, a
  # replication server and a membership server.
  defp start_peer do
    {_peer, node, name} = Distribution.start_peer("unexpected")
    {:ok, _apps} = :erpc.call(node, :application, :ensure_all_started, [:logger])
    {:ok, _apps} = :erpc.call(node, :application, :ensure_all_started, [:telemetry])
    # The drops are asserted through telemetry on this node; their warnings would only be noise here.
    :ok = :erpc.call(node, Logger, :configure, [[level: :none]])

    directory = Path.join(System.tmp_dir!(), "#{name}_data")
    on_exit(fn -> File.rm_rf!(directory) end)

    # Started unlinked: linked, they would die with the short-lived `:erpc` worker that started them.
    repl_opts = [name: @replication, directory: directory]
    {:ok, _pid} = :erpc.call(node, GenServer, :start, [ReplicationServer, repl_opts, [name: @replication]])
    node
  end

  defp start_membership(node, peer_node) do
    opts = [
      name: @membership,
      self_ref: {@membership, node},
      peers: [{@membership, peer_node}],
      protocol_period: 50,
      ack_timeout: 100,
      suspicion_timeout: 1_000
    ]

    {:ok, _pid} = :erpc.call(node, GenServer, :start, [MembershipServer, opts, [name: @membership]])
    :ok
  end

  # Forwards the drops of `server_ref`'s process on its own node to this test process.
  defp watch_drops({name, node}) do
    pid = :erpc.call(node, Process, :whereis, [name])
    config = %{pid: pid, test: self()}

    :ok =
      :erpc.call(node, :telemetry, :attach, [
        {__MODULE__, name},
        @event,
        &UnknownMessages.forward_event/4,
        config
      ])

    pid
  end

  defp whereis({name, node}), do: :erpc.call(node, Process, :whereis, [name])

  defp eventually(check, remaining_ms \\ 5_000) do
    cond do
      check.() -> true
      remaining_ms <= 0 -> false
      true -> Process.sleep(25) && eventually(check, remaining_ms - 25)
    end
  end

  defp read_values(ref, segment) do
    case ReplicationServer.read(ref, segment, 0, 100) do
      {:ok, records} -> Enum.map(records, & &1.value)
      _eof_or_error -> []
    end
  end

  test "a newer node's unknown replication and SWIM messages are dropped, and replication still commits" do
    newer = start_peer()
    older = start_peer()
    :ok = start_membership(newer, older)
    :ok = start_membership(older, newer)

    newer_repl = {@replication, newer}
    older_repl = {@replication, older}
    newer_membership = {@membership, newer}
    older_membership = {@membership, older}

    assert eventually(fn -> older_membership in MembershipServer.alive_members(newer_membership) end)

    repl_pid = watch_drops(older_repl)
    membership_pid = watch_drops(older_membership)
    segment = {{"upgrade", 0}, 0}

    # Sent FROM the newer node, as a newer primary and a newer SWIM member would.
    :ok =
      :erpc.call(newer, GenServer, :cast, [
        older_repl,
        {:replica_append_v2, segment, 0, 0, [Record.new(UnknownMessages.secret())], 0, newer_repl, :zstd}
      ])

    :ok = :erpc.call(newer, GenServer, :cast, [older_membership, {:ping_v2, newer_membership, [], %{}}])

    assert_receive {:unexpected_event, ^repl_pid, :replication, :cast, {:replica_append_v2, 8}, 1}, 5_000
    assert_receive {:unexpected_event, ^membership_pid, :membership, :cast, {:ping_v2, 4}, 1}, 5_000

    # A replicate across both nodes still closes its quorum on the older follower that dropped the message.
    records = [Record.new("a", key: "a"), Record.new("b", key: "b")]
    assert {:ok, 1} = ReplicationServer.replicate(newer_repl, segment, [newer_repl, older_repl], 0, records)
    assert eventually(fn -> read_values(older_repl, segment) == ["a", "b"] end)

    # Neither server on the older node was restarted, and membership still sees the older node alive
    # after several more protocol periods.
    assert whereis(older_repl) == repl_pid
    assert whereis(older_membership) == membership_pid
    Process.sleep(300)
    assert older_membership in MembershipServer.alive_members(newer_membership)
  end
end
