defmodule Malachi.ApplicationExpireSegmentTest do
  # The production delete path of retention, called directly: until #191 the retention coordinator's own
  # test passed a copy of it, so nothing exercised the function the application wires up.
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  import Malachi.Test.PollingHelper
  import Malachi.Test.TeardownHelper

  alias Malachi.BrokerServer
  alias Malachi.Cluster.DSRSM
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record
  alias Malachi.Metadata

  @moduletag :tmp_dir

  setup %{tmp_dir: directory} do
    replication = :"expire_repl_#{System.unique_integer([:positive])}"
    start_supervised!({ReplicationServer, name: replication, directory: Path.join(directory, "repl")}, id: replication)

    one_record = Record.encoded_size(Record.new("value", key: "key"))
    {:ok, broker} = BrokerServer.start_link(directory, brokers: [replication], segment_max_bytes: one_record)
    on_exit(fn -> stop_quietly(broker) end)

    {:ok, root_id} = BrokerServer.create_topic(broker, "events", 4)
    # Two produces, as in the retention coordinator's end-to-end test: the second is what finds the first
    # segment's roll owed and sends its fence, whose answer records the seal.
    {:ok, _placements} = BrokerServer.produce(broker, "events", [Record.new("value", key: "k0")])
    {:ok, _placements} = BrokerServer.produce(broker, "events", [Record.new("value", key: "k1")])

    sealed = fn -> broker |> BrokerServer.metadata() |> Metadata.get_segment({root_id, 0}) end
    wait_until!(fn -> sealed.().state == :sealed end)

    %{broker: broker, replication: replication, segment: sealed.()}
  end

  test "answers what the control plane answered, and deletes the stored copy on :ok", context do
    %{broker: broker, replication: replication, segment: segment} = context
    assert {:ok, [_ | _]} = ReplicationServer.read(replication, segment.id, segment.start_offset, 10)

    assert Malachi.Application.expire_segment(segment, broker) == :ok

    assert broker |> BrokerServer.metadata() |> Metadata.get_segment(segment.id) == nil
    assert ReplicationServer.read(replication, segment.id, segment.start_offset, 10) == :eof
  end

  test "a segment already deleted answers :no_such_segment", %{broker: broker, segment: segment} do
    :ok = Malachi.Application.expire_segment(segment, broker)

    assert Malachi.Application.expire_segment(segment, broker) == {:error, :no_such_segment}
  end

  test "an active segment is refused, and its stored copy is kept", %{tmp_dir: directory, replication: replication} do
    # A broker with the default segment size, so the head written here stays active: no roll to race.
    {:ok, broker} = BrokerServer.start_link(Path.join(directory, "active"), brokers: [replication])
    on_exit(fn -> stop_quietly(broker) end)
    {:ok, root_id} = BrokerServer.create_topic(broker, "orders", 4)
    {:ok, _placements} = BrokerServer.produce(broker, "orders", [Record.new("value", key: "k1")])

    [active] = broker |> BrokerServer.metadata() |> Metadata.segments_of_range(root_id)
    assert active.state == :active

    assert Malachi.Application.expire_segment(active, broker) == {:error, :segment_active}

    # The control plane still lists the segment, so its bytes are still owed to a reader.
    assert {:ok, [_ | _]} = ReplicationServer.read(replication, active.id, active.start_offset, 10)
  end

  # Each refusal keeps the copy for the same reason: the segment stays listed with its replica set
  # intact, and emptying every replica under it leaves self-healing nothing to repair from.
  for {name, reply} <- [
        {"a topic being migrated between vnodes", {:error, :migrating}},
        {"a Raft timeout, which may or may not have committed", {:error, :ra_timeout}},
        {"an answer no version of this code knows", {:error, :from_the_future}}
      ] do
    test "the stored copy survives a delete refused because of #{name}", context do
      %{broker: broker, replication: replication, segment: segment} = context
      refuse_delete(broker, unquote(Macro.escape(reply)))

      assert Malachi.Application.expire_segment(segment, broker) == unquote(Macro.escape(reply))

      assert {:ok, [_ | _]} = ReplicationServer.read(replication, segment.id, segment.start_offset, 10)
      assert broker |> BrokerServer.metadata() |> Metadata.get_segment(segment.id) != nil
    end
  end

  test "a control plane call that exits keeps the copy", context do
    %{replication: replication, segment: segment} = context
    # A broker that is not there exits the caller the same way one that times out does, and the caller
    # is the retention coordinator mid-sweep: letting the exit through would cost the rest of that
    # sweep instead of one segment.
    gone = spawn(fn -> :ok end)
    ref = Process.monitor(gone)
    assert_receive {:DOWN, ^ref, :process, ^gone, _reason}

    log = capture_log(fn -> assert Malachi.Application.expire_segment(segment, gone) == {:error, :call_failed} end)

    assert log =~ "did not answer the delete"
    assert {:ok, [_ | _]} = ReplicationServer.read(replication, segment.id, segment.start_offset, 10)
  end

  test "a replica that did not answer its delete is counted as an orphan left behind", context do
    %{broker: broker, segment: segment} = context
    events = attach_orphan_telemetry()

    # A replica reference that answers nothing: `ReplicationServer.delete/2` catches the exit and says
    # so, which is what lets the expire count the directory it just orphaned.
    segment = %{segment | replica_set: [{:no_such_replication_server, node()}]}
    assert Malachi.Application.expire_segment(segment, broker) == :ok

    assert_receive {^events, %{count: 1}, %{topic: "events"}}
  end

  # Replaces the RUNNING broker's own `:command_fun` seam so a segment delete gets `reply`.
  # `BrokerServer` builds its broker options itself rather than forwarding the caller's, and widening
  # its option list to reach one failure mode from a test would put a seam in production code that only
  # tests use (the same reasoning as in `Malachi.BrokerServerTest`).
  defp refuse_delete(broker, reply) do
    refusing = fn dsrsm, topic, command ->
      case command do
        {:delete_segment, _id} -> {dsrsm, reply}
        _other -> DSRSM.command(dsrsm, topic, command)
      end
    end

    :sys.replace_state(broker, fn state -> put_in(state.broker.command_fun, refusing) end)
  end

  defp attach_orphan_telemetry do
    handler = "expire-orphan-#{System.unique_integer([:positive])}"
    test_pid = self()

    :telemetry.attach(
      handler,
      [:malachi, :retention, :orphan_left],
      fn _event, measurements, metadata, _config -> send(test_pid, {handler, measurements, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    handler
  end
end
