defmodule Malachi.ApplicationExpireSegmentTest do
  # The production delete path of retention, called directly: until #191 the retention coordinator's own
  # test passed a copy of it, so nothing exercised the function the application wires up.
  use ExUnit.Case, async: true

  import Malachi.Test.PollingHelper
  import Malachi.Test.TeardownHelper

  alias Malachi.BrokerServer
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

  test "an active segment is refused, and the refusal is handed back", %{tmp_dir: directory, replication: replication} do
    # A broker with the default segment size, so the head written here stays active: no roll to race.
    {:ok, broker} = BrokerServer.start_link(Path.join(directory, "active"), brokers: [replication])
    on_exit(fn -> stop_quietly(broker) end)
    {:ok, root_id} = BrokerServer.create_topic(broker, "orders", 4)
    {:ok, _placements} = BrokerServer.produce(broker, "orders", [Record.new("value", key: "k1")])

    [active] = broker |> BrokerServer.metadata() |> Metadata.segments_of_range(root_id)
    assert active.state == :active

    # Only the reply is asserted: that the replica copy is still deleted after a refusal is the defect
    # #190 fixes, not something this function promises yet.
    assert Malachi.Application.expire_segment(active, broker) == {:error, :segment_active}
  end
end
