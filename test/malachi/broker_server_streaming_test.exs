defmodule Malachi.BrokerServerStreamingTest do
  # B2-a: streaming subscribers with a credit window and durable group commit, in-process (the test is
  # the subscriber, receiving {:log_records, ...} into its own mailbox).
  use ExUnit.Case, async: false

  alias Malachi.BrokerServer
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record

  setup do
    dir = Path.join(System.tmp_dir!(), "malachi_stream_#{System.unique_integer([:positive])}")
    repl = :"repl_#{System.unique_integer([:positive])}"
    start_supervised!({ReplicationServer, name: repl, directory: Path.join(dir, "repl")}, id: repl)
    {:ok, broker} = BrokerServer.start_link(Path.join(dir, "b"), brokers: [repl])
    {:ok, _root} = BrokerServer.create_topic(broker, "t", 4)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{broker: broker}
  end

  defp produce(broker, values) do
    {:ok, _} = BrokerServer.produce(broker, "t", Enum.map(values, &Record.new/1))
  end

  # receive one push, returning {values, positions}
  defp recv_push do
    receive do
      {:log_records, "t", records, positions} -> {Enum.map(records, & &1.value), positions}
    after
      1_000 -> flunk("expected a {:log_records, ...} push")
    end
  end

  test "subscribe pushes the backlog, then a later produce pushes the new record", %{broker: broker} do
    produce(broker, ["a", "b"])
    :ok = BrokerServer.subscribe(broker, "t", "g", 10, 100)

    assert {["a", "b"], _positions} = recv_push()

    produce(broker, ["c"])
    assert {["c"], _positions} = recv_push()
  end

  test "the credit window bounds in-flight records until acked", %{broker: broker} do
    produce(broker, ["a", "b", "c", "d", "e"])
    :ok = BrokerServer.subscribe(broker, "t", "g", 2, 100)

    # only the window's worth is pushed, though 5 are available
    assert {["a", "b"], positions} = recv_push()
    refute_receive {:log_records, _, _, _}, 100

    # acking 2 returns 2 credit → the next 2 are pushed
    :ok = BrokerServer.stream_ack(broker, "t", "g", positions, 2)
    assert {["c", "d"], _positions} = recv_push()
  end

  test "ack commits the group's position durably", %{broker: broker} do
    produce(broker, ["a", "b", "c"])
    :ok = BrokerServer.subscribe(broker, "t", "g", 2, 100)
    assert {["a", "b"], positions} = recv_push()

    :ok = BrokerServer.stream_ack(broker, "t", "g", positions, 2)

    # the committed position is durable and equals what was acked
    assert BrokerServer.committed_offsets(broker, "g", "t") == positions
  end

  test "a fresh subscription resumes from the group's committed position", %{broker: broker} do
    produce(broker, ["a", "b", "c"])

    # first subscriber consumes and acks "a","b"
    :ok = BrokerServer.subscribe(broker, "t", "g", 2, 100)
    assert {["a", "b"], positions} = recv_push()
    :ok = BrokerServer.stream_ack(broker, "t", "g", positions, 2)
    _ = recv_push()
    :ok = BrokerServer.unsubscribe(broker, "t")

    # a fresh subscription of the same group resumes past the committed "a","b"
    flush()
    :ok = BrokerServer.subscribe(broker, "t", "g", 10, 100)
    assert {["c"], _positions} = recv_push()
  end

  test "a dead subscriber is dropped from the topic (via :DOWN)", %{broker: broker} do
    {sub, ref} =
      spawn_monitor(fn ->
        BrokerServer.subscribe(broker, "t", "g", 10, 100)
        receive do: (:never -> :ok)
      end)

    wait_until(fn -> length(subscribers(broker)) == 1 end)
    Process.exit(sub, :kill)
    assert_receive {:DOWN, ^ref, :process, _pid, _reason}

    wait_until(fn -> subscribers(broker) == [] end)
  end

  defp subscribers(broker), do: Map.get(:sys.get_state(broker).subscribers, "t", [])

  defp flush do
    receive do
      _ -> flush()
    after
      0 -> :ok
    end
  end

  defp wait_until(fun, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 1_000

    cond do
      fun.() -> :ok
      System.monotonic_time(:millisecond) > deadline -> flunk("condition not met in time")
      true -> Process.sleep(5) && wait_until(fun, deadline)
    end
  end
end
