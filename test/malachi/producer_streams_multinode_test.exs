defmodule Malachi.ProducerStreamsMultinodeTest do
  @moduledoc """
  Producer streams across real BEAM nodes: three peers, each running a replication server and a broker
  over one replicated control plane, with segments small enough to roll every few appends. A producer
  streams to the broker leading the range's active segment, follows every move (each roll seals its
  segment, and halfway through the primary's replication server is killed and the heal pass seals its
  segment on the survivors), and reopens where the move says.

  At the end every acknowledged record is in the log exactly once, and nothing is there that was not sent,
  in the order it was sent: a move never loses or repeats an acknowledged record.
  """
  use ExUnit.Case, async: false

  @moduletag :multinode
  @moduletag timeout: 240_000

  alias Malachi.BrokerServer
  alias Malachi.Cluster.HealCoordinator
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record
  alias Malachi.Test.RaPeers
  alias Malachi.Test.StreamCluster

  @broker :stream_mn_broker
  @repl :stream_mn_repl
  @appends 40

  setup_all do
    RaPeers.ensure_distribution()
  end

  setup do
    peers = Enum.map(1..3, fn _ -> RaPeers.start(nil) end)
    on_exit(fn -> Enum.each(peers, &RaPeers.stop/1) end)
    nodes = Enum.map(peers, & &1.node)
    refs = Enum.map(nodes, &{@repl, &1})
    cluster = :"stream_mn_#{System.unique_integer([:positive])}"

    for node <- nodes do
      {:ok, _apps} = :erpc.call(node, :application, :ensure_all_started, [:telemetry])
      :ok = :erpc.call(node, Logger, :configure, [[level: :none]])
      :ok = :erpc.call(node, StreamCluster, :put_live, [refs])
      :ok = :erpc.call(node, StreamCluster, :start_task_supervisor, [])
      directory = Path.join(System.tmp_dir!(), "stream_mn_#{node}")
      on_exit(fn -> File.rm_rf!(directory) end)

      {:ok, _repl} =
        :erpc.call(node, GenServer, :start, [
          ReplicationServer,
          [name: @repl, directory: Path.join(directory, "repl")],
          [name: @repl]
        ])
    end

    for node <- nodes do
      directory = Path.join(System.tmp_dir!(), "stream_mn_#{node}")

      opts = [
        brokers: refs,
        replication_factor: 2,
        metadata_cluster: cluster,
        metadata_nodes: nodes,
        live_brokers: &StreamCluster.live_brokers/0,
        policy_fun: &StreamCluster.no_policy/1,
        segment_max_bytes: 3 * Record.encoded_size(Record.new("r00", key: "k"))
      ]

      {:ok, _broker} =
        :erpc.call(
          node,
          GenServer,
          :start,
          [BrokerServer, {Path.join(directory, "broker"), opts}, [name: @broker]],
          30_000
        )
    end

    {:ok, root} = BrokerServer.create_topic({@broker, hd(nodes)}, "events", 4)
    %{nodes: nodes, refs: refs, root: root}
  end

  # Opens a stream on `range_id`, starting at `node` and following the broker to the node that leads the
  # range's segment; retried while the control plane settles after a failure.
  defp open(range_id, node, attempts \\ 50) do
    case BrokerServer.open_stream({@broker, node}, range_id) do
      {:ok, token, _segment, _broker} -> %{node: node, token: token}
      {:moved, :elsewhere, [{_range, {_segment, {@repl, primary}}}]} -> open(range_id, primary, attempts)
      _not_yet when attempts > 0 -> Process.sleep(100) && open(range_id, node, attempts - 1)
    end
  end

  # One append, sent once and waited for. Returns its outcome (`:acked` or `:failed`), the stream to go on
  # with (nil once a move closed it), and the reason of that move, if one came.
  defp append(stream, range_id, index) do
    record = Record.new(value(index), key: "k")

    reqids =
      BrokerServer.send_stream_produce({@broker, stream.node}, range_id, [record], index, :gen_server.reqids_new())

    await(stream, reqids, nil)
  end

  defp await(stream, reqids, moved) do
    receive do
      {:stream_moved, token, reason, _targets} when token == stream.token ->
        # the append's own answer still arrives and says whether it landed
        await(stream, reqids, reason)

      message ->
        case :gen_server.check_response(message, reqids, true) do
          {{:reply, {{:ok, _placements}, _scale}}, _index, _reqids} -> {:acked, if(moved, do: nil, else: stream), moved}
          {{:reply, {{:error, _reason}, _scale}}, _index, _reqids} -> {:failed, nil, moved}
          {{:error, _reason}, _index, _reqids} -> {:failed, nil, moved}
          # a move of a stream this driver already left
          :no_request -> await(stream, reqids, moved)
          :no_reply -> await(stream, reqids, moved)
        end
    after
      # bounded so that the whole run stays well inside the module's timeout
      5_000 -> {:failed, nil, moved}
    end
  end

  defp value(index), do: "r" <> String.pad_leading(Integer.to_string(index), 2, "0")

  defp fail_primary(ctx) do
    [segment] =
      Enum.filter(
        Malachi.Metadata.segments_of_range(StreamCluster.metadata_on(hd(ctx.nodes)), ctx.root),
        &(&1.state == :active)
      )

    {@repl, primary_node} = hd(segment.replica_set)
    survivors = Enum.reject(ctx.refs, &(&1 == {@repl, primary_node}))

    :erpc.call(primary_node, Process, :exit, [:erpc.call(primary_node, Process, :whereis, [@repl]), :kill])
    for node <- ctx.nodes, do: :ok = :erpc.call(node, StreamCluster, :put_live, [survivors])

    {@repl, healer} = hd(survivors)

    coordinator_opts = [
      live_brokers: &StreamCluster.live_brokers/0,
      metadata_source: &StreamCluster.metadata/0,
      apply_command: &StreamCluster.apply_heal/1,
      replication_factor: 2,
      interval: 60_000,
      probe_timeout: 2_000
    ]

    {:ok, coordinator} = :erpc.call(healer, GenServer, :start, [HealCoordinator, coordinator_opts])
    _report = HealCoordinator.heal_now(coordinator)
    {primary_node, healer}
  end

  defp read_until(node, acked, remaining_ms) do
    stored = read_all(node)

    if acked -- stored == [] or remaining_ms <= 0,
      do: stored,
      else: Process.sleep(200) && read_until(node, acked, remaining_ms - 200)
  end

  defp read_all(node) do
    {records, _cursor, _skips} = BrokerServer.consume({@broker, node}, "events", %{}, 10_000, 0)
    Enum.map(records, & &1.value)
  end

  test "every acknowledged record survives the rolls and a failover exactly once, in order", ctx do
    failover_at = div(@appends, 2)

    {acked, _stream, failover, moves, opened_after} =
      Enum.reduce(0..(@appends - 1), {[], nil, nil, [], []}, fn index, {acked, stream, failover, moves, opened_after} ->
        failover = if index == failover_at, do: fail_primary(ctx), else: failover
        excluded = if failover, do: [elem(failover, 0)], else: []
        {stream, opened_after} = reopen(stream, ctx, excluded, failover, opened_after)
        {outcome, stream, moved} = append(stream, ctx.root, index)
        acked = if outcome == :acked, do: [index | acked], else: acked
        {acked, stream, failover, if(moved, do: [moved | moves], else: moves), opened_after}
      end)

    {failed_node, healer} = failover
    sent = Enum.map(0..(@appends - 1), &value/1)
    acked_values = acked |> Enum.reverse() |> Enum.map(&value/1)
    # A broker's read horizon takes in another broker's writes on its next reconcile, so the log is read
    # until every acknowledged record is visible or the wait runs out.
    stored = read_until(healer, acked_values, 10_000)

    assert Enum.count(moves, &(&1 == :sealed)) >= 3, "the segments did not roll under the stream: #{inspect(moves)}"

    # Right after the failure a node may still count the failed broker as live for up to a refresh, so the
    # first reopenings can land there; once the heal and the refresh settle, the stream is on a survivor.
    assert opened_after != [] and hd(opened_after) != failed_node,
           "the stream did not settle off the failed node: #{inspect(Enum.reverse(opened_after))}"

    assert Enum.any?(acked, &(&1 > failover_at)), "no append was acknowledged after the failover: #{inspect(acked)}"

    assert length(acked_values) > div(@appends, 2),
           "too few appends were acknowledged to mean anything: #{inspect(acked_values)}"

    assert stored -- sent == [], "the log holds records nobody sent: #{inspect(stored -- sent)}"
    assert Enum.uniq(stored) == stored, "a record is in the log twice: #{inspect(stored)}"

    assert acked_values -- stored == [],
           "acknowledged records are missing after 10s: #{inspect(acked_values -- stored)}"

    assert stored == Enum.sort(stored), "the log is out of order: #{inspect(stored)}"
  end

  # The stream to append on: the one held, or one opened now, recording where each stream opened after the
  # failover.
  defp reopen(nil, ctx, excluded, failover, opened_after) do
    stream = open(ctx.root, hd(ctx.nodes -- excluded))
    {stream, if(failover, do: [stream.node | opened_after], else: opened_after)}
  end

  defp reopen(stream, _ctx, _excluded, _failover, opened_after), do: {stream, opened_after}
end
