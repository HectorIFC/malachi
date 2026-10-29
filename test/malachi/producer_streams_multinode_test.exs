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
  alias Malachi.Log.Record
  alias Malachi.Metadata
  alias Malachi.Test.PollingHelper
  alias Malachi.Test.RaPeers
  alias Malachi.Test.ReadBack
  alias Malachi.Test.StreamCluster

  @broker StreamCluster.broker()
  @repl StreamCluster.repl()
  @appends 40

  setup_all do
    RaPeers.ensure_distribution()
  end

  setup do
    StreamCluster.start()
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
      BrokerServer.send_stream_produce(
        {@broker, stream.node},
        range_id,
        [record],
        index,
        :gen_server.reqids_new(),
        :otel_ctx.get_current()
      )

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

  # Kills the replication server leading the range's active segment and heals the cluster around it.
  defp fail_primary(ctx) do
    {@repl, primary_node} = hd(active_segment(ctx).replica_set)
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

  # The range's one active segment, as the first node sees it. A roll whose seal has landed leaves the range
  # with none until an append or an open places the successor (`Malachi.Broker.stream_segment/2`), and no
  # append comes while the failover is being set up, so a stream is opened (and closed) to place it, as the
  # producer's next reopening would. The first node's metadata may trail the control plane's, so it is read
  # until it shows exactly one.
  defp active_segment(ctx) do
    if active_segments(ctx) == [] do
      stream = open(ctx.root, hd(ctx.nodes))
      :ok = BrokerServer.close_stream({@broker, stream.node}, stream.token)
    end

    :ok = PollingHelper.wait_until!(fn -> match?([_], active_segments(ctx)) end, timeout: 10_000)
    [segment] = active_segments(ctx)
    segment
  end

  defp active_segments(ctx) do
    ctx.nodes
    |> hd()
    |> StreamCluster.metadata_on()
    |> Metadata.segments_of_range(ctx.root)
    |> Enum.filter(&(&1.state == :active))
  end

  # What `node` reads back, read again every 200 ms while a read is refused (`ReadBack` answers an error
  # while the failover leaves the range unreadable) or misses an acknowledged record, until the wait runs
  # out. A read still refused then fails the test with its reason: a refusal is never read as an empty log.
  defp read_until(node, acked, remaining_ms) do
    read = ReadBack.values({@broker, node}, "events")

    cond do
      match?({:error, _reason}, read) and remaining_ms <= 0 ->
        flunk("the log on #{node} could not be read: #{inspect(read)}")

      not match?({:error, _reason}, read) and (acked -- read == [] or remaining_ms <= 0) ->
        read

      true ->
        Process.sleep(200) && read_until(node, acked, remaining_ms - 200)
    end
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

  # Every node of the test now places new segments on `ref` alone (replication factor 1, clamped to the one
  # broker), once its broker has taken the narrowed set in (its own refresh, read from its state here).
  defp place_on(ctx, ref) do
    for node <- ctx.nodes do
      :ok = :erpc.call(node, StreamCluster, :put_live, [[ref]])

      :ok =
        PollingHelper.wait_until!(fn -> :sys.get_state({@broker, node}).broker.brokers == [ref] end, timeout: 5_000)
    end

    :ok
  end

  # Waits until `node`'s view of the range holds `count` segments, the last `state` and led by `primary`
  # (`nil` for any), every one before it sealed.
  defp await_segments(ctx, node, count, state, primary \\ nil) do
    PollingHelper.wait_until!(
      fn ->
        segments = node |> StreamCluster.metadata_on() |> Metadata.segments_of_range(ctx.root)

        with true <- length(segments) == count,
             %{state: ^state, replica_set: [leader | _]} <- Enum.max_by(segments, & &1.start_offset) do
          Enum.count(segments, &(&1.state == :sealed)) == count - if(state == :active, do: 1, else: 0) and
            (primary == nil or leader == primary)
        else
          _not_yet -> false
        end
      end,
      timeout: 10_000
    )
  end

  defp acked!(stream, range_id, index), do: assert({:acked, _stream, _moved} = append(stream, range_id, index))

  test "an append after a roll placed the successor on another node is readable there, with no produce after it",
       ctx do
    [p, f | _] = ctx.nodes
    {p_ref, f_ref} = {{@repl, p}, {@repl, f}}

    # the first segment on p, written through p, so p knows the range's read horizon from its own answers
    # and never asks the replicas for it again
    :ok = place_on(ctx, p_ref)
    first = open(ctx.root, p)
    assert first.node == p
    for index <- 0..2, do: acked!(first, ctx.root, index)
    :ok = await_segments(ctx, f, 1, :sealed)

    # the second on f, through a stream f holds: the frontend whose segment the next roll seals
    :ok = place_on(ctx, f_ref)
    second = open(ctx.root, f)
    assert second.node == f
    for index <- 3..5, do: acked!(second, ctx.root, index)
    :ok = await_segments(ctx, f, 2, :sealed)

    # the successor of f's segment goes to p: the next append through f's stream lands in a segment p leads
    :ok = place_on(ctx, p_ref)

    case append(second, ctx.root, 6) do
      {:acked, _stream, _moved} ->
        :ok

      # refused rather than written through f: the producer opens where the range went and sends it again
      {:failed, _stream, _moved} ->
        third = open(ctx.root, f)
        assert third.node == p
        acked!(third, ctx.root, 6)
    end

    # p serves the range's reads once its view names the successor it leads
    :ok = await_segments(ctx, p, 3, :active, p_ref)
    assert ReadBack.values({@broker, p}, "events") == Enum.map(0..6, &value/1)
  end

  # The stream to append on: the one held, or one opened now, recording where each stream opened after the
  # failover.
  defp reopen(nil, ctx, excluded, failover, opened_after) do
    stream = open(ctx.root, hd(ctx.nodes -- excluded))
    {stream, if(failover, do: [stream.node | opened_after], else: opened_after)}
  end

  defp reopen(stream, _ctx, _excluded, _failover, opened_after), do: {stream, opened_after}
end
