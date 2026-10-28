defmodule Malachi.GroupKeysTest do
  # Consumer groups over the real TCP server (`Malachi.Wire` keys 32 to 34): a member's assignment and the
  # session it heartbeats within, the rebalance a heartbeat reports, eviction by `leave_group` (7), and the
  # checkpoint a member commits for its own ranges at its generation, which a consume stream then resumes
  # from. Refused commits leave the checkpoint where it was.
  #
  # async: false. The coordinator is the node-wide one the router resolves to once its topology is cleared here.
  use ExUnit.Case, async: false

  import Malachi.Test.StreamTCP

  alias Malachi.BrokerServer
  alias Malachi.ConnectionStreams
  alias Malachi.Consumer.Assignment
  alias Malachi.Consumer.GroupCoordinator
  alias Malachi.DataPlaneRouter
  alias Malachi.Test.RouterTopology
  alias Malachi.Test.TCPHelper
  alias Malachi.Wire

  setup do
    :ok = RouterTopology.clear()

    topic = "groups_#{System.unique_integer([:positive])}"
    socket = connect()
    {code, _} = TCPHelper.request(socket, Wire.create_topic_key(), 2, Wire.encode_create_topic_req(topic, 4))
    assert code == Wire.ok_code()

    # three records in the topic's one range, then a split: two active ranges whose history holds them
    {:ok, %{stream_id: id}} = open(socket, topic)
    send_append(socket, id, 0, batch(["a", "b", "c"]))
    assert {_ack, []} = acked_through(socket, 0)
    {:ok, {_, left}, {_, right}} = BrokerServer.split_range(DataPlaneRouter.shard_for(topic), {topic, 0})
    assert {:moved, _moved} = push(socket)

    %{socket: socket, topic: topic, ranges: [left, right], group: "g_#{System.unique_integer([:positive])}"}
  end

  defp connect do
    {:ok, socket} = TCPHelper.connect()
    {:ok, _token} = TCPHelper.authenticate_wire(socket, "app", "app123")
    on_exit(fn -> :gen_tcp.close(socket) end)
    socket
  end

  defp answer({0, payload}, decode), do: {:ok, decode.(payload)}
  defp answer({1, payload}, _decode), do: {:error, Wire.decode_error_reason(payload)}

  defp join(ctx, member) do
    req = %{topic: ctx.topic, group: ctx.group, member: member}

    ctx.socket
    |> TCPHelper.request(Wire.join_group_key(), 3, Wire.encode_join_group_req(req))
    |> answer(&Wire.decode_assignment_resp/1)
  end

  defp heartbeat(ctx, member, generation) do
    req = %{topic: ctx.topic, group: ctx.group, member: member, generation: generation}

    ctx.socket
    |> TCPHelper.request(Wire.group_heartbeat_key(), 4, Wire.encode_group_heartbeat_req(req))
    |> answer(&Wire.decode_assignment_resp/1)
  end

  defp commit(ctx, member, generation, positions) do
    req = %{
      topic: ctx.topic,
      group: ctx.group,
      member: member,
      generation: generation,
      positions: for({range, position} <- positions, do: %{range: range, position: position})
    }

    ctx.socket
    |> TCPHelper.request(Wire.commit_offsets_key(), 5, Wire.encode_commit_offsets_req(req))
    |> answer(fn <<>> -> :ok end)
  end

  defp leave(ctx, member) do
    {code, _} =
      TCPHelper.request(
        ctx.socket,
        Wire.leave_group_key(),
        6,
        Wire.encode_leave_group_req(ctx.topic, ctx.group, member)
      )

    assert code == Wire.ok_code()
  end

  # A member that shares the ranges with "m1": the assignment hashes each range to a member, so which name
  # takes one of them depends on the topic; this finds one that takes exactly one.
  defp sharing_member(ctx) do
    range_ids = Enum.map(ctx.ranges, &{ctx.topic, &1})

    Enum.find(for(i <- 2..100, do: "m#{i}"), fn name ->
      Assignment.assign(range_ids, ["m1", name]) |> Map.values() |> Enum.all?(&(length(&1) == 1))
    end)
  end

  defp heartbeat_calls(coordinator) do
    {:messages, messages} = Process.info(coordinator, :messages)
    Enum.count(messages, &match?({:"$gen_call", _from, {:heartbeat_group, _, _, _}}, &1))
  end

  defp commit_calls(broker) do
    {:messages, messages} = Process.info(broker, :messages)
    Enum.count(messages, &match?({:"$gen_call", _from, {:commit_offset, _, _, _, _}}, &1))
  end

  defp wait_until(check, tries \\ 100) do
    cond do
      check.() -> true
      tries == 0 -> false
      true -> Process.sleep(10) && wait_until(check, tries - 1)
    end
  end

  defp committed_start(ctx, range) do
    {:ok, %{position: position}} = open_consume(connect(), ctx.topic, range: range, start: {:committed, ctx.group})
    position
  end

  test "a lone member is handed every range, the session to heartbeat within and a generation of this coordinator's epoch",
       ctx do
    assert {:ok, %{generation: generation, session_ms: session_ms, ranges: ranges}} = join(ctx, "m1")
    assert Enum.sort(ranges) == ctx.ranges
    assert session_ms == Application.get_env(:malachi, :group_session_ms, 30_000)
    # the epoch sits above the rebalance count, so the first generation is not a bare 1
    assert generation > 0xFFFFFFFF
    assert Bitwise.band(generation, 0xFFFFFFFF) == 1
  end

  test "a heartbeat at an old generation is answered with the rebalance a new member caused", ctx do
    {:ok, %{generation: first}} = join(ctx, "m1")
    {:ok, %{generation: second, ranges: theirs}} = join(ctx, sharing_member(ctx))
    assert second == first + 1

    assert {:ok, %{generation: ^second, ranges: mine}} = heartbeat(ctx, "m1", first)
    assert Enum.sort(mine ++ theirs) == ctx.ranges
    assert length(mine) == 1 and length(theirs) == 1
  end

  test "a member that never joined, or left, is unknown to a heartbeat and to a commit", ctx do
    assert heartbeat(ctx, "ghost", 0) == {:error, "unknown_member"}
    assert commit(ctx, "ghost", 0, [{hd(ctx.ranges), {0, 1}}]) == {:error, "unknown_member"}

    {:ok, %{generation: generation}} = join(ctx, "m1")
    leave(ctx, "m1")
    assert heartbeat(ctx, "m1", generation) == {:error, "unknown_member"}
    assert commit(ctx, "m1", generation, [{hd(ctx.ranges), {0, 1}}]) == {:error, "unknown_member"}
  end

  test "a member commits its ranges at its generation, and a consume stream resumes from the checkpoint", ctx do
    {:ok, %{generation: generation}} = join(ctx, "m1")
    [left, right] = ctx.ranges

    assert commit(ctx, "m1", generation, [{left, {0, 2}}, {right, {0, 1}}]) == {:ok, :ok}
    assert committed_start(ctx, left) == {0, 2}
    assert committed_start(ctx, right) == {0, 1}
  end

  test "a rebalance that leaves a member's range with it does not fence its commits", ctx do
    {:ok, %{generation: old}} = join(ctx, "m1")
    {:ok, %{generation: new}} = join(ctx, sharing_member(ctx))
    {:ok, %{ranges: [mine]}} = heartbeat(ctx, "m1", old)
    assert new > old

    assert commit(ctx, "m1", old, [{mine, {0, 1}}]) == {:ok, :ok}
    assert commit(ctx, "m1", new, [{mine, {0, 2}}]) == {:ok, :ok}
    assert committed_start(ctx, mine) == {0, 2}
  end

  test "a range handed back after the generation a member commits at is refused and leaves the checkpoint",
       ctx do
    {:ok, %{generation: old}} = join(ctx, "m1")
    other = sharing_member(ctx)
    {:ok, %{generation: shared, ranges: [taken]}} = join(ctx, other)
    assert commit(ctx, other, shared, [{taken, {0, 1}}]) == {:ok, :ok}
    leave(ctx, other)

    # the range is m1's again from a later generation than the one it still holds
    assert commit(ctx, "m1", old, [{taken, {0, 3}}]) == {:error, "stale_generation"}
    assert committed_start(ctx, taken) == {0, 1}
    {:ok, %{generation: back}} = heartbeat(ctx, "m1", old)
    assert commit(ctx, "m1", back, [{taken, {0, 3}}]) == {:ok, :ok}
  end

  test "the control plane refuses a commit a later generation already passed, though the coordinator took it",
       ctx do
    # what a commit validated before a rebalance and written after the range's next owner committed looks
    # like at the log: that owner's higher generation is already on the range
    {:ok, %{generation: generation, ranges: [range | _]}} = join(ctx, "m1")

    :ok =
      BrokerServer.commit_offset(
        DataPlaneRouter.shard_for(ctx.topic),
        ctx.group,
        ctx.topic,
        %{{ctx.topic, range} => {0, 2}},
        generation + 1
      )

    assert commit(ctx, "m1", generation, [{range, {0, 1}}]) == {:error, "stale_generation"}
    assert committed_start(ctx, range) == {0, 2}
  end

  test "a coordinator another one superseded has its commit refused, opens the group again, and its member recovers",
       ctx do
    {:ok, %{generation: generation, ranges: [range | _]}} = join(ctx, "m1")
    # another coordinator of the group opening it, as one on another node of an unsharded cluster does
    {:ok, _epoch} = BrokerServer.open_group(DataPlaneRouter.shard_for(ctx.topic), ctx.group, ctx.topic)

    assert commit(ctx, "m1", generation, [{range, {0, 1}}]) == {:error, "stale_generation"}
    assert committed_start(ctx, range) == {0, 0}

    # the drop is a cast; the heartbeat after it carries the group's next epoch
    assert {:ok, %{generation: next}} =
             Enum.find_value(1..50, fn _ ->
               reply = heartbeat(ctx, "m1", generation)
               if match?({:ok, %{generation: g}} when g != generation, reply), do: reply, else: Process.sleep(10) && nil
             end)

    assert Bitwise.bsr(next, 32) > Bitwise.bsr(generation, 32)
    assert commit(ctx, "m1", next, [{range, {0, 1}}]) == {:ok, :ok}
    assert committed_start(ctx, range) == {0, 1}
  end

  test "on a connection that holds streams, a member's commits go to the control plane one at a time, in order",
       ctx do
    {:ok, %{stream_id: _id}} = open(ctx.socket, ctx.topic, range: hd(ctx.ranges))
    {:ok, %{generation: generation, ranges: [range | _]}} = join(ctx, "m1")
    broker = Process.whereis(DataPlaneRouter.shard_for(ctx.topic))
    :ok = :sys.suspend(broker)
    on_exit(fn -> if Process.alive?(broker), do: :sys.resume(broker) end)

    for {corr, offset} <- [{51, 100}, {52, 200}] do
      req = %{
        topic: ctx.topic,
        group: ctx.group,
        member: "m1",
        generation: generation,
        positions: [%{range: range, position: {0, offset}}]
      }

      :ok =
        :gen_tcp.send(
          ctx.socket,
          Wire.encode_request(Wire.commit_offsets_key(), corr, Wire.encode_commit_offsets_req(req))
        )
    end

    # the second commit waits for the first to be answered: one commit reaches the broker, and no second
    # one follows while the first is held (the broker's own timers land in its queue too, so only the
    # commit calls are counted)
    assert wait_until(fn -> commit_calls(broker) == 1 end)
    Process.sleep(200)
    assert commit_calls(broker) == 1
    :ok = :sys.resume(broker)

    answers =
      for _ <- 1..2 do
        {:ok, body} = TCPHelper.recv_frame(ctx.socket)
        {corr, code, _payload} = Wire.decode_response(body)
        {corr, code}
      end

    assert answers == [{51, 0}, {52, 0}]
    assert committed_start(ctx, range) == {0, 200}
  end

  test "on a connection that holds streams, a leave waits for the commits sent before it", ctx do
    {:ok, _stream} = open(ctx.socket, ctx.topic, range: hd(ctx.ranges))
    {:ok, %{generation: generation, ranges: [range | _]}} = join(ctx, "m1")
    broker = Process.whereis(DataPlaneRouter.shard_for(ctx.topic))
    :ok = :sys.suspend(broker)
    on_exit(fn -> if Process.alive?(broker), do: :sys.resume(broker) end)

    req = %{
      topic: ctx.topic,
      group: ctx.group,
      member: "m1",
      generation: generation,
      positions: [%{range: range, position: {0, 1}}]
    }

    :ok =
      :gen_tcp.send(ctx.socket, Wire.encode_request(Wire.commit_offsets_key(), 61, Wire.encode_commit_offsets_req(req)))

    leave = Wire.encode_leave_group_req(ctx.topic, ctx.group, "m1")
    :ok = :gen_tcp.send(ctx.socket, Wire.encode_request(Wire.leave_group_key(), 62, leave))

    # the commit is held at the control plane, and the member is still in the group behind it
    assert wait_until(fn -> commit_calls(broker) == 1 end)
    Process.sleep(100)

    assert {:ok, _assignment} =
             GroupCoordinator.heartbeat_group(Malachi.LogGroupCoordinator, ctx.group, ctx.topic, "m1")

    :ok = :sys.resume(broker)

    answers =
      for _ <- 1..2 do
        {:ok, body} = TCPHelper.recv_frame(ctx.socket)
        {corr, code, _payload} = Wire.decode_response(body)
        {corr, code}
      end

    assert answers == [{61, 0}, {62, 0}]

    assert GroupCoordinator.heartbeat_group(Malachi.LogGroupCoordinator, ctx.group, ctx.topic, "m1") ==
             {:error, :unknown_member}
  end

  test "one delivery of more requests than a connection may hold starts no more jobs than it may hold", ctx do
    {:ok, _stream} = open(ctx.socket, ctx.topic, range: hd(ctx.ranges))
    {:ok, %{generation: generation}} = join(ctx, "m1")
    coordinator = Process.whereis(Malachi.LogGroupCoordinator)
    :ok = :sys.suspend(coordinator)
    on_exit(fn -> if Process.alive?(coordinator), do: :sys.resume(coordinator) end)

    count = ConnectionStreams.max_jobs() + 36

    heartbeat =
      Wire.encode_group_heartbeat_req(%{topic: ctx.topic, group: ctx.group, member: "m1", generation: generation})

    burst =
      for corr <- 1_000..(1_000 + count - 1),
          into: <<>>,
          do: Wire.encode_request(Wire.group_heartbeat_key(), corr, heartbeat)

    :ok = :gen_tcp.send(ctx.socket, burst)

    assert wait_until(fn -> heartbeat_calls(coordinator) == ConnectionStreams.max_jobs() end)
    Process.sleep(200)
    assert heartbeat_calls(coordinator) == ConnectionStreams.max_jobs()

    # the rest are read as the first ones end, and every request is answered
    :ok = :sys.resume(coordinator)

    answered =
      for _ <- 1..count do
        {:ok, body} = TCPHelper.recv_frame(ctx.socket, timeout: 10_000)
        {corr, 0, _payload} = Wire.decode_response(body)
        corr
      end

    assert Enum.sort(answered) == Enum.to_list(1_000..(1_000 + count - 1))
  end

  test "on a connection that holds streams, a join waits for the leave sent before it", ctx do
    {:ok, _stream} = open(ctx.socket, ctx.topic, range: hd(ctx.ranges))
    {:ok, %{generation: generation, ranges: [range | _]}} = join(ctx, "m1")
    broker = Process.whereis(DataPlaneRouter.shard_for(ctx.topic))
    :ok = :sys.suspend(broker)
    on_exit(fn -> if Process.alive?(broker), do: :sys.resume(broker) end)

    commit = %{
      topic: ctx.topic,
      group: ctx.group,
      member: "m1",
      generation: generation,
      positions: [%{range: range, position: {0, 1}}]
    }

    rejoin = Wire.encode_join_group_req(%{topic: ctx.topic, group: ctx.group, member: "m1"})

    for {key, corr, payload} <- [
          {Wire.commit_offsets_key(), 71, Wire.encode_commit_offsets_req(commit)},
          {Wire.leave_group_key(), 72, Wire.encode_leave_group_req(ctx.topic, ctx.group, "m1")},
          {Wire.join_group_key(), 73, rejoin}
        ] do
      :ok = :gen_tcp.send(ctx.socket, Wire.encode_request(key, corr, payload))
    end

    assert wait_until(fn -> commit_calls(broker) == 1 end)
    :ok = :sys.resume(broker)

    answers =
      for _ <- 1..3 do
        {:ok, body} = TCPHelper.recv_frame(ctx.socket)
        {corr, code, _payload} = Wire.decode_response(body)
        {corr, code}
      end

    # the leave went first and the join after it, so the member is in the group at the end
    assert answers == [{71, 0}, {72, 0}, {73, 0}]

    assert {:ok, _assignment} =
             GroupCoordinator.heartbeat_group(Malachi.LogGroupCoordinator, ctx.group, ctx.topic, "m1")
  end

  test "a leave still waiting when the client closes the connection is carried out all the same", ctx do
    socket = connect()
    {:ok, _stream} = open(socket, ctx.topic, range: hd(ctx.ranges))
    {:ok, %{generation: generation, ranges: [range | _]}} = join(ctx, "m1")
    broker = Process.whereis(DataPlaneRouter.shard_for(ctx.topic))
    :ok = :sys.suspend(broker)
    on_exit(fn -> if Process.alive?(broker), do: :sys.resume(broker) end)

    commit = %{
      topic: ctx.topic,
      group: ctx.group,
      member: "m1",
      generation: generation,
      positions: [%{range: range, position: {0, 1}}]
    }

    :ok =
      :gen_tcp.send(socket, Wire.encode_request(Wire.commit_offsets_key(), 81, Wire.encode_commit_offsets_req(commit)))

    :ok =
      :gen_tcp.send(
        socket,
        Wire.encode_request(Wire.leave_group_key(), 82, Wire.encode_leave_group_req(ctx.topic, ctx.group, "m1"))
      )

    assert wait_until(fn -> commit_calls(broker) == 1 end)
    :ok = :gen_tcp.close(socket)
    Process.sleep(100)

    :ok = :sys.resume(broker)

    assert wait_until(fn ->
             GroupCoordinator.heartbeat_group(Malachi.LogGroupCoordinator, ctx.group, ctx.topic, "m1") ==
               {:error, :unknown_member}
           end)

    assert committed_start(ctx, range) == {0, 1}
  end

  test "a leave still waiting when an oversized frame ends the connection is carried out all the same", ctx do
    socket = connect()
    {:ok, _stream} = open(socket, ctx.topic, range: hd(ctx.ranges))
    {:ok, %{generation: generation, ranges: [range | _]}} = join(ctx, "m1")
    broker = Process.whereis(DataPlaneRouter.shard_for(ctx.topic))
    :ok = :sys.suspend(broker)
    on_exit(fn -> if Process.alive?(broker), do: :sys.resume(broker) end)

    commit = %{
      topic: ctx.topic,
      group: ctx.group,
      member: "m1",
      generation: generation,
      positions: [%{range: range, position: {0, 1}}]
    }

    leave = Wire.encode_leave_group_req(ctx.topic, ctx.group, "m1")

    :ok =
      :gen_tcp.send(socket, Wire.encode_request(Wire.commit_offsets_key(), 91, Wire.encode_commit_offsets_req(commit)))

    assert wait_until(fn -> commit_calls(broker) == 1 end)
    # the leave and a frame too large to read arrive together: the leave is queued, then the connection ends
    :ok = :gen_tcp.send(socket, Wire.encode_request(Wire.leave_group_key(), 92, leave) <> <<0xFFFFFFF0::32>>)
    Process.sleep(200)

    :ok = :sys.resume(broker)

    assert wait_until(fn ->
             GroupCoordinator.heartbeat_group(Malachi.LogGroupCoordinator, ctx.group, ctx.topic, "m1") ==
               {:error, :unknown_member}
           end)
  end

  test "a join still waiting when the client closes the connection is not made", ctx do
    socket = connect()
    {:ok, _stream} = open(socket, ctx.topic, range: hd(ctx.ranges))
    {:ok, %{generation: generation, ranges: [range | _]}} = join(ctx, "m1")
    broker = Process.whereis(DataPlaneRouter.shard_for(ctx.topic))
    :ok = :sys.suspend(broker)
    on_exit(fn -> if Process.alive?(broker), do: :sys.resume(broker) end)

    commit = %{
      topic: ctx.topic,
      group: ctx.group,
      member: "m1",
      generation: generation,
      positions: [%{range: range, position: {0, 1}}]
    }

    late = Wire.encode_join_group_req(%{topic: ctx.topic, group: ctx.group, member: "late"})

    :ok =
      :gen_tcp.send(socket, Wire.encode_request(Wire.commit_offsets_key(), 95, Wire.encode_commit_offsets_req(commit)))

    assert wait_until(fn -> commit_calls(broker) == 1 end)
    :ok = :gen_tcp.send(socket, Wire.encode_request(Wire.join_group_key(), 96, late))
    # a heartbeat sent after the join runs at once; its answer shows the join was read and queued
    heartbeat =
      Wire.encode_group_heartbeat_req(%{topic: ctx.topic, group: ctx.group, member: "m1", generation: generation})

    :ok = :gen_tcp.send(socket, Wire.encode_request(Wire.group_heartbeat_key(), 97, heartbeat))
    assert {:ok, body} = TCPHelper.recv_frame(socket)
    assert {97, 0, _assignment} = Wire.decode_response(body)
    :ok = :gen_tcp.close(socket)
    Process.sleep(100)

    :ok = :sys.resume(broker)
    # the held commit lands, and nobody joins on behalf of the client that went
    assert wait_until(fn -> committed_start(ctx, range) == {0, 1} end)
    Process.sleep(200)

    assert GroupCoordinator.heartbeat_group(Malachi.LogGroupCoordinator, ctx.group, ctx.topic, "late") ==
             {:error, :unknown_member}
  end

  test "a leave its coordinator cannot be reached for is not_owner, and the connection lives on", ctx do
    coordinator = Process.whereis(Malachi.LogGroupCoordinator)
    true = Process.unregister(Malachi.LogGroupCoordinator)

    on_exit(fn ->
      if Process.whereis(Malachi.LogGroupCoordinator) == nil,
        do: Process.register(coordinator, Malachi.LogGroupCoordinator)
    end)

    leave = Wire.encode_leave_group_req(ctx.topic, ctx.group, "m1")
    assert {1, reason} = TCPHelper.request(ctx.socket, Wire.leave_group_key(), 6, leave)
    assert Wire.decode_error_reason(reason) == "not_owner"

    true = Process.register(coordinator, Malachi.LogGroupCoordinator)
    assert {:ok, _} = join(ctx, "m1")
  end

  test "a malformed group frame on a connection that holds streams is malformed_request, as on any other", ctx do
    {:ok, _stream} = open(ctx.socket, ctx.topic, range: hd(ctx.ranges))
    # a heartbeat with no generation after the member
    payload = Wire.encode_join_group_req(%{topic: ctx.topic, group: ctx.group, member: "m1"})
    :ok = :gen_tcp.send(ctx.socket, Wire.encode_request(Wire.group_heartbeat_key(), 4, payload))
    {:ok, body} = TCPHelper.recv_frame(ctx.socket)
    assert {4, 1, reason} = Wire.decode_response(body)
    assert Wire.decode_error_reason(reason) == "malformed_request"
  end

  test "a checkpoint past the range's history does not start a reader there", ctx do
    [range | _] = ctx.ranges

    # generation 0: no coordinator opened this group, so its epoch is 0 and the fence takes the commit
    :ok =
      BrokerServer.commit_offset(
        DataPlaneRouter.shard_for(ctx.topic),
        ctx.group,
        ctx.topic,
        %{{ctx.topic, range} => {9, 0}},
        0
      )

    assert open_consume(connect(), ctx.topic, range: range, start: {:committed, ctx.group}) ==
             {:error, "invalid_position"}
  end

  test "a commit naming a range that is not the member's is refused whole", ctx do
    {:ok, _} = join(ctx, "m1")
    {:ok, %{generation: generation, ranges: theirs}} = join(ctx, sharing_member(ctx))
    {:ok, %{ranges: [own]}} = heartbeat(ctx, "m1", generation)
    [other] = theirs

    assert commit(ctx, "m1", generation, [{own, {0, 1}}, {other, {0, 1}}]) == {:error, "not_assigned"}
    assert committed_start(ctx, own) == {0, 0}
    assert committed_start(ctx, other) == {0, 0}
  end

  test "a coordinator that is not there is not_owner to the client, and the connection lives on", ctx do
    coordinator = Process.whereis(Malachi.LogGroupCoordinator)
    true = Process.unregister(Malachi.LogGroupCoordinator)

    on_exit(fn ->
      if Process.whereis(Malachi.LogGroupCoordinator) == nil,
        do: Process.register(coordinator, Malachi.LogGroupCoordinator)
    end)

    assert join(ctx, "m1") == {:error, "not_owner"}
    true = Process.register(coordinator, Malachi.LogGroupCoordinator)
    assert {:ok, %{ranges: [_, _]}} = join(ctx, "m1")
  end

  test "a control plane that does not answer a commit in time is timeout, and the connection lives on", ctx do
    {:ok, %{generation: generation, ranges: [range | _]}} = join(ctx, "m1")
    broker = Process.whereis(DataPlaneRouter.shard_for(ctx.topic))
    :ok = :sys.suspend(broker)
    on_exit(fn -> if Process.alive?(broker), do: :sys.resume(broker) end)

    req = %{
      topic: ctx.topic,
      group: ctx.group,
      member: "m1",
      generation: generation,
      positions: [%{range: range, position: {0, 1}}]
    }

    :ok =
      :gen_tcp.send(ctx.socket, Wire.encode_request(Wire.commit_offsets_key(), 5, Wire.encode_commit_offsets_req(req)))

    {:ok, body} = TCPHelper.recv_frame(ctx.socket, timeout: 8_000)
    :ok = :sys.resume(broker)
    assert {5, 1, reason} = Wire.decode_response(body)
    assert Wire.decode_error_reason(reason) == "timeout"
    assert {:ok, _} = heartbeat(ctx, "m1", generation)
  end

  test "on a connection that holds streams, a group key waiting on its coordinator holds no append back", ctx do
    {:ok, %{stream_id: id}} = open(ctx.socket, ctx.topic, range: hd(ctx.ranges))
    coordinator = Process.whereis(Malachi.LogGroupCoordinator)
    :ok = :sys.suspend(coordinator)
    on_exit(fn -> if Process.alive?(coordinator), do: :sys.resume(coordinator) end)

    req = Wire.encode_join_group_req(%{topic: ctx.topic, group: ctx.group, member: "m1"})
    :ok = :gen_tcp.send(ctx.socket, Wire.encode_request(Wire.join_group_key(), 3, req))
    send_append(ctx.socket, id, 0, batch(["while it waits"]))
    # answered (whatever the key's range says) while the join still waits
    assert {%{acked_sequence: 1}, _errors} = acked_through(ctx.socket, 0)

    :ok = :sys.resume(coordinator)
    {:ok, body} = TCPHelper.recv_frame(ctx.socket, timeout: 8_000)
    assert {3, 0, payload} = Wire.decode_response(body)
    assert %{ranges: [_, _]} = Wire.decode_assignment_resp(payload)
  end

  test "a coordinator that does not answer is not_owner to the client, and the connection lives on", ctx do
    coordinator = Process.whereis(Malachi.LogGroupCoordinator)
    :ok = :sys.suspend(coordinator)
    on_exit(fn -> if Process.alive?(coordinator), do: :sys.resume(coordinator) end)

    # the call gives up after its 5 s default, so the answer is read with more room than the helper's
    req = Wire.encode_join_group_req(%{topic: ctx.topic, group: ctx.group, member: "m1"})
    :ok = :gen_tcp.send(ctx.socket, Wire.encode_request(Wire.join_group_key(), 3, req))
    {:ok, body} = TCPHelper.recv_frame(ctx.socket, timeout: 8_000)
    assert {3, 1, reason} = Wire.decode_response(body)
    assert Wire.decode_error_reason(reason) == "not_owner"

    :ok = :sys.resume(coordinator)
    assert {:ok, %{ranges: [_, _]}} = join(ctx, "m1")
  end
end
