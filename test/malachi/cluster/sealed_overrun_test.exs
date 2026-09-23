defmodule Malachi.Cluster.SealedOverrunTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Cluster.HealCoordinator
  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Cluster.SealedOverrun
  alias Malachi.Log.Record
  alias Malachi.Metadata
  alias Malachi.Storage.Layout
  alias Malachi.Test.FaultySegmentStore

  # `topics` is `[{topic_name, [{seq, replica_set, start_offset, seal_length}]}]`, a nil length leaving
  # the segment active. Same shape as `Malachi.Cluster.OrphanedFenceTest`'s, because the two policies
  # are the two halves of one split and reading them side by side is the point.
  defp metadata_with(topics) do
    Enum.reduce(topics, {Metadata.new(), []}, fn {name, segments}, {meta, ids} ->
      {meta, {:ok, root}} = Metadata.apply(meta, {:create_topic, name, 8})
      Enum.reduce(segments, {meta, ids}, &register(&1, &2, root))
    end)
  end

  defp register({seq, replica_set, start_offset, seal_length}, {meta, ids}, root) do
    id = {root, seq}
    {meta, :ok} = Metadata.apply(meta, {:register_segment, root, id, replica_set, start_offset})

    meta =
      if seal_length,
        do: elem(Metadata.apply(meta, {:seal_segment, id, seal_length, 0, 0}), 0),
        else: meta

    {meta, ids ++ [id]}
  end

  defp topic_with(segments), do: metadata_with([{"events", segments}])

  describe "plan/3 (the pure decision)" do
    test "a copy of a sealed segment becomes one call at the recorded end" do
      {metadata, [id]} = topic_with([{0, [:a, :b, :c], 100, 8}])

      assert SealedOverrun.plan(metadata, [{id, :c}], 10) == [{:c, id, 100, 108}]
    end

    test "the end comes from the control plane, never from the copy" do
      # The whole rule in one assertion: two copies of the same segment get the same number, and it is
      # the recorded one. A copy that disagrees is what this pass exists to correct, so asking the copy
      # would ask the thing being corrected.
      {metadata, [id]} = topic_with([{0, [:a, :b], 0, 42}])

      assert SealedOverrun.plan(metadata, [{id, :b}, {id, :a}], 10) == [{:a, id, 0, 42}, {:b, id, 0, 42}]
    end

    test "a segment the control plane still calls active is skipped" do
      # No length has been decided for it, so there is nothing to bring a copy down to. Choosing a safe
      # seal point for an active segment is issue #210's half of the same split.
      {metadata, [id]} = topic_with([{0, [:a, :b], 0, nil}])

      assert SealedOverrun.plan(metadata, [{id, :a}], 10) == []
    end

    test "a segment retention dropped between the probe and this call is skipped" do
      {metadata, [id]} = topic_with([{0, [:a, :b], 0, 4}])
      gone = {elem(id, 0), 99}

      assert SealedOverrun.plan(metadata, [{gone, :a}], 10) == []
    end

    test "a segment sealed at length 0 is settled at its base offset, not skipped" do
      # Seen once on the drill (issue #173): a segment sealed empty whose directory one replica never
      # created. A copy of it that holds anything at all holds records the seal excludes.
      {metadata, [id]} = topic_with([{0, [:a, :b], 70, 0}])

      assert SealedOverrun.plan(metadata, [{id, :b}], 10) == [{:b, id, 70, 70}]
    end

    test "the batch bounds the pass, and cuts the same prefix every time" do
      {metadata, [id]} = topic_with([{0, [:a, :b, :c], 0, 5}])
      unsettled = [{id, :c}, {id, :a}, {id, :b}]

      assert SealedOverrun.plan(metadata, unsettled, 2) == [{:a, id, 0, 5}, {:b, id, 0, 5}]
      assert SealedOverrun.plan(metadata, Enum.reverse(unsettled), 2) == [{:a, id, 0, 5}, {:b, id, 0, 5}]
    end

    test "the same copy reported twice is settled once" do
      {metadata, [id]} = topic_with([{0, [:a], 0, 3}])

      assert SealedOverrun.plan(metadata, [{id, :a}, {id, :a}], 10) == [{:a, id, 0, 3}]
    end

    test "nothing unsettled is nothing to do" do
      {metadata, [_id]} = topic_with([{0, [:a, :b], 0, 3}])

      assert SealedOverrun.plan(metadata, [], 10) == []
    end
  end

  describe "plan/3 properties" do
    defp segment_specs_gen do
      gen all(
            specs <-
              list_of(
                tuple({
                  boolean(),
                  integer(0..1_000),
                  integer(0..100),
                  list_of(member_of([:a, :b, :c, :d]), min_length: 1, max_length: 4)
                }),
                min_length: 1,
                max_length: 6
              )
          ) do
        specs
      end
    end

    # One topic per segment: a range has one write head, so putting every generated segment in its own
    # topic keeps the fixture legal whatever offsets and seal states come out of the generator.
    defp metadata_from(specs) do
      topics =
        specs
        |> Enum.with_index()
        |> Enum.map(fn {{sealed?, start_offset, length, replicas}, index} ->
          {"t#{index}", [{0, Enum.uniq(replicas), start_offset, if(sealed?, do: length)}]}
        end)

      metadata_with(topics)
    end

    property "every call names a sealed segment and its recorded end, and never more than the batch" do
      check all(specs <- segment_specs_gen(), batch_size <- integer(1..8)) do
        {metadata, ids} = metadata_from(specs)
        unsettled = for id <- ids, replica <- [:a, :b, :c, :d], do: {id, replica}

        plan = SealedOverrun.plan(metadata, unsettled, batch_size)

        assert length(plan) <= batch_size

        for {_replica, id, start_offset, end_offset} <- plan do
          segment = Map.fetch!(metadata.segments, id)
          assert segment.state == :sealed
          assert segment.start_offset == start_offset
          assert segment.start_offset + segment.length == end_offset
        end
      end
    end

    property "the plan does not depend on the order the copies were reported in" do
      check all(specs <- segment_specs_gen(), batch_size <- integer(1..8)) do
        {metadata, ids} = metadata_from(specs)
        unsettled = for id <- ids, replica <- [:a, :b, :c, :d], do: {id, replica}

        assert SealedOverrun.plan(metadata, unsettled, batch_size) ==
                 SealedOverrun.plan(metadata, Enum.shuffle(unsettled), batch_size)
      end
    end

    property "a copy of a segment that is not sealed is never acted on" do
      check all(specs <- segment_specs_gen()) do
        {metadata, ids} = metadata_from(specs)
        unsettled = for id <- ids, replica <- [:a, :b, :c, :d], do: {id, replica}

        active = for {id, %{state: :active}} <- metadata.segments, do: id
        planned = for {_replica, id, _start, _end} <- SealedOverrun.plan(metadata, unsettled, 100), do: id

        assert planned -- active == planned
      end
    end
  end

  describe "the settling pass, end to end" do
    defp start_broker(opts \\ []), do: elem(start_broker_at(opts), 0)

    defp start_broker_at(opts) do
      name = :"overrun_#{System.unique_integer([:positive])}"
      directory = Path.join(System.tmp_dir!(), "malachi_overrun_#{System.unique_integer([:positive])}")

      on_exit(fn ->
        FaultySegmentStore.clear(directory)
        File.rm_rf!(directory)
      end)

      start_supervised!({ReplicationServer, [name: name, directory: directory] ++ opts}, id: name)
      {name, directory}
    end

    # Killed rather than stopped, and the difference is the whole point of the case it sets up: a clean
    # shutdown closes every log, and closing gives the preallocated tail back, so a copy only keeps its
    # tail when its process died without getting there. The test supervisor restarts it under the same
    # name, which is what a real node coming back looks like.
    defp kill_broker(name) do
      pid = Process.whereis(name)
      monitor = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}, 2_000
      wait_until(fn -> is_pid(Process.whereis(name)) and Process.whereis(name) != pid end)
    end

    defp wait_until(check, remaining_ms \\ 2_000) do
      cond do
        check.() -> :ok
        remaining_ms <= 0 -> flunk("condition never held")
        true -> Process.sleep(10) && wait_until(check, remaining_ms - 10)
      end
    end

    defp records(values), do: for(value <- values, do: Record.new(value, key: value))

    defp read_values(ref, segment_id) do
      case ReplicationServer.read(ref, segment_id, 0, 100) do
        {:ok, records} -> Enum.map(records, & &1.value)
        :eof -> []
      end
    end

    defp fenced?(ref, segment_id) do
      {:ok, fenced} = ReplicationServer.fenced_segments(ref, [{segment_id, 0}])
      Map.has_key?(fenced, segment_id)
    end

    defp metadata_store(metadata) do
      {:ok, agent} = start_supervised({Agent, fn -> metadata end}, id: {:meta, System.unique_integer([:positive])})
      source = fn -> Agent.get(agent, & &1) end
      apply = fn command -> Agent.update(agent, fn meta -> elem(Metadata.apply(meta, command), 0) end) end
      {source, apply}
    end

    defp start_coordinator(opts) do
      defaults = [interval: 60_000, replication_factor: 3]
      start_supervised!({HealCoordinator, Keyword.merge(defaults, opts)}, id: {:co, System.unique_integer([:positive])})
    end

    # Every replica holds `values`, and the segment is sealed with the byte size they actually have,
    # which is the shape the integrity probe compares against.
    defp sealed_everywhere(replica_set, values) do
      {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 4})
      segment_id = {root, 0}
      {metadata, :ok} = Metadata.apply(metadata, {:register_segment, root, segment_id, replica_set, 0})

      for replica <- replica_set do
        {:ok, _last} = ReplicationServer.follow(replica, segment_id, 0, records(values))
      end

      bytes = ReplicationServer.stored_bytes(hd(replica_set), segment_id)
      {metadata, :ok} = Metadata.apply(metadata, {:seal_segment, segment_id, length(values), bytes, 0})
      {metadata, segment_id}
    end

    # Hypothesis 2 of issue #175: the old primary wrote a record locally, lost quorum before it was
    # acknowledged, and failover sealed the segment on the two replicas that answered. It comes back
    # holding one record more than the seal says, and the fence never reached it, so it never refused
    # anything. `follow/4` is how a write lands on a copy without passing the fence, which is what the
    # primary's own local append did before it lost quorum.
    test "an old primary that comes back holding a record past the sealed end is brought back to it" do
      [a, b, c] = [start_broker(), start_broker(), start_broker()]
      {metadata, segment_id} = sealed_everywhere([a, b, c], ["x", "y", "z"])
      {:ok, 3} = ReplicationServer.follow(c, segment_id, 3, records(["surplus"]))
      assert read_values(c, segment_id) == ["x", "y", "z", "surplus"]

      {source, apply} = metadata_store(metadata)
      coordinator = start_coordinator(live_brokers: fn -> [a, b, c] end, metadata_source: source, apply_command: apply)

      log = ExUnit.CaptureLog.capture_log(fn -> HealCoordinator.heal_now(coordinator) end)

      assert read_values(c, segment_id) == ["x", "y", "z"]
      assert fenced?(c, segment_id)
      assert ReplicationServer.stored_bytes(c, segment_id) == Metadata.get_segment(source.(), segment_id).byte_size
      assert log =~ "dropping 1 record(s) past the sealed end"
    end

    # Hypothesis 1 of the same issue: a follower whose own log was never fenced accepts a push for a
    # segment the control plane has already sealed. `follow_push/3` refuses only when THIS replica's log
    # is sealed, and a produce roll fences the primary alone, so a follower's log is almost never sealed.
    test "a push that lands on an unfenced follower after the seal is undone by the next pass" do
      [a, b, c] = [start_broker(), start_broker(), start_broker()]
      {metadata, segment_id} = sealed_everywhere([a, b, c], ["x", "y", "z"])

      refute fenced?(c, segment_id)
      GenServer.cast(c, {:replica_append, segment_id, 0, 3, records(["late"]), 2, self()})
      assert_receive {:"$gen_cast", {:replica_ack, ^segment_id, _ref, {:ok, 3}}}, 2_000
      assert read_values(c, segment_id) == ["x", "y", "z", "late"]

      {source, apply} = metadata_store(metadata)
      coordinator = start_coordinator(live_brokers: fn -> [a, b, c] end, metadata_source: source, apply_command: apply)

      ExUnit.CaptureLog.capture_log(fn -> HealCoordinator.heal_now(coordinator) end)

      assert read_values(c, segment_id) == ["x", "y", "z"]
      assert fenced?(c, segment_id)
    end

    # The routine half, and the other outcome the counters have to keep apart. A copy whose process died
    # before it could give its preallocated tail back holds the right records in a bigger file, so it is
    # settled, trimmed and fenced while dropping nothing. On the first pass after an upgrade this is
    # every copy in the cluster, which is why it must not log like the case above.
    test "a copy that only kept a preallocated tail is trimmed and fenced, with nothing dropped" do
      [a, b] = [start_broker(prealloc_bytes: 8192), start_broker(prealloc_bytes: 8192)]
      {metadata, {:ok, root}} = Metadata.apply(Metadata.new(), {:create_topic, "events", 4})
      segment_id = {root, 0}
      {metadata, :ok} = Metadata.apply(metadata, {:register_segment, root, segment_id, [a, b], 0})
      for replica <- [a, b], do: {:ok, 2} = ReplicationServer.follow(replica, segment_id, 0, records(["x", "y", "z"]))

      # `a` is sealed the way a primary's roll seals it, so the recorded size is the trimmed one.
      {:ok, 3, bytes} = ReplicationServer.seal(a, segment_id, 0)
      {metadata, :ok} = Metadata.apply(metadata, {:seal_segment, segment_id, 3, bytes, 0})

      kill_broker(b)
      assert ReplicationServer.stored_bytes(b, segment_id) > bytes

      {source, apply} = metadata_store(metadata)

      coordinator =
        start_coordinator(
          live_brokers: fn -> [a, b] end,
          metadata_source: source,
          apply_command: apply,
          replication_factor: 2
        )

      log = ExUnit.CaptureLog.capture_log(fn -> HealCoordinator.heal_now(coordinator) end)

      assert ReplicationServer.stored_bytes(b, segment_id) == bytes
      assert read_values(b, segment_id) == ["x", "y", "z"]
      assert fenced?(b, segment_id)
      refute log =~ "past the sealed end"
    end

    test "a copy that is already byte exact is left alone, so a settled cluster does no work" do
      [a, b] = [start_broker(), start_broker()]
      {metadata, segment_id} = sealed_everywhere([a, b], ["x", "y"])
      {source, apply} = metadata_store(metadata)

      settled =
        start_coordinator(
          live_brokers: fn -> [a, b] end,
          metadata_source: source,
          apply_command: apply,
          replication_factor: 2,
          settle_copy: fn _replica, _segment, _base, _end -> flunk("a byte exact copy must not be settled") end
        )

      HealCoordinator.heal_now(settled)

      assert read_values(b, segment_id) == ["x", "y"]
    end

    test "the batch bounds one pass, and the next pass finishes what it left" do
      [a, b, c] = [start_broker(), start_broker(), start_broker()]
      {metadata, segment_id} = sealed_everywhere([a, b, c], ["x", "y"])
      for replica <- [b, c], do: {:ok, 2} = ReplicationServer.follow(replica, segment_id, 2, records(["surplus"]))

      {source, apply} = metadata_store(metadata)

      coordinator =
        start_coordinator(
          live_brokers: fn -> [a, b, c] end,
          metadata_source: source,
          apply_command: apply,
          settle_batch_size: 1
        )

      ExUnit.CaptureLog.capture_log(fn -> HealCoordinator.heal_now(coordinator) end)
      assert Enum.count([b, c], &(read_values(&1, segment_id) == ["x", "y"])) == 1

      ExUnit.CaptureLog.capture_log(fn -> HealCoordinator.heal_now(coordinator) end)
      assert Enum.count([b, c], &(read_values(&1, segment_id) == ["x", "y"])) == 2
    end

    # The same failure through the REAL seam rather than an injected one, which is the only way to
    # exercise how `seal_at/5`'s error is turned into a pass result. The copy stays divergent and the
    # pass says so rather than reporting work it did not do.
    test "a copy whose storage refuses the settle is reported through the default seam" do
      a = start_broker(store: FaultySegmentStore)
      {b, directory} = start_broker_at(store: FaultySegmentStore)
      {metadata, segment_id} = sealed_everywhere([a, b], ["x", "y"])
      {:ok, 2} = ReplicationServer.follow(b, segment_id, 2, records(["surplus"]))

      # The cut reopens the file it lands in, and that is the call this refuses.
      FaultySegmentStore.fail(Layout.segment_directory(directory, segment_id), :recover, {:error, :eio})

      {source, apply} = metadata_store(metadata)

      coordinator =
        start_coordinator(
          live_brokers: fn -> [a, b] end,
          metadata_source: source,
          apply_command: apply,
          replication_factor: 2
        )

      log = ExUnit.CaptureLog.capture_log(fn -> HealCoordinator.heal_now(coordinator) end)

      assert log =~ "still divergent"

      # A storage error during the cut takes the copy out of service, the same rule every whole-log
      # operation on this server follows: the heal pass is what deals with a failed copy from here.
      assert ReplicationServer.failed_segments(b, [segment_id]) == {:ok, MapSet.new([segment_id])}
    end

    test "a copy that cannot be settled is reported and tried again next pass" do
      [a, b] = [start_broker(), start_broker()]
      {metadata, segment_id} = sealed_everywhere([a, b], ["x", "y"])
      {:ok, 2} = ReplicationServer.follow(b, segment_id, 2, records(["surplus"]))

      {source, apply} = metadata_store(metadata)
      parent = self()

      coordinator =
        start_coordinator(
          live_brokers: fn -> [a, b] end,
          metadata_source: source,
          apply_command: apply,
          replication_factor: 2,
          settle_copy: fn replica, segment, _base, _end ->
            send(parent, {:attempted, segment, replica})
            :error
          end
        )

      log = ExUnit.CaptureLog.capture_log(fn -> HealCoordinator.heal_now(coordinator) end)

      assert_received {:attempted, ^segment_id, ^b}
      assert log =~ "still divergent"

      # Level triggered: the copy is still reported by the integrity probe, so the next pass tries again.
      ExUnit.CaptureLog.capture_log(fn -> HealCoordinator.heal_now(coordinator) end)
      assert_received {:attempted, ^segment_id, ^b}
    end
  end
end
