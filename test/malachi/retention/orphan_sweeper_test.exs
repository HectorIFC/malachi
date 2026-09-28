defmodule Malachi.Retention.OrphanSweeperTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Malachi.Cluster.ReplicationServer
  alias Malachi.I18n
  alias Malachi.Log.Record
  alias Malachi.Metadata
  alias Malachi.Retention.Orphans
  alias Malachi.Retention.OrphanSweeper
  alias Malachi.Storage.Layout
  alias Malachi.Test.UnknownMessages

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    directory = Path.join(tmp_dir, "data")
    File.mkdir_p!(directory)

    name = :"orphan_repl_#{System.unique_integer([:positive])}"
    start_supervised!({ReplicationServer, name: name, directory: directory}, id: name)

    %{directory: directory, replication: name}
  end

  defp start_sweeper(context, opts) do
    defaults = [
      authority: authority(Metadata.new()),
      local_ref: context.replication,
      directory: context.directory,
      on_result: fn _result -> :ok end,
      interval: 60_000,
      min_age_ms: 0,
      sightings: 1
    ]

    # `:default` for a seam drops it, so the module's own behaviour runs (as in the scrubber's tests).
    opts =
      defaults
      |> Keyword.merge(opts)
      |> Enum.reject(fn {_key, value} -> value == :default end)

    start_supervised!({OrphanSweeper, opts}, id: {:sweeper, System.unique_integer([:positive])})
  end

  # A directory that exists on disk and is not a segment any metadata lists.
  defp orphan!(directory, name) do
    path = Path.join(directory, name)
    File.mkdir_p!(path)
    File.write!(Path.join(path, "00000000000000000000.log"), "not a real segment")
    name
  end

  describe "a pass" do
    test "removes a directory no segment explains", context do
      orphan!(context.directory, "gone-r0-s1")
      sweeper = start_sweeper(context, [])

      assert %{removed: ["gone-r0-s1"], held: [], failed: []} = OrphanSweeper.sweep_now(sweeper)
      refute File.exists?(Path.join(context.directory, "gone-r0-s1"))
    end

    test "leaves a directory the metadata still lists", context do
      metadata = with_segment()
      [segment_id] = Map.keys(metadata.segments)
      name = Path.basename(Layout.segment_directory(context.directory, segment_id))
      orphan!(context.directory, name)

      sweeper = start_sweeper(context, authority: authority(metadata))

      assert %{removed: [], held: []} = OrphanSweeper.sweep_now(sweeper)
      assert File.exists?(Path.join(context.directory, name))
    end

    test "leaves the data directory's own files alone", context do
      File.write!(Path.join(context.directory, "malachi.format"), "format=1\n")
      File.mkdir_p!(Path.join(context.directory, "shard_0"))

      sweeper = start_sweeper(context, [])

      assert %{removed: [], held: []} = OrphanSweeper.sweep_now(sweeper)
      assert File.exists?(Path.join(context.directory, "malachi.format"))
      assert File.exists?(Path.join(context.directory, "shard_0"))
    end

    test "ignores a plain file at the root, which is never a segment directory", context do
      File.write!(Path.join(context.directory, "notes.txt"), "hello")
      sweeper = start_sweeper(context, [])

      assert %{scanned: 0, removed: []} = OrphanSweeper.sweep_now(sweeper)
      assert File.exists?(Path.join(context.directory, "notes.txt"))
    end
  end

  describe "the guards" do
    test "does nothing while an owning vnode does not answer", context do
      # A vnode that did not answer knows nothing the sweep can act on, and a partial answer is not an
      # answer: the directory of a segment registered on the silent vnode would look orphaned.
      orphan!(context.directory, "gone-r0-s1")
      sweeper = start_sweeper(context, authority: fn _names -> {:error, {:vnodes_unreachable, [:vnode_2]}} end)

      assert %{skipped: {:vnodes_unreachable, [:vnode_2]}, removed: [], scanned: 0} =
               OrphanSweeper.sweep_now(sweeper)

      assert File.exists?(Path.join(context.directory, "gone-r0-s1"))
    end

    test "does nothing when the topology changed while it asked", context do
      # The answers may have come from owners the ring no longer routes to.
      orphan!(context.directory, "gone-r0-s1")
      sweeper = start_sweeper(context, authority: fn _names -> {:error, {:topology_changed, 3, 4}} end)

      assert %{skipped: {:topology_changed, 3, 4}, removed: []} = OrphanSweeper.sweep_now(sweeper)
      assert File.exists?(Path.join(context.directory, "gone-r0-s1"))
    end

    test "does nothing without a ring to route by", context do
      orphan!(context.directory, "gone-r0-s1")
      sweeper = start_sweeper(context, authority: fn _names -> {:error, :no_topology} end)

      assert %{skipped: :no_topology, removed: []} = OrphanSweeper.sweep_now(sweeper)
      assert File.exists?(Path.join(context.directory, "gone-r0-s1"))
    end

    test "any other refusal from the authority skips the pass as unreachable", context do
      orphan!(context.directory, "gone-r0-s1")
      sweeper = start_sweeper(context, authority: fn _names -> {:error, :topology_unavailable} end)

      assert %{skipped: {:unreachable, :topology_unavailable}, removed: []} = OrphanSweeper.sweep_now(sweeper)
      assert File.exists?(Path.join(context.directory, "gone-r0-s1"))
    end

    test "keeps an undecided directory, and reports it held", context do
      # A segment id without a topic has no owner to route to, and a pending split may be moving the
      # topic between the two owners' reads. Undecided is not the same as unlisted.
      orphan!(context.directory, "gone-r0-s1")

      sweeper =
        start_sweeper(context, authority: fn names -> {:ok, %{known: MapSet.new(), undecided: MapSet.new(names)}} end)

      assert %{removed: [], held: ["gone-r0-s1"]} = OrphanSweeper.sweep_now(sweeper)
      assert %{removed: [], held: ["gone-r0-s1"]} = OrphanSweeper.sweep_now(sweeper)
      assert File.exists?(Path.join(context.directory, "gone-r0-s1"))
    end

    test "asks only about candidates, oldest first, at most max_tracked of them", context do
      for n <- 1..3, do: orphan!(context.directory, "gone-r0-s#{n}")
      test_pid = self()

      sweeper =
        start_sweeper(context,
          max_tracked: 2,
          authority: fn names ->
            send(test_pid, {:asked, names})
            {:ok, %{known: MapSet.new(), undecided: MapSet.new()}}
          end
        )

      assert %{removed: removed} = OrphanSweeper.sweep_now(sweeper)
      assert_received {:asked, asked}
      assert length(asked) == 2
      assert Enum.sort(removed) == Enum.sort(asked)
      # Exactly one question per pass: the answer the pass acts on is one answer.
      refute_received {:asked, _names}
    end

    test "says when candidates were left for a later pass", context do
      # The cap postpones the overflow, which only delays a removal; the line is what says it waits.
      for n <- 1..3, do: orphan!(context.directory, "gone-r0-s#{n}")
      sweeper = start_sweeper(context, max_tracked: 2)

      assert capture_log(fn -> OrphanSweeper.sweep_now(sweeper) end) =~
               I18n.t(:retention_orphan_tracking_capped, limit: 2)

      roomy = start_sweeper(context, max_tracked: 5)

      refute capture_log(fn -> OrphanSweeper.sweep_now(roomy) end) =~
               I18n.t(:retention_orphan_tracking_capped, limit: 5)
    end

    test "a disk with nothing old enough asks nobody", context do
      orphan!(context.directory, "gone-r0-s1")
      test_pid = self()

      sweeper =
        start_sweeper(context,
          min_age_ms: 60_000,
          authority: fn names ->
            send(test_pid, {:asked, names})
            {:error, :should_not_be_asked}
          end
        )

      assert %{skipped: nil, removed: []} = OrphanSweeper.sweep_now(sweeper)
      refute_received {:asked, _names}
    end

    test "does not even list in :off mode", context do
      orphan!(context.directory, "gone-r0-s1")
      sweeper = start_sweeper(context, mode: :off)

      assert %{skipped: :off, scanned: 0} = OrphanSweeper.sweep_now(sweeper)
      assert File.exists?(Path.join(context.directory, "gone-r0-s1"))
    end

    test "holds a candidate until it has been unexplained for the required passes", context do
      orphan!(context.directory, "gone-r0-s1")
      sweeper = start_sweeper(context, sightings: 2)

      assert %{removed: [], held: ["gone-r0-s1"]} = OrphanSweeper.sweep_now(sweeper)
      assert File.exists?(Path.join(context.directory, "gone-r0-s1"))

      assert %{removed: ["gone-r0-s1"]} = OrphanSweeper.sweep_now(sweeper)
      refute File.exists?(Path.join(context.directory, "gone-r0-s1"))
    end

    test "holds a directory younger than the minimum age", context do
      orphan!(context.directory, "gone-r0-s1")
      sweeper = start_sweeper(context, min_age_ms: 60_000)

      assert %{removed: [], held: [], scanned: 1} = OrphanSweeper.sweep_now(sweeper)
      assert File.exists?(Path.join(context.directory, "gone-r0-s1"))
    end

    test "removes at most max_per_pass in one pass", context do
      for n <- 1..4, do: orphan!(context.directory, "gone-r0-s#{n}")
      sweeper = start_sweeper(context, max_per_pass: 2)

      assert %{removed: removed, held: held} = OrphanSweeper.sweep_now(sweeper)
      assert length(removed) == 2
      assert length(held) == 2
    end

    test ":report says exactly what :delete would have taken, and takes nothing", context do
      orphan!(context.directory, "gone-r0-s1")
      sweeper = start_sweeper(context, mode: :report)

      assert %{removed: [], held: ["gone-r0-s1"]} = OrphanSweeper.sweep_now(sweeper)
      assert File.exists?(Path.join(context.directory, "gone-r0-s1"))
      assert OrphanSweeper.mode(sweeper) == :report
    end

    test "a bound that cannot be a bound falls back to the default instead of taking the node down", context do
      sweeper = start_sweeper(context, sightings: 0, mode: :sometimes)

      # `:report`, not the `:delete` default: the fallback for a mode is the side that removes nothing.
      # The environment never reaches here with a bad value anyway, since
      # `Malachi.Config.retention_orphan_sweep/1` refuses one at boot.
      assert OrphanSweeper.mode(sweeper) == :report
      orphan!(context.directory, "gone-r0-s1")
      # The default of two sightings applies, so one pass is not enough either way.
      assert %{removed: []} = OrphanSweeper.sweep_now(sweeper)
    end
  end

  describe "removal" do
    test "closes an open log before dropping its directory", context do
      segment_id = {{"t", 0}, 1}
      {:ok, _last} = ReplicationServer.follow(context.replication, segment_id, 0, [Record.new("value", key: "k")])
      name = Path.basename(Layout.segment_directory(context.directory, segment_id))
      assert File.exists?(Path.join(context.directory, name))

      sweeper = start_sweeper(context, [])

      assert %{removed: [^name]} = OrphanSweeper.sweep_now(sweeper)
      refute File.exists?(Path.join(context.directory, name))
      # The server forgot it too, so a later read is a miss rather than a handle to deleted files.
      assert ReplicationServer.read(context.replication, segment_id, 0, 10) == :eof
    end

    test "a removal that fails is reported and retried on the next pass", context do
      orphan!(context.directory, "gone-r0-s1")
      sweeper = start_sweeper(context, local_ref: {:no_such_replication_server, node()})

      assert %{removed: [], failed: [{"gone-r0-s1", :unreachable}]} = OrphanSweeper.sweep_now(sweeper)
      assert File.exists?(Path.join(context.directory, "gone-r0-s1"))
    end

    test "counts what it reclaimed", context do
      orphan!(context.directory, "gone-r0-s1")
      handler = "orphan-removed-#{System.unique_integer([:positive])}"
      test_pid = self()

      :telemetry.attach(
        handler,
        [:malachi, :retention, :orphan_removed],
        fn _event, measurements, _metadata, _config -> send(test_pid, {handler, measurements}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      sweeper = start_sweeper(context, [])
      assert %{removed: ["gone-r0-s1"]} = OrphanSweeper.sweep_now(sweeper)
      assert_receive {^handler, %{count: 1}}
    end
  end

  describe "what it reports by default" do
    test "the default result handler logs what it reclaimed", context do
      orphan!(context.directory, "gone-r0-s1")
      # `:default` drops the quiet seam the other tests inject, so the module's own handler runs.
      sweeper = start_sweeper(context, on_result: :default)

      log = capture_log(fn -> assert %{removed: ["gone-r0-s1"]} = OrphanSweeper.sweep_now(sweeper) end)

      assert log =~ "reclaimed 1 replica directories"
      assert log =~ "gone-r0-s1"
    end

    test "the default result handler says which removals failed", context do
      orphan!(context.directory, "gone-r0-s1")

      sweeper =
        start_sweeper(context, on_result: :default, local_ref: {:no_such_replication_server, node()})

      log = capture_log(fn -> assert %{failed: [_ | _]} = OrphanSweeper.sweep_now(sweeper) end)

      assert log =~ "could not remove"
    end

    test "a quiet pass says nothing", context do
      sweeper = start_sweeper(context, on_result: :default)

      # Asserted by absence of this handler's own lines rather than of all output: the suite is async,
      # so `capture_log` also sees whatever another test logged in the same window.
      log = capture_log(fn -> assert %{removed: [], failed: []} = OrphanSweeper.sweep_now(sweeper) end)

      refute log =~ "reclaimed"
      refute log =~ "could not remove"
    end
  end

  test "a pass runs on the tick, not only when asked", context do
    orphan!(context.directory, "gone-r0-s1")
    start_sweeper(context, interval: 10)

    assert eventually(fn -> not File.exists?(Path.join(context.directory, "gone-r0-s1")) end)
  end

  test "the line about an unanswered pass is logged once, and again after an answer", context do
    orphan!(context.directory, "gone-r0-s1")
    answer = :counters.new(1, [])
    silent = {:error, {:vnodes_unreachable, [:vnode_2]}}
    empty = {:ok, %{known: MapSet.new(), undecided: MapSet.new()}}

    sweeper =
      start_sweeper(context,
        on_result: :default,
        sightings: 5,
        authority: fn _names -> if :counters.get(answer, 1) == 0, do: silent, else: empty end
      )

    holding =
      I18n.t(:retention_orphan_authority_unavailable,
        directory: context.directory,
        reason: inspect({:vnodes_unreachable, [:vnode_2]})
      )

    assert capture_log(fn -> OrphanSweeper.sweep_now(sweeper) end) =~ holding

    # A node whose owners stay silent would otherwise say the same thing every interval.
    refute capture_log(fn -> OrphanSweeper.sweep_now(sweeper) end) =~ holding

    :counters.put(answer, 1, 1)
    OrphanSweeper.sweep_now(sweeper)
    :counters.put(answer, 1, 0)
    assert capture_log(fn -> OrphanSweeper.sweep_now(sweeper) end) =~ holding
  end

  test "an authority that cannot answer skips the pass instead of taking the sweeper down", context do
    # Measured on the reshard drill: a sharded control plane comes back from a full-cluster restart
    # healthy but unable to serve metadata (#136), and the scrubber died once per tick for as long as
    # that lasted. A worker that cannot read the state authorizing it to act must not act, and must
    # not die either: the next pass asks again.
    orphan!(context.directory, "gone-r0-s1")
    gone = spawn(fn -> :ok end)
    ref = Process.monitor(gone)
    assert_receive {:DOWN, ^ref, :process, ^gone, _reason}

    sweeper = start_sweeper(context, authority: fn _names -> GenServer.call(gone, :topology) end)

    assert %{skipped: {:unreachable, _reason}, removed: []} = OrphanSweeper.sweep_now(sweeper)
    assert Process.alive?(sweeper)
    assert File.exists?(Path.join(context.directory, "gone-r0-s1"))
  end

  test "an answer that never comes ends the pass at the deadline, and the sweeper lives on", context do
    # ra follows a leader redirect with a fresh timeout, so during an election no single read times out
    # while the pass keeps waiting. The deadline is on the whole question.
    orphan!(context.directory, "gone-r0-s1")
    sweeper = start_sweeper(context, authority_deadline_ms: 100, authority: fn _names -> Process.sleep(:infinity) end)

    {elapsed_us, result} = :timer.tc(fn -> OrphanSweeper.sweep_now(sweeper) end)

    assert %{skipped: {:unreachable, :deadline}, removed: []} = result
    assert elapsed_us < 2_000_000, "the pass waited #{div(elapsed_us, 1000)}ms past a 100ms deadline"
    assert Process.alive?(sweeper)
    assert File.exists?(Path.join(context.directory, "gone-r0-s1"))
  end

  test "an authority that raises skips the pass instead of taking the sweeper down", context do
    orphan!(context.directory, "gone-r0-s1")
    sweeper = start_sweeper(context, authority: fn _names -> raise "the control plane answered nonsense" end)

    capture_log(fn ->
      assert %{skipped: {:unreachable, _reason}, removed: []} = OrphanSweeper.sweep_now(sweeper)
    end)

    assert Process.alive?(sweeper)
    assert File.exists?(Path.join(context.directory, "gone-r0-s1"))
  end

  test "the directories held as undecided are named when that set changes, not every pass", context do
    orphan!(context.directory, "gone-r0-s1")
    orphan!(context.directory, "gone-r0-s2")
    held = :counters.new(1, [])

    sweeper =
      start_sweeper(context,
        authority: fn names ->
          undecided = if :counters.get(held, 1) == 0, do: ["gone-r0-s1"], else: names
          {:ok, %{known: MapSet.new(), undecided: MapSet.new(undecided)}}
        end,
        sightings: 5
      )

    line = &I18n.t(:retention_orphan_undecided, count: length(&1), directories: inspect(&1))

    assert capture_log(fn -> OrphanSweeper.sweep_now(sweeper) end) =~ line.(["gone-r0-s1"])
    refute capture_log(fn -> OrphanSweeper.sweep_now(sweeper) end) =~ "gone-r0-s1"

    :counters.put(held, 1, 1)
    assert capture_log(fn -> OrphanSweeper.sweep_now(sweeper) end) =~ line.(["gone-r0-s1", "gone-r0-s2"])
  end

  test "a data directory it cannot read is reported, not guessed at", context do
    not_a_directory = Path.join(context.directory, "a_file")
    File.write!(not_a_directory, "")
    sweeper = start_sweeper(context, directory: not_a_directory)

    # A missing directory is a node that has not written a segment yet, which is not an error. One that
    # cannot be listed is: guessing would mean treating every directory as absent.
    assert %{skipped: {:unreadable, :enotdir}, removed: []} = OrphanSweeper.sweep_now(sweeper)
  end

  test "a missing data directory has swept nothing, rather than failed", context do
    sweeper = start_sweeper(context, directory: Path.join(context.directory, "not_yet"))

    assert %{scanned: 0, removed: [], skipped: nil} = OrphanSweeper.sweep_now(sweeper)
  end

  test "a replication server reference resolved per pass follows a restart", context do
    orphan!(context.directory, "gone-r0-s1")
    # The single-node shape passes a function, because the broker owns an unnamed replication server
    # whose pid a restart replaces (the same reasoning as in `Malachi.Cluster.Scrubber`).
    sweeper = start_sweeper(context, local_ref: fn -> context.replication end)

    assert %{removed: ["gone-r0-s1"]} = OrphanSweeper.sweep_now(sweeper)
  end

  test "an unknown cast, info message or call is counted and survived", context do
    sweeper = start_sweeper(context, [])

    UnknownMessages.assert_survives_unknown(sweeper, :orphan_sweeper, fn ->
      OrphanSweeper.mode(sweeper)
    end)
  end

  defp eventually(check, remaining_ms \\ 2_000) do
    cond do
      check.() -> true
      remaining_ms <= 0 -> false
      true -> Process.sleep(10) && eventually(check, remaining_ms - 10)
    end
  end

  # What the single-node wiring asks: the broker's own metadata, which is the truth when there is no
  # control plane. The clustered authorities are exercised against real vnodes in the ra tests.
  defp authority(%Metadata{} = metadata) do
    &Orphans.explain(&1, fn ids ->
      {:ok, %{known: Orphans.known_among(metadata.segments, ids), unroutable: [], migrating: [], misplaced: []}}
    end)
  end

  defp with_segment do
    base = elem(Metadata.apply(Metadata.new(), {:create_topic, "t", 4}), 0)
    elem(Metadata.apply(base, {:register_segment, {"t", 0}, {{"t", 0}, 1}, [:b1], 0}), 0)
  end
end
