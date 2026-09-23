defmodule Malachi.Retention.OrphanSweeperTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Malachi.Cluster.ReplicationServer
  alias Malachi.Log.Record
  alias Malachi.Metadata
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
      metadata_source: fn -> Metadata.new() end,
      metadata_ready?: fn -> true end,
      unreachable_vnodes: fn -> [] end,
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

      sweeper = start_sweeper(context, metadata_source: fn -> metadata end)

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
    test "does nothing at all while the metadata is not ready", context do
      orphan!(context.directory, "gone-r0-s1")
      sweeper = start_sweeper(context, metadata_ready?: fn -> false end)

      assert %{skipped: :metadata_not_ready, removed: [], scanned: 0} = OrphanSweeper.sweep_now(sweeper)
      assert File.exists?(Path.join(context.directory, "gone-r0-s1"))
    end

    test "does nothing while a vnode did not answer the last refresh", context do
      # The guard that readiness cannot give. A vnode that goes silent AFTER being read keeps the view
      # it had, so its old segments stay explained while ones registered on it since are missing from
      # the merge. Their replicas still land here over the data plane, which is a different channel
      # from the ra query this node cannot make, so without this the sweep deletes a live copy once
      # the silence outlasts the minimum age.
      orphan!(context.directory, "gone-r0-s1")
      sweeper = start_sweeper(context, unreachable_vnodes: fn -> [:vnode_2] end)

      assert %{skipped: {:vnodes_unreachable, [:vnode_2]}, removed: [], scanned: 0} =
               OrphanSweeper.sweep_now(sweeper)

      assert File.exists?(Path.join(context.directory, "gone-r0-s1"))
    end

    test "a broker that cannot say which vnodes answered skips the pass", context do
      orphan!(context.directory, "gone-r0-s1")
      gone = spawn(fn -> :ok end)
      ref = Process.monitor(gone)
      assert_receive {:DOWN, ^ref, :process, ^gone, _reason}

      sweeper = start_sweeper(context, unreachable_vnodes: fn -> GenServer.call(gone, :unreachable) end)

      assert %{skipped: {:unreachable, _reason}, removed: []} = OrphanSweeper.sweep_now(sweeper)
      assert Process.alive?(sweeper)
      assert File.exists?(Path.join(context.directory, "gone-r0-s1"))
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

  test "the line about waiting for metadata is logged once, not every pass", context do
    sweeper = start_sweeper(context, on_result: :default, metadata_ready?: fn -> false end)

    first = capture_log(fn -> OrphanSweeper.sweep_now(sweeper) end)
    assert first =~ "waiting for metadata"

    # A node that boots with a silent vnode would otherwise say the same thing every interval for as
    # long as that vnode stays silent.
    refute capture_log(fn -> OrphanSweeper.sweep_now(sweeper) end) =~ "waiting for metadata"
  end

  test "a broker that cannot answer skips the pass instead of taking the sweeper down", context do
    # Measured on the reshard drill: a sharded control plane comes back from a full-cluster restart
    # healthy but unable to serve metadata (#136), and the scrubber died once per tick for as long as
    # that lasted. A worker that cannot read the state authorizing it to act must not act, and must
    # not die either: the next pass asks again.
    orphan!(context.directory, "gone-r0-s1")
    gone = spawn(fn -> :ok end)
    ref = Process.monitor(gone)
    assert_receive {:DOWN, ^ref, :process, ^gone, _reason}

    sweeper = start_sweeper(context, metadata_ready?: fn -> GenServer.call(gone, :metadata_ready?) end)

    assert %{skipped: {:unreachable, _reason}, removed: []} = OrphanSweeper.sweep_now(sweeper)
    assert Process.alive?(sweeper)
    assert File.exists?(Path.join(context.directory, "gone-r0-s1"))
  end

  test "a broker that answers ready and then stops answering also skips the pass", context do
    orphan!(context.directory, "gone-r0-s1")
    gone = spawn(fn -> :ok end)
    ref = Process.monitor(gone)
    assert_receive {:DOWN, ^ref, :process, ^gone, _reason}

    sweeper = start_sweeper(context, metadata_source: fn -> GenServer.call(gone, :metadata) end)

    assert %{skipped: {:unreachable, _reason}, removed: []} = OrphanSweeper.sweep_now(sweeper)
    assert Process.alive?(sweeper)
    assert File.exists?(Path.join(context.directory, "gone-r0-s1"))
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

  defp with_segment do
    base = elem(Metadata.apply(Metadata.new(), {:create_topic, "t", 4}), 0)
    elem(Metadata.apply(base, {:register_segment, {"t", 0}, {{"t", 0}, 1}, [:b1], 0}), 0)
  end
end
