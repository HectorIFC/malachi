defmodule Malachi.Cluster.ClusterFlagsGateTest do
  @moduledoc """
  The local flag pass: the cache, the adoption step and the refusal.

  Every seam is injected, so nothing here reaches `ra` and nothing takes the VM down: `:halt_fun`
  records the status the way `Malachi.Storage.FormatMarker`'s gate is tested, which is the same
  refusal surface underneath.

  The cache these tests drive is the running application's own, so the suite's reconciler is paused for
  their duration and the value it had is put back afterwards. Without that, its tick refreshes the cache
  from the real store between an injected write and the assertion that reads it, and the teardown leaves
  the application with a cache it never validated at boot.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO
  import ExUnit.CaptureLog

  alias Malachi.Cluster.ClusterFlags
  alias Malachi.Cluster.ClusterFlagsCache

  @cap :batch_format
  @reconciler Malachi.LogClusterFlagsReconciler

  setup do
    # `enabled/0` answers `[]` both for a cache nobody has read and for one that was read and holds
    # nothing, so restoring that list alone would turn the first into the second and make a later
    # readiness check pass for a node whose store never answered. The read state is snapshotted too.
    restore = snapshot()
    _ = Supervisor.terminate_child(Malachi.Supervisor, @reconciler)
    ClusterFlagsCache.forget()

    on_exit(fn ->
      restore.()
      _ = Supervisor.restart_child(Malachi.Supervisor, @reconciler)
    end)

    :ok
  end

  # What the cache held, as a function that puts it back exactly, unread included.
  defp snapshot do
    if ClusterFlagsCache.read?() do
      published = ClusterFlagsCache.enabled()
      fn -> ClusterFlagsCache.put(published) end
    else
      &ClusterFlagsCache.forget/0
    end
  end

  defp store(flags) do
    Enum.reduce(flags, ClusterFlags.new(), fn flag, state ->
      state |> ClusterFlags.apply({:enable_flag, flag}) |> elem(0)
    end)
  end

  # Records which read mode it was asked for, so the "first consistent, then local" rule is observable.
  defp recording_read(flags, modes) do
    fn mode ->
      Agent.update(modes, &[mode | &1])
      {:ok, store(flags)}
    end
  end

  defp modes_agent do
    {:ok, modes} = Agent.start_link(fn -> [] end)
    on_exit(fn -> if Process.alive?(modes), do: Agent.stop(modes) end)
    modes
  end

  defp seen(modes), do: modes |> Agent.get(& &1) |> Enum.reverse()

  describe "read?/0" do
    test "a node that has not read the store is not ready" do
      refute ClusterFlagsCache.read?()
      assert ClusterFlagsCache.enabled() == []
    end

    test "reading the store, even an empty one, makes the node ready" do
      # `[]` from enabled/0 has to mean two different things and be told apart: nothing is on, and
      # nothing has been asked yet. Publishing the empty answer is what marks the node as having asked.
      ClusterFlagsCache.refresh(read: fn _mode -> {:ok, store([])} end, advertised: [])

      assert ClusterFlagsCache.read?()
      assert ClusterFlagsCache.enabled() == []
    end

    test "a store it cannot read leaves the node not ready, and it keeps serving nothing" do
      # This is the case a rolling upgrade spends its first minutes in: the flag store cannot reach a
      # quorum until a second node runs the new build. The node has to come up and join anyway, because
      # its own Raft server is what makes that quorum possible, so it stays out of rotation instead.
      assert ClusterFlagsCache.refresh(read: fn _mode -> {:error, :noproc} end, advertised: []) == :ok

      refute ClusterFlagsCache.read?()
    end

    test "a node stays ready once it has read, even if a later read fails" do
      ClusterFlagsCache.refresh(read: fn _mode -> {:ok, store([@cap])} end, advertised: [@cap])
      ClusterFlagsCache.refresh(read: fn _mode -> {:error, :timeout} end, advertised: [@cap])

      assert ClusterFlagsCache.read?()
      assert ClusterFlagsCache.enabled() == [@cap]
    end
  end

  describe "before the store has been read" do
    test "nothing is enabled" do
      assert ClusterFlagsCache.enabled() == []
      refute ClusterFlagsCache.enabled?(@cap)
    end
  end

  describe "refresh/1" do
    test "publishes what the store says and answers it on the hot path" do
      assert ClusterFlagsCache.refresh(read: fn _mode -> {:ok, store([@cap])} end, advertised: [@cap]) == :ok

      assert ClusterFlagsCache.enabled() == [@cap]
      assert ClusterFlagsCache.enabled?(@cap)
      refute ClusterFlagsCache.enabled?(:something_else)
    end

    test "the first read is consistent and every read after it is local" do
      modes = modes_agent()
      read = recording_read([], modes)

      ClusterFlagsCache.refresh(read: read, advertised: [])
      ClusterFlagsCache.refresh(read: read, advertised: [])
      ClusterFlagsCache.refresh(read: read, advertised: [])

      # The first answer decides whether this node may serve at all, so it cannot come from a replica
      # that has not caught up. After that the cache is allowed to lag.
      assert seen(modes) == [:consistent, :local, :local]
    end

    test "a store it cannot read neither refuses nor publishes, and the next read is consistent again" do
      modes = modes_agent()

      assert ClusterFlagsCache.refresh(
               read: fn mode ->
                 Agent.update(modes, &[mode | &1])
                 {:error, :noproc}
               end,
               advertised: [],
               halt_fun: fn status -> flunk("halted with #{status} on a store it could not read") end
             ) == :ok

      assert ClusterFlagsCache.enabled() == []
      ClusterFlagsCache.refresh(read: recording_read([], modes), advertised: [])

      # "I could not ask" is not "no flag is on": the node stays unread and asks consistently again.
      assert seen(modes) == [:consistent, :consistent]
    end

    test "writes to persistent_term only when the value changed" do
      # persistent_term.put triggers a global garbage-collection scan of every process in the VM, so a
      # tick that changes nothing must not write. Writing unconditionally would cost every node that
      # scan twice a minute forever, and no behavioural assertion would notice.
      assert ClusterFlagsCache.put([]) == :published
      assert ClusterFlagsCache.put([]) == :unchanged
      assert ClusterFlagsCache.put([@cap]) == :published
      assert ClusterFlagsCache.put([@cap]) == :unchanged
      assert ClusterFlagsCache.put([@cap, :compaction]) == :published
    end

    test "an unread cache and an empty published one are told apart" do
      # Both answer `[]` to a reader, and they mean different things: one has never asked the store, the
      # other has and was told nothing is on. Publishing the empty list is what marks the node as read.
      assert ClusterFlagsCache.enabled() == []
      assert ClusterFlagsCache.put([]) == :published
      assert ClusterFlagsCache.enabled() == []
      assert ClusterFlagsCache.put([]) == :unchanged
    end

    test "a tick that changes nothing does not rewrite the cache" do
      read = fn _mode -> {:ok, store([@cap])} end
      ClusterFlagsCache.refresh(read: read, advertised: [@cap])

      Enum.each(1..5, fn _ -> ClusterFlagsCache.refresh(read: read, advertised: [@cap]) end)

      assert ClusterFlagsCache.put([@cap]) == :unchanged
      assert ClusterFlagsCache.enabled() == [@cap]
    end

    test "a lagging local read never turns a published flag off" do
      # The first read is consistent and goes to the leader; a later local read comes from this node's
      # own replica, which can be behind it. Taking the newer answer at face value would flip a flag
      # from on to off here, which is the one thing the replicated set cannot do.
      ClusterFlagsCache.refresh(read: fn _mode -> {:ok, store([@cap])} end, advertised: [@cap])
      assert ClusterFlagsCache.enabled?(@cap)

      ClusterFlagsCache.refresh(read: fn _mode -> {:ok, store([])} end, advertised: [@cap])

      assert ClusterFlagsCache.enabled?(@cap)
      assert ClusterFlagsCache.enabled() == [@cap]
    end

    test "a lagging read still picks up a flag the node had not seen" do
      ClusterFlagsCache.refresh(read: fn _mode -> {:ok, store([@cap])} end, advertised: [@cap, :compaction])

      ClusterFlagsCache.refresh(read: fn _mode -> {:ok, store([:compaction])} end, advertised: [@cap, :compaction])

      assert ClusterFlagsCache.enabled() == [@cap, :compaction]
    end

    test "a new flag reaches the cache" do
      ClusterFlagsCache.refresh(read: fn _mode -> {:ok, store([@cap])} end, advertised: [@cap, :compaction])

      ClusterFlagsCache.refresh(
        read: fn _mode -> {:ok, store([@cap, :compaction])} end,
        advertised: [@cap, :compaction]
      )

      assert ClusterFlagsCache.enabled() == [@cap, :compaction]
    end
  end

  describe "adoption" do
    test "the local side effect runs before the flag is published" do
      parent = self()

      adopt = fn flag ->
        # #207 raises the data-directory format marker here, and the marker has to be up before anything
        # believes the feature is on. Reading the cache from inside the side effect is how that order is
        # pinned.
        send(parent, {:adopting, flag, ClusterFlagsCache.enabled()})
      end

      capture_log(fn ->
        ClusterFlagsCache.refresh(read: fn _mode -> {:ok, store([@cap])} end, advertised: [@cap], adopt: adopt)
      end)

      assert_received {:adopting, @cap, []}
      assert ClusterFlagsCache.enabled() == [@cap]
    end

    test "a flag is adopted once, not on every tick" do
      parent = self()
      read = fn _mode -> {:ok, store([@cap])} end
      adopt = fn flag -> send(parent, {:adopted, flag}) end

      capture_log(fn ->
        Enum.each(1..3, fn _ -> ClusterFlagsCache.refresh(read: read, advertised: [@cap], adopt: adopt) end)
      end)

      assert_received {:adopted, @cap}
      refute_received {:adopted, @cap}
    end

    test "a side effect that raises publishes nothing, so the next tick retries it" do
      read = fn _mode -> {:ok, store([@cap])} end

      assert_raise RuntimeError, "marker not written", fn ->
        ClusterFlagsCache.refresh(
          read: read,
          advertised: [@cap],
          adopt: fn _flag -> raise "marker not written" end
        )
      end

      assert ClusterFlagsCache.enabled() == []

      parent = self()

      capture_log(fn ->
        ClusterFlagsCache.refresh(read: read, advertised: [@cap], adopt: fn flag -> send(parent, {:retried, flag}) end)
      end)

      assert_received {:retried, @cap}
      assert ClusterFlagsCache.enabled() == [@cap]
    end

    test "logs the adoption through I18n" do
      log =
        capture_log(fn ->
          ClusterFlagsCache.refresh(read: fn _mode -> {:ok, store([@cap])} end, advertised: [@cap])
        end)

      assert log =~ Malachi.I18n.t(:cluster_flag_adopted, flag: @cap)
    end
  end

  describe "the gate" do
    test "refuses to serve when an enabled flag names a capability this build lacks" do
      parent = self()

      stderr =
        capture_io(:stderr, fn ->
          capture_log(fn ->
            ClusterFlagsCache.refresh(
              read: fn _mode -> {:ok, store([@cap])} end,
              advertised: [],
              halt_fun: &send(parent, {:halted, &1})
            )
          end)
        end)

      assert_received {:halted, 78}
      assert stderr =~ "REFUSING TO START (exit 78):"
      assert stderr =~ "batch_format"
      # The build's own capability list belongs in the message: it is how an operator sees the gap
      # rather than only the demand.
      assert stderr =~ "[]"
    end

    test "names every flag it cannot honour, not just the first" do
      parent = self()

      stderr =
        capture_io(:stderr, fn ->
          capture_log(fn ->
            ClusterFlagsCache.refresh(
              read: fn _mode -> {:ok, store([@cap, :compaction])} end,
              advertised: [],
              halt_fun: &send(parent, {:halted, &1})
            )
          end)
        end)

      assert_received {:halted, 78}
      assert stderr =~ "batch_format"
      assert stderr =~ "compaction"
    end

    test "refuses before adopting or publishing anything" do
      parent = self()

      capture_io(:stderr, fn ->
        capture_log(fn ->
          ClusterFlagsCache.refresh(
            read: fn _mode -> {:ok, store([@cap, :compaction])} end,
            advertised: [:compaction],
            adopt: fn flag -> send(parent, {:adopted, flag}) end,
            halt_fun: &send(parent, {:halted, &1})
          )
        end)
      end)

      assert_received {:halted, 78}
      refute_received {:adopted, _flag}
      assert ClusterFlagsCache.enabled() == []
    end

    test "does not refuse when this build advertises more than the cluster has enabled" do
      parent = self()

      capture_log(fn ->
        ClusterFlagsCache.refresh(
          read: fn _mode -> {:ok, store([@cap])} end,
          advertised: [@cap, :compaction],
          halt_fun: fn status -> send(parent, {:halted, status}) end
        )
      end)

      refute_received {:halted, _status}
      assert ClusterFlagsCache.enabled() == [@cap]
    end
  end
end
