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

  alias Malachi.Application, as: App
  alias Malachi.Cluster.ClusterFlags
  alias Malachi.Cluster.ClusterFlagsCache
  alias Malachi.Cluster.ClusterFlagsServer

  @cap :batch_format
  @reconciler Malachi.LogClusterFlagsReconciler

  setup do
    published = ClusterFlagsCache.enabled()
    _ = Supervisor.terminate_child(Malachi.Supervisor, @reconciler)
    ClusterFlagsCache.forget()

    on_exit(fn ->
      ClusterFlagsCache.put(published)
      _ = Supervisor.restart_child(Malachi.Supervisor, @reconciler)
    end)

    :ok
  end

  # A gate test drives an injected store, so it must not form this node's real one: that would make the
  # boot evidence below pass because a test ran rather than because the node booted.
  defp no_start, do: fn -> {:ok, {:not_started, node()}} end

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

  describe "ensure_cluster_flags/2" do
    test "the application's own boot ran it: the store is formed on a running node" do
      # The suite boots the application once (test_helper.exs). The flag store is formed by
      # ensure_cluster_flags/2 inside start/2 and by nothing else, so a store that answers on a booted
      # node is the evidence that the gate ran, and ran before the supervision tree.
      assert {:ok, flags} = ClusterFlagsServer.read({Malachi.LogClusterFlags, node()}, :consistent)
      assert ClusterFlags.enabled(flags) == []
    end

    test "reads the store consistently and publishes before the supervision tree exists" do
      # The whole point of the gate being here: by the time it answers :ok, this node already knows
      # which flags are on. A tick that ran alongside the tree would let the acceptor open first.
      parent = self()

      read = fn mode ->
        send(parent, {:read, mode})
        {:ok, store([:batch_format])}
      end

      assert App.ensure_cluster_flags([node()], start: no_start(), read: read, advertised: [:batch_format]) == :ok
      assert_received {:read, :consistent}
      assert ClusterFlagsCache.enabled() == [:batch_format]
    end

    test "refuses with exit 78 when an enabled flag names a capability this build lacks" do
      parent = self()

      stderr =
        capture_io(:stderr, fn ->
          capture_log(fn ->
            App.ensure_cluster_flags([node()],
              start: no_start(),
              read: fn _mode -> {:ok, store([:batch_format])} end,
              advertised: [],
              halt_fun: &send(parent, {:halted, &1})
            )
          end)
        end)

      assert_received {:halted, 78}
      assert stderr =~ "REFUSING TO START (exit 78):"
      assert stderr =~ "batch_format"
    end

    test "retries an unreadable store, then raises rather than assuming no flag is on" do
      # A read that failed is not an answer. Treating it as one is what lets a node rolled back onto an
      # older build serve past a flag it cannot honour, which is the failure the gate exists to stop.
      {:ok, attempts} = Agent.start_link(fn -> 0 end)
      on_exit(fn -> if Process.alive?(attempts), do: Agent.stop(attempts) end)

      read = fn _mode ->
        Agent.update(attempts, &(&1 + 1))
        {:error, :noproc}
      end

      assert_raise RuntimeError, ~r/could not read the cluster flags/, fn ->
        App.ensure_cluster_flags([node()], start: no_start(), read: read, timeout_ms: 30, advertised: [])
      end

      assert Agent.get(attempts, & &1) > 1, "the gate gave up without retrying"
      assert ClusterFlagsCache.enabled() == []
    end

    test "a store that answers on a later attempt is adopted, not refused" do
      {:ok, attempts} = Agent.start_link(fn -> 0 end)
      on_exit(fn -> if Process.alive?(attempts), do: Agent.stop(attempts) end)

      read = fn _mode ->
        if Agent.get_and_update(attempts, &{&1 + 1, &1 + 1}) < 3, do: {:error, :noproc}, else: {:ok, store([])}
      end

      assert App.ensure_cluster_flags([node()], start: no_start(), read: read, timeout_ms: 5_000, advertised: []) == :ok
      assert ClusterFlagsCache.enabled() == []
    end

    test "the raise is not exit 78, because an unreachable store is worth restarting for" do
      # Exit 78 tells a service manager to stop restarting. That is right for a binary that cannot
      # honour a flag and wrong for a store that is not up yet.
      parent = self()

      assert_raise RuntimeError, fn ->
        App.ensure_cluster_flags([node()],
          start: no_start(),
          read: fn _mode -> {:error, :timeout} end,
          timeout_ms: 20,
          advertised: [],
          halt_fun: &send(parent, {:halted, &1})
        )
      end

      refute_received {:halted, _status}
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
