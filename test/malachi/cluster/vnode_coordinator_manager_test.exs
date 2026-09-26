defmodule Malachi.Cluster.VnodeCoordinatorManagerTest do
  use ExUnit.Case, async: true

  alias Malachi.Cluster.VnodeCoordinatorManager, as: Manager
  alias Malachi.Test.UnknownMessages

  # Starts a manager whose placement read comes from an Agent (so a test can change what the cluster
  # looks like, or make the read fail, between reconciles) and whose spawn/stop report to the test as
  # {:spawn, vnode_id, pid} / {:stop, pid}. spawn returns a real, monitorable pid (the manager monitors
  # it); stop kills it. The interval defaults long so the scheduled tick never fires mid-test and
  # reconcile_now/1 drives it; the death tests pass a short interval so the tick drives the respawn.
  #
  # The Agent holds the seam's answer whole ({:ok, ids} or {:error, reason}), and here the placement IS
  # the list of led vnode ids: these tests are about the manager's reconcile, and the derivation from a
  # real placement to led vnodes is exercised against production seams in vnode_coordinator_test.exs.
  defp start_manager(leading_agent, opts \\ []) do
    test_pid = self()

    # The fake coordinators below are unlinked sleepers, so nothing reaps them when the test ends and
    # they would pile up in the VM across runs. Collect every spawned pid (post-death replacements
    # included) and kill them in on_exit. The collector is unlinked so it outlives the manager (which
    # is linked to the test pid and so already dead by the time on_exit runs).
    {:ok, spawned} = Agent.start(fn -> [] end)

    on_exit(fn ->
      spawned |> Agent.get(& &1) |> Enum.each(&Process.exit(&1, :kill))
      Agent.stop(spawned)
    end)

    {:ok, manager} =
      Manager.start_link(
        placement: fn -> Agent.get(leading_agent, & &1) end,
        leading: fn placement -> placement end,
        spawn: fn vnode_id ->
          pid = spawn(fn -> Process.sleep(:infinity) end)
          Agent.update(spawned, &[pid | &1])
          send(test_pid, {:spawn, vnode_id, pid})
          pid
        end,
        stop: fn pid ->
          send(test_pid, {:stop, pid})
          Process.exit(pid, :kill)
        end,
        interval: Keyword.get(opts, :interval, 60_000)
      )

    manager
  end

  test "starts a coordinator handle for each led vnode on startup" do
    {:ok, leading} = Agent.start_link(fn -> {:ok, [:a, :b]} end)
    manager = start_manager(leading)

    assert_receive {:spawn, :a, _}
    assert_receive {:spawn, :b, _}
    assert Enum.sort(Manager.reconcile_now(manager)) == [:a, :b]
  end

  test "stops vnodes it no longer leads and starts newly-led ones, leaving unchanged ones alone" do
    {:ok, leading} = Agent.start_link(fn -> {:ok, [:a, :b]} end)
    manager = start_manager(leading)
    assert_receive {:spawn, :a, pid_a}
    assert_receive {:spawn, :b, pid_b}

    Agent.update(leading, fn _ -> {:ok, [:b, :c]} end)
    assert Enum.sort(Manager.reconcile_now(manager)) == [:b, :c]

    assert_receive {:stop, ^pid_a}
    assert_receive {:spawn, :c, _}
    # b was already running and is still led: it must not be stopped or respawned
    refute_receive {:stop, ^pid_b}
    refute_receive {:spawn, :b, _}
  end

  test "is idempotent while leadership is stable" do
    {:ok, leading} = Agent.start_link(fn -> {:ok, [:a]} end)
    manager = start_manager(leading)
    assert_receive {:spawn, :a, _}

    assert Manager.reconcile_now(manager) == [:a]
    refute_receive {:spawn, :a, _}
    refute_receive {:stop, _}
  end

  test "stops every coordinator once this node leads no vnodes" do
    {:ok, leading} = Agent.start_link(fn -> {:ok, [:a, :b]} end)
    manager = start_manager(leading)
    assert_receive {:spawn, :a, pid_a}
    assert_receive {:spawn, :b, pid_b}

    Agent.update(leading, fn _ -> {:ok, []} end)
    assert Manager.reconcile_now(manager) == []
    assert_receive {:stop, ^pid_a}
    assert_receive {:stop, ^pid_b}
  end

  @tag :capture_log
  test "restarts a vnode whose coordinator tree dies while this node still leads it" do
    {:ok, leading} = Agent.start_link(fn -> {:ok, [:a]} end)
    manager = start_manager(leading, interval: 50)
    assert_receive {:spawn, :a, pid1}

    # The coordinator tree dies on its own (its supervisor exhausted its restart intensity), not via a
    # deliberate stop. The manager must notice and start a fresh one while :a is still led. On main
    # nothing monitors the handle, so no replacement is ever spawned and this assertion times out.
    Process.exit(pid1, :kill)

    assert_receive {:spawn, :a, pid2}, 1_000
    assert pid2 != pid1
    assert Manager.reconcile_now(manager) == [:a]
  end

  test "reconciles on a topology_changed nudge without waiting for the tick" do
    {:ok, leading} = Agent.start_link(fn -> {:ok, []} end)
    manager = start_manager(leading)
    refute_receive {:spawn, _, _}, 100

    # The interval is a minute out, so only the nudge can explain a coordinator starting here.
    Agent.update(leading, fn _ -> {:ok, [:a]} end)
    :ok = Manager.topology_changed(manager)

    assert_receive {:spawn, :a, _}, 1_000
  end

  test "an unknown cast, info message or call is counted and survived" do
    {:ok, leading} = Agent.start_link(fn -> {:ok, [:a]} end)
    manager = start_manager(leading)
    assert_receive {:spawn, :a, _}

    UnknownMessages.assert_survives_unknown(manager, :vnode_coordinator, fn ->
      assert Manager.reconcile_now(manager) == [:a]
    end)
  end

  test "a placement that flaps leaves no orphaned handle behind" do
    {:ok, leading} = Agent.start_link(fn -> {:ok, [:a]} end)
    manager = start_manager(leading)
    assert_receive {:spawn, :a, pid1}

    Agent.update(leading, fn _ -> {:ok, []} end)
    assert Manager.reconcile_now(manager) == []
    assert_receive {:stop, ^pid1}

    Agent.update(leading, fn _ -> {:ok, [:a]} end)
    assert Manager.reconcile_now(manager) == [:a]
    assert_receive {:spawn, :a, pid2}
    assert pid2 != pid1

    # the handle the manager holds is the live one, so the next stop reaches pid2 and not the dead pid1
    Agent.update(leading, fn _ -> {:ok, []} end)
    assert Manager.reconcile_now(manager) == []
    assert_receive {:stop, ^pid2}
    refute_receive {:stop, ^pid1}
  end

  test "a DOWN for something it never spawned is ignored" do
    {:ok, leading} = Agent.start_link(fn -> {:ok, [:a]} end)
    manager = start_manager(leading)
    assert_receive {:spawn, :a, pid}

    # e.g. a monitor set by a seam, or a stale DOWN: it names no running vnode, so nothing is dropped
    send(manager, {:DOWN, make_ref(), :process, self(), :normal})

    assert Manager.reconcile_now(manager) == [:a]
    refute_receive {:spawn, :a, _}
    assert Process.alive?(pid)
  end

  test "a deliberate stop does not trigger a respawn" do
    {:ok, leading} = Agent.start_link(fn -> {:ok, [:a]} end)
    manager = start_manager(leading, interval: 50)
    assert_receive {:spawn, :a, pid1}

    # Stop :a deliberately by dropping leadership. The stop kills pid1, but because stop_vnodes
    # demonitors first, that death must not be delivered as a :DOWN and respawn :a.
    Agent.update(leading, fn _ -> {:ok, []} end)
    assert Manager.reconcile_now(manager) == []
    assert_receive {:stop, ^pid1}

    refute_receive {:spawn, :a, _}, 300
  end
end

defmodule Malachi.Cluster.VnodeCoordinatorManagerVersionTest do
  # async: false: the stuck member is produced with the node-wide machine version pin.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Malachi.Cluster.MetadataMachine
  alias Malachi.Cluster.VnodeCoordinatorManager, as: Manager
  alias Malachi.Test.StuckRaMember

  defp start_manager(version_servers) do
    {:ok, manager} =
      Manager.start_link(
        placement: fn -> {:ok, []} end,
        leading: fn _placement -> [] end,
        spawn: fn _vnode_id -> spawn(fn -> :ok end) end,
        stop: fn _pid -> :ok end,
        version_servers: fn _placement -> version_servers.() end,
        interval: 60_000
      )

    manager
  end

  test "watches no member unless told to" do
    {:ok, manager} =
      Manager.start_link(
        placement: fn -> {:ok, []} end,
        leading: fn _placement -> [] end,
        spawn: fn _ -> self() end,
        stop: fn _ -> :ok end,
        interval: 60_000
      )

    assert Manager.reconcile_now(manager) == []
    assert Manager.version_status(manager) == %{}
  end

  test "keeps a status per local vnode member, logs a stuck one once, and forgets members no longer listed" do
    name = :"vcm_stuck_#{System.unique_integer([:positive])}"
    on_exit(fn -> StuckRaMember.cleanup({name, node()}) end)
    server_id = StuckRaMember.start(name)
    ghost = {:"vcm_ghost_#{System.unique_integer([:positive])}", node()}
    stuck = {:stuck, StuckRaMember.effective_version(), StuckRaMember.rolled_back_version()}
    {:ok, listed} = Agent.start_link(fn -> [{MetadataMachine, server_id}, {MetadataMachine, ghost}] end)

    log =
      capture_log(fn ->
        manager = start_manager(fn -> Agent.get(listed, & &1) end)
        Manager.reconcile_now(manager)
        Manager.reconcile_now(manager)

        assert Manager.version_status(manager) == %{server_id => stuck, ghost => :ok}

        Agent.update(listed, fn _ -> [{MetadataMachine, ghost}] end)
        Manager.reconcile_now(manager)
        assert Manager.version_status(manager) == %{ghost => :ok}
      end)

    assert length(Regex.scan(~r/stopped applying entries/, log)) == 1
  end
end

defmodule Malachi.Cluster.VnodeCoordinatorManagerPlacementTest do
  @moduledoc """
  What the manager does when it cannot read the placement. The rule is that it stays exactly as the
  last good read left it: treating an unreadable placement as "this node leads nothing" would stop
  every coordinator on the node over a transient read error.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Malachi.Cluster.MetadataMachine
  alias Malachi.Cluster.VnodeCoordinatorManager, as: Manager

  defp start_manager(placement) do
    test_pid = self()
    {:ok, spawned} = Agent.start(fn -> [] end)

    on_exit(fn ->
      spawned |> Agent.get(& &1) |> Enum.each(&Process.exit(&1, :kill))
      Agent.stop(spawned)
    end)

    {:ok, manager} =
      Manager.start_link(
        placement: placement,
        leading: fn placement -> placement end,
        version_servers: fn placement -> Enum.map(placement, &{MetadataMachine, {&1, node()}}) end,
        spawn: fn vnode_id ->
          pid = spawn(fn -> Process.sleep(:infinity) end)
          Agent.update(spawned, &[pid | &1])
          send(test_pid, {:spawn, vnode_id, pid})
          pid
        end,
        stop: fn pid ->
          send(test_pid, {:stop, pid})
          Process.exit(pid, :kill)
        end,
        interval: 60_000
      )

    manager
  end

  defp agent_placement(initial) do
    {:ok, agent} = Agent.start_link(fn -> initial end)
    {agent, fn -> Agent.get(agent, & &1) end}
  end

  test "an empty placement is not an error: it stops the coordinators, and says the read was fine" do
    {agent, placement} = agent_placement({:ok, [:a]})
    manager = start_manager(placement)
    assert_receive {:spawn, :a, pid}

    Agent.update(agent, fn _ -> {:ok, []} end)
    assert Manager.reconcile_now(manager) == []
    assert_receive {:stop, ^pid}
    assert Manager.placement_status(manager) == :ok
  end

  test "an unreadable placement keeps the running coordinators and the version statuses" do
    {agent, placement} = agent_placement({:ok, [:a, :b]})
    manager = start_manager(placement)
    assert_receive {:spawn, :a, _}
    assert_receive {:spawn, :b, _}
    before = Manager.version_status(manager)

    capture_log(fn ->
      Agent.update(agent, fn _ -> {:error, :membership_down} end)
      assert Enum.sort(Manager.reconcile_now(manager)) == [:a, :b]
    end)

    refute_receive {:stop, _}
    refute_receive {:spawn, _, _}
    assert Manager.version_status(manager) == before
    assert Manager.placement_status(manager) == :unreadable
  end

  test "logs the unreadable placement once, and logs again only when it recovers" do
    {agent, placement} = agent_placement({:ok, [:a]})
    manager = start_manager(placement)
    assert_receive {:spawn, :a, _}

    log =
      capture_log(fn ->
        Agent.update(agent, fn _ -> {:error, :membership_down} end)
        Manager.reconcile_now(manager)
        Manager.reconcile_now(manager)
        Manager.reconcile_now(manager)
      end)

    assert length(Regex.scan(~r/could not be read/, log)) == 1
    assert log =~ ":membership_down"

    recovery =
      capture_log(fn ->
        Agent.update(agent, fn _ -> {:ok, [:a]} end)
        assert Manager.reconcile_now(manager) == [:a]
        Manager.reconcile_now(manager)
      end)

    assert length(Regex.scan(~r/readable again/, recovery)) == 1
    assert Manager.placement_status(manager) == :ok
    # the coordinator that was already running is not restarted on the way back
    refute_receive {:spawn, :a, _}
  end

  # The first reconcile runs in handle_continue, which is asynchronous to start_link returning: the
  # capture has to wrap the start AND wait for that pass, or it can close before the log is written.
  # `placement_status/1` is a call, so it cannot be served until the continue has finished.
  defp start_and_capture(placement) do
    parent = self()

    log =
      capture_log(fn ->
        manager = start_manager(placement)
        send(parent, {:manager, manager, Manager.placement_status(manager)})
      end)

    assert_receive {:manager, manager, status}
    {manager, status, log}
  end

  test "a placement seam that raises is inert, not a crash that takes the coordinator tree down" do
    {manager, status, log} = start_and_capture(fn -> raise "membership exploded" end)

    assert log =~ "could not be read"
    assert log =~ "membership exploded"
    assert Process.alive?(manager)
    assert status == :unreadable
    assert Manager.reconcile_now(manager) == []
  end

  test "a placement seam that exits is inert too" do
    {manager, status, log} = start_and_capture(fn -> exit(:noproc) end)

    assert log =~ "could not be read"
    assert Process.alive?(manager)
    assert status == :unreadable
  end

  test "a placement seam that answers a shape the manager does not know is inert" do
    {manager, status, log} = start_and_capture(fn -> :surprise end)

    assert log =~ "unexpected_placement"
    assert Process.alive?(manager)
    assert status == :unreadable
  end

  test "the led vnodes and the watched members are derived from one and the same read" do
    test_pid = self()
    {:ok, reads} = Agent.start_link(fn -> 0 end)

    placement = fn ->
      n = Agent.get_and_update(reads, &{&1, &1 + 1})
      {:ok, [:"vn_#{n}"]}
    end

    {:ok, manager} =
      Manager.start_link(
        placement: placement,
        leading: fn placement -> send(test_pid, {:leading, placement}) && [] end,
        version_servers: fn placement -> send(test_pid, {:version_servers, placement}) && [] end,
        spawn: fn _ -> spawn(fn -> :ok end) end,
        stop: fn _ -> :ok end,
        interval: 60_000
      )

    # One read per reconcile: a second read could answer a different ring, and the two derivations would
    # then disagree about which vnodes exist within the same pass.
    assert_receive {:leading, first}
    assert_receive {:version_servers, ^first}
    assert Manager.reconcile_now(manager) == []
    assert_receive {:leading, second}
    assert_receive {:version_servers, ^second}
    assert first != second
  end

  describe "telemetry" do
    setup do
      handler = "vnode-reconcile-#{System.unique_integer([:positive])}"
      test_pid = self()

      :telemetry.attach(
        handler,
        [:malachi, :cluster, :vnode_reconcile],
        fn _event, measurements, metadata, _config -> send(test_pid, {:reconciled, measurements, metadata}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)
      :ok
    end

    test "reports what the pass saw and changed" do
      {agent, placement} = agent_placement({:ok, [:a, :b]})
      manager = start_manager(placement)

      assert_receive {:reconciled, %{ring: 2, hosted: 2, leading: 2, started: 2, stopped: 0}, %{placement: :ok}}

      Agent.update(agent, fn _ -> {:ok, [:b]} end)
      Manager.reconcile_now(manager)
      assert_receive {:reconciled, %{ring: 1, hosted: 1, leading: 1, started: 0, stopped: 1}, %{placement: :ok}}
    end

    test "reports an unreadable placement on every pass, with nothing started or stopped" do
      {agent, placement} = agent_placement({:ok, [:a]})
      manager = start_manager(placement)
      assert_receive {:reconciled, _measurements, %{placement: :ok}}

      capture_log(fn ->
        Agent.update(agent, fn _ -> {:error, :membership_down} end)
        Manager.reconcile_now(manager)
        Manager.reconcile_now(manager)
      end)

      # the set still running from the last good read is what it reports while it cannot read
      assert_receive {:reconciled, %{ring: 0, leading: 1, started: 0, stopped: 0}, %{placement: :unreadable}}
      assert_receive {:reconciled, %{ring: 0, leading: 1, started: 0, stopped: 0}, %{placement: :unreadable}}
    end
  end
end
