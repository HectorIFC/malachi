defmodule Malachi.Cluster.RingBootTest do
  @moduledoc """
  The boot precedence rule, one named case per row of the matrix. Getting a row wrong is exactly the
  failure this module exists to prevent, so each is asserted on its own rather than inferred from a
  broader end-to-end test.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  import ExUnit.CaptureLog

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.RingBoot
  alias Malachi.Cluster.RingTopology

  defp topology(count, version \\ 0) do
    ring =
      Enum.reduce(0..(count - 1)//1, HashRing.new(), fn index, ring ->
        {:ok, ring} = HashRing.add_vnode(ring, :"vn_#{index}", index * 1_000)
        ring
      end)

    placements = Map.new(HashRing.vnode_ids(ring), &{&1, [node()]})
    %{RingTopology.new(ring, placements) | version: version}
  end

  describe "resolve/2 precedence" do
    test "a recorded ring wins and is adopted as is" do
      durable = topology(6, 4)

      assert RingBoot.resolve({:ok, durable}, topology(6)) == {:durable, durable}
    end

    test "a recorded ring wins over a diverging environment, loudly" do
      durable = topology(6, 4)

      log = capture_log(fn -> assert {:durable, ^durable} = RingBoot.resolve({:ok, durable}, topology(4)) end)

      assert log =~ "durable ring"
      assert log =~ "version 4"
      assert log =~ "6 vnodes"
      assert log =~ "MALACHI_LOG_VNODES=4"
      assert log =~ "mix malachi.ring --show"
    end

    test "a recorded ring wins even when the environment switched sharding off entirely" do
      durable = topology(6, 4)

      log = capture_log(fn -> assert {:durable, ^durable} = RingBoot.resolve({:ok, durable}, nil) end)

      assert log =~ "MALACHI_LOG_VNODES=0",
             "sharding switched off after a reshard is the most dangerous divergence and must be named"
    end

    test "a matching environment stays quiet" do
      durable = topology(4, 2)

      log = capture_log(fn -> RingBoot.resolve({:ok, durable}, topology(4)) end)

      refute log =~ "ignored"
    end

    test "an affirmed empty store with a sharded environment seeds from the environment" do
      env = topology(8)

      assert RingBoot.resolve({:ok, :none}, env) == {:seed, env}
    end

    test "an affirmed empty store with an unsharded environment stays unsharded" do
      assert RingBoot.resolve({:ok, :none}, nil) == :unsharded
    end

    test "an unreadable store never falls back to the environment" do
      assert RingBoot.resolve({:error, :timeout}, topology(8)) == {:error, :timeout}
      assert RingBoot.resolve({:error, :noproc}, nil) == {:error, :noproc}
    end

    test "a recorded ring carrying a pending split is adopted intent and all" do
      durable = %{topology(4, 3) | pending: %{new_vnode: :vn_new, token: 500, nodes: [node()]}}

      assert {:durable, adopted} = RingBoot.resolve({:ok, durable}, topology(4))
      assert adopted.pending.new_vnode == :vn_new
    end
  end

  describe "read_until/2" do
    test "returns a recorded ring immediately, without retrying" do
      durable = topology(4)
      counter = :counters.new(1, [])

      read = fn ->
        :counters.add(counter, 1, 1)
        {:ok, durable}
      end

      assert RingBoot.read_until(read, sleep: fn _ms -> :ok end) == {:ok, durable}
      assert :counters.get(counter, 1) == 1
    end

    test "returns an affirmed empty store immediately, without retrying" do
      counter = :counters.new(1, [])

      read = fn ->
        :counters.add(counter, 1, 1)
        {:ok, :none}
      end

      assert RingBoot.read_until(read, sleep: fn _ms -> :ok end) == {:ok, :none}
      assert :counters.get(counter, 1) == 1
    end

    test "retries an unreachable store until it answers" do
      counter = :counters.new(1, [])

      read = fn ->
        :counters.add(counter, 1, 1)
        if :counters.get(counter, 1) < 3, do: {:error, :noproc}, else: {:ok, :none}
      end

      assert RingBoot.read_until(read, sleep: fn _ms -> :ok end) == {:ok, :none}
      assert :counters.get(counter, 1) == 3
    end

    test "gives up at the timeout and reports the last error" do
      elapsed = :counters.new(1, [])

      opts = [
        sleep: fn _ms -> :ok end,
        elapsed_ms: fn ->
          :counters.add(elapsed, 1, 400)
          :counters.get(elapsed, 1)
        end,
        timeout_ms: 1_000
      ]

      assert RingBoot.read_until(fn -> {:error, :timeout} end, opts) == {:error, :timeout}
    end
  end

  describe "confirm_seed/2" do
    test "keeps the seed this node planted" do
      seed = topology(8)

      assert RingBoot.confirm_seed(seed, fn ^seed -> :ok end) == {:ok, seed}
    end

    test "adopts the winner when another node seeded first" do
      seed = topology(8)
      # a different ring at a different version: with an identical one the assertion would pass even if
      # confirm_seed/2 handed back the seed instead of the topology the cluster actually agreed on
      winner = topology(6, 3)

      log =
        capture_log(fn ->
          assert RingBoot.confirm_seed(seed, fn _ -> {:error, {:exists, winner}} end) == {:ok, winner}
        end)

      assert log =~ "Another node seeded the ring first"
    end

    test "reports a seed that did not land, rather than serving it anyway" do
      assert RingBoot.confirm_seed(topology(8), fn _ -> {:error, :timeout} end) == {:error, :timeout}
    end
  end

  describe "vnode_count/1 and unreadable_message/2" do
    test "counts the vnodes on the ring, and zero when there is none" do
      assert RingBoot.vnode_count(topology(5)) == 5
      assert RingBoot.vnode_count(%RingTopology{}) == 0
    end

    test "a rejected seed write gets its own message, not the read-timeout one" do
      seeded = RingBoot.unseeded_message(:noproc)
      read = RingBoot.unreadable_message(:noproc, 60_000)

      assert seeded =~ ":noproc"
      assert seeded =~ "could not be recorded"

      refute seeded =~ "MALACHI_LOG_RING_BOOT_TIMEOUT_MS",
             "the boot timeout bounds the read; naming it after a rejected write sends the operator to the wrong knob"

      refute seeded == read
    end

    test "the refusal message names the reason and the knob that lengthens the wait" do
      message = RingBoot.unreadable_message(:timeout, 60_000)

      assert message =~ ":timeout"
      assert message =~ "60000"
      assert message =~ "MALACHI_LOG_RING_BOOT_TIMEOUT_MS"
    end
  end

  property "the environment never wins over a recorded ring, whatever the two describe" do
    check all(
            durable_count <- StreamData.integer(1..12),
            env_count <- StreamData.integer(1..12),
            version <- StreamData.integer(0..50),
            max_runs: 200
          ) do
      durable = topology(durable_count, version)

      capture_log(fn ->
        assert RingBoot.resolve({:ok, durable}, topology(env_count)) == {:durable, durable}
      end)
    end
  end
end
