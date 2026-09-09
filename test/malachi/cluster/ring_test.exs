defmodule Malachi.Cluster.RingTest do
  @moduledoc """
  The durable ring store's pure decider: the first-boot seed, the fenced compare-and-set that
  publishes a ring change, and the three-valued read boot depends on.

  The properties pin the two invariants the whole design rests on: the stored version only ever moves
  forward, and the stored fence never goes backwards, no matter what sequence of writers (including
  stale ones) reach the log.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.Ring
  alias Malachi.Cluster.RingTopology

  defp topology(vnodes, version \\ 0) do
    ring =
      Enum.reduce(vnodes, HashRing.new(), fn {id, token}, ring ->
        {:ok, ring} = HashRing.add_vnode(ring, id, token)
        ring
      end)

    placements = Map.new(vnodes, fn {id, _token} -> {id, [:"n1@127.0.0.1"]} end)
    %{RingTopology.new(ring, placements) | version: version}
  end

  describe "init" do
    test "seeds a store that has never held a ring" do
      seed = topology([{:vn_a, 0}])

      assert {state, :ok} = Ring.apply(Ring.new(), {:init, seed})
      assert Ring.topology(state) == {:ok, seed}
      assert Ring.version(state) == 0
    end

    test "refuses to overwrite an existing ring and hands back the one already stored" do
      seed = topology([{:vn_a, 0}])
      {seeded, :ok} = Ring.apply(Ring.new(), {:init, seed})
      other = topology([{:vn_b, 7}])

      assert {^seeded, {:error, {:exists, ^seed}}} = Ring.apply(seeded, {:init, other})
    end

    test "is auto-fenced, so concurrent first-boot seeds converge on exactly one winner" do
      first = topology([{:vn_a, 0}])
      second = topology([{:vn_b, 7}])

      {state, :ok} = Ring.apply(Ring.new(), {:init, first})
      {state, {:error, {:exists, stored}}} = Ring.apply(state, {:init, second})

      assert stored == first
      assert Ring.topology(state) == {:ok, first}
    end
  end

  describe "advance" do
    setup do
      seed = topology([{:vn_a, 0}])
      {seeded, :ok} = Ring.apply(Ring.new(), {:init, seed})
      %{seeded: seeded, seed: seed}
    end

    test "publishes a change that extends the version the writer read", %{seeded: seeded} do
      next = topology([{:vn_a, 0}, {:vn_b, 7}], 1)

      assert {state, :ok} = Ring.apply(seeded, {:advance, 0, 1, next})
      assert Ring.topology(state) == {:ok, next}
      assert state.fence == 1
    end

    test "refuses a writer that read a stale version", %{seeded: seeded} do
      {state, :ok} = Ring.apply(seeded, {:advance, 0, 1, topology([{:vn_a, 0}, {:vn_b, 7}], 1)})
      stale = topology([{:vn_a, 0}, {:vn_c, 9}], 1)

      assert {^state, {:error, {:conflict, returned}}} = Ring.apply(state, {:advance, 0, 1, stale})
      assert returned == state
    end

    test "refuses a writer carrying a fence older than one already seen", %{seeded: seeded} do
      {state, :ok} = Ring.apply(seeded, {:advance, 0, 4, topology([{:vn_a, 0}, {:vn_b, 7}], 1)})
      next = topology([{:vn_a, 0}, {:vn_b, 7}, {:vn_c, 9}], 2)

      assert {^state, {:error, {:conflict, _}}} = Ring.apply(state, {:advance, 1, 3, next})
    end

    test "accepts the same fence twice, because a renewing holder keeps its token", %{seeded: seeded} do
      {state, :ok} = Ring.apply(seeded, {:advance, 0, 2, topology([{:vn_a, 0}, {:vn_b, 7}], 1)})
      next = topology([{:vn_a, 0}, {:vn_b, 7}, {:vn_c, 9}], 2)

      assert {state, :ok} = Ring.apply(state, {:advance, 1, 2, next})
      assert state.fence == 2
    end

    test "refuses a topology whose version does not move forward", %{seeded: seeded} do
      sideways = topology([{:vn_a, 0}, {:vn_b, 7}], 0)

      assert {^seeded, {:error, {:conflict, _}}} = Ring.apply(seeded, {:advance, 0, 1, sideways})
    end

    test "refuses to advance a store that was never seeded" do
      assert {state, {:error, {:conflict, _}}} =
               Ring.apply(Ring.new(), {:advance, 0, 1, topology([{:vn_a, 0}], 1)})

      assert Ring.topology(state) == :none
    end
  end

  describe "topology/1 and version/1" do
    test "an unseeded store affirms it has never held a ring" do
      assert Ring.topology(Ring.new()) == :none
      assert Ring.version(Ring.new()) == nil
    end

    test "a seeded store answers with the stored topology and its version" do
      seed = topology([{:vn_a, 0}], 3)
      {state, :ok} = Ring.apply(Ring.new(), {:init, seed})

      assert Ring.topology(state) == {:ok, seed}
      assert Ring.version(state) == 3
    end
  end

  describe "properties" do
    property "the stored version never moves backwards, whatever sequence of writers reaches the log" do
      check all(commands <- StreamData.list_of(command(), max_length: 40), max_runs: 200) do
        {_state, versions} =
          Enum.reduce(commands, {Ring.new(), []}, fn command, {state, seen} ->
            {next, _reply} = Ring.apply(state, command)
            {next, [Ring.version(next) | seen]}
          end)

        recorded = versions |> Enum.reverse() |> Enum.reject(&is_nil/1)
        assert recorded == Enum.sort(recorded), "stored versions must be non-decreasing"
      end
    end

    property "the stored fence never moves backwards" do
      check all(commands <- StreamData.list_of(command(), max_length: 40), max_runs: 200) do
        {_state, fences} =
          Enum.reduce(commands, {Ring.new(), []}, fn command, {state, seen} ->
            {next, _reply} = Ring.apply(state, command)
            {next, [next.fence | seen]}
          end)

        recorded = Enum.reverse(fences)
        assert recorded == Enum.sort(recorded), "stored fences must be non-decreasing"
      end
    end

    property "a refused command leaves the state untouched" do
      check all(commands <- StreamData.list_of(command(), max_length: 40), max_runs: 200) do
        Enum.reduce(commands, Ring.new(), fn command, state ->
          case Ring.apply(state, command) do
            {next, :ok} -> next
            {next, {:error, _reason}} -> assert next == state && next
          end
        end)
      end
    end

    defp command do
      StreamData.one_of([
        StreamData.map(random_topology(), &{:init, &1}),
        StreamData.bind(random_topology(), fn topology ->
          StreamData.bind(StreamData.integer(0..4), fn expected ->
            StreamData.map(StreamData.integer(0..4), &{:advance, expected, &1, topology})
          end)
        end)
      ])
    end

    defp random_topology do
      StreamData.bind(StreamData.integer(0..5), fn version ->
        StreamData.map(
          StreamData.uniq_list_of(StreamData.integer(0..1_000), min_length: 1, max_length: 4),
          fn tokens -> topology(Enum.map(tokens, &{:"vn_#{&1}", &1}), version) end
        )
      end)
    end
  end
end
