defmodule Malachi.Cluster.DSRSMPropertyTest do
  @moduledoc """
  Model-based property tests for the DS-RSM: random sequences of topic operations and
  **vnode splits**, asserting that no topic's metadata is ever orphaned (it always lives in
  the vnode it routes to), per-vnode coverage holds, and the whole thing is deterministic.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Malachi.Cluster.DSRSM
  alias Malachi.Metadata
  alias Malachi.Test.KeyspaceHelper

  @ring_bits 5
  @keyspace_bits 4
  @topic_pool ["a", "b", "c", "d"]

  property "a topic always lives in the vnode it routes to (no orphans), through vnode splits" do
    check all(ops <- StreamData.list_of(op(), max_length: 40), max_runs: 200) do
      dsrsm = run(ops)

      assert no_orphans?(dsrsm), "every topic must live in the vnode that routes to it"
      assert all_topics_retrievable?(dsrsm), "every stored topic must be retrievable via the DS-RSM"
      assert per_vnode_coverage?(dsrsm), "each active topic's ranges must tile the keyspace"
    end
  end

  property "the DS-RSM is deterministic (same ops -> same state)" do
    check all(ops <- StreamData.list_of(op(), max_length: 40), max_runs: 100) do
      assert run(ops) == run(ops)
    end
  end

  # --- op generator ---

  defp op do
    StreamData.one_of([
      StreamData.tuple({StreamData.constant(:create_topic), StreamData.member_of(@topic_pool)}),
      StreamData.tuple(
        {StreamData.constant(:split_range), StreamData.member_of(@topic_pool), StreamData.positive_integer()}
      ),
      StreamData.tuple({StreamData.constant(:split_vnode), StreamData.integer(0..(ring_size() - 1))})
    ])
  end

  # --- interpreter ---

  defp run(ops) do
    {:ok, dsrsm} = DSRSM.new(ring_bits: @ring_bits) |> DSRSM.add_vnode({:v, 0}, 0)
    Enum.reduce(ops, dsrsm, &apply_op(&2, &1))
  end

  defp apply_op(dsrsm, {:create_topic, name}) do
    {dsrsm, _reply} = DSRSM.command(dsrsm, name, {:create_topic, name, @keyspace_bits})
    dsrsm
  end

  defp apply_op(dsrsm, {:split_range, name, picker}) do
    case pick_active_range(dsrsm, name, picker) do
      nil -> dsrsm
      range_id -> dsrsm |> DSRSM.command(name, {:split_range, range_id}) |> elem(0)
    end
  end

  defp apply_op(dsrsm, {:split_vnode, token}) do
    # a fresh, deterministic vnode id derived from the token; no-op if the token is taken
    case DSRSM.split_vnode(dsrsm, {:v, token}, token) do
      {:ok, dsrsm} -> dsrsm
      {:error, _reason} -> dsrsm
    end
  end

  defp pick_active_range(dsrsm, name, picker) do
    case DSRSM.active_ranges_of_topic(dsrsm, name) do
      [] -> nil
      ranges -> ranges |> Enum.sort_by(& &1.id) |> Enum.at(rem(picker, length(ranges))) |> Map.fetch!(:id)
    end
  end

  # --- invariants ---

  # Every topic stored in a vnode's shard must route to that vnode.
  defp no_orphans?(dsrsm) do
    Enum.all?(dsrsm.vnodes, fn {vnode_id, metadata} ->
      Enum.all?(Map.keys(metadata.topics), fn name ->
        DSRSM.vnode_for(dsrsm, name) == {:ok, vnode_id}
      end)
    end)
  end

  defp all_topics_retrievable?(dsrsm) do
    dsrsm.vnodes
    |> Enum.flat_map(fn {_id, metadata} -> Map.keys(metadata.topics) end)
    |> Enum.all?(fn name -> DSRSM.get_topic(dsrsm, name) != nil end)
  end

  defp per_vnode_coverage?(dsrsm) do
    Enum.all?(dsrsm.vnodes, fn {_id, metadata} ->
      Enum.all?(metadata.topics, fn {name, topic} ->
        topic.state == :sealed or
          metadata
          |> Metadata.active_ranges_of_topic(name)
          |> Enum.sort_by(& &1.key_start)
          |> KeyspaceHelper.tiles?(0, topic.keyspace_size)
      end)
    end)
  end

  defp ring_size, do: Bitwise.bsl(1, @ring_bits)
end
