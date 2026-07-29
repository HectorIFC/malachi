# Secondary-index benchmark: `ranges_of_topic`/`segments_of_range` served from the reverse index
# (`topic_ranges`/`range_segments`, V-idx-a/b) vs the old full scan (`Map.values |> Enum.filter`),
# on the SAME metadata, as the total number of ranges/segments grows.
#
# Each topic is fixed-size (3 ranges, 2 segments), so a topic's own set (k) is constant while the vnode's
# total (n) grows with the topic count. The point: the index lookup stays flat (O(k)) while the scan
# grows linearly (O(n)): the tax every produce/consume paid before, and why it mattered as a broker
# accumulates topics and (via retention) sealed segments.
#
# Run: mix run benchmark/metadata_index_bench.exs

defmodule MetadataIndexBench do
  alias Malachi.Metadata

  @sizes [100, 1_000, 10_000, 50_000]
  @iters 2_000

  # The old scan readers (pre V-idx-b), reimplemented here to compare on the same state.
  defp scan_ranges_of_topic(state, name), do: state.ranges |> Map.values() |> Enum.filter(&(&1.topic == name))
  defp scan_segments_of_range(state, range_id), do: state.segments |> Map.values() |> Enum.filter(&(&1.range_id == range_id))

  # A metadata with `n` fixed-size topics: each is created, its root split (→ sealed root + 2 active
  # children = 3 ranges), and one segment registered on each child (2 segments). Segment ids are
  # {topic, tag}, globally unique, no dynamic atoms.
  defp build(n) do
    Enum.reduce(1..n, Metadata.new(), fn i, state ->
      topic = "t#{i}"
      {state, {:ok, root}} = Metadata.apply(state, {:create_topic, topic, 4})
      {state, {:ok, left, right}} = Metadata.apply(state, {:split_range, root})
      {state, :ok} = Metadata.apply(state, {:register_segment, left, {topic, :a}, [:b1], 0})
      {state, :ok} = Metadata.apply(state, {:register_segment, right, {topic, :b}, [:b1], 0})
      state
    end)
  end

  defp avg_us(fun, iters) do
    {us, _} = :timer.tc(fn -> for _ <- 1..iters, do: fun.() end)
    Float.round(us / iters, 3)
  end

  defp row(label, n, total, idx_us, scan_us) do
    speedup = if idx_us > 0, do: Float.round(scan_us / idx_us, 1), else: :inf
    :io.format(~c"~-9s ~9B ~11B ~12.3f ~12.3f ~9wx~n", [label, n, total, idx_us, scan_us, speedup])
  end

  def run do
    IO.puts("secondary index vs scan: per-call latency (µs), averaged over #{@iters} calls\n")

    IO.puts("ranges_of_topic (one topic; k = 3 ranges)")
    :io.format(~c"~-9s ~9s ~11s ~12s ~12s ~9s~n", [~c"", ~c"topics", ~c"total_rng", ~c"index_us", ~c"scan_us", ~c"speedup"])

    for n <- @sizes do
      state = build(n)
      total_ranges = map_size(state.ranges)
      idx = avg_us(fn -> Metadata.ranges_of_topic(state, "t1") end, @iters)
      scan = avg_us(fn -> scan_ranges_of_topic(state, "t1") end, @iters)
      row("ranges", n, total_ranges, idx, scan)
    end

    IO.puts("\nsegments_of_range (one range; k = 1 segment)")
    :io.format(~c"~-9s ~9s ~11s ~12s ~12s ~9s~n", [~c"", ~c"topics", ~c"total_seg", ~c"index_us", ~c"scan_us", ~c"speedup"])

    for n <- @sizes do
      state = build(n)
      total_segments = map_size(state.segments)
      range_id = {"t1", 1}
      idx = avg_us(fn -> Metadata.segments_of_range(state, range_id) end, @iters)
      scan = avg_us(fn -> scan_segments_of_range(state, range_id) end, @iters)
      row("segments", n, total_segments, idx, scan)
    end

    IO.puts("\nindex latency is flat (O(k)); scan latency grows with the total (O(n)).")
  end
end

MetadataIndexBench.run()
