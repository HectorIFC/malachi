defmodule Malachi.Histogram do
  @moduledoc """
  A lock-free latency histogram: an `:atomics` array of log-spaced buckets that many processes write to
  concurrently, with no per-op allocation and no shared GenServer. The load generator records request
  latency in it, and `Malachi.Metrics` records storage flush latency in it.

  Latencies are recorded in microseconds. Bucket `b` covers `(2^((b-1)/scale), 2^(b/scale)]` us, so the
  resolution is `2^(1/scale) - 1` (about 4.4% at `scale = 16`), fine enough for tail percentiles while
  keeping the array tiny. Percentiles return the representative us of the bucket the rank falls in.

  Writers pay two `:atomics.add/3` calls on the caller (the bucket and the running sum), which is why this
  is safe on a hot path: routing the samples through a GenServer would serialize every writer behind one
  process instead.

  `cumulative/1` exports the counts at a coarser, fixed set of edges (`edges/0`, four per octave), which is
  what a Prometheus histogram needs: a stable `le` set whose counts can be subtracted between two scrapes.
  Every edge is the upper bound of an internal bucket, so those counts are exact rather than interpolated.
  A bucket includes its upper bound, so a count is of samples at or below its edge, which is what a
  Prometheus `le` means; the only integer samples that can equal an edge are exact powers of two.
  """

  import Bitwise

  @buckets 1024
  # buckets per octave (per power of two)
  @scale 16
  # The slot after the buckets holds the running sum of the recorded microseconds.
  @sum_slot @buckets + 1
  # :atomics refuses an increment wider than its 64-bit word, so one sample adds at most this much to the
  # sum (about 12.7 days); a sample that large is already a stuck disk, not a latency.
  @max_sum_sample 1 <<< 40
  # The exported edges: 2^(k/4) us for k in 12..96, 8us to about 16.8s, one every four internal buckets.
  @edge_step div(@scale, 4)
  @edge_range 12..96

  @opaque t :: :atomics.atomics_ref()

  @spec new() :: t()
  def new, do: :atomics.new(@sum_slot, signed: false)

  @doc "Records one latency sample (microseconds)."
  @spec record(t(), number()) :: :ok
  def record(hist, us) do
    :atomics.add(hist, bucket(us), 1)
    :atomics.add(hist, @sum_slot, sum_sample(us))
  end

  @doc "Total number of samples recorded."
  @spec count(t()) :: non_neg_integer()
  def count(hist), do: Enum.reduce(1..@buckets, 0, fn i, acc -> acc + :atomics.get(hist, i) end)

  @doc "Sum of every recorded sample, in microseconds (non-positive samples add 0, fractions truncate)."
  @spec sum(t()) :: non_neg_integer()
  def sum(hist), do: :atomics.get(hist, @sum_slot)

  @doc "The exported edges in microseconds, ascending: four per octave from 8us to about 16.8s."
  @spec edges() :: [float()]
  def edges, do: Enum.map(@edge_range, &edge_us/1)

  @doc """
  The number of samples at or below each of `edges/0`, as `[{edge_us, count}]` in ascending order,
  and the total count, all read in one pass so they agree with each other while writers keep adding (the
  total is never below the last edge's count). Samples above the last edge are only in the total.
  """
  @spec cumulative(t()) :: {[{float(), non_neg_integer()}], non_neg_integer()}
  def cumulative(hist) do
    {pairs, total} =
      Enum.reduce(1..@buckets, {[], 0}, fn b, {pairs, cum} ->
        cum = cum + :atomics.get(hist, b)
        {edge_pair(b, cum, pairs), cum}
      end)

    {Enum.reverse(pairs), total}
  end

  defp edge_pair(b, cum, pairs) do
    k = div(b, @edge_step)

    if rem(b, @edge_step) == 0 and k in @edge_range do
      [{edge_us(k), cum} | pairs]
    else
      pairs
    end
  end

  @doc """
  The `p`-th percentile in microseconds (`p` in 0..100), or `0.0` if empty. Walks the cumulative counts
  and returns the representative us of the bucket where the rank lands.
  """
  @spec percentile(t(), number()) :: float()
  def percentile(hist, p) do
    total = count(hist)

    if total == 0 do
      0.0
    else
      target = max(1, round(p / 100 * total))
      find_bucket(hist, target, 1, 0)
    end
  end

  defp find_bucket(_hist, _target, i, _cum) when i > @buckets, do: bucket_us(@buckets)

  defp find_bucket(hist, target, i, cum) do
    cum = cum + :atomics.get(hist, i)
    if cum >= target, do: bucket_us(i), else: find_bucket(hist, target, i + 1, cum)
  end

  # Bucket index for a latency; clamps to [1, @buckets]. Anything up to 1us (zero, negative, or a fraction,
  # whose log is not positive) lands in bucket 1. Rounding up puts a sample equal to an upper bound in the
  # bucket that bound closes, so an exported `le` counts it.
  defp bucket(us) when us <= 1, do: 1
  defp bucket(us), do: min(@buckets, ceil(:math.log2(us) * @scale))

  defp sum_sample(us) when us <= 0, do: 0
  defp sum_sample(us), do: min(trunc(us), @max_sum_sample)

  # Representative microseconds for a bucket (its lower edge).
  defp bucket_us(b), do: Float.round(:math.pow(2, (b - 1) / @scale), 1)

  # The upper edge of internal bucket k * @edge_step.
  defp edge_us(k), do: :math.pow(2, k / 4)
end
