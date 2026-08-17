defmodule Malachi.Loadtest.Histogram do
  @moduledoc """
  A lock-free latency histogram for the load generator: an `:atomics` array of log-spaced buckets that
  every connection process writes to concurrently, with no per-op allocation and no shared GenServer.

  Latencies are recorded in microseconds. Bucket `b` covers `[2^((b-1)/scale), 2^(b/scale))` us, so the
  resolution is `2^(1/scale) - 1` (about 4.4% at `scale = 16`), fine enough for tail percentiles while
  keeping the array tiny. Percentiles return the representative us of the bucket the rank falls in.
  """

  @buckets 1024
  # buckets per octave (per power of two)
  @scale 16

  @opaque t :: :atomics.atomics_ref()

  @spec new() :: t()
  def new, do: :atomics.new(@buckets, signed: false)

  @doc "Records one latency sample (microseconds)."
  @spec record(t(), number()) :: :ok
  def record(hist, us) do
    :atomics.add(hist, bucket(us), 1)
  end

  @doc "Total number of samples recorded."
  @spec count(t()) :: non_neg_integer()
  def count(hist), do: Enum.reduce(1..@buckets, 0, fn i, acc -> acc + :atomics.get(hist, i) end)

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

  # Bucket index for a latency; clamps to [1, @buckets]. us <= 0 lands in bucket 1.
  defp bucket(us) when us <= 0, do: 1
  defp bucket(us), do: min(@buckets, trunc(:math.log2(us) * @scale) + 1)

  # Representative microseconds for a bucket (its lower edge).
  defp bucket_us(b), do: Float.round(:math.pow(2, (b - 1) / @scale), 1)
end
