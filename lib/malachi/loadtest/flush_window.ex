defmodule Malachi.Loadtest.FlushWindow do
  @moduledoc """
  The server's group-commit flush latency over one measured window, from two scrapes of `/metrics`.

  `malachi_storage_flush_duration_seconds` is a cumulative histogram: its buckets count every flush since
  the node booted, including the setup, prepopulate and warmup the benchmark harnesses do before the
  window they measure. A quantile of one scrape describes all of that. Subtracting a scrape taken when
  the window opens from one taken when it closes leaves only the flushes in between, and a quantile of
  that difference describes the window. Buckets from several nodes add up the same way, which is how a
  cluster benchmark gets one distribution for all of its nodes.

  Quantiles use the interpolation Prometheus's `histogram_quantile` uses: find the bucket the rank falls
  in and interpolate linearly inside it, with 0 as the lower bound of the first bucket and the highest
  finite edge as the answer when the rank lands in `+Inf`. With four buckets per octave, the answer is
  within 19% of the flush it stands for.

  Pure: text in, maps out. `mix malachi.loadtest.ceiling flush-window` is the command-line side, which
  `scripts/loadtest-ceiling.sh` and `benchmark/docker-cluster.sh` call.
  """

  @histogram "malachi_storage_flush_duration_seconds"
  @bytes "malachi_storage_flushed_bytes_total"
  @records "malachi_storage_flushed_records_total"
  @quantiles [p50: 0.5, p99: 0.99, p999: 0.999]

  @typedoc """
  The flush series of one scrape, or the difference of two. `buckets` are `{le, count}` in exposition
  order, with `le` kept as the text the server printed so two scrapes are compared exactly.
  """
  @type snapshot :: %{
          buckets: [{String.t(), non_neg_integer()}],
          count: non_neg_integer(),
          sum: float(),
          bytes: non_neg_integer(),
          records: non_neg_integer()
        }

  @type error :: :series_missing | :counter_reset | :bucket_mismatch

  @typedoc "One window, in seconds. The latencies are `nil` when the window saw no flush."
  @type summary :: %{
          p50: float() | nil,
          p99: float() | nil,
          p999: float() | nil,
          mean: float() | nil,
          flushes: non_neg_integer(),
          bytes: non_neg_integer(),
          records: non_neg_integer()
        }

  @doc """
  Reads the flush series out of a Prometheus text exposition. Every one of them has to be there (the
  finite buckets, `+Inf`, `_sum`, `_count` and the two totals), or the answer is `:series_missing`: a
  server that predates them, or a response that is not the exposition at all.
  """
  @spec parse(String.t()) :: {:ok, snapshot()} | {:error, error()}
  def parse(text) when is_binary(text) do
    samples =
      for line <- String.split(text, "\n"),
          sample = sample(String.trim_trailing(line, "\r")),
          sample != nil,
          do: sample

    buckets = for {:bucket, le, value} <- samples, do: {le, value}

    with false <- :malformed in samples,
         {:ok, count} <- single(samples, :count),
         {:ok, sum} <- single(samples, :sum),
         {:ok, bytes} <- single(samples, :bytes),
         {:ok, records} <- single(samples, :records),
         {:ok, buckets} <- finite_then_inf(buckets),
         true <- Enum.all?([count, bytes, records | Enum.map(buckets, &elem(&1, 1))], &is_integer/1) do
      {:ok, %{buckets: buckets, count: count, sum: sum / 1, bytes: bytes, records: records}}
    else
      _missing_or_malformed -> {:error, :series_missing}
    end
  end

  @doc """
  The flushes between two scrapes of the same node. `:counter_reset` when any series went down, which
  means the node restarted in between and the difference describes nothing; `:bucket_mismatch` when the
  two scrapes do not have the same edges.
  """
  @spec diff(snapshot(), snapshot()) :: {:ok, snapshot()} | {:error, error()}
  def diff(before, later) do
    with :ok <- same_edges(before, later) do
      delta = combine(later, before, &-/2)

      if Enum.any?(
           [delta.count, delta.sum, delta.bytes, delta.records | Enum.map(delta.buckets, &elem(&1, 1))],
           &(&1 < 0)
         ) do
        {:error, :counter_reset}
      else
        {:ok, delta}
      end
    end
  end

  @doc "Adds windows from several nodes into one. `:bucket_mismatch` when their edges differ."
  @spec merge([snapshot(), ...]) :: {:ok, snapshot()} | {:error, error()}
  def merge([first | rest]) do
    Enum.reduce_while(rest, {:ok, first}, fn window, {:ok, acc} ->
      case same_edges(acc, window) do
        :ok -> {:cont, {:ok, combine(acc, window, &+/2)}}
        error -> {:halt, error}
      end
    end)
  end

  @doc "The quantiles, mean and totals of a window, in seconds."
  @spec summarize(snapshot()) :: summary()
  def summarize(%{count: 0} = window) do
    %{p50: nil, p99: nil, p999: nil, mean: nil, flushes: 0, bytes: window.bytes, records: window.records}
  end

  def summarize(window) do
    @quantiles
    |> Map.new(fn {key, q} -> {key, quantile(window.buckets, q)} end)
    |> Map.merge(%{
      mean: window.sum / window.count,
      flushes: window.count,
      bytes: window.bytes,
      records: window.records
    })
  end

  @doc "What each error means, in the words a benchmark result records."
  @spec describe(error()) :: String.t()
  def describe(:series_missing), do: "the scrape has no flush latency series"
  def describe(:counter_reset), do: "a flush counter went backwards between the scrapes (the node restarted)"
  def describe(:bucket_mismatch), do: "the scrapes have different histogram buckets"

  # --- parsing ---

  # A line of one of the flush series becomes a sample, or `:malformed` when it cannot be read, which
  # fails the whole scrape rather than quietly dropping a bucket. Any other line is ignored.
  defp sample(line) do
    case String.split(line, " ") do
      [name, value] -> classify(series(name), number(value))
      _other -> if flush_series?(line), do: :malformed
    end
  end

  defp classify(nil, _value), do: nil
  defp classify(_series, nil), do: :malformed
  defp classify(:malformed, _value), do: :malformed
  defp classify({:bucket, le}, value), do: {:bucket, le, value}
  defp classify(key, value), do: {key, value}

  defp series(@histogram <> "_count"), do: :count
  defp series(@histogram <> "_sum"), do: :sum
  defp series(@bytes), do: :bytes
  defp series(@records), do: :records

  defp series(@histogram <> "_bucket{le=\"" <> rest) do
    case String.split(rest, "\"}") do
      [le, ""] -> {:bucket, le}
      _other -> :malformed
    end
  end

  defp series(name), do: if(flush_series?(name), do: :malformed)

  defp flush_series?(text), do: String.starts_with?(text, [@histogram, @bytes, @records])

  # Integers stay integers (counts must be); anything else numeric is a float.
  defp number(text) do
    case Integer.parse(text) do
      {integer, ""} ->
        integer

      _not_integer ->
        case Float.parse(text) do
          {float, ""} -> float
          _not_number -> nil
        end
    end
  end

  defp single(samples, key) do
    case for({^key, value} <- samples, do: value) do
      [value] -> {:ok, value}
      _none_or_many -> :error
    end
  end

  # At least one finite edge, strictly ascending, then `+Inf` last, and nothing after it.
  defp finite_then_inf(buckets) do
    case Enum.split(buckets, -1) do
      {[_ | _] = finite, [{"+Inf", _count}] = inf} ->
        edges = Enum.map(finite, fn {le, _count} -> number(le) end)

        if Enum.all?(edges, &is_number/1) and ascending?(edges) do
          {:ok, finite ++ inf}
        else
          :error
        end

      _other ->
        :error
    end
  end

  defp ascending?(edges), do: edges |> Enum.chunk_every(2, 1, :discard) |> Enum.all?(fn [a, b] -> a < b end)

  # --- arithmetic ---

  defp same_edges(a, b) do
    if Enum.map(a.buckets, &elem(&1, 0)) == Enum.map(b.buckets, &elem(&1, 0)),
      do: :ok,
      else: {:error, :bucket_mismatch}
  end

  defp combine(a, b, op) do
    %{
      buckets: Enum.zip_with(a.buckets, b.buckets, fn {le, x}, {le, y} -> {le, op.(x, y)} end),
      count: op.(a.count, b.count),
      sum: op.(a.sum, b.sum),
      bytes: op.(a.bytes, b.bytes),
      records: op.(a.records, b.records)
    }
  end

  # Prometheus's histogram_quantile: the rank is q times the +Inf count, the answer lies in the first
  # bucket whose cumulative count reaches it, interpolated linearly between that bucket's bounds.
  defp quantile(buckets, q) do
    {finite, [{"+Inf", total}]} = Enum.split(buckets, -1)
    rank = q * total

    finite
    |> Enum.map(fn {le, cumulative} -> {number(le) / 1, cumulative} end)
    |> Enum.reduce_while({0.0, 0}, fn {upper, cumulative}, {lower, below} ->
      if cumulative >= rank do
        {:halt, {:found, lower + (upper - lower) * (rank - below) / (cumulative - below)}}
      else
        {:cont, {upper, cumulative}}
      end
    end)
    |> case do
      {:found, value} -> value
      # Past the highest finite edge: that edge is the most a histogram can say.
      {highest_edge, _below} -> highest_edge
    end
  end
end
