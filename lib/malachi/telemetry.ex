defmodule Malachi.Telemetry do
  @moduledoc """
  The telemetry events Malachi emits on its hot paths. Attach a handler with `:telemetry.attach/4` (or
  `attach_many/4`) to feed metrics, logs, or traces: Malachi ships a default handler that folds a few of
  these into the ETS `Malachi.Metrics` (which the Prometheus endpoint exposes).

  Events, `event name` - `measurements` / `metadata`:

    * `[:malachi, :produce]`: `%{count, bytes}` / `%{topic}` - records appended to a topic.
    * `[:malachi, :consume]`: `%{count}` / `%{topic}` - records read from a topic.
    * `[:malachi, :auth]`. `%{count: 1}` / `%{result: :ok | :error}` - an authentication attempt.
    * `[:malachi, :replication, :commit]`. `%{count}` / `%{result: :ok | :no_quorum}` - a quorum
      replication of a batch (`count` = records in the batch; `result` is whether a quorum stored it).
    * `[:malachi, :storage, :integrity]`. `%{position, unreadable_bytes}` /
      `%{result, sealed, source, segment}` - a stored segment failed its checksum verification.
      `result` is the framing/checksum reason (`:bad_crc`, `:bad_magic`, `:incomplete`), `position`
      the byte where the damage starts, `sealed` whether the segment was immutable (damage there is
      corruption at rest, not a crash mid-write), and `source` where the verdict came from
      (`:recover` when a segment was opened, `:scrub` from the background verification pass).

  Emitting is a no-op fast path when nothing is attached, so these are safe on the hot path.
  """

  @doc "Records appended to `topic` (`count` records, `bytes` total value bytes)."
  @spec produce(String.t(), non_neg_integer(), non_neg_integer()) :: :ok
  def produce(topic, count, bytes) do
    :telemetry.execute([:malachi, :produce], %{count: count, bytes: bytes}, %{topic: topic})
  end

  @doc "Records read from `topic` (`count` records)."
  @spec consume(String.t(), non_neg_integer()) :: :ok
  def consume(topic, count) do
    :telemetry.execute([:malachi, :consume], %{count: count}, %{topic: topic})
  end

  @doc "An authentication attempt with its `result` (`:ok` or `:error`)."
  @spec auth(:ok | :error) :: :ok
  def auth(result) do
    :telemetry.execute([:malachi, :auth], %{count: 1}, %{result: result})
  end

  @doc "A quorum replication of a batch of `count` records with its `result`."
  @spec replication_commit(non_neg_integer(), :ok | :no_quorum) :: :ok
  def replication_commit(count, result) do
    :telemetry.execute([:malachi, :replication, :commit], %{count: count}, %{result: result})
  end

  @doc """
  A stored segment failed verification: `verdict` is the storage layer's finding (`:reason`,
  `:position`, `:unreadable_bytes`, `:sealed?`), `segment_id` names the segment and `source` says
  whether it surfaced while opening the segment (`:recover`) or during the background scrub.
  """
  @spec storage_integrity(map(), term(), :recover | :scrub) :: :ok
  def storage_integrity(verdict, segment_id, source) do
    :telemetry.execute(
      [:malachi, :storage, :integrity],
      %{position: verdict.position, unreadable_bytes: verdict.unreadable_bytes},
      %{result: verdict.reason, sealed: verdict.sealed?, source: source, segment: segment_id}
    )
  end
end
