defmodule Malachi.Test.FakeSegmentStore do
  @moduledoc """
  In-memory stand-in for `Malachi.Cluster.ReplicationServer` storage, for unit-testing the
  `Malachi.Broker` router (routing, offsets, segment lifecycle, cross-epoch history) without
  processes or files. Records are keyed by `{ref, segment_id}` and assigned contiguous offsets
  starting at the segment's `base_offset`, mirroring the real per-segment log.

  It models the write fence too: `seal/4` closes a segment and answers where it ended, and
  `replicate/6` then refuses it with `{:error, {:sealed, end_offset}}`, exactly as the real server
  does. Without that a broker test could never reach the refusal branches, which are the whole point
  of the seal design.
  """

  use Agent

  alias Malachi.Log.Record

  @spec start_link(term()) :: Agent.on_start()
  def start_link(_opts \\ []), do: Agent.start_link(fn -> %{segments: %{}, sealed: MapSet.new()} end)

  @doc "A `replicate_fun` matching `Malachi.Broker`'s effect contract, bound to `agent`."
  def replicate(agent, ref, segment_id, _replica_set, base_offset, records) do
    Agent.get_and_update(agent, fn state ->
      key = {ref, segment_id}
      existing = Map.get(state.segments, key, [])
      start = base_offset + length(existing)

      if MapSet.member?(state.sealed, key) do
        {{:error, {:sealed, start}}, state}
      else
        assigned = for {record, offset} <- Enum.with_index(records, start), do: %{record | offset: offset}
        last = start + length(records) - 1
        {{:ok, last}, put_in(state.segments[key], existing ++ assigned)}
      end
    end)
  end

  @doc """
  Appends `records` to `segment_id` WITHOUT honoring the fence, the way a replica that was never
  fenced can still grow: the surplus a stale primary leaves above a sealed edge. Only a fixture
  builder should reach for this; the produce path must go through `replicate/6`.
  """
  def force_replicate(agent, ref, segment_id, base_offset, records) do
    Agent.get_and_update(agent, fn state ->
      key = {ref, segment_id}
      existing = Map.get(state.segments, key, [])
      start = base_offset + length(existing)
      assigned = for {record, offset} <- Enum.with_index(records, start), do: %{record | offset: offset}
      {{:ok, start + length(records) - 1}, put_in(state.segments[key], existing ++ assigned)}
    end)
  end

  @doc """
  The write fence: seals `segment_id` on `ref` and answers `{:ok, end_offset, byte_size}`. Idempotent,
  and a segment this store never held seals at `{:ok, base_offset, 0}`, matching
  `Malachi.Cluster.ReplicationServer.seal/4`.
  """
  def seal(agent, ref, segment_id, base_offset) do
    Agent.get_and_update(agent, fn state ->
      key = {ref, segment_id}
      records = Map.get(state.segments, key, [])
      bytes = Enum.reduce(records, 0, fn record, sum -> sum + Record.encoded_size(record) end)
      reply = {:ok, base_offset + length(records), bytes}
      {reply, %{state | sealed: MapSet.put(state.sealed, key)}}
    end)
  end

  @doc "Whether `segment_id` is fenced on `ref`."
  def sealed?(agent, ref, segment_id) do
    Agent.get(agent, fn state -> MapSet.member?(state.sealed, {ref, segment_id}) end)
  end

  @doc "A `read_fun` matching `Malachi.Broker`'s effect contract, bound to `agent`."
  def read(agent, ref, segment_id, offset, max_records) do
    Agent.get(agent, fn state ->
      records =
        state.segments
        |> Map.get({ref, segment_id}, [])
        |> Enum.filter(&(&1.offset >= offset))
        |> Enum.take(max_records)

      if records == [], do: :eof, else: {:ok, records}
    end)
  end
end
