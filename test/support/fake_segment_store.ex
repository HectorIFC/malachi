defmodule Malachi.Test.FakeSegmentStore do
  @moduledoc """
  In-memory stand-in for `Malachi.Cluster.ReplicationServer` storage, for unit-testing the
  `Malachi.Broker` router (routing, offsets, segment lifecycle, cross-epoch history) without
  processes or files. Records are keyed by `{ref, segment_id}` and assigned contiguous offsets
  starting at the segment's `base_offset`, mirroring the real per-segment log.
  """

  use Agent

  @spec start_link(term()) :: Agent.on_start()
  def start_link(_opts \\ []), do: Agent.start_link(fn -> %{} end)

  @doc "A `replicate_fun` matching `Malachi.Broker`'s effect contract, bound to `agent`."
  def replicate(agent, ref, segment_id, _replica_set, base_offset, records) do
    Agent.get_and_update(agent, fn store ->
      key = {ref, segment_id}
      existing = Map.get(store, key, [])
      start = base_offset + length(existing)
      assigned = for {record, offset} <- Enum.with_index(records, start), do: %{record | offset: offset}
      last = start + length(records) - 1
      {{:ok, last}, Map.put(store, key, existing ++ assigned)}
    end)
  end

  @doc "A `read_fun` matching `Malachi.Broker`'s effect contract, bound to `agent`."
  def read(agent, ref, segment_id, offset, max_records) do
    Agent.get(agent, fn store ->
      records =
        store
        |> Map.get({ref, segment_id}, [])
        |> Enum.filter(&(&1.offset >= offset))
        |> Enum.take(max_records)

      if records == [], do: :eof, else: {:ok, records}
    end)
  end
end
