defmodule Malachi.Test.ReadBack do
  @moduledoc """
  Reads a topic back in process, the way a `fetch_range` reader does (`Malachi.BrokerServer.fetch_range/4`
  and `Malachi.ConsumeStreams.read_page/6`, what wire key 31 answers with), for tests that write and then
  check what the broker holds.

  Every active range is read from the start of its history to its end. A range's history holds its
  ancestors' records, of which the broker hands it only those in its own key block (`Malachi.Broker`
  filters an ancestor's records by the slice the range reads), so across splits each record is read once,
  by the range that holds its key now.

  A range is read where its primary is, as `fetch_range` is served: a broker that answers `moved` to
  another one is asked there, and the page is read on that broker's node, as a connection there reads it.
  A topic whose metadata this broker cannot see yet answers `{:error, :metadata_unavailable}`, never an
  empty list, as `fetch_range` refuses it.
  """

  alias Malachi.BrokerServer
  alias Malachi.ConsumeStreams
  alias Malachi.Log.Record
  alias Malachi.Metadata
  alias Malachi.Wire.Batch

  @page 1_000
  @max_bytes 64 * 1_048_576

  @doc """
  The records of `topic` on `server`, range by range in the order of their key blocks, each range's in the
  order it holds them. `{:error, reason}` when a range cannot be read (`fetch_range`'s own refusals).
  """
  @spec records(GenServer.server(), String.t()) :: [Record.t()] | {:error, term()}
  def records(server, topic) do
    case server |> BrokerServer.metadata() |> Metadata.active_ranges_of_topic(topic) do
      [] -> no_ranges(server, topic)
      ranges -> ranges |> Enum.sort_by(& &1.key_start) |> Enum.reduce_while([], &read_into(server, &1, &2))
    end
  end

  defp read_into(server, range, acc) do
    case read_range(server, range.id, :earliest, []) do
      {:ok, records} -> {:cont, acc ++ records}
      {:error, _reason} = error -> {:halt, error}
    end
  end

  # No active range in the metadata this broker sees: a topic that does not exist, or one whose metadata
  # it cannot see yet, which `fetch_range` tells apart.
  defp no_ranges(server, topic) do
    case BrokerServer.fetch_range(server, {topic, 0}, :earliest, 0) do
      {:error, :metadata_unavailable} = error -> error
      _absent -> []
    end
  end

  @doc """
  The records of range `range_id` from `start` on (`:earliest`, `{:position, position}`, or a group's
  checkpoint, `{:committed, group}`), read to the range's end.
  """
  @spec range_records(GenServer.server(), Metadata.range_id(), term()) :: [Record.t()] | {:error, term()}
  def range_records(server, range_id, start) do
    case read_range(server, range_id, start, []) do
      {:ok, records} -> records
      {:error, _reason} = error -> error
    end
  end

  @doc "The values of `records/2`."
  @spec values(GenServer.server(), String.t()) :: [binary()] | {:error, term()}
  def values(server, topic) do
    case records(server, topic) do
      {:error, _reason} = error -> error
      records -> Enum.map(records, & &1.value)
    end
  end

  defp read_range(server, range_id, start, acc) do
    with {:ok, position, view, reporter} <- BrokerServer.fetch_range(server, range_id, start, 0),
         {:ok, page, _count} <- read_page(server, [view, range_id, position, @page, @max_bytes, reporter]) do
      {:ok, entries} = Batch.decode(page.batch, max_inflated_bytes: @max_bytes, layout: :positioned)
      mine = for {_position, record, _tombstone} <- Enum.drop(entries, page.skip), do: record

      if entries == [] or page.next == position,
        do: {:ok, acc ++ mine},
        else: read_range(server, range_id, {:position, page.next}, acc ++ mine)
    else
      {:moved, :elsewhere, [{^range_id, {_segment, primary}}]} when primary != server ->
        read_range(primary, range_id, start, acc)

      {:moved, reason, _targets} ->
        {:error, {:moved, reason}}

      {:error, _reason} = error ->
        error
    end
  end

  # The page read where the broker that answered lives: its segments are read through that node's
  # replication servers.
  defp read_page({_name, node}, args) when node != node(), do: :erpc.call(node, ConsumeStreams, :read_page, args)
  defp read_page(_local, args), do: apply(ConsumeStreams, :read_page, args)
end
