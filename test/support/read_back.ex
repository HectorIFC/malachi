defmodule Malachi.Test.ReadBack do
  @moduledoc """
  Reads a topic back in process, the way a `fetch_range` reader does (`Malachi.BrokerServer.fetch_range/4`
  and `Malachi.ConsumeStreams.read_page/6`, what wire key 31 answers with), for tests that write and then
  check what the broker holds.

  Every active range is read from the start of its history to its end. A range's history holds its
  ancestors' records, of which the broker hands it only those in its own key block (`Malachi.Broker`
  filters an ancestor's records by the slice the range reads), so across splits each record is read once,
  by the range that holds its key now.

  A range is read where its primary is, as `fetch_range` is served: a broker that answers `moved` names
  the segment's primary replication server, and the range is asked again of the broker under the same
  name on that server's node, whose page is read on that node, as a connection there reads it. The read
  of one range follows at most 3 moves and then answers `{:error, {:moved, :too_many_hops}}`, so brokers
  that keep pointing at each other (or at themselves) end the read with an error rather than an endless
  recursion or an empty list.

  A topic whose metadata this broker cannot see yet answers `{:error, :metadata_unavailable}`, never an
  empty list, as `fetch_range` refuses it. A server that is not a broker (`{:error, :unknown_call}`)
  raises: that is a bug in the caller, never an empty range.
  """

  alias Malachi.BrokerServer
  alias Malachi.ConsumeStreams
  alias Malachi.Log.Record
  alias Malachi.Metadata
  alias Malachi.Wire.Batch

  @page 1_000
  @max_bytes 64 * 1_048_576
  # The moves the read of one range follows before it gives up.
  @max_hops 3

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
    case read_range(server, range.id, :earliest, [], @max_hops) do
      {:ok, records} -> {:cont, acc ++ records}
      {:error, _reason} = error -> {:halt, error}
    end
  end

  # No active range in the metadata this broker sees: a topic that does not exist, or one whose metadata
  # it cannot see yet, which `fetch_range` tells apart.
  defp no_ranges(server, topic) do
    case BrokerServer.fetch_range(server, {topic, 0}, :earliest, 0) do
      {:error, :metadata_unavailable} = error -> error
      {:error, :unknown_call} -> not_a_broker!(server)
      _absent -> []
    end
  end

  @doc """
  The records of range `range_id` from `start` on (`:earliest`, `{:position, position}`, or a group's
  checkpoint, `{:committed, group}`), read to the range's end.
  """
  @spec range_records(GenServer.server(), Metadata.range_id(), term()) :: [Record.t()] | {:error, term()}
  def range_records(server, range_id, start) do
    case read_range(server, range_id, start, [], @max_hops) do
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

  defp read_range(server, range_id, start, acc, hops) do
    with {:ok, position, view, reporter} <- BrokerServer.fetch_range(server, range_id, start, 0),
         {:ok, page, _count} <- read_page(server, [view, range_id, position, @page, @max_bytes, reporter]) do
      {:ok, entries} = Batch.decode(page.batch, max_inflated_bytes: @max_bytes, layout: :positioned)
      mine = for {_position, record, _tombstone} <- Enum.drop(entries, page.skip), do: record

      if entries == [] or page.next == position,
        do: {:ok, acc ++ mine},
        else: read_range(server, range_id, {:position, page.next}, acc ++ mine, hops)
    else
      {:moved, :elsewhere, [{^range_id, {_segment, _primary}}]} when hops == 0 ->
        {:error, {:moved, :too_many_hops}}

      {:moved, :elsewhere, [{^range_id, {_segment, primary}}]} ->
        read_range(broker_beside(server, primary), range_id, start, acc, hops - 1)

      {:moved, reason, _targets} ->
        {:error, {:moved, reason}}

      {:error, :unknown_call} ->
        not_a_broker!(server)

      {:error, _reason} = error ->
        error
    end
  end

  # A move names the primary of the range's segment, which is a replication server (`{name, node}`, or a
  # pid), not a broker: the range is read through the broker on that server's node, under the name of the
  # broker that answered the move.
  defp broker_beside(server, primary), do: {broker_name(server), node_of(primary)}

  defp broker_name({name, _node}) when is_atom(name), do: name
  defp broker_name(name) when is_atom(name), do: name

  defp broker_name(pid) when is_pid(pid) do
    case :erpc.call(node(pid), Process, :info, [pid, :registered_name]) do
      {:registered_name, name} -> name
      _unnamed -> raise ArgumentError, "a move cannot be followed from #{inspect(pid)}: it has no registered name"
    end
  end

  defp node_of({_name, node}) when is_atom(node), do: node
  defp node_of(pid) when is_pid(pid), do: node(pid)
  defp node_of(name) when is_atom(name), do: node()

  defp not_a_broker!(server),
    do: raise(ArgumentError, "#{inspect(server)} answered :unknown_call to fetch_range: it is not a broker")

  # The page read where the broker that answered lives: its segments are read through that node's
  # replication servers.
  defp read_page({_name, node}, args) when node != node(), do: :erpc.call(node, ConsumeStreams, :read_page, args)
  defp read_page(_local, args), do: apply(ConsumeStreams, :read_page, args)
end
