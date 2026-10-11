defmodule Malachi.ProducerDirectory do
  @moduledoc """
  Where this node's open producer streams are, for the operator interfaces (`GET /api/v1/producers`).

  A producer stream lives in the connection that holds it (`Malachi.ProducerStreams`), and only there: its
  window, its sequences and what is in flight change with every append, so copying them anywhere would be
  copying them on the hot path. This directory keeps only who holds some: each connection that holds a
  producer stream has one entry in a `Registry`, under its pid, valued by how many it holds; it is counted
  up as a stream opens and down as one ends, at the cost of one entry whatever the node holds, and the
  registry drops it when the connection exits, however it exits. A listing asks each connection for its
  streams, by message, and waits a bounded time for the answers (`list/1`); a connection busy past it is
  counted as unanswered rather than waited for.

  NorthGuard keeps track of its clients in a client management layer (its Conductor); here the node a
  producer is connected to answers for it.
  """

  alias Malachi.ConnectionStreams

  @registry __MODULE__

  # How long a listing waits for the connections: each answers from its own loop, between frames.
  @default_timeout_ms 500

  @doc "The `Registry` child the application starts."
  @spec child_spec(term()) :: Supervisor.child_spec()
  def child_spec(_arg), do: Registry.child_spec(keys: :unique, name: @registry)

  @doc "Counts one more producer stream held by the calling connection."
  @spec track() :: :ok
  def track do
    case Registry.update_value(@registry, self(), &(&1 + 1)) do
      :error ->
        {:ok, _owner} = Registry.register(@registry, self(), 1)
        :ok

      {_new, _old} ->
        :ok
    end
  end

  @doc "Counts one producer stream fewer for the calling connection, which leaves the directory at none."
  @spec untrack() :: :ok
  def untrack do
    case Registry.update_value(@registry, self(), &(&1 - 1)) do
      {0, _old} -> Registry.unregister(@registry, self())
      _held_or_none -> :ok
    end
  end

  @doc """
  How many producer streams are open on this node, one value per connection that holds some; 0 while the
  directory is not running (the node starting or stopping).
  """
  @spec open_count() :: non_neg_integer()
  def open_count do
    if Process.whereis(@registry),
      do: @registry |> Registry.select([{{:_, :_, :"$1"}, [], [:"$1"]}]) |> Enum.sum(),
      else: 0
  end

  @doc """
  Every producer stream open on this node, as its connection describes it
  (`Malachi.ProducerStreams.describe/1`) and named by its `connection` (the pid's text), in the order
  `(opened_at, connection, stream_id)`, which a page token resumes from; and how many connections did not
  answer within `timeout_ms`.
  """
  @spec list(non_neg_integer()) :: %{streams: [map()], unanswered: non_neg_integer()}
  def list(timeout_ms \\ @default_timeout_ms) do
    ref = make_ref()

    asked =
      for pid <- Registry.select(@registry, [{{:"$1", :_, :_}, [], [:"$1"]}]), into: %{} do
        monitor = Process.monitor(pid)
        send(pid, {:describe_producer_streams, self(), ref})
        {pid, monitor}
      end

    deadline = System.monotonic_time(:millisecond) + timeout_ms
    {streams, unanswered} = collect(asked, ref, deadline, [])
    %{streams: Enum.sort_by(streams, &order_key/1), unanswered: unanswered}
  end

  @doc "Where `stream` sorts in a listing: what a page token holds."
  @spec order_key(map()) :: {integer(), String.t(), pos_integer()}
  def order_key(stream), do: {stream.opened_at, stream.connection, stream.stream_id}

  defp collect(asked, _ref, _deadline, acc) when map_size(asked) == 0, do: {acc, 0}

  defp collect(asked, ref, deadline, acc) do
    wait = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {^ref, pid, streams} when is_map_key(asked, pid) ->
        {monitor, asked} = Map.pop!(asked, pid)
        Process.demonitor(monitor, [:flush])
        connection = inspect(pid)
        collect(asked, ref, deadline, Enum.map(streams, &Map.put(&1, :connection, connection)) ++ acc)

      # a connection that went away holds no stream any more
      {:DOWN, _monitor, :process, pid, _reason} when is_map_key(asked, pid) ->
        collect(Map.delete(asked, pid), ref, deadline, acc)
    after
      wait ->
        for {_pid, monitor} <- asked, do: Process.demonitor(monitor, [:flush])
        {acc, map_size(asked)}
    end
  end

  @doc """
  The answer a connection sends a listing (`list/1`): its producer streams, with its client address.
  Called from the connection's loop with its `Malachi.ConnectionStreams`.
  """
  @spec answer({:describe_producer_streams, pid(), reference()}, ConnectionStreams.t()) :: :ok
  def answer({:describe_producer_streams, from, ref}, %ConnectionStreams{} = streams) do
    send(from, {ref, self(), ConnectionStreams.describe_producers(streams)})
    :ok
  end
end
