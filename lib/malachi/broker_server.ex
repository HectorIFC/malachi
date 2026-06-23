defmodule Malachi.BrokerServer do
  @moduledoc """
  A `GenServer` that owns a `Malachi.Broker`, serializing concurrent access and adding the
  **time-based flush** trigger (NorthGuard's ~10ms): after a produce leaves buffered
  records, a flush is scheduled `:flush_interval_ms` later (default 10ms) so a slow trickle
  of small writes still becomes durable promptly. The size/count triggers already live in
  `Malachi.Storage.ElixirStore`.

  The `Broker` (and the layers it composes) are pure immutable values; routing all
  mutations through this single process is what makes concurrent producers/consumers safe.
  Buffered records are flushed on the timer and again on `terminate/2` for a clean shutdown.

  Supersedes `Malachi.TopicServer`.
  """

  use GenServer

  alias Malachi.Broker

  @default_flush_interval_ms 10

  # --- client API ---

  @doc """
  Starts a server owning a broker rooted at `directory`.

  ## Options
    * `:flush_interval_ms` - time-based flush delay (default 10)
    * remaining options are forwarded to `Malachi.Broker.open/2` (segment options).
    * standard `GenServer` options (`:name`, etc.) are honored.
  """
  @spec start_link(Path.t(), keyword()) :: GenServer.on_start()
  def start_link(directory, opts \\ []) do
    {gen_server_opts, broker_opts} =
      Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt, :hibernate_after])

    GenServer.start_link(__MODULE__, {directory, broker_opts}, gen_server_opts)
  end

  @doc "Creates a topic (and its root range); returns `{:ok, root_range_id}` or an error."
  @spec create_topic(GenServer.server(), Malachi.Metadata.topic_name(), pos_integer()) :: term()
  def create_topic(server, name, keyspace_bits),
    do: GenServer.call(server, {:create_topic, name, keyspace_bits})

  @doc "Routes and appends records; returns `{:ok, placements}` or an error."
  @spec produce(GenServer.server(), Malachi.Metadata.topic_name(), [Malachi.Log.Record.t()]) ::
          {:ok, %{Malachi.Metadata.range_id() => {non_neg_integer(), non_neg_integer()}}}
          | {:error, term()}
  def produce(server, topic, records), do: GenServer.call(server, {:produce, topic, records})

  @doc "Reads up to `max_records` committed records from a range's log."
  @spec read(GenServer.server(), Malachi.Metadata.range_id(), non_neg_integer(), pos_integer()) ::
          {:ok, [Malachi.Log.Record.t()]} | :eof | {:error, term()}
  def read(server, range_id, offset, max_records),
    do: GenServer.call(server, {:read, range_id, offset, max_records})

  @doc "Streams one bounded page of a range's cross-epoch history (see `Malachi.Broker.stream_history/4`)."
  @spec stream_history(GenServer.server(), Malachi.Metadata.range_id(), Broker.history_cursor(), pos_integer()) ::
          {:ok, [Malachi.Log.Record.t()], Broker.history_cursor()} | {:error, term()}
  def stream_history(server, range_id, cursor \\ :start, max_records \\ 1000),
    do: GenServer.call(server, {:stream_history, range_id, cursor, max_records})

  @doc "Forces an immediate flush+fsync of all buffered records."
  @spec sync(GenServer.server()) :: :ok
  def sync(server), do: GenServer.call(server, :sync)

  @doc "Splits a range; returns `{:ok, left_id, right_id}` or an error."
  @spec split_range(GenServer.server(), Malachi.Metadata.range_id()) ::
          {:ok, Malachi.Metadata.range_id(), Malachi.Metadata.range_id()} | {:error, term()}
  def split_range(server, range_id), do: GenServer.call(server, {:split_range, range_id})

  @doc "Merges two buddy ranges; returns `{:ok, child_id}` or an error."
  @spec merge_ranges(GenServer.server(), Malachi.Metadata.range_id(), Malachi.Metadata.range_id()) ::
          {:ok, Malachi.Metadata.range_id()} | {:error, term()}
  def merge_ranges(server, range_id_a, range_id_b),
    do: GenServer.call(server, {:merge_ranges, range_id_a, range_id_b})

  @doc "The ids of a topic's active ranges."
  @spec active_range_ids(GenServer.server(), Malachi.Metadata.topic_name()) :: [Malachi.Metadata.range_id()]
  def active_range_ids(server, topic), do: GenServer.call(server, {:active_range_ids, topic})

  @doc "Flushes and stops the server."
  @spec stop(GenServer.server()) :: :ok
  def stop(server), do: GenServer.stop(server)

  # --- server callbacks ---

  @impl true
  def init({directory, broker_opts}) do
    flush_interval_ms = Keyword.get(broker_opts, :flush_interval_ms, @default_flush_interval_ms)
    broker_opts = Keyword.delete(broker_opts, :flush_interval_ms)
    {:ok, broker} = Broker.open(directory, broker_opts)

    {:ok, %{broker: broker, flush_interval_ms: flush_interval_ms, flush_timer: nil}}
  end

  @impl true
  def handle_call({:create_topic, name, keyspace_bits}, _from, state) do
    {broker, reply} = Broker.create_topic(state.broker, name, keyspace_bits)
    {:reply, reply, %{state | broker: broker}}
  end

  def handle_call({:produce, topic, records}, _from, state) do
    case Broker.produce(state.broker, topic, records) do
      {:ok, broker, placements} ->
        {:reply, {:ok, placements}, arm_flush_timer(%{state | broker: broker})}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:read, range_id, offset, max_records}, _from, state) do
    {:reply, Broker.read(state.broker, range_id, offset, max_records), state}
  end

  def handle_call({:stream_history, range_id, cursor, max_records}, _from, state) do
    {:reply, Broker.stream_history(state.broker, range_id, cursor, max_records), state}
  end

  def handle_call(:sync, _from, state) do
    {:reply, :ok, flush(state)}
  end

  def handle_call({:split_range, range_id}, _from, state) do
    {broker, reply} = Broker.split_range(state.broker, range_id)
    {:reply, reply, %{state | broker: broker}}
  end

  def handle_call({:merge_ranges, range_id_a, range_id_b}, _from, state) do
    {broker, reply} = Broker.merge_ranges(state.broker, range_id_a, range_id_b)
    {:reply, reply, %{state | broker: broker}}
  end

  def handle_call({:active_range_ids, topic}, _from, state) do
    {:reply, Broker.active_range_ids(state.broker, topic), state}
  end

  @impl true
  def handle_info(:flush, state) do
    {:noreply, flush(state)}
  end

  @impl true
  def terminate(_reason, state) do
    state = flush(state)
    Broker.close(state.broker)
    :ok
  end

  # --- internals ---

  # Schedule a flush only when there is buffered data and no timer is already pending, so an
  # idle broker never triggers wasteful fsyncs.
  defp arm_flush_timer(%{flush_timer: nil} = state) do
    if Broker.pending?(state.broker) do
      %{state | flush_timer: Process.send_after(self(), :flush, state.flush_interval_ms)}
    else
      state
    end
  end

  defp arm_flush_timer(state), do: state

  defp flush(state) do
    cancel_flush_timer(state.flush_timer)
    {:ok, broker} = Broker.sync(state.broker)
    %{state | broker: broker, flush_timer: nil}
  end

  defp cancel_flush_timer(nil), do: :ok
  defp cancel_flush_timer(timer), do: Process.cancel_timer(timer)
end
