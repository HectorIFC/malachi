defmodule Malachi.TopicServer do
  @moduledoc """
  A `GenServer` that owns a `Malachi.Topic`, serializing concurrent access and adding the
  **time-based flush** trigger (NorthGuard's ~10ms): after an append leaves buffered
  records, a flush is scheduled `:flush_interval_ms` later (default 10ms) so a slow trickle
  of small writes still becomes durable promptly. The size and count triggers already live
  in `Malachi.Storage.ElixirStore`.

  The underlying `Topic`/`Range`/`Log` modules are pure immutable handles; routing all
  mutations through this single process is what makes concurrent producers/consumers safe.
  Buffered records are flushed on the timer and again on `terminate/2` for a clean shutdown.
  """

  use GenServer

  alias Malachi.Range
  alias Malachi.Topic

  @default_flush_interval_ms 10

  # --- client API ---

  @doc """
  Starts a server owning a topic named `name` under `parent_directory`.

  ## Options
    * `:flush_interval_ms` - time-based flush delay (default 10)
    * any other options are forwarded to `Malachi.Topic.create/3`.
    * standard `GenServer` options (`:name`, etc.) are honored.
  """
  @spec start_link(Path.t(), String.t(), keyword()) :: GenServer.on_start()
  def start_link(parent_directory, name, opts \\ []) do
    {gen_server_opts, topic_opts} = Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt, :hibernate_after])
    GenServer.start_link(__MODULE__, {parent_directory, name, topic_opts}, gen_server_opts)
  end

  @doc "Routes and appends records; returns the placements map."
  @spec append(GenServer.server(), [Malachi.Log.Record.t()]) ::
          {:ok, %{Malachi.Range.id() => {non_neg_integer(), non_neg_integer()}}} | {:error, term()}
  def append(server, records), do: GenServer.call(server, {:append, records})

  @doc "Reads up to `max_records` committed records from a range."
  @spec read(GenServer.server(), Malachi.Range.id(), non_neg_integer(), pos_integer()) ::
          {:ok, [Malachi.Log.Record.t()]} | :eof | {:error, term()}
  def read(server, range_id, offset, max_records),
    do: GenServer.call(server, {:read, range_id, offset, max_records})

  @doc """
  Streams one bounded page of a range's cross-epoch history (see
  `Malachi.Topic.stream_history/4`). Call again with the returned cursor until `:done`.
  Paging keeps each call's work bounded, so a large history can't hold the server.
  """
  @spec stream_history(GenServer.server(), Malachi.Range.id(), Topic.history_cursor(), pos_integer()) ::
          {:ok, [Malachi.Log.Record.t()], Topic.history_cursor()} | {:error, term()}
  def stream_history(server, range_id, cursor \\ :start, max_records \\ 1000),
    do: GenServer.call(server, {:stream_history, range_id, cursor, max_records})

  @doc "Forces an immediate flush+fsync of all buffered records."
  @spec sync(GenServer.server()) :: :ok
  def sync(server), do: GenServer.call(server, :sync)

  @doc "Splits an active range; returns the new left/right range ids."
  @spec split_range(GenServer.server(), Malachi.Range.id()) ::
          {:ok, Malachi.Range.id(), Malachi.Range.id()} | {:error, term()}
  def split_range(server, range_id), do: GenServer.call(server, {:split_range, range_id})

  @doc "Merges two active buddy ranges; returns the new child range id."
  @spec merge_ranges(GenServer.server(), Malachi.Range.id(), Malachi.Range.id()) ::
          {:ok, Malachi.Range.id()} | {:error, term()}
  def merge_ranges(server, range_id_a, range_id_b),
    do: GenServer.call(server, {:merge_ranges, range_id_a, range_id_b})

  @doc """
  Returns a metadata-only view (`Malachi.Range.info/1`) of the active range owning `key`.
  Returns a plain map (no file handle), so it is safe to use outside the server process.
  """
  @spec route(GenServer.server(), term()) :: {:ok, map()} | {:error, :uncovered}
  def route(server, key), do: GenServer.call(server, {:route, key})

  @doc "The ids of the topic's active ranges."
  @spec active_range_ids(GenServer.server()) :: [Malachi.Range.id()]
  def active_range_ids(server), do: GenServer.call(server, :active_range_ids)

  @doc "Seals the topic and all its ranges."
  @spec seal(GenServer.server()) :: :ok
  def seal(server), do: GenServer.call(server, :seal)

  @doc "Flushes and stops the server."
  @spec stop(GenServer.server()) :: :ok
  def stop(server), do: GenServer.stop(server)

  # --- server callbacks ---

  @impl true
  def init({parent_directory, name, topic_opts}) do
    flush_interval_ms = Keyword.get(topic_opts, :flush_interval_ms, @default_flush_interval_ms)
    topic_opts = Keyword.delete(topic_opts, :flush_interval_ms)
    {:ok, topic} = Topic.create(parent_directory, name, topic_opts)

    {:ok, %{topic: topic, flush_interval_ms: flush_interval_ms, flush_timer: nil}}
  end

  @impl true
  def handle_call({:append, records}, _from, state) do
    case Topic.append(state.topic, records) do
      {:ok, topic, placements} ->
        {:reply, {:ok, placements}, arm_flush_timer(%{state | topic: topic})}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:read, range_id, offset, max_records}, _from, state) do
    {:reply, Topic.read(state.topic, range_id, offset, max_records), state}
  end

  def handle_call({:stream_history, range_id, cursor, max_records}, _from, state) do
    {:reply, Topic.stream_history(state.topic, range_id, cursor, max_records), state}
  end

  def handle_call(:sync, _from, state) do
    {:reply, :ok, flush(state)}
  end

  def handle_call({:split_range, range_id}, _from, state) do
    case Topic.split_range(state.topic, range_id) do
      {:ok, topic, left_id, right_id} -> {:reply, {:ok, left_id, right_id}, %{state | topic: topic}}
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  def handle_call({:merge_ranges, range_id_a, range_id_b}, _from, state) do
    case Topic.merge_ranges(state.topic, range_id_a, range_id_b) do
      {:ok, topic, child_id} -> {:reply, {:ok, child_id}, %{state | topic: topic}}
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  def handle_call({:route, key}, _from, state) do
    reply =
      case Topic.route(state.topic, key) do
        {:ok, range} -> {:ok, Range.info(range)}
        {:error, _reason} = error -> error
      end

    {:reply, reply, state}
  end

  def handle_call(:active_range_ids, _from, state) do
    {:reply, Topic.active_range_ids(state.topic), state}
  end

  def handle_call(:seal, _from, state) do
    {:ok, topic} = Topic.seal(state.topic)
    {:reply, :ok, %{state | topic: topic}}
  end

  @impl true
  def handle_info(:flush, state) do
    {:noreply, flush(state)}
  end

  @impl true
  def terminate(_reason, state) do
    state = flush(state)
    Topic.close(state.topic)
    :ok
  end

  # --- internals ---

  # Schedule a flush only when there is buffered data and no timer is already pending,
  # so an idle topic never triggers wasteful fsyncs.
  defp arm_flush_timer(%{flush_timer: nil} = state) do
    if Topic.pending?(state.topic) do
      %{state | flush_timer: Process.send_after(self(), :flush, state.flush_interval_ms)}
    else
      state
    end
  end

  defp arm_flush_timer(state), do: state

  defp flush(state) do
    cancel_flush_timer(state.flush_timer)
    {:ok, topic} = Topic.sync(state.topic)
    %{state | topic: topic, flush_timer: nil}
  end

  defp cancel_flush_timer(nil), do: :ok
  defp cancel_flush_timer(timer), do: Process.cancel_timer(timer)
end
