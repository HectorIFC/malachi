defmodule Malachi.Topic do
  @moduledoc """
  A topic: a named collection of `Malachi.Range`s that together cover the full keyspace.

  Producers "produce to a topic" — `append/2` hashes each record's key and routes it to
  the active range that owns that slice of the keyspace. The active ranges always **tile**
  `[0, keyspace_size)` with no gaps or overlaps; `split_range/2` and `merge_ranges/3`
  preserve that invariant (a split replaces one range with its two halves, a merge
  replaces two buddies with their union).

  ## Metadata is in-memory (for now)

  The *data* is durable — it lives in each range's segment files. The topic's *shape*
  (which ranges exist, their bounds and lineage) is held in memory here. In NorthGuard
  this metadata lives in the coordinator/vnode (Raft); persisting it is the job of the
  upcoming DS-RSM phase, so this module has no `recover/2` yet. See
  `docs/NORTHGUARD_PORT.md`.

  Sealed parents from splits/merges are kept (in `sealed_ranges`) so their records stay
  readable; routing only ever uses the active ranges.
  """

  alias Malachi.Range

  @type t :: %__MODULE__{
          name: String.t(),
          directory: Path.t(),
          keyspace_size: pos_integer(),
          state: :active | :sealed,
          active_ranges: %{Range.id() => Range.t()},
          sealed_ranges: %{Range.id() => Range.t()}
        }

  defstruct [
    :name,
    :directory,
    :keyspace_size,
    state: :active,
    active_ranges: %{},
    sealed_ranges: %{}
  ]

  @doc """
  Creates a topic named `name` under `parent_directory`, starting with a single root
  range that covers the whole keyspace.

  Options (`:keyspace_bits` plus any segment options like `:max_bytes`, `:index_interval`,
  `:flush_bytes`) are passed through to the ranges.
  """
  @spec create(Path.t(), String.t(), keyword()) :: {:ok, t()}
  def create(parent_directory, name, opts \\ []) do
    directory = Path.join(parent_directory, name)
    {:ok, root_range} = Range.open(directory, opts)

    {:ok,
     %__MODULE__{
       name: name,
       directory: directory,
       keyspace_size: root_range.keyspace_size,
       active_ranges: %{root_range.id => root_range}
     }}
  end

  @doc """
  Routes each record to the active range owning its key's keyspace slice and appends it
  there. Returns the updated topic and a `placements` map of
  `range_id => {first_offset, last_offset}` for the ranges that received records.
  Fails with `{:error, :sealed}` on a sealed topic.
  """
  @spec append(t(), [Malachi.Log.Record.t()]) ::
          {:ok, t(), %{Range.id() => {non_neg_integer(), non_neg_integer()}}} | {:error, term()}
  def append(%__MODULE__{state: :sealed}, _records), do: {:error, :sealed}

  def append(%__MODULE__{} = topic, records) when is_list(records) do
    records
    |> Enum.group_by(&covering_range_id(topic, &1.key))
    |> Enum.reduce_while({:ok, topic, %{}}, fn {range_id, group}, {:ok, current_topic, placements} ->
      range = Map.fetch!(current_topic.active_ranges, range_id)

      case Range.append(range, group) do
        {:ok, updated_range, first_offset, last_offset} ->
          updated_topic = put_active_range(current_topic, updated_range)
          placements = Map.put(placements, range_id, {first_offset, last_offset})
          {:cont, {:ok, updated_topic, placements}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  @doc "Returns the active range that owns `key`'s keyspace slice."
  @spec route(t(), term()) :: {:ok, Range.t()} | {:error, :uncovered}
  def route(%__MODULE__{} = topic, key) do
    case Enum.find(topic.active_ranges, fn {_id, range} -> Range.covers?(range, key) end) do
      {_id, range} -> {:ok, range}
      nil -> {:error, :uncovered}
    end
  end

  @doc "Flushes and fsyncs every active range."
  @spec sync(t()) :: {:ok, t()}
  def sync(%__MODULE__{} = topic) do
    active_ranges =
      Map.new(topic.active_ranges, fn {range_id, range} ->
        {:ok, synced_range} = Range.sync(range)
        {range_id, synced_range}
      end)

    {:ok, %{topic | active_ranges: active_ranges}}
  end

  @doc """
  Reads up to `max_records` committed records from the range `range_id`, starting at
  `offset`. The range may be active or a sealed parent. `{:error, :no_such_range}` if
  unknown.
  """
  @spec read(t(), Range.id(), non_neg_integer(), pos_integer()) ::
          {:ok, [Malachi.Log.Record.t()]} | :eof | {:error, term()}
  def read(%__MODULE__{} = topic, range_id, offset, max_records) do
    case find_range(topic, range_id) do
      nil -> {:error, :no_such_range}
      range -> Range.read(range, offset, max_records)
    end
  end

  @doc """
  Splits the active range `range_id` into two halves, keeping keyspace coverage. The
  sealed parent is retained for reads. Returns the new left/right range ids.
  """
  @spec split_range(t(), Range.id()) ::
          {:ok, t(), left :: Range.id(), right :: Range.id()} | {:error, term()}
  def split_range(%__MODULE__{} = topic, range_id) do
    with {:ok, range} <- fetch_active(topic, range_id),
         {:ok, sealed, left, right} <- Range.split(range) do
      topic =
        topic
        |> remove_active_range(range_id)
        |> put_active_range(left)
        |> put_active_range(right)
        |> put_sealed_range(sealed)

      {:ok, topic, left.id, right.id}
    end
  end

  @doc """
  Merges two active buddy ranges into one, keeping keyspace coverage. The two sealed
  parents are retained for reads. Returns the new child range id.
  """
  @spec merge_ranges(t(), Range.id(), Range.id()) ::
          {:ok, t(), child :: Range.id()} | {:error, term()}
  def merge_ranges(%__MODULE__{} = topic, range_id_a, range_id_b) do
    with {:ok, range_a} <- fetch_active(topic, range_id_a),
         {:ok, range_b} <- fetch_active(topic, range_id_b),
         {:ok, sealed_a, sealed_b, child} <- Range.merge(range_a, range_b) do
      topic =
        topic
        |> remove_active_range(range_id_a)
        |> remove_active_range(range_id_b)
        |> put_active_range(child)
        |> put_sealed_range(sealed_a)
        |> put_sealed_range(sealed_b)

      {:ok, topic, child.id}
    end
  end

  @doc "Seals the topic and all of its active ranges (they remain readable)."
  @spec seal(t()) :: {:ok, t()}
  def seal(%__MODULE__{} = topic) do
    active_ranges =
      Map.new(topic.active_ranges, fn {range_id, range} ->
        {:ok, sealed_range} = Range.seal(range)
        {range_id, sealed_range}
      end)

    {:ok, %{topic | active_ranges: active_ranges, state: :sealed}}
  end

  @typedoc "Opaque cursor for `stream_history/4`: `:start`, an internal position, or `:done`."
  @type history_cursor :: :start | {non_neg_integer(), non_neg_integer()} | :done

  @doc """
  Streams one **bounded** page of a range's cross-epoch history: the records its sealed
  ancestors hold for this slice (oldest first, in happens-before order), followed by the
  range's own records. This is how you follow a key through a split — the parent's records
  for the child's slice come before the child's.

  Returns `{:ok, records, next_cursor}`; call again with `next_cursor` until it is `:done`.
  A page may be empty (e.g. at a source boundary or when a parent page is fully filtered
  out) while `next_cursor` is not `:done` — keep going. Each call reads at most
  `max_records` raw records, so memory and per-call work stay bounded (unlike loading a
  whole ancestor at once). `{:error, :no_such_range}` if the range is unknown.
  """
  @spec stream_history(t(), Range.id(), history_cursor(), pos_integer()) ::
          {:ok, [Malachi.Log.Record.t()], history_cursor()} | {:error, term()}
  def stream_history(topic, range_id, cursor \\ :start, max_records \\ 1000)

  def stream_history(%__MODULE__{}, _range_id, :done, _max_records), do: {:ok, [], :done}

  def stream_history(%__MODULE__{} = topic, range_id, cursor, max_records)
      when is_integer(max_records) and max_records > 0 do
    case find_range(topic, range_id) do
      nil ->
        {:error, :no_such_range}

      range ->
        {source_index, source_offset} = normalize_cursor(cursor)
        read_history_page(history_sources(topic, range), source_index, source_offset, max_records)
    end
  end

  @doc """
  Convenience that pages `stream_history/4` to the end and returns every record as one
  ordered list. Loads the whole history into memory — intended for bounded/administrative
  use; for large histories, page with `stream_history/4` instead.
  """
  @spec read_history(t(), Range.id()) :: {:ok, [Malachi.Log.Record.t()]} | {:error, term()}
  def read_history(%__MODULE__{} = topic, range_id) do
    drain_history(topic, range_id, :start, [])
  end

  @doc "The ids of the topic's active ranges."
  @spec active_range_ids(t()) :: [Range.id()]
  def active_range_ids(%__MODULE__{} = topic), do: Map.keys(topic.active_ranges)

  @doc "Whether any active range has buffered records not yet flushed."
  @spec pending?(t()) :: boolean()
  def pending?(%__MODULE__{} = topic) do
    Enum.any?(topic.active_ranges, fn {_id, range} -> Range.pending?(range) end)
  end

  @doc "Whether the active ranges tile the whole keyspace `[0, keyspace_size)` with no gaps."
  @spec keyspace_covered?(t()) :: boolean()
  def keyspace_covered?(%__MODULE__{} = topic) do
    topic.active_ranges
    |> Map.values()
    |> Enum.sort_by(& &1.key_start)
    |> contiguous_cover?(0, topic.keyspace_size)
  end

  @doc "Closes the file handles of all active ranges."
  @spec close(t()) :: :ok
  def close(%__MODULE__{} = topic) do
    Enum.each(topic.active_ranges, fn {_id, range} -> Range.close(range) end)
    :ok
  end

  # --- internals ---

  defp covering_range_id(topic, key) do
    {:ok, range} = route(topic, key)
    range.id
  end

  defp find_range(topic, range_id) do
    Map.get(topic.active_ranges, range_id) || Map.get(topic.sealed_ranges, range_id)
  end

  # The ordered list of {source_range, filter_range_or_nil} a range's history reads from:
  # each sealed ancestor (records filtered to this range's slice), then the range itself.
  defp history_sources(topic, range) do
    ancestors =
      Enum.flat_map(range.parents, fn ancestor_id ->
        case find_range(topic, ancestor_id) do
          nil -> []
          ancestor -> [{ancestor, range}]
        end
      end)

    ancestors ++ [{range, nil}]
  end

  defp normalize_cursor(:start), do: {0, 0}
  defp normalize_cursor({source_index, source_offset}), do: {source_index, source_offset}

  defp read_history_page(sources, source_index, source_offset, max_records) do
    case Enum.at(sources, source_index) do
      nil ->
        {:ok, [], :done}

      {source_range, filter_range} ->
        case Range.read(source_range, source_offset, max_records) do
          # source exhausted: advance to the next source within this call (cheap)
          :eof ->
            read_history_page(sources, source_index + 1, 0, max_records)

          {:ok, raw_records} ->
            records = filter_records(raw_records, filter_range)
            {:ok, records, {source_index, source_offset + length(raw_records)}}

          {:error, _reason} = error ->
            error
        end
    end
  end

  defp filter_records(records, nil), do: records
  defp filter_records(records, filter_range), do: Enum.filter(records, &Range.covers?(filter_range, &1.key))

  defp drain_history(topic, range_id, cursor, pages) do
    case stream_history(topic, range_id, cursor, 1000) do
      {:ok, records, :done} -> {:ok, [records | pages] |> Enum.reverse() |> List.flatten()}
      {:ok, records, next_cursor} -> drain_history(topic, range_id, next_cursor, [records | pages])
      {:error, _reason} = error -> error
    end
  end

  defp fetch_active(topic, range_id) do
    case Map.fetch(topic.active_ranges, range_id) do
      {:ok, range} -> {:ok, range}
      :error -> {:error, :no_such_range}
    end
  end

  defp put_active_range(topic, range) do
    %{topic | active_ranges: Map.put(topic.active_ranges, range.id, range)}
  end

  defp remove_active_range(topic, range_id) do
    %{topic | active_ranges: Map.delete(topic.active_ranges, range_id)}
  end

  defp put_sealed_range(topic, range) do
    %{topic | sealed_ranges: Map.put(topic.sealed_ranges, range.id, range)}
  end

  defp contiguous_cover?([], position, keyspace_size), do: position == keyspace_size

  defp contiguous_cover?([range | rest], position, keyspace_size) do
    range.key_start == position and contiguous_cover?(rest, range.key_end, keyspace_size)
  end
end
