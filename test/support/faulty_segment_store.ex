defmodule Malachi.Test.FaultySegmentStore do
  @moduledoc """
  A `Malachi.Storage.SegmentStore` that delegates everything to `Malachi.Storage.ElixirStore`, except the
  operations a test told it to fail, and counts what it was asked to do.

  It exists because a storage failure is the one input the replication layer has to survive without
  being able to cause it on demand: a full volume or a failing device cannot be reproduced portably in a
  test, and `Malachi.Storage.ElixirStore`'s own tests already prove it hands such errors back. What the
  layers above need to be tested against is the error arriving, and this is how it arrives.

  Rules and counters are scoped by DIRECTORY PREFIX, not by process or test. The caller of a store is a
  `Malachi.Cluster.ReplicationServer`, never the test process, and a segment lives in a directory nested
  under that server's own, so the test names the server's directory and every segment below it is
  covered. Tests use unique directories, so they stay independent under `async: true`.

      FaultySegmentStore.fail(directory, :sync, {:error, :enospc})
      FaultySegmentStore.count(directory, :flushing_sync)

  Failable operations: `:open`, `:recover`, `:open_read`, `:append`, `:sync`, `:seal`, `:read`,
  `:verify`, `:rebuild_index`. Counted ones are the same, plus `:flushing_sync`, a sync that actually
  had buffered records to write (a sync with nothing buffered is a no-op in the real store, and counting
  it would make a coalescing assertion meaningless).

  The table lives in a process of its own, started by `start/0`, so it survives whichever process
  created it: on a peer node that creator is a short-lived `:erpc` worker.
  """

  @behaviour Malachi.Storage.SegmentStore

  alias Malachi.Storage.ElixirStore

  @table __MODULE__
  @owner Module.concat(__MODULE__, Owner)

  @failable [:open, :recover, :open_read, :append, :sync, :seal, :read, :verify, :rebuild_index]

  @doc "Creates the rules table on this node. Idempotent."
  @spec start() :: :ok
  def start do
    case Process.whereis(@owner) do
      nil ->
        parent = self()
        pid = spawn(fn -> own_table(parent) end)

        receive do
          {:table_ready, ^pid} -> :ok
        end

      _pid ->
        :ok
    end
  end

  defp own_table(parent) do
    Process.register(self(), @owner)
    :ets.new(@table, [:named_table, :public, :set, write_concurrency: true])
    send(parent, {:table_ready, self()})
    Process.sleep(:infinity)
  end

  @doc "From now on, `operation` on any segment under `directory` answers `reply` instead of running."
  @spec fail(Path.t(), atom(), term()) :: :ok
  def fail(directory, operation, reply) when operation in @failable do
    true = :ets.insert(@table, {{:rule, Path.expand(directory), operation}, reply})
    :ok
  end

  @doc "Removes every rule and counter under `directory`."
  @spec clear(Path.t()) :: :ok
  def clear(directory) do
    prefix = Path.expand(directory)

    for {{kind, scope, _operation} = key, _value} <- :ets.tab2list(@table),
        kind in [:rule, :count],
        under?(scope, prefix),
        do: :ets.delete(@table, key)

    :ok
  end

  @doc "How many times `operation` was asked of segments under `directory`, failed calls included."
  @spec count(Path.t(), atom()) :: non_neg_integer()
  def count(directory, operation) do
    prefix = Path.expand(directory)

    :ets.select(@table, [{{{:count, :"$1", operation}, :"$2"}, [], [{{:"$1", :"$2"}}]}])
    |> Enum.filter(fn {scope, _n} -> under?(scope, prefix) end)
    |> Enum.reduce(0, fn {_scope, n}, sum -> sum + n end)
  end

  # --- SegmentStore ---

  @impl true
  def open(directory, segment_id, opts),
    do: intercept(directory, :open, fn -> ElixirStore.open(directory, segment_id, opts) end)

  @impl true
  def recover(directory, segment_id, opts),
    do: intercept(directory, :recover, fn -> ElixirStore.recover(directory, segment_id, opts) end)

  @impl true
  def open_read(directory, segment_id, opts),
    do: intercept(directory, :open_read, fn -> ElixirStore.open_read(directory, segment_id, opts) end)

  @impl true
  def append(handle, records),
    do: intercept(directory_of(handle), :append, fn -> ElixirStore.append(handle, records) end)

  @impl true
  def sync(handle) do
    directory = directory_of(handle)
    if ElixirStore.pending?(handle), do: bump(directory, :flushing_sync)
    intercept(directory, :sync, fn -> ElixirStore.sync(handle) end)
  end

  @impl true
  def read(handle, offset, max_records),
    do: intercept(directory_of(handle), :read, fn -> ElixirStore.read(handle, offset, max_records) end)

  @impl true
  def seal(handle), do: intercept(directory_of(handle), :seal, fn -> ElixirStore.seal(handle) end)

  @impl true
  def verify(directory, segment_id, opts),
    do: intercept(directory, :verify, fn -> ElixirStore.verify(directory, segment_id, opts) end)

  @impl true
  def rebuild_index(directory, segment_id, opts),
    do: intercept(directory, :rebuild_index, fn -> ElixirStore.rebuild_index(directory, segment_id, opts) end)

  @impl true
  def next_offset(handle), do: ElixirStore.next_offset(handle)

  @impl true
  def logical_bytes(handle), do: ElixirStore.logical_bytes(handle)

  @impl true
  def sealed?(handle), do: ElixirStore.sealed?(handle)

  @impl true
  def integrity(handle), do: ElixirStore.integrity(handle)

  @impl true
  def pending?(handle), do: ElixirStore.pending?(handle)

  @impl true
  def should_seal?(handle, now_ms), do: ElixirStore.should_seal?(handle, now_ms)

  @impl true
  def close(handle), do: ElixirStore.close(handle)

  # --- internals ---

  defp intercept(directory, operation, run) do
    directory = Path.expand(directory)
    bump(directory, operation)

    case rule_for(directory, operation) do
      {:ok, reply} -> reply
      :none -> run.()
    end
  end

  defp rule_for(directory, operation) do
    :ets.select(@table, [{{{:rule, :"$1", operation}, :"$2"}, [], [{{:"$1", :"$2"}}]}])
    |> Enum.find(fn {scope, _reply} -> under?(directory, scope) end)
    |> case do
      {_scope, reply} -> {:ok, reply}
      nil -> :none
    end
  end

  defp bump(directory, operation) do
    :ets.update_counter(@table, {:count, Path.expand(directory), operation}, 1, {{:count, nil, nil}, 0})
    :ok
  end

  defp directory_of(handle), do: handle.segment.directory

  # Whether `path` is `prefix` or lies below it. A plain string prefix would let "/tmp/a1" match a rule
  # for "/tmp/a", and two tests with those directories would then fail each other's segments.
  defp under?(path, prefix), do: path == prefix or String.starts_with?(path, prefix <> "/")
end
