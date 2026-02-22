defmodule MalachiMQ.Test.MassSpawnHelper do
  @moduledoc """
  Helpers to spawn sharded logical subscribers for large-scale channel tests.

  Each spawned process represents `logical_per_process` subscribers. On receive,
  the process increments an ETS counter for the channel by `logical_per_process`.

  ## Channel Name Limits

  Channel names are limited to 100 bytes to keep ETS keys manageable and prevent
  excessive memory usage in the shared `:test_deliveries` table.

  ## Cleanup Functions

  - `cleanup_shards/1` - Cleans up spawned processes and ETS table for a specific test
  - `cleanup_all/0` - Removes all test delivery tables (useful for orphaned tables)
  """

  require Logger

  def start_shards(channel, shard_count, logical_per_process)
      when is_binary(channel) and byte_size(channel) <= 100 do
    check_atom_limit()

    # Create ETS table only if it doesn't exist (idempotent)
    case :ets.whereis(:test_deliveries) do
      :undefined ->
        :ets.new(:test_deliveries, [:named_table, :public, read_concurrency: true, write_concurrency: true])

      _table_ref ->
        :ok
    end

    :ets.insert(:test_deliveries, {{:delivered, channel}, 0})

    pids =
      1..shard_count
      |> Task.async_stream(
        fn _i ->
          pid = spawn_link(fn -> shard_loop(channel, logical_per_process) end)
          pid
        end,
        max_concurrency: 1000,
        timeout: :infinity
      )
      |> Enum.map(fn {:ok, pid} -> pid end)

    Logger.info("Spawned #{length(pids)} shard processes for channel '#{channel}'")
    {:ok, pids}
  end

  def start_shards(channel, _shard_count, _logical_per_process) do
    raise ArgumentError,
          "Channel name too long: #{byte_size(channel)} bytes (max: 100). " <>
            "Limit keeps ETS keys manageable."
  end

  defp shard_loop(channel, logical_per_process) do
    # subscribe this process to channel
    MalachiMQ.Channel.subscribe(channel, self())

    receive do
      {:channel_message, _message} ->
        increment_delivered(channel, logical_per_process)
        shard_loop(channel, logical_per_process)

      {:kicked_from_channel, _chan} ->
        :ok

      _ ->
        shard_loop(channel, logical_per_process)
    end
  end

  defp increment_delivered(channel, amount) do
    key = {:delivered, channel}

    try do
      :ets.update_counter(:test_deliveries, key, {2, amount}, {key, 0})
    rescue
      ArgumentError ->
        :ets.insert(:test_deliveries, {key, amount})
    end
  end

  def delivered_count(channel) do
    case :ets.lookup(:test_deliveries, {:delivered, channel}) do
      [{{:delivered, ^channel}, value}] -> value
      _ -> 0
    end
  end

  @doc """
  Cleans up spawned shard processes and the ETS table.

  This should be called in `on_exit/1` callbacks after test assertions complete
  to ensure proper resource cleanup.
  """
  def cleanup_shards(pids) when is_list(pids) do
    # Kill all alive processes
    alive_count =
      Enum.reduce(pids, 0, fn pid, acc ->
        if Process.alive?(pid) do
          Process.exit(pid, :kill)
          acc + 1
        else
          acc
        end
      end)

    # Delete the ETS table if it exists
    if :ets.whereis(:test_deliveries) != :undefined do
      :ets.delete(:test_deliveries)
    end

    Logger.info("Cleaned up #{alive_count}/#{length(pids)} shard processes")
    :ok
  end

  @doc """
  Removes all test delivery tables matching the "test_deliveries*" pattern.

  Useful for cleaning up orphaned tables from failed test runs.
  Call this in test setup to ensure a clean state.
  """
  def cleanup_all do
    :ets.all()
    |> Enum.filter(&matches_test_deliveries?/1)
    |> Enum.each(&safe_delete/1)

    :ok
  end

  # Private helper functions

  defp check_atom_limit do
    atom_count = :erlang.system_info(:atom_count)
    atom_limit = :erlang.system_info(:atom_limit)
    usage_percent = div(atom_count * 100, atom_limit)

    if usage_percent >= 80 do
      Logger.warning(
        "Atom table at #{usage_percent}% capacity (#{atom_count}/#{atom_limit}). " <>
          "Consider reducing test channel name variety."
      )
    end
  end

  defp matches_test_deliveries?(table) do
    case :ets.info(table, :name) do
      :undefined ->
        false

      name when is_atom(name) ->
        name_str = Atom.to_string(name)
        String.starts_with?(name_str, "test_deliveries")

      _ ->
        false
    end
  end

  defp safe_delete(table) do
    :ets.delete(table)
  rescue
    ArgumentError -> :ok
  end
end
