defmodule Malachi.DataPlaneRouter do
  @moduledoc """
  Single source of truth for data-plane sharding: which `BrokerServer` owns a topic.

  By default there is one shard (`Malachi.LogBroker`) and every topic maps to it, so routing is a pure
  no-op and a deployment is byte-identical to one that never knew about sharding. Setting
  `MALACHI_DATA_SHARDS` to N > 1 (single-node only, gated in `Malachi.Application`) runs N independent
  in-memory BrokerServer shards; a topic is pinned to one shard by `:erlang.phash2/2`, so every operation
  for that topic (create, produce, fetch, consume) lands on the same shard and same-key ordering is
  preserved.

  This is an experimental measurement mode for quantifying how far parallel brokers lift the networked
  throughput ceiling (the single serial `BrokerServer` mailbox), not a production feature: consumer-group
  coordination, retention, and the dashboard still target shard 0 only.
  """

  # Shard 0 keeps the historical name, so a single-shard deployment is unchanged and every existing
  # reference to `Malachi.LogBroker` still resolves.
  @base Malachi.LogBroker

  @doc "The configured shard count (always >= 1)."
  @spec shard_count() :: pos_integer()
  def shard_count, do: max(1, Application.get_env(:malachi, :data_shards, 1))

  @doc "The registered name of shard `index` (0-based). Shard 0 is `Malachi.LogBroker`."
  @spec shard_name(non_neg_integer()) :: module()
  def shard_name(0), do: @base
  def shard_name(index) when is_integer(index) and index > 0, do: Module.concat(@base, Integer.to_string(index))

  @doc """
  The BrokerServer name that owns `topic`. With one shard this is always `Malachi.LogBroker`, so callers
  can route through it unconditionally on the hot path.
  """
  @spec shard_for(String.t()) :: module()
  def shard_for(topic), do: shard_name(:erlang.phash2(topic, shard_count()))

  @doc """
  The `{shard_name, data_dir}` pairs to supervise, given the base log directory. One shard keeps the base
  directory as-is; multiple shards each get an isolated `shard_<i>` subdirectory (siblings, so no shard
  recovers or enumerates another's segments).
  """
  @spec shards(String.t()) :: [{module(), String.t()}]
  def shards(base_dir) do
    case shard_count() do
      1 -> [{shard_name(0), base_dir}]
      n -> for index <- 0..(n - 1), do: {shard_name(index), Path.join(base_dir, "shard_#{index}")}
    end
  end
end
