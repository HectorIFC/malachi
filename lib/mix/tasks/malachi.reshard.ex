defmodule Mix.Tasks.Malachi.Reshard do
  @shortdoc "Grow the metadata sharding to a target vnode count on a running node"

  @moduledoc """
  Grows the cluster's **metadata sharding**: the number of vnodes (Raft-backed metadata shards) - on a
  **running** Malachi node, over Erlang distribution (RPC).

      mix malachi.reshard --to 16

  Each additional vnode is created by one **split** of the vnode owning the largest hash arc: a new `ra`
  cluster is started and the displaced topics' metadata is migrated to it (fenced, copy-first), then the
  versioned ring is published and gossiped so every node routes to the new shard. Splits run **one at a
  time**, driven only by the node holding the cluster lease.

  Growing is **resumable**: if a reshard is interrupted, re-run the same `--to` target and it continues from
  the current ring: the splits already done are reflected in it. Only **growing** is supported; a target
  below the current count is rejected.

  > #### Runtime operation {: .warning}
  > The ring is gossiped cluster state, not durable across a **full-cluster** restart (it reseeds from
  > `MALACHIMQ_LOG_VNODES`). Treat a reshard as effective while the cluster is up.

  Options:

    * `--to`: the target vnode count (required; must exceed the current count)
    * `--node`: the target node (default `$MALACHI_NODE` or `malachi@127.0.0.1`)
    * `--cookie`: the Erlang cookie (default `$RELEASE_COOKIE`, else `~/.erlang.cookie`)

  The target node must be **named** and running a **sharded** control plane (`MALACHIMQ_LOG_VNODES` > 1 with
  a clustered log); otherwise there is no ring to grow.
  """
  use Mix.Task

  alias Malachi.CLI.Rpc

  @coordinator Malachi.LogReshardCoordinator
  @switches [node: :string, cookie: :string, to: :integer]

  @impl Mix.Task
  def run(argv) do
    {opts, args, _invalid} = OptionParser.parse(argv, strict: @switches)
    node = Rpc.target_node(opts)

    case Rpc.connect(node, opts[:cookie]) do
      :ok -> report(execute(args, opts, Rpc.rpc(node)))
      {:error, message} -> Mix.raise(message)
    end
  end

  defp report({:ok, message}), do: Mix.shell().info(message)

  defp report({:error, message}) do
    Mix.shell().error(message)
    exit({:shutdown, 1})
  end

  @doc """
  The testable core: asks the remote `ReshardCoordinator` to grow to `--to` through `call` (a
  `(module, fun, args -> {:ok, value} | {:error, reason})` seam that in production is an RPC to the target
  node). Returns `{:ok, message}` / `{:error, message}` for the caller to print.
  """
  @spec execute([String.t()], keyword(), (module(), atom(), list() -> {:ok, term()} | {:error, term()})) ::
          {:ok, String.t()} | {:error, String.t()}
  def execute([], opts, call) do
    case opts[:to] do
      target when is_integer(target) and target > 0 ->
        finish(
          call.(Malachi.Cluster.ReshardCoordinator, :reshard, [@coordinator, target]),
          "resharded: the ring now has #{target} vnodes"
        )

      _missing_or_invalid ->
        {:error, usage()}
    end
  end

  def execute(_args, _opts, _call), do: {:error, usage()}

  # `call` returns `{:ok, coordinator_reply}` (`:ok` or `{:error, reason}`) or `{:error, rpc_reason}`.
  defp finish({:ok, :ok}, message), do: {:ok, message}
  defp finish({:ok, {:error, reason}}, _message), do: {:error, reshard_error(reason)}
  defp finish({:error, reason}, _message), do: {:error, Rpc.rpc_error(reason)}

  defp reshard_error(:not_leader), do: "this node does not hold the cluster lease (try another node)"
  defp reshard_error(:no_topology), do: "the cluster has no ring yet (is the control plane sharded?)"
  defp reshard_error(:cannot_shrink), do: "the target is below the current vnode count (growing only)"
  defp reshard_error(reason), do: inspect(reason)

  defp usage do
    """
    usage:
      mix malachi.reshard --to <target_vnode_count>

    grows the metadata sharding to the target count (one vnode split at a time, lease-gated).
    re-run the same target to resume an interrupted reshard; shrinking is not supported.

    connection: --node (or $MALACHI_NODE, default malachi@127.0.0.1), --cookie (or $RELEASE_COOKIE)
    """
  end
end
