defmodule Mix.Tasks.Malachi.Ring do
  @shortdoc "Show the durable vnode ring recorded for a running cluster"

  @moduledoc """
  Prints the cluster's **durable ring**: the vnode topology of record, read from a **running** Malachi
  node over Erlang distribution (RPC).

      mix malachi.ring --show

  The ring outranks `MALACHI_LOG_VNODES` at boot, because a ring grown by re-sharding no longer matches
  the environment's even geometry and honouring the environment would orphan the migrated metadata. So
  when a node logs that it ignored the environment, this is the command that says what it used instead:
  the recorded version, the vnodes and their tokens, and where each one's `ra` cluster lives.

  A **pending** line means a vnode split was interrupted; the node holding the lease carries it to
  completion, and it survives a full-cluster restart because the intent is part of the recorded ring.

  Options:

    * `--show`: print the ring (the default, and currently the only action)
    * `--node`: the target node (default `$MALACHI_NODE` or `malachi@127.0.0.1`)
    * `--cookie`: the Erlang cookie (default `$RELEASE_COOKIE`, else `~/.erlang.cookie`)

  The target node must be **named** and running a **clustered** control plane; an unclustered node has
  no ring store to read.
  """
  use Mix.Task

  alias Malachi.CLI.Options
  alias Malachi.CLI.Rpc
  alias Malachi.Cluster.HashRing
  alias Malachi.Cluster.Ring
  alias Malachi.Cluster.RingTopology

  @ring Malachi.LogRing
  @switches [node: :string, cookie: :string, show: :boolean]

  @impl Mix.Task
  def run(argv) do
    case Options.parse(argv, @switches) do
      # refuse before resolving or connecting: an unknown option is absent from `opts`, so falling
      # through would target $MALACHI_NODE (or the default) as though it had been asked for
      {:ok, {opts, args}} -> connect_and_show(args, opts)
      {:error, message} -> Mix.raise(message <> "\n\n" <> usage())
    end
  end

  defp connect_and_show(args, opts) do
    node = Rpc.target_node(opts)

    case Rpc.connect(node, opts[:cookie]) do
      :ok -> report(execute(args, node, Rpc.rpc(node)))
      {:error, message} -> Mix.raise(message)
    end
  end

  defp report({:ok, message}), do: Mix.shell().info(message)

  defp report({:error, message}) do
    Mix.shell().error(message)
    exit({:shutdown, 1})
  end

  @doc """
  The testable core: reads the ring store on `node` through `call` (a
  `(module, fun, args -> {:ok, value} | {:error, reason})` seam that in production is an RPC to the
  target node) and renders it. Returns `{:ok, text}` / `{:error, message}` for the caller to print.

  Stray positional arguments return usage rather than being ignored.
  """
  @spec execute([String.t()], node(), (module(), atom(), list() -> {:ok, term()} | {:error, term()})) ::
          {:ok, String.t()} | {:error, String.t()}
  def execute([], node, call) do
    case call.(Malachi.Cluster.RingServer, :get, [{@ring, node}]) do
      {:ok, {:ok, %Ring{} = ring}} -> {:ok, render(ring)}
      {:ok, {:error, reason}} -> {:error, ring_error(reason)}
      {:error, reason} -> {:error, Rpc.rpc_error(reason)}
    end
  end

  def execute(_args, _node, _call), do: {:error, usage()}

  defp render(%Ring{} = ring) do
    case Ring.topology(ring) do
      :none ->
        "no durable ring recorded: this cluster has never been sharded, so MALACHI_LOG_VNODES still decides"

      {:ok, topology} ->
        Enum.join(
          [header(ring, topology), "", vnode_lines(topology), pending_line(topology)]
          |> List.flatten()
          |> Enum.reject(&is_nil/1),
          "\n"
        )
    end
  end

  defp header(ring, topology) do
    "durable ring: version #{topology.version}, fence #{ring.fence}, " <>
      "#{HashRing.size(topology.ring)} vnodes (this wins over MALACHI_LOG_VNODES at boot)"
  end

  defp vnode_lines(topology) do
    for {vnode_id, token, nodes} <- RingTopology.vnode_placement(topology) do
      "  #{token}\t#{vnode_id}\t#{Enum.map_join(nodes, ",", &to_string/1)}"
    end
  end

  defp pending_line(%{pending: nil}), do: nil

  defp pending_line(%{pending: pending}) do
    "\npending split: #{pending.new_vnode} at token #{pending.token} on " <>
      "#{Enum.map_join(pending.nodes, ",", &to_string/1)} (the lease holder will carry it to completion)"
  end

  defp ring_error(:timeout), do: "the ring store did not answer in time (is a quorum of nodes up?)"

  defp ring_error(reason) do
    "could not read the ring store (#{inspect(reason)}); is this node running a clustered control plane?"
  end

  defp usage do
    """
    usage:
      mix malachi.ring --show

    prints the durable vnode ring recorded for the cluster: version, vnodes and their placements.
    the recorded ring outranks MALACHI_LOG_VNODES at boot, so this is what a node actually routes by.

    connection: --node (or $MALACHI_NODE, default malachi@127.0.0.1), --cookie (or $RELEASE_COOKIE)
    """
  end
end
