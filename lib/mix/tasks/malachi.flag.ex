defmodule Mix.Tasks.Malachi.Flag do
  @shortdoc "List the cluster's feature flags, or switch one on, on a running node"

  @moduledoc """
  Reads and switches on the cluster's **feature flags** on a **running** Malachi node, over Erlang
  distribution (RPC).

      mix malachi.flag --list
      mix malachi.flag enable batch_format

  A flag is the moment an operator commits to a feature that changes a format or a protocol. It is
  separate from finishing a rolling upgrade on purpose: while every node runs the new build and no flag
  is on, rolling the build back is still free.

  Switching one on is **refused** unless every node in `MALACHI_LOG_NODES` is alive and advertises the
  capability the flag names, and the refusal says which nodes do not. Upgrade them, or take them out of
  the configured set, and run the command again. There is no command to switch a flag off: once a
  feature is on, peers and stored data may already be in the new shape, and going back is a migration.

  Options:

    * `--list`: print the flags this build knows and which are on (no positional argument)
    * `--node`: the target node (default `$MALACHI_NODE` or `malachi@127.0.0.1`)
    * `--cookie`: the Erlang cookie (default `$RELEASE_COOKIE`, else `~/.erlang.cookie`)
  """
  use Mix.Task

  alias Malachi.CLI.Options
  alias Malachi.CLI.Rpc

  @switches [node: :string, cookie: :string, list: :boolean]

  @impl Mix.Task
  def run(argv) do
    case Options.parse(argv, @switches) do
      # refuse before resolving or connecting: an unknown option is absent from `opts`, so falling
      # through would target $MALACHI_NODE (or the default) as though it had been asked for
      {:ok, {opts, args}} -> connect_and_run(args, opts)
      {:error, message} -> Mix.raise(message <> "\n\n" <> usage())
    end
  end

  defp connect_and_run(args, opts) do
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
  The testable core: lists or enables through `call` (a `(module, fun, args -> {:ok, value} |
  {:error, reason})` seam that in production is an RPC to the target node). Returns `{:ok, message}` /
  `{:error, message}` for the caller to print.
  """
  @spec execute([String.t()], keyword(), (module(), atom(), list() -> {:ok, term()} | {:error, term()})) ::
          {:ok, String.t()} | {:error, String.t()}
  def execute([], opts, call) do
    if opts[:list], do: list(call), else: {:error, usage()}
  end

  def execute(["enable", name], opts, call) do
    # `--list` and `enable` are two different commands, and the invocation asks for both. Resolving it
    # in favour of `enable` would switch a flag on because of a switch the operator meant as a read,
    # and a flag is never switched back off, so it is refused before the RPC instead.
    if opts[:list] do
      {:error, usage()}
    else
      finish(call.(Malachi.Application, :enable_cluster_flag, [name]), "cluster flag enabled: #{name}")
    end
  end

  def execute(_args, _opts, _call), do: {:error, usage()}

  defp list(call) do
    case call.(Malachi.Application, :cluster_flags, []) do
      {:ok, {:ok, flags}} -> {:ok, render(flags)}
      {:ok, {:error, reason}} -> {:error, flag_error(reason)}
      {:error, reason} -> {:error, Rpc.rpc_error(reason)}
    end
  end

  # An empty registry is the normal state of the bridge release: it ships the gate, and the first flag
  # arrives with the first feature that needs one. Saying so beats printing an empty list.
  defp render(%{known: []}), do: "this build knows no cluster flags yet"

  defp render(%{known: known, enabled: enabled}) do
    Enum.map_join(known, "\n", fn flag ->
      state = if flag in enabled, do: "on", else: "off"
      "#{flag}\t#{state}"
    end)
  end

  # `call` returns `{:ok, node_reply}` (`:ok` or `{:error, reason}`) or `{:error, rpc_reason}`.
  defp finish({:ok, :ok}, message), do: {:ok, message}
  defp finish({:ok, {:error, reason}}, _message), do: {:error, flag_error(reason)}
  defp finish({:error, reason}, _message), do: {:error, Rpc.rpc_error(reason)}

  defp flag_error(:unknown_flag),
    do: "this build knows no flag by that name (run `mix malachi.flag --list`)"

  defp flag_error({:unsupported, nodes}) do
    "these nodes are not alive or do not support that flag: #{Enum.map_join(nodes, ", ", &to_string/1)}. " <>
      "Upgrade them, or take them out of MALACHI_LOG_NODES, then run this again"
  end

  defp flag_error({:unsupported_command, _key, introduced, effective}) do
    "the control plane is still at machine version #{effective} and this command needs #{introduced}; " <>
      "finish the rolling upgrade (see the operations guide on the machine version pin)"
  end

  defp flag_error(:timeout), do: "the flag store did not answer in time (is a quorum of nodes up?)"
  defp flag_error(reason), do: inspect(reason)

  defp usage do
    """
    usage:
      mix malachi.flag --list
      mix malachi.flag enable <flag>

    lists the cluster's feature flags, or switches one on once every configured node advertises the
    capability it names. a flag is never switched back off.

    connection: --node (or $MALACHI_NODE, default malachi@127.0.0.1), --cookie (or $RELEASE_COOKIE)
    """
  end
end
