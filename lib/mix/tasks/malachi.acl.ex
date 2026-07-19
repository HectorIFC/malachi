defmodule Mix.Tasks.Malachi.Acl do
  @shortdoc "Manage per-topic ACLs on a running node (list/grant/revoke)"

  @moduledoc """
  Administer per-topic ACLs on a **running** Malachi node from the box, over Erlang distribution (RPC).

  This is the operator-on-the-host surface (the counterpart to a release's `bin/malachi rpc`); for remote or
  programmatic management use the wire ops (`scripts/acl.js`) or the dashboard REST API. Because ACLs live in
  the replicated store, a change made through any node propagates cluster-wide.

      mix malachi.acl list <username>
      mix malachi.acl grant <username> <operation> <pattern>
      mix malachi.acl revoke <username> <operation> <pattern>

  `operation` is `produce` or `consume`; `pattern` is an exact topic (`orders.eu`) or a `*`-suffixed prefix
  (`orders.*` = every topic starting with `orders.`). ACLs are enforced when `MALACHIMQ_ACL_STRICT` is on;
  otherwise a user's global produce/consume permission already grants every topic and ACLs only add access.

  Options:

    * `--node`   — the target node (default `$MALACHI_NODE` or `malachi@127.0.0.1`)
    * `--cookie` — the Erlang cookie (default `$RELEASE_COOKIE`, else `~/.erlang.cookie`)

  The target node must be **named** (a release, or `iex --name ... -S mix`); an unnamed `mix run` node is not
  reachable.
  """
  use Mix.Task

  alias Malachi.CLI.Rpc

  @switches [node: :string, cookie: :string]

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
  The testable core: maps a parsed command to an `Auth` ACL call through `call` (a `(module, fun, args ->
  {:ok, value} | {:error, reason})` seam that in production is an RPC to the target node). Returns
  `{:ok, message}` / `{:error, message}` for the caller to print.
  """
  @spec execute([String.t()], keyword(), (module(), atom(), list() -> {:ok, term()} | {:error, term()})) ::
          {:ok, String.t()} | {:error, String.t()}
  def execute(["list", username], _opts, call) do
    case call.(Malachi.Auth, :list_acls, [username]) do
      {:ok, acls} -> {:ok, format_acls(acls)}
      {:error, reason} -> {:error, Rpc.rpc_error(reason)}
    end
  end

  def execute(["grant", username, operation, pattern], _opts, call) do
    acl_command(call, :grant_acl, username, operation, pattern, "granted #{operation} on #{pattern} to #{username}")
  end

  def execute(["revoke", username, operation, pattern], _opts, call) do
    acl_command(call, :revoke_acl, username, operation, pattern, "revoked #{operation} on #{pattern} from #{username}")
  end

  def execute(_args, _opts, _call), do: {:error, usage()}

  # Parses the operation string locally (atom-safe), then runs the Auth ACL call through the seam.
  defp acl_command(call, fun, username, operation, pattern, message) do
    case Malachi.Auth.parse_acl_operation(operation) do
      {:ok, op} -> finish(call.(Malachi.Auth, fun, [username, op, pattern]), message)
      :error -> {:error, "invalid operation (allowed: produce, consume)"}
    end
  end

  # `call` returns `{:ok, auth_reply}` (`:ok` or `{:error, reason}`) or `{:error, rpc_reason}`.
  defp finish({:ok, :ok}, message), do: {:ok, message}
  defp finish({:ok, {:error, reason}}, _message), do: {:error, to_string(reason)}
  defp finish({:error, reason}, _message), do: {:error, Rpc.rpc_error(reason)}

  defp format_acls([]), do: "(no acls)"

  defp format_acls(acls) do
    acls
    |> Enum.sort_by(fn %{operation: op, resource: resource} -> {to_string(op), resource} end)
    |> Enum.map_join("\n", fn %{operation: op, resource: resource} -> "#{op}\t#{resource}" end)
  end

  defp usage do
    """
    usage:
      mix malachi.acl list <username>
      mix malachi.acl grant <username> <operation> <pattern>
      mix malachi.acl revoke <username> <operation> <pattern>

    operation: produce | consume    pattern: a topic (orders.eu) or a *-suffixed prefix (orders.*)
    connection: --node (or $MALACHI_NODE, default malachi@127.0.0.1), --cookie (or $RELEASE_COOKIE)
    """
  end
end
