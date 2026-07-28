defmodule Mix.Tasks.Malachi.User do
  @shortdoc "Manage Malachi users on a running node (list/create/passwd/delete)"

  @moduledoc """
  Administer users on a **running** Malachi node from the box, over Erlang distribution (RPC).

  This is the operator-on-the-host surface (the counterpart to a release's `bin/malachi rpc`); for remote
  or programmatic management use the wire ops (`scripts/user.js`) or the dashboard REST API. Because users
  live in the replicated store, a change made through any node propagates cluster-wide.

      mix malachi.user list
      mix malachi.user create <username> <password> [--perms produce,consume]
      mix malachi.user passwd <username> <newpassword>
      mix malachi.user delete <username>

  Options:

    * `--node`: the target node (default `$MALACHI_NODE` or `malachi@127.0.0.1`)
    * `--cookie`: the Erlang cookie (default `$RELEASE_COOKIE`, else `~/.erlang.cookie`)
    * `--perms`: comma-separated permissions for `create` (admin|produce|consume; default produce,consume)

  The target node must be **named** (a release, or `iex --name ... -S mix`); an unnamed `mix run` node is
  not reachable. Passwords are passed as plain arguments, so prefer this on a trusted host.
  """
  use Mix.Task

  alias Malachi.CLI.Rpc

  @switches [node: :string, cookie: :string, perms: :string]

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
  The testable core: maps a parsed command to an `Auth` call through `call` (a `(module, fun, args ->
  {:ok, value} | {:error, reason})` seam that in production is an RPC to the target node). Returns
  `{:ok, message}` / `{:error, message}` for the caller to print.
  """
  @spec execute([String.t()], keyword(), (module(), atom(), list() -> {:ok, term()} | {:error, term()})) ::
          {:ok, String.t()} | {:error, String.t()}
  def execute(["list"], _opts, call) do
    case call.(Malachi.Auth, :list_users, []) do
      {:ok, users} -> {:ok, format_users(users)}
      {:error, reason} -> {:error, rpc_error(reason)}
    end
  end

  def execute(["create", username, password], opts, call) do
    case Malachi.Auth.parse_permissions(permissions(opts)) do
      {:ok, perms} ->
        finish(
          call.(Malachi.Auth, :add_user, [username, password, perms]),
          "created user #{username} #{inspect(perms)}"
        )

      :error ->
        {:error, "invalid permissions (allowed: admin, produce, consume)"}
    end
  end

  def execute(["passwd", username, new_password], _opts, call) do
    finish(call.(Malachi.Auth, :change_password, [username, new_password]), "changed password for #{username}")
  end

  def execute(["delete", username], _opts, call) do
    finish(call.(Malachi.Auth, :remove_user, [username]), "deleted user #{username}")
  end

  def execute(_args, _opts, _call), do: {:error, usage()}

  # --- result handling ---

  # `call` returns `{:ok, machine_reply}` (the Auth function's return) or `{:error, rpc_reason}`. The Auth
  # reply is `:ok` on success or `{:error, reason}` (e.g. :user_exists / :user_not_found).
  defp finish({:ok, :ok}, message), do: {:ok, message}
  defp finish({:ok, {:error, reason}}, _message), do: {:error, to_string(reason)}
  defp finish({:error, reason}, _message), do: {:error, rpc_error(reason)}

  defp permissions(opts), do: (opts[:perms] || "produce,consume") |> String.split(",", trim: true)

  defp format_users([]), do: "(no users)"

  defp format_users(users) do
    users
    |> Enum.sort_by(& &1.username)
    |> Enum.map_join("\n", fn %{username: u, permissions: perms} ->
      "#{u}\t[#{perms |> Enum.map_join(", ", &to_string/1)}]"
    end)
  end

  defp rpc_error(reason), do: Rpc.rpc_error(reason)

  defp usage do
    """
    usage:
      mix malachi.user list
      mix malachi.user create <username> <password> [--perms produce,consume]
      mix malachi.user passwd <username> <newpassword>
      mix malachi.user delete <username>

    connection: --node (or $MALACHI_NODE, default malachi@127.0.0.1), --cookie (or $RELEASE_COOKIE)
    """
  end
end
