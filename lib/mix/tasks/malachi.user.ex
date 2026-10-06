defmodule Mix.Tasks.Malachi.User do
  @shortdoc "Manage Malachi users on a running node (list/create/role/passwd/delete)"

  # The commands, listed once for the moduledoc and for the usage text.
  @commands """
  mix malachi.user list
  mix malachi.user create <username> <password> [--perms produce,consume] [--role viewer]
  mix malachi.user role <username> <viewer|editor|admin|none>
  mix malachi.user passwd <username> <newpassword>
  mix malachi.user delete <username>
  """

  @moduledoc """
  Administer users on a **running** Malachi node from the box, over Erlang distribution (RPC).

  This is the operator-on-the-host surface (the counterpart to a release's `bin/malachi rpc`); for remote
  or programmatic management use the wire ops (`scripts/user.js`) or the dashboard REST API. Because users
  live in the replicated store, a change made through any node propagates cluster-wide.

  #{String.replace(@commands, ~r/^/m, "    ")}
  Options:

    * `--node`: the target node (default `$MALACHI_NODE` or `malachi@127.0.0.1`)
    * `--cookie`: the Erlang cookie (default `$RELEASE_COOKIE`, else `~/.erlang.cookie`)
    * `--perms`: comma-separated wire permissions for `create` (admin|produce|consume; default
      produce,consume; `--perms ""` for none)
    * `--role`: the console role for `create` (viewer|editor|admin, nested; see
      `Malachi.Auth.ConsoleRole`). `role ... none` removes it.

  A console role is what the dashboard and the console allow; a wire permission is what the binary
  protocol allows, and only the wire `admin` implies a console role. Setting a role on a cluster still
  rolling to the release that introduced roles is refused with what to finish first.

  The target node must be **named** (a release, or `iex --name ... -S mix`); an unnamed `mix run` node is
  not reachable. Passwords are passed as plain arguments, so prefer this on a trusted host.
  """
  use Mix.Task

  alias Malachi.Auth.ConsoleRole
  alias Malachi.CLI.Options
  alias Malachi.CLI.Rpc
  alias Malachi.Cluster.MachineVersion

  @invalid_role "invalid console role (allowed: viewer, editor, admin, none)"

  @switches [node: :string, cookie: :string, perms: :string, role: :string]

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
    with {:perms, {:ok, perms}} <- {:perms, Malachi.Auth.parse_permissions(permissions(opts))},
         {:role, {:ok, role}} <- {:role, ConsoleRole.parse(opts[:role])} do
      args = if role, do: [username, password, perms, role], else: [username, password, perms]
      finish(call.(Malachi.Auth, :add_user, args), "created user #{username} #{inspect(perms)}#{role_note(role)}")
    else
      {:perms, :error} -> {:error, "invalid permissions (allowed: admin, produce, consume)"}
      {:role, :error} -> {:error, @invalid_role}
    end
  end

  def execute(["role", username, raw_role], _opts, call) do
    case ConsoleRole.parse(raw_role) do
      {:ok, role} ->
        finish(
          call.(Malachi.Auth, :set_role, [username, role, "mix malachi.user"]),
          "set console role of #{username} to #{raw_role}"
        )

      :error ->
        {:error, @invalid_role}
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
  defp finish({:ok, {:error, reason}}, _message), do: {:error, auth_error(reason)}
  defp finish({:error, reason}, _message), do: {:error, rpc_error(reason)}

  # A version refusal says what to finish; anything else is the reason's name.
  defp auth_error({:unsupported_command, _key, introduced, effective}),
    do: MachineVersion.upgrade_pending_message(introduced, effective)

  defp auth_error(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp auth_error(reason), do: inspect(reason)

  defp permissions(opts), do: (opts[:perms] || "produce,consume") |> String.split(",", trim: true)

  defp role_note(nil), do: ""
  defp role_note(role), do: " with console role #{role}"

  defp format_users([]), do: "(no users)"

  defp format_users(users) do
    users
    |> Enum.sort_by(& &1.username)
    |> Enum.map_join("\n", fn %{username: u, permissions: perms} = user ->
      "#{u}\t[#{perms |> Enum.map_join(", ", &to_string/1)}]#{list_role(Map.get(user, :role))}"
    end)
  end

  defp list_role(nil), do: ""
  defp list_role(role), do: "\trole: #{role}"

  defp rpc_error(reason), do: Rpc.rpc_error(reason)

  defp usage do
    """
    usage:
    #{String.replace(@commands, ~r/^/m, "  ")}
    connection: --node (or $MALACHI_NODE, default malachi@127.0.0.1), --cookie (or $RELEASE_COOKIE)
    """
  end
end
