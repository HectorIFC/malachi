defmodule Malachi.Auth.UserStore do
  @moduledoc """
  The cluster's user store: a thin **facade** over the ra-replicated user cluster
  (`Malachi.Auth.UserServer`). Replaces the old node-local Mnesia store: users now replicate across the
  cluster, so a user created on one node exists on every node.

  Stateless: the cluster name is fixed and the local server id is `{name, node()}`. `Malachi.Application`
  starts the ra cluster and a reconciler at boot; this module just routes CRUD to it: **writes** go through
  the log (consensus); **reads** come from the local replica (`:ra.local_query`, fast and eventually
  consistent). The public API (`insert_user`/`set_role`/`delete_user`/`update_password`/`get_user`/
  `get_principal`/`list_users`/`export_users`/`import_users`) keeps `Malachi.Auth` and its callers agnostic
  to the backend.

  The console role arrived at machine version 5 (`Malachi.Auth.UserRegistry`). A write that carries no role
  uses the older command shape, so creating or importing a user without a role keeps working on a cluster
  still mid upgrade; only a write that carries a role can meet the version refusal.
  """

  alias Malachi.Auth.ConsoleRole
  alias Malachi.Auth.UserServer

  # The dedicated ra cluster's name (see Malachi.Application). Reads/writes address the local member.
  @cluster Malachi.LogUsers

  @doc "The user cluster's ra cluster name."
  @spec cluster_name() :: atom()
  def cluster_name, do: @cluster

  @doc """
  Inserts a new user, holding console `role` when it is not `nil`. Returns `:ok`, `{:error, :user_exists}`,
  `{:error, :invalid_role}`, a version refusal (see `Malachi.Cluster.MachineVersion.refusal?/1`) for a role
  on a cluster below version 5, or `{:error, reason}` when the store is unreachable.
  """
  @spec insert_user(String.t(), String.t(), [atom()], ConsoleRole.t() | nil) :: :ok | {:error, term()}
  def insert_user(username, password_hash, permissions, role \\ nil)

  def insert_user(username, password_hash, permissions, nil) do
    unwrap(UserServer.put_user(server_id(), username, password_hash, permissions))
  end

  def insert_user(username, password_hash, permissions, role) do
    unwrap(UserServer.put_user(server_id(), username, password_hash, permissions, role))
  end

  @doc """
  Sets a user's console role, or removes it with `nil`. Returns `:ok`, `{:error, :user_not_found}`,
  `{:error, :invalid_role}`, a version refusal, or `{:error, reason}` when the store is unreachable.
  """
  @spec set_role(String.t(), ConsoleRole.t() | nil) :: :ok | {:error, term()}
  def set_role(username, role), do: unwrap(UserServer.set_role(server_id(), username, role))

  @doc "Deletes a user (idempotent). Returns `:ok`."
  @spec delete_user(String.t()) :: :ok | {:error, atom()}
  def delete_user(username), do: unwrap(UserServer.delete_user(server_id(), username))

  @doc "Updates a user's password hash. Returns `:ok` or `{:error, :user_not_found}`."
  @spec update_password(String.t(), String.t()) :: :ok | {:error, atom()}
  def update_password(username, new_password_hash) do
    unwrap(UserServer.update_password(server_id(), username, new_password_hash))
  end

  @doc "Reads a user as `{username, hash, permissions}` from the local replica, or `{:error, :user_not_found}`."
  @spec get_user(String.t()) :: {:ok, {String.t(), String.t(), [atom()]}} | {:error, atom()}
  def get_user(username), do: UserServer.get_user(server_id(), username)

  @doc """
  Reads a user's permissions and console role (no hash) from the local replica as
  `%{username, permissions, role}`, or `{:error, :user_not_found}`.
  """
  @spec get_principal(String.t()) :: {:ok, map()} | {:error, term()}
  def get_principal(username), do: UserServer.get_principal(server_id(), username)

  @doc "Lists all users (no hashes) as `%{username, permissions, role}` maps."
  @spec list_users() :: [map()]
  def list_users do
    case UserServer.list_users(server_id()) do
      {:ok, users} -> users
      {:error, _reason} -> []
    end
  end

  @doc "Exports all users (no hashes) as JSON-serializable maps. Returns `{:ok, list}`."
  @spec export_users() :: {:ok, [map()]} | {:error, atom()}
  def export_users do
    case UserServer.export_users(server_id()) do
      {:ok, users} -> {:ok, users}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Imports users from a list of maps (each `:username`, `:password_hash`, `:permissions` and an optional
  `:role`; string keys also accepted, so an `export_users/0` round trips). Existing and malformed entries,
  including an unknown role, are skipped. When no entry carries a role the older command shape is used, so
  an import without roles works on a cluster still below version 5. Returns
  `{:ok, %{imported: n, skipped: n}}`, a version refusal, or `{:error, reason}`.
  """
  @spec import_users([map()]) :: {:ok, map()} | {:error, term()}
  def import_users(users) when is_list(users) do
    {command, skipped_here} = import_command(users)

    reply =
      case command do
        {:import_users, entries} -> UserServer.import_users(server_id(), entries)
        {:import_users_with_roles, entries} -> UserServer.import_users_with_roles(server_id(), entries)
      end

    case reply do
      {:ok, {:ok, counts}} -> {:ok, %{counts | skipped: counts.skipped + skipped_here}}
      {:ok, other} -> other
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  The registry command an import of `users` (the maps `import_users/1` takes) is sent as, and how many
  entries were skipped before it for an unknown role. An entry with an unknown role is dropped here, before
  the command is chosen, so it cannot push an import that is otherwise free of roles onto the version 5
  command, which a cluster still below 5 refuses whole. With no role left, the command is the version 0
  `{:import_users, entries}`.
  """
  @spec import_command([map()]) ::
          {{:import_users, [tuple()]} | {:import_users_with_roles, [tuple()]}, non_neg_integer()}
  def import_command(users) when is_list(users) do
    {entries, bad_roles} = users |> Enum.map(&to_entry/1) |> Enum.split_with(&ConsoleRole.valid?(elem(&1, 3)))

    command =
      if Enum.all?(entries, &match?({_username, _hash, _permissions, nil}, &1)),
        do: {:import_users, Enum.map(entries, fn {u, h, p, nil} -> {u, h, p} end)},
        else: {:import_users_with_roles, entries}

    {command, length(bad_roles)}
  end

  # -- Private --

  defp server_id, do: {@cluster, node()}

  # Unwraps a UserServer command result: the machine reply (`:ok`, `{:error, :user_exists | :user_not_found |
  # :invalid_role}` or a version refusal), or a transport error passed through.
  defp unwrap({:ok, machine_reply}), do: machine_reply
  defp unwrap({:error, reason}), do: {:error, reason}

  # Normalizes an import map to a `{username, hash, permissions, role}` entry. A non-binary username or hash
  # is left for the registry to skip; an unknown role becomes `:invalid`, which `import_command/1` drops and
  # counts as skipped before the command is chosen.
  defp to_entry(user) do
    username = Map.get(user, :username) || Map.get(user, "username")
    hash = Map.get(user, :password_hash) || Map.get(user, "password_hash")

    permissions =
      (Map.get(user, :permissions) || Map.get(user, "permissions") || []) |> Enum.map(&normalize_permission/1)

    {username, hash, permissions, normalize_role(Map.get(user, :role) || Map.get(user, "role"))}
  end

  # An unknown role, or a value that is not a role at all, becomes `:invalid`, which the import skips; never
  # a new atom.
  defp normalize_role(role) when is_binary(role) or is_nil(role) do
    case ConsoleRole.parse(role) do
      {:ok, parsed} -> parsed
      :error -> :invalid
    end
  end

  defp normalize_role(role), do: if(ConsoleRole.valid?(role), do: role, else: :invalid)

  defp normalize_permission(perm) when is_atom(perm), do: perm
  defp normalize_permission(perm) when is_binary(perm), do: String.to_existing_atom(perm)
end
