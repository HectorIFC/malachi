defmodule Malachi.Auth.UserStore do
  @moduledoc """
  The cluster's user store: a thin **facade** over the ra-replicated user cluster
  (`Malachi.Auth.UserServer`). Replaces the old node-local Mnesia store: users now replicate across the
  cluster, so a user created on one node exists on every node.

  Stateless: the cluster name is fixed and the local server id is `{name, node()}`. `Malachi.Application`
  starts the ra cluster and a reconciler at boot; this module just routes CRUD to it: **writes** go through
  the log (consensus); **reads** come from the local replica (`:ra.local_query`, fast and eventually
  consistent). The public API (`insert_user`/`delete_user`/`update_password`/`get_user`/`list_users`/
  `export_users`/`import_users`) is unchanged, so `Malachi.Auth` and its callers are agnostic to the backend.
  """

  alias Malachi.Auth.UserServer

  # The dedicated ra cluster's name (see Malachi.Application). Reads/writes address the local member.
  @cluster Malachi.LogUsers

  @doc "The user cluster's ra cluster name."
  @spec cluster_name() :: atom()
  def cluster_name, do: @cluster

  @doc "Inserts a new user. Returns `:ok` or `{:error, :user_exists}` / `{:error, reason}`."
  @spec insert_user(String.t(), String.t(), [atom()]) :: :ok | {:error, atom()}
  def insert_user(username, password_hash, permissions) do
    unwrap(UserServer.put_user(server_id(), username, password_hash, permissions))
  end

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

  @doc "Lists all users (no hashes) as `%{username, permissions}` maps."
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
  Imports users from a list of maps (each `:username`, `:password_hash`, `:permissions`; string keys also
  accepted). Existing and malformed entries are skipped. Returns `{:ok, %{imported: n, skipped: n}}`.
  """
  @spec import_users([map()]) :: {:ok, map()} | {:error, atom()}
  def import_users(users) when is_list(users) do
    entries = Enum.map(users, &to_entry/1)

    case UserServer.import_users(server_id(), entries) do
      {:ok, {:ok, counts}} -> {:ok, counts}
      {:ok, other} -> other
      {:error, reason} -> {:error, reason}
    end
  end

  # -- Private --

  defp server_id, do: {@cluster, node()}

  # Unwraps a UserServer command result to the old contract: the machine reply on success (`:ok` or
  # `{:error, :user_exists | :user_not_found}`), or a transport error passed through.
  defp unwrap({:ok, machine_reply}), do: machine_reply
  defp unwrap({:error, reason}), do: {:error, reason}

  # Normalizes an import map to a `{username, hash, permissions}` entry. Malformed entries (non-binary
  # username/hash) are left as-is and skipped by the registry, so they still count as skipped.
  defp to_entry(user) do
    username = Map.get(user, :username) || Map.get(user, "username")
    hash = Map.get(user, :password_hash) || Map.get(user, "password_hash")

    permissions =
      (Map.get(user, :permissions) || Map.get(user, "permissions") || []) |> Enum.map(&normalize_permission/1)

    {username, hash, permissions}
  end

  defp normalize_permission(perm) when is_atom(perm), do: perm
  defp normalize_permission(perm) when is_binary(perm), do: String.to_existing_atom(perm)
end
