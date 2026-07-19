defmodule Malachi.Auth.AclStore do
  @moduledoc """
  The cluster's per-topic ACL store — a thin **facade** over the ra-replicated ACL cluster
  (`Malachi.Auth.AclServer`), the counterpart of `Malachi.Auth.UserStore`. `Malachi.Application` starts the
  ra cluster at boot; this module routes grants/revokes through the log (consensus) and authorization reads
  to the local replica (`:ra.local_query`, fast and eventually consistent).
  """

  alias Malachi.Auth.AclRegistry
  alias Malachi.Auth.AclServer

  # The dedicated ra cluster's name (see Malachi.Application). Reads/writes address the local member.
  @cluster Malachi.LogAcls

  @doc "The ACL cluster's ra cluster name."
  @spec cluster_name() :: atom()
  def cluster_name, do: @cluster

  @doc "Grants `username` an `operation` on `resource`. Returns `:ok` or `{:error, reason}`."
  @spec grant(String.t(), AclRegistry.operation(), AclRegistry.resource()) :: :ok | {:error, term()}
  def grant(username, operation, resource), do: unwrap(AclServer.grant(server_id(), username, operation, resource))

  @doc "Revokes a single grant (idempotent). Returns `:ok` or `{:error, reason}`."
  @spec revoke(String.t(), AclRegistry.operation(), AclRegistry.resource()) :: :ok | {:error, term()}
  def revoke(username, operation, resource), do: unwrap(AclServer.revoke(server_id(), username, operation, resource))

  @doc "Revokes every grant for `username` (e.g. on user deletion). Returns `:ok` or `{:error, reason}`."
  @spec revoke_user(String.t()) :: :ok | {:error, term()}
  def revoke_user(username), do: unwrap(AclServer.revoke_user(server_id(), username))

  @doc """
  Whether `username` has a grant for `operation` on `topic`, from the local replica. **Fails closed**: an
  unreachable ACL store returns `false` (deny) — authorization must not grant access it cannot verify.
  """
  @spec authorized?(String.t(), AclRegistry.operation(), String.t()) :: boolean()
  def authorized?(username, operation, topic) do
    case AclServer.authorized?(server_id(), username, operation, topic) do
      {:ok, allowed?} -> allowed?
      {:error, _reason} -> false
    end
  end

  @doc "The grants for `username` as `{operation, resource}` (empty on error)."
  @spec list_grants(String.t()) :: [{AclRegistry.operation(), AclRegistry.resource()}]
  def list_grants(username) do
    case AclServer.list_grants(server_id(), username) do
      {:ok, grants} -> grants
      {:error, _reason} -> []
    end
  end

  @doc "Every grant across all users as `{username, operation, resource}` (empty on error)."
  @spec list_all() :: [AclRegistry.grant()]
  def list_all do
    case AclServer.list_all(server_id()) do
      {:ok, grants} -> grants
      {:error, _reason} -> []
    end
  end

  defp server_id, do: {@cluster, node()}

  defp unwrap({:ok, :ok}), do: :ok
  defp unwrap({:error, reason}), do: {:error, reason}
end
