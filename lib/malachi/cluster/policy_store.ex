defmodule Malachi.Cluster.PolicyStore do
  @moduledoc """
  The cluster's storage policy definitions: a thin **facade** over the ra-replicated policy cluster
  (`Malachi.Cluster.PolicyServer`), the counterpart of `Malachi.Auth.AclStore`. `Malachi.Application`
  starts the ra cluster at boot; this module routes definitions through the log (consensus) and reads to
  the local replica.

  **Placement reads fail open; retention reads fail closed.** The two callers get opposite treatment on
  purpose, because the cost of being wrong is not symmetric.

  `get/1` answers `nil` when the store cannot be read, which is the same answer a topic with no policy
  gets, so placement falls back to the cluster-wide spread. A segment placed without its operator's rack
  rule is a worse placement, and healing can move it later.

  `fetch_all/0` hands the read error back instead, and `Malachi.Cluster.RetentionCoordinator` skips the
  whole sweep on it. Falling back there would expire under the cluster-wide limits exactly the data a
  more permissive policy was written to keep, on every replica, with no way back. A sweep that did not
  run is recovered by the next one. A deletion is not recovered at all.

  There is no wire key, mix task or dashboard route reaching these yet: #194 is what opens the door. This
  module is where it will knock.
  """

  alias Malachi.Cluster.Policy
  alias Malachi.Cluster.PolicyServer

  # The dedicated ra cluster's name (see `Malachi.Application`). Reads and writes address the local member.
  @cluster Malachi.LogPolicies

  @doc "The policy store's ra cluster name."
  @spec cluster_name() :: atom()
  def cluster_name, do: @cluster

  @doc "Defines (or replaces) the policy named `name`. Returns `:ok` or `{:error, reason}`."
  @spec define(Policy.name(), Policy.t()) :: :ok | {:error, term()}
  def define(name, policy), do: unwrap(PolicyServer.define(server_id(), name, policy))

  @doc "Removes the policy named `name` (idempotent). Returns `:ok` or `{:error, reason}`."
  @spec delete(Policy.name()) :: :ok | {:error, term()}
  def delete(name), do: unwrap(PolicyServer.delete(server_id(), name))

  @doc "The policy named `name`, or `nil` when it is undefined or the store cannot be read."
  @spec get(Policy.name() | nil) :: Policy.t() | nil
  def get(nil), do: nil

  def get(name) do
    case PolicyServer.get(server_id(), name) do
      {:ok, policy} -> policy
      {:error, _reason} -> nil
    end
  end

  @doc """
  Every definition as a map from name to policy, or the read error.

  Hands the failure back rather than answering with an empty map, which a caller cannot tell from a
  cluster that has defined no policies. Retention is that caller: see the module doc for why the two
  answers must not look the same there.
  """
  @spec fetch_all() :: {:ok, %{Policy.name() => Policy.t()}} | {:error, term()}
  def fetch_all, do: PolicyServer.all(server_id())

  defp server_id, do: {@cluster, node()}

  defp unwrap({:ok, :ok}), do: :ok
  defp unwrap({:ok, {:error, reason}}), do: {:error, reason}
  defp unwrap({:error, reason}), do: {:error, reason}
end
