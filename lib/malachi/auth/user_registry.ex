defmodule Malachi.Auth.UserRegistry do
  @moduledoc """
  The pure state of the cluster's **user registry** — the credentials and permissions of every principal
  (admins, producers, consumers, service accounts). It is the deterministic core replicated by
  `Malachi.Auth.UserMachine` over a dedicated `ra` cluster (exactly as `Malachi.Cluster.Lease` sits behind
  `LeaseMachine`), so every node reaches the same user set from the same command log — replacing the old
  node-local Mnesia store, which never replicated.

  Users are **global, small, rarely-written metadata**: a single replicated Raft group is the right home
  (as Kafka KRaft / Redpanda keep credentials + ACLs in one controller quorum), while the data plane scales
  by sharding vnodes. Auth reads come from the local `ra` replica (fast, local); writes go through the log.

  `apply/3` takes the current time from the caller — `UserMachine` passes the ra leader's `system_time` —
  and never reads a clock itself: reading a wall clock inside `apply` would be non-deterministic and break
  Raft. `created_at`/`updated_at` are therefore the leader's stamp, replicated in the log.
  """

  defstruct users: %{}

  @type username :: String.t()
  @type password_hash :: String.t()
  @type permissions :: [atom()]
  @type user :: %{
          hash: password_hash(),
          permissions: permissions(),
          created_at: integer(),
          updated_at: integer()
        }
  @type t :: %__MODULE__{users: %{username() => user()}}

  @type command ::
          {:put_user, username(), password_hash(), permissions()}
          | {:delete_user, username()}
          | {:update_password, username(), password_hash()}
          | {:import_users, [{username(), password_hash(), permissions()}]}

  @type reply ::
          :ok
          | {:error, :user_exists | :user_not_found | :unknown_command}
          | {:ok, %{imported: non_neg_integer(), skipped: non_neg_integer()}}

  @doc "An empty registry."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc """
  Applies a `command` at time `now` (the ra leader's `system_time`). Returns `{new_state, reply}`.
  Deterministic given `now`.
  """
  @spec apply(t(), command(), integer()) :: {t(), reply()}
  def apply(%__MODULE__{} = state, {:put_user, username, hash, permissions}, now) do
    if Map.has_key?(state.users, username) do
      {state, {:error, :user_exists}}
    else
      {put(state, username, hash, permissions, now), :ok}
    end
  end

  def apply(%__MODULE__{} = state, {:delete_user, username}, _now) do
    # idempotent: deleting an absent user is a no-op
    {%{state | users: Map.delete(state.users, username)}, :ok}
  end

  def apply(%__MODULE__{} = state, {:update_password, username, new_hash}, now) do
    case Map.fetch(state.users, username) do
      {:ok, user} ->
        updated = %{user | hash: new_hash, updated_at: now}
        {%{state | users: Map.put(state.users, username, updated)}, :ok}

      :error ->
        {state, {:error, :user_not_found}}
    end
  end

  def apply(%__MODULE__{} = state, {:import_users, users}, now) do
    {new_state, counts} =
      Enum.reduce(users, {state, %{imported: 0, skipped: 0}}, fn
        {username, hash, permissions}, {st, acc}
        when is_binary(username) and is_binary(hash) ->
          if Map.has_key?(st.users, username) do
            {st, %{acc | skipped: acc.skipped + 1}}
          else
            {put(st, username, hash, permissions, now), %{acc | imported: acc.imported + 1}}
          end

        _invalid, {st, acc} ->
          {st, %{acc | skipped: acc.skipped + 1}}
      end)

    {new_state, {:ok, counts}}
  end

  # Defensive catch-all: an unknown command must NOT crash the machine. Once replicated by Raft, a command
  # that raised in apply would crash every replica deterministically (and on replay) — e.g. an older replica
  # seeing a newer command during a rolling upgrade. Keep the replica alive and surface the problem.
  def apply(%__MODULE__{} = state, _unknown_command, _now), do: {state, {:error, :unknown_command}}

  @doc "The user record as `{username, hash, permissions}`, or `{:error, :user_not_found}`."
  @spec get_user(t(), username()) :: {:ok, {username(), password_hash(), permissions()}} | {:error, :user_not_found}
  def get_user(%__MODULE__{users: users}, username) do
    case Map.fetch(users, username) do
      {:ok, user} -> {:ok, {username, user.hash, user.permissions}}
      :error -> {:error, :user_not_found}
    end
  end

  @doc "Every user as `%{username, permissions}` (no hashes)."
  @spec list_users(t()) :: [%{username: username(), permissions: permissions()}]
  def list_users(%__MODULE__{users: users}) do
    for {username, user} <- users, do: %{username: username, permissions: user.permissions}
  end

  @doc """
  Every user as a JSON-serializable map (no hashes): `%{username, permissions (as strings), created_at,
  updated_at}` — the export shape consumed by `import_users`.
  """
  @spec export_users(t()) :: [
          %{username: username(), permissions: [String.t()], created_at: integer(), updated_at: integer()}
        ]
  def export_users(%__MODULE__{users: users}) do
    for {username, user} <- users do
      %{
        username: username,
        permissions: Enum.map(user.permissions, &to_string/1),
        created_at: user.created_at,
        updated_at: user.updated_at
      }
    end
  end

  defp put(state, username, hash, permissions, now) do
    user = %{hash: hash, permissions: permissions, created_at: now, updated_at: now}
    %{state | users: Map.put(state.users, username, user)}
  end
end
