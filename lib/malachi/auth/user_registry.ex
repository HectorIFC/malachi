defmodule Malachi.Auth.UserRegistry do
  @moduledoc """
  The pure state of the cluster's **user registry**: the credentials and permissions of every principal
  (admins, producers, consumers, service accounts). It is the deterministic core replicated by
  `Malachi.Auth.UserMachine` over a dedicated `ra` cluster (exactly as `Malachi.Cluster.Lease` sits behind
  `LeaseMachine`), so every node reaches the same user set from the same command log, replacing the old
  node-local Mnesia store, which never replicated.

  Users are **global, small, rarely-written metadata**: a single replicated Raft group is the right home
  (as Kafka KRaft / Redpanda keep credentials + ACLs in one controller quorum), while the data plane scales
  by sharding vnodes. Auth reads come from the local `ra` replica (fast, local); writes go through the log.

  Each user may also hold a **console role** (`:viewer`, `:editor` or `:admin`, see
  `Malachi.Console.Access`), orthogonal to its wire permissions. The role arrived at machine version 5 as
  three new command shapes (`set_role`, a five element `put_user`, `import_users_with_roles`); the older
  shapes stay, so a log written before version 5 replays unchanged. A record written before then has no
  `:role` key at all, and every read treats that as no role.

  `apply/3` takes the current time from the caller: `UserMachine` passes the ra leader's `system_time`:
  and never reads a clock itself: reading a wall clock inside `apply` would be non-deterministic and break
  Raft. `created_at`/`updated_at` are therefore the leader's stamp, replicated in the log.
  """

  alias Malachi.Auth.ConsoleRole

  defstruct users: %{}

  @type username :: String.t()
  @type password_hash :: String.t()
  @type permissions :: [atom()]
  @type role :: Malachi.Auth.ConsoleRole.t()
  @type user :: %{
          required(:hash) => password_hash(),
          required(:permissions) => permissions(),
          optional(:role) => role() | nil,
          required(:created_at) => integer(),
          required(:updated_at) => integer()
        }
  @type role_entry :: {username(), password_hash(), permissions(), role() | nil}
  @type principal :: %{username: username(), permissions: permissions(), role: role() | nil}
  @type t :: %__MODULE__{users: %{username() => user()}}

  @type command ::
          {:put_user, username(), password_hash(), permissions()}
          | {:delete_user, username()}
          | {:update_password, username(), password_hash()}
          | {:import_users, [{username(), password_hash(), permissions()}]}
          | {:put_user, username(), password_hash(), permissions(), role() | nil}
          | {:set_role, username(), role() | nil}
          | {:import_users_with_roles, [role_entry()]}

  @type reply ::
          :ok
          | {:error, :user_exists | :user_not_found | :invalid_role | :unknown_command}
          | {:ok, %{imported: non_neg_integer(), skipped: non_neg_integer()}}

  @behaviour Malachi.Cluster.MachineVersion

  @doc "An empty registry."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc "Every command shape, mapped to the machine version that introduced it (see `Malachi.Cluster.MachineVersion`)."
  @impl Malachi.Cluster.MachineVersion
  def command_versions do
    %{
      {:put_user, 4} => 0,
      {:delete_user, 2} => 0,
      {:update_password, 3} => 0,
      {:import_users, 2} => 0,
      {:put_user, 5} => 5,
      {:set_role, 3} => 5,
      {:import_users_with_roles, 2} => 5
    }
  end

  @doc """
  Applies a `command` at time `now` (the ra leader's `system_time`). Returns `{new_state, reply}`.
  Deterministic given `now`.
  """
  @spec apply(t(), command(), integer()) :: {t(), reply()}
  def apply(%__MODULE__{} = state, {:put_user, username, hash, permissions}, now) do
    __MODULE__.apply(state, {:put_user, username, hash, permissions, nil}, now)
  end

  def apply(%__MODULE__{} = state, {:put_user, username, hash, permissions, role}, now) do
    cond do
      not valid_role?(role) -> {state, {:error, :invalid_role}}
      Map.has_key?(state.users, username) -> {state, {:error, :user_exists}}
      true -> {put(state, username, hash, permissions, role, now), :ok}
    end
  end

  def apply(%__MODULE__{} = state, {:set_role, username, role}, now) do
    case {valid_role?(role), Map.fetch(state.users, username)} do
      {false, _user} ->
        {state, {:error, :invalid_role}}

      {true, {:ok, user}} ->
        updated = Map.merge(user, %{role: role, updated_at: now})
        {%{state | users: Map.put(state.users, username, updated)}, :ok}

      {true, :error} ->
        {state, {:error, :user_not_found}}
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
    import_entries(state, users, now, fn
      {username, hash, permissions} -> {username, hash, permissions, nil}
      _invalid -> :invalid
    end)
  end

  def apply(%__MODULE__{} = state, {:import_users_with_roles, users}, now) do
    import_entries(state, users, now, fn
      {_username, _hash, _permissions, _role} = entry -> entry
      _invalid -> :invalid
    end)
  end

  # Defensive catch-all for callers outside ra (tests, direct use): an unknown command must not raise. Inside
  # ra, `Malachi.Cluster.MachineVersion` refuses an unknown or not-yet-effective command before it gets here,
  # so an older replica never skips a newer command while a newer one applies it.
  def apply(%__MODULE__{} = state, _unknown_command, _now), do: {state, {:error, :unknown_command}}

  @doc "The user record as `{username, hash, permissions}`, or `{:error, :user_not_found}`."
  @spec get_user(t(), username()) :: {:ok, {username(), password_hash(), permissions()}} | {:error, :user_not_found}
  def get_user(%__MODULE__{users: users}, username) do
    case Map.fetch(users, username) do
      {:ok, user} -> {:ok, {username, user.hash, user.permissions}}
      :error -> {:error, :user_not_found}
    end
  end

  @doc """
  The user's permissions and console role, without the hash, or `{:error, :user_not_found}`. The read the
  console access rules make on every HTTP request (`Malachi.Console.Access`).
  """
  @spec get_principal(t(), username()) :: {:ok, principal()} | {:error, :user_not_found}
  def get_principal(%__MODULE__{users: users}, username) do
    case Map.fetch(users, username) do
      {:ok, user} -> {:ok, principal(username, user)}
      :error -> {:error, :user_not_found}
    end
  end

  @doc "Every user as `%{username, permissions, role}` (no hashes)."
  @spec list_users(t()) :: [principal()]
  def list_users(%__MODULE__{users: users}) do
    for {username, user} <- users, do: principal(username, user)
  end

  @doc """
  Every user as a JSON-serializable map (no hashes): `%{username, permissions (as strings), role (a string
  or nil), created_at, updated_at}`: the export shape consumed by `Malachi.Auth.UserStore.import_users/1`.
  """
  @spec export_users(t()) :: [
          %{
            username: username(),
            permissions: [String.t()],
            role: String.t() | nil,
            created_at: integer(),
            updated_at: integer()
          }
        ]
  def export_users(%__MODULE__{users: users}) do
    for {username, user} <- users do
      %{
        username: username,
        permissions: Enum.map(user.permissions, &to_string/1),
        role: user |> role_of() |> role_string(),
        created_at: user.created_at,
        updated_at: user.updated_at
      }
    end
  end

  # Imports normalized entries, skipping existing users and anything malformed (a non-binary username or
  # hash, an unknown role, an entry of the wrong shape). `normalize` turns one raw entry into
  # `{username, hash, permissions, role}` or `:invalid`.
  defp import_entries(state, users, now, normalize) do
    {new_state, counts} =
      Enum.reduce(users, {state, %{imported: 0, skipped: 0}}, fn raw, {st, acc} ->
        case normalize.(raw) do
          {username, hash, permissions, role}
          when is_binary(username) and is_binary(hash) and is_list(permissions) ->
            if Map.has_key?(st.users, username) or not valid_role?(role) do
              {st, %{acc | skipped: acc.skipped + 1}}
            else
              {put(st, username, hash, permissions, role, now), %{acc | imported: acc.imported + 1}}
            end

          _invalid ->
            {st, %{acc | skipped: acc.skipped + 1}}
        end
      end)

    {new_state, {:ok, counts}}
  end

  defp put(state, username, hash, permissions, role, now) do
    user = %{hash: hash, permissions: permissions, role: role, created_at: now, updated_at: now}
    %{state | users: Map.put(state.users, username, user)}
  end

  # Validated inside `apply/3` as well as at every surface, so a forged command cannot write anything but
  # a role into the replicated state.
  defp valid_role?(role), do: ConsoleRole.valid?(role)

  # A record written before machine version 5 carries no :role key.
  defp role_of(user), do: Map.get(user, :role)

  defp role_string(nil), do: nil
  defp role_string(role), do: Atom.to_string(role)

  defp principal(username, user), do: %{username: username, permissions: user.permissions, role: role_of(user)}
end
