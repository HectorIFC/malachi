defmodule Malachi.Auth.UserStore do
  @moduledoc """
  Persistent user storage backed by Mnesia with ETS cache.

  Provides CRUD operations for user credentials that survive server restarts.
  The ETS table `:malachi_users` is used as an in-memory read cache,
  while Mnesia provides disk-backed persistence.

  ## Architecture

  - **Writes**: Go to Mnesia first, then sync to ETS cache
  - **Reads**: Served from ETS cache (populated on startup from Mnesia)
  - **Startup**: Loads persisted users from Mnesia → ETS; merges with config defaults

  ## Edge Cases Handled

  - Corrupted Mnesia schema → falls back to default users from config
  - Disk full / write errors → returns `{:error, reason}`, ETS not updated
  - Concurrent writes → Mnesia transactions provide serialization
  - First startup (no Mnesia data) → seeds from config default users
  """
  use GenServer
  require Logger
  alias Malachi.I18n

  @mnesia_table :malachi_mnesia_users
  @users_table :malachi_users

  # Record fields: username, password_hash, permissions, created_at, updated_at
  @record_fields [:username, :password_hash, :permissions, :created_at, :updated_at]

  # -- Public API --

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc """
  Inserts a new user into Mnesia and syncs to ETS cache.
  Returns `:ok` or `{:error, :user_exists}` / `{:error, reason}`.
  """
  @spec insert_user(String.t(), String.t(), [atom()]) :: :ok | {:error, atom()}
  def insert_user(username, password_hash, permissions) do
    GenServer.call(__MODULE__, {:insert_user, username, password_hash, permissions})
  end

  @doc """
  Deletes a user from Mnesia and ETS cache.
  Returns `:ok` (idempotent).
  """
  @spec delete_user(String.t()) :: :ok | {:error, atom()}
  def delete_user(username) do
    GenServer.call(__MODULE__, {:delete_user, username})
  end

  @doc """
  Updates a user's password hash in Mnesia and syncs to ETS cache.
  Returns `:ok` or `{:error, :user_not_found}`.
  """
  @spec update_password(String.t(), String.t()) :: :ok | {:error, atom()}
  def update_password(username, new_password_hash) do
    GenServer.call(__MODULE__, {:update_password, username, new_password_hash})
  end

  @doc """
  Gets a user record from Mnesia.
  Returns `{:ok, {username, password_hash, permissions}}` or `{:error, :user_not_found}`.
  """
  @spec get_user(String.t()) :: {:ok, {String.t(), String.t(), [atom()]}} | {:error, :user_not_found}
  def get_user(username) do
    case :mnesia.transaction(fn -> :mnesia.read(@mnesia_table, username) end) do
      {:atomic, [{@mnesia_table, ^username, hash, perms, _created, _updated}]} ->
        {:ok, {username, hash, perms}}

      {:atomic, []} ->
        {:error, :user_not_found}

      {:aborted, reason} ->
        Logger.error(I18n.t(:user_store_persist_error, reason: inspect(reason)))
        {:error, :mnesia_error}
    end
  end

  @doc """
  Lists all users from Mnesia (without password hashes).
  Returns a list of `%{username: ..., permissions: ...}` maps.
  """
  @spec list_users() :: [map()]
  def list_users do
    case :mnesia.transaction(fn ->
           :mnesia.foldl(
             fn {_table, username, _hash, permissions, _created, _updated}, acc ->
               [%{username: username, permissions: permissions} | acc]
             end,
             [],
             @mnesia_table
           )
         end) do
      {:atomic, users} -> users
      {:aborted, _reason} -> []
    end
  end

  @doc """
  Exports all users to a JSON-serializable list.
  Returns `{:ok, list}` with user data (without password hashes for security).
  """
  @spec export_users() :: {:ok, [map()]}
  def export_users do
    case :mnesia.transaction(fn ->
           :mnesia.foldl(
             fn {_table, username, _hash, permissions, created_at, updated_at}, acc ->
               [
                 %{
                   username: username,
                   permissions: Enum.map(permissions, &to_string/1),
                   created_at: created_at,
                   updated_at: updated_at
                 }
                 | acc
               ]
             end,
             [],
             @mnesia_table
           )
         end) do
      {:atomic, users} ->
        Logger.info(I18n.t(:user_store_export_success, count: length(users)))
        {:ok, users}

      {:aborted, reason} ->
        Logger.error(I18n.t(:user_store_persist_error, reason: inspect(reason)))
        {:error, :export_failed}
    end
  end

  @doc """
  Imports users from a list of maps. Each map must have `:username`, `:password_hash`, `:permissions`.
  Existing users are skipped (not overwritten).
  Returns `{:ok, %{imported: n, skipped: n}}`.
  """
  @spec import_users([map()]) :: {:ok, map()} | {:error, atom()}
  def import_users(users) when is_list(users) do
    GenServer.call(__MODULE__, {:import_users, users})
  end

  @doc """
  Returns the Mnesia table name (for testing/inspection).
  """
  @spec table_name() :: atom()
  def table_name, do: @mnesia_table

  @doc """
  Loads all persisted users from Mnesia into the ETS cache.
  Called during startup and can be called to force a full sync.
  """
  @spec load_into_ets() :: :ok
  def load_into_ets do
    GenServer.call(__MODULE__, :load_into_ets)
  end

  # -- GenServer Callbacks --

  @impl true
  def init(:ok) do
    case setup_mnesia() do
      :ok ->
        Logger.info(I18n.t(:user_store_started))
        {:ok, %{}, {:continue, :sync_ets}}

      {:error, reason} ->
        Logger.error(I18n.t(:user_store_persist_error, reason: inspect(reason)))
        # Start anyway — Auth will load defaults from config
        {:ok, %{}}
    end
  end

  @impl true
  def handle_continue(:sync_ets, state) do
    sync_mnesia_to_ets()
    {:noreply, state}
  end

  @impl true
  def handle_call({:insert_user, username, password_hash, permissions}, _from, state) do
    now = System.system_time(:second)

    result =
      :mnesia.transaction(fn ->
        case :mnesia.read(@mnesia_table, username) do
          [] ->
            :mnesia.write({@mnesia_table, username, password_hash, permissions, now, now})
            :ok

          _ ->
            :user_exists
        end
      end)

    case result do
      {:atomic, :ok} ->
        :ets.insert(@users_table, {username, password_hash, permissions})
        {:reply, :ok, state}

      {:atomic, :user_exists} ->
        {:reply, {:error, :user_exists}, state}

      {:aborted, reason} ->
        Logger.error(I18n.t(:user_store_persist_error, reason: inspect(reason)))
        {:reply, {:error, :persist_failed}, state}
    end
  end

  @impl true
  def handle_call({:delete_user, username}, _from, state) do
    result =
      :mnesia.transaction(fn ->
        :mnesia.delete({@mnesia_table, username})
      end)

    case result do
      {:atomic, :ok} ->
        :ets.delete(@users_table, username)
        {:reply, :ok, state}

      {:aborted, reason} ->
        Logger.error(I18n.t(:user_store_persist_error, reason: inspect(reason)))
        {:reply, {:error, :persist_failed}, state}
    end
  end

  @impl true
  def handle_call({:update_password, username, new_password_hash}, _from, state) do
    now = System.system_time(:second)

    result =
      :mnesia.transaction(fn ->
        case :mnesia.read(@mnesia_table, username) do
          [{@mnesia_table, ^username, _old_hash, permissions, created_at, _updated_at}] ->
            :mnesia.write({@mnesia_table, username, new_password_hash, permissions, created_at, now})
            {:ok, permissions}

          [] ->
            :user_not_found
        end
      end)

    case result do
      {:atomic, {:ok, permissions}} ->
        :ets.insert(@users_table, {username, new_password_hash, permissions})
        {:reply, :ok, state}

      {:atomic, :user_not_found} ->
        {:reply, {:error, :user_not_found}, state}

      {:aborted, reason} ->
        Logger.error(I18n.t(:user_store_persist_error, reason: inspect(reason)))
        {:reply, {:error, :persist_failed}, state}
    end
  end

  @impl true
  def handle_call(:load_into_ets, _from, state) do
    sync_mnesia_to_ets()
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:import_users, users}, _from, state) do
    now = System.system_time(:second)

    result =
      :mnesia.transaction(fn ->
        Enum.reduce(users, %{imported: 0, skipped: 0}, fn user, acc ->
          username = Map.get(user, :username) || Map.get(user, "username")
          password_hash = Map.get(user, :password_hash) || Map.get(user, "password_hash")

          permissions =
            (Map.get(user, :permissions) || Map.get(user, "permissions", []))
            |> Enum.map(&normalize_permission/1)

          case :mnesia.read(@mnesia_table, username) do
            [] when is_binary(username) and is_binary(password_hash) ->
              :mnesia.write({@mnesia_table, username, password_hash, permissions, now, now})
              %{acc | imported: acc.imported + 1}

            _ ->
              %{acc | skipped: acc.skipped + 1}
          end
        end)
      end)

    case result do
      {:atomic, counts} ->
        sync_mnesia_to_ets()
        Logger.info(I18n.t(:user_store_import_success, imported: counts.imported, skipped: counts.skipped))
        {:reply, {:ok, counts}, state}

      {:aborted, reason} ->
        Logger.error(I18n.t(:user_store_persist_error, reason: inspect(reason)))
        {:reply, {:error, :import_failed}, state}
    end
  end

  # -- Private --

  defp setup_mnesia do
    node = node()
    ensure_mnesia_schema(node)

    # Start Mnesia
    :ok = :mnesia.start()

    # Determine table type based on environment
    storage_type =
      if Application.get_env(:malachi, :mnesia_ram_only, false) do
        :ram_copies
      else
        :disc_copies
      end

    create_users_table(storage_type, node)
  end

  # Points mnesia at the configured data dir (if any) and creates the schema on `node`, tolerating a
  # schema that already exists.
  defp ensure_mnesia_schema(node) do
    mnesia_dir = Application.get_env(:malachi, :mnesia_dir)

    if mnesia_dir do
      # Ensure the directory exists before create_schema writes the schema file there
      File.mkdir_p!(mnesia_dir)
      Application.put_env(:mnesia, :dir, to_charlist(mnesia_dir))
    end

    case :mnesia.create_schema([node]) do
      :ok -> :ok
      {:error, {_, {:already_exists, _}}} -> :ok
      {:error, reason} -> Logger.warning("Mnesia schema creation warning: #{inspect(reason)}")
    end
  end

  # Creates the users table with `storage_type`, waiting on it if it already exists. A bad_type for
  # disc_copies (an unnamed node) falls back to ram_copies.
  defp create_users_table(storage_type, node) do
    table_opts = [
      {:attributes, @record_fields},
      {:type, :set},
      {storage_type, [node]}
    ]

    case :mnesia.create_table(@mnesia_table, table_opts) do
      {:atomic, :ok} ->
        Logger.info(I18n.t(:user_store_mnesia_initialized, storage: storage_type))
        :ok

      {:aborted, {:already_exists, @mnesia_table}} ->
        # Table exists — wait for it to be ready
        :mnesia.wait_for_tables([@mnesia_table], 10_000)
        :ok

      {:aborted, {:bad_type, @mnesia_table, :disc_copies, node_name}} ->
        # disc_copies requires a named Erlang node (--sname/--name).
        # Falls back to ram_copies when running on an unnamed node (e.g. plain mix run).
        Logger.warning(I18n.t(:user_store_disc_copies_fallback, node: inspect(node_name)))
        create_ram_users_table(node)

      {:aborted, reason} ->
        {:error, reason}
    end
  end

  # Creates the users table as ram_copies — the fallback when disc_copies is unavailable.
  defp create_ram_users_table(node) do
    ram_opts = [
      {:attributes, @record_fields},
      {:type, :set},
      {:ram_copies, [node]}
    ]

    case :mnesia.create_table(@mnesia_table, ram_opts) do
      {:atomic, :ok} ->
        :ok

      {:aborted, {:already_exists, @mnesia_table}} ->
        :mnesia.wait_for_tables([@mnesia_table], 10_000)
        :ok

      {:aborted, reason} ->
        {:error, reason}
    end
  end

  defp sync_mnesia_to_ets do
    case :mnesia.transaction(fn ->
           :mnesia.foldl(
             fn {_table, username, hash, permissions, _created, _updated}, acc ->
               [{username, hash, permissions} | acc]
             end,
             [],
             @mnesia_table
           )
         end) do
      {:atomic, users} ->
        # Load users from Mnesia into ETS
        Enum.each(users, fn {username, hash, permissions} ->
          :ets.insert(@users_table, {username, hash, permissions})
        end)

        if users != [] do
          Logger.info(I18n.t(:user_store_loaded_from_mnesia, count: length(users)))
        end

      {:aborted, reason} ->
        Logger.error(I18n.t(:user_store_persist_error, reason: inspect(reason)))
    end
  end

  defp normalize_permission(perm) when is_atom(perm), do: perm
  defp normalize_permission(perm) when is_binary(perm), do: String.to_existing_atom(perm)
end
