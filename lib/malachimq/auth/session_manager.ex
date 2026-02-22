defmodule MalachiMQ.Auth.SessionManager do
  @moduledoc """
  Advanced session management with IP binding and trusted proxy support.

  Extends the basic session system with:

  - **IP Binding**: Binds sessions to source IP address to prevent hijacking
  - **Trusted Proxies**: Supports CIDR ranges of trusted proxies (NAT, load balancers)
  - **Activity Tracking**: Tracks last activity for dynamic timeout
  - **Session Revocation**: Allows manual revocation of specific sessions or all sessions for a user

  ## Configuration

  - `session_timeout_seconds` - Session expiration time (default: 3600 = 1 hour)
  - `session_ip_binding` - Enables IP binding (default: true)
  - `session_ua_binding` - Enables User-Agent binding (default: false, not implemented)
  - `trusted_proxy_ranges` - List of CIDR ranges for trusted proxies

  ## Trusted Proxies

  For environments with NAT or corporate proxies, configure trusted ranges:

      config :malachimq, trusted_proxy_ranges: [
        "10.0.0.0/8",        # RFC1918 - Class A private network
        "172.16.0.0/12",     # RFC1918 - Class B private network
        "192.168.0.0/16"     # RFC1918 - Class C private network
      ]

  Sessions created from IPs in these ranges will have IP binding disabled.
  """
  require Logger
  alias MalachiMQ.I18n

  @table_sessions :malachimq_sessions

  ## Client API

  @doc """
  Creates a new session with optional IP binding.

  ## Parameters

  - `username` - Username
  - `permissions` - List of permissions (`:admin`, `:produce`, `:consume`)
  - `client_ip` - Client IP address (tuple)
  - `user_agent` - User-Agent string (not implemented, always "")

  ## Returns

  - `{:ok, token}` - Token of the created session

  ## Examples

      iex> SessionManager.create_session("admin", [:admin], {192, 168, 1, 1}, "")
      {:ok, "abc123..."}
  """
  def create_session(username, permissions, client_ip, user_agent \\ "") do
    token = generate_token()
    now = System.system_time(:second)
    timeout = Application.get_env(:malachimq, :session_timeout_seconds, 3600)
    bind_ip = Application.get_env(:malachimq, :session_ip_binding, true)

    # Checks if IP is in trusted proxy range
    ip_binding_disabled = bind_ip && trusted_proxy?(client_ip)

    session_data = %{
      username: username,
      permissions: permissions,
      ip: client_ip,
      user_agent: user_agent,
      created_at: now,
      expires_at: now + timeout,
      last_activity: now,
      ip_binding_disabled: ip_binding_disabled
    }

    :ets.insert(@table_sessions, {token, session_data})

    # Audit log
    MalachiMQ.AuditLog.log_event(
      :session_created,
      %{username: username, ip: client_ip},
      "create_session",
      :success,
      %{
        token_prefix: String.slice(token, 0, 8),
        ip_binding_disabled: ip_binding_disabled
      }
    )

    {:ok, token}
  end

  @doc """
  Validates an existing session.

  Checks:
  - Token exists
  - Session has not expired
  - IP binding (if enabled and not trusted proxy)
  - User-Agent binding (if enabled, not implemented)

  ## Parameters

  - `token` - Session token
  - `client_ip` - Current client IP
  - `user_agent` - Current User-Agent (not implemented)

  ## Returns

  - `{:ok, session_data}` - Valid session, returns data
  - `{:error, :session_expired}` - Session expired
  - `{:error, :session_hijack_attempt}` - IP or UA do not match
  - `{:error, :invalid_session}` - Token does not exist
  """
  def validate_session(token, client_ip, user_agent \\ "") do
    case :ets.lookup(@table_sessions, token) do
      [{^token, session_data}] ->
        now = System.system_time(:second)

        cond do
          session_data.expires_at < now ->
            :ets.delete(@table_sessions, token)

            MalachiMQ.AuditLog.log_event(
              :session_expired,
              %{username: session_data.username},
              "validate_session",
              :expired,
              %{token_prefix: String.slice(token, 0, 8)}
            )

            {:error, :session_expired}

          not valid_client_binding?(session_data, client_ip, user_agent) ->
            Logger.warning(
              I18n.t(:session_hijack_attempt,
                username: session_data.username,
                ip: format_ip(client_ip)
              ),
              username: session_data.username,
              session_ip: format_ip(session_data.ip),
              request_ip: format_ip(client_ip),
              token_prefix: String.slice(token, 0, 8)
            )

            MalachiMQ.AuditLog.log_event(
              :session_hijack_attempt,
              %{username: session_data.username, ip: client_ip},
              "validate_session",
              :failure,
              %{
                session_ip: format_ip(session_data.ip),
                request_ip: format_ip(client_ip),
                token_prefix: String.slice(token, 0, 8)
              }
            )

            {:error, :session_hijack_attempt}

          true ->
            # Updates last activity
            updated_session = %{session_data | last_activity: now}
            :ets.insert(@table_sessions, {token, updated_session})
            {:ok, session_data}
        end

      [] ->
        {:error, :invalid_session}
    end
  end

  @doc """
  Revokes a specific session.

  ## Parameters

  - `token` - Token of the session to revoke

  ## Returns

  - `:ok` - Session successfully revoked
  - `{:error, :session_not_found}` - Token does not exist
  """
  def revoke_session(token) do
    case :ets.lookup(@table_sessions, token) do
      [{^token, session_data}] ->
        :ets.delete(@table_sessions, token)

        MalachiMQ.AuditLog.log_event(
          :session_revoked,
          %{username: session_data.username},
          "revoke_session",
          :success,
          %{token_prefix: String.slice(token, 0, 8)}
        )

        :ok

      [] ->
        {:error, :session_not_found}
    end
  end

  @doc """
  Revokes all sessions for a user.

  ## Parameters

  - `username` - Username

  ## Returns

  - `{:ok, count}` - Number of sessions revoked
  """
  def revoke_all_sessions(username) do
    # Busca todas as sessões do usuário
    sessions =
      :ets.select(@table_sessions, [
        {{:"$1", %{username: :"$2"}}, [{:==, :"$2", username}], [:"$1"]}
      ])

    count = length(sessions)

    # Remove todas
    Enum.each(sessions, fn token ->
      :ets.delete(@table_sessions, token)
    end)

    if count > 0 do
      MalachiMQ.AuditLog.log_event(
        :session_revoked,
        %{username: username},
        "revoke_all_sessions",
        :success,
        %{session_count: count}
      )
    end

    {:ok, count}
  end

  @doc """
  Lists active sessions.

  ## Parameters

  - `username` - Username (`:all` for all sessions)

  ## Returns

  List of maps with session information:
  ```elixir
  [
    %{
      token_prefix: "abc123...",
      username: "admin",
      ip: "192.168.1.1",
      created_at: 1234567890,
      expires_at: 1234571490,
      last_activity: 1234570000,
      ip_binding_disabled: false
    },
    ...
  ]
  ```
  """
  def list_sessions(username \\ :all) do
    now = System.system_time(:second)

    sessions =
      if username == :all do
        :ets.tab2list(@table_sessions)
      else
        :ets.select(@table_sessions, [
          {{:"$1", %{username: :"$2"}}, [{:==, :"$2", username}], [{{:"$1", :"$2"}}]}
        ])
        |> Enum.map(fn {token, _} ->
          case :ets.lookup(@table_sessions, token) do
            [{^token, session_data}] -> {token, session_data}
            [] -> nil
          end
        end)
        |> Enum.reject(&is_nil/1)
      end

    # Filtra sessões expiradas e formata
    sessions
    |> Enum.filter(fn {_token, session_data} ->
      session_data.expires_at > now
    end)
    |> Enum.map(fn {token, session_data} ->
      %{
        token_prefix: String.slice(token, 0, 8),
        username: session_data.username,
        ip: format_ip(session_data.ip),
        created_at: session_data.created_at,
        expires_at: session_data.expires_at,
        last_activity: session_data.last_activity,
        ip_binding_disabled: Map.get(session_data, :ip_binding_disabled, false),
        time_until_expiry: session_data.expires_at - now
      }
    end)
  end

  ## Private Functions

  defp generate_token do
    :crypto.strong_rand_bytes(32)
    |> Base.url_encode64(padding: false)
  end

  defp valid_client_binding?(session_data, client_ip, _user_agent) do
    ip_binding = Application.get_env(:malachimq, :session_ip_binding, true)
    ua_binding = Application.get_env(:malachimq, :session_ua_binding, false)

    # Se binding foi desabilitado para proxy confiável, sempre válido
    if Map.get(session_data, :ip_binding_disabled, false) do
      true
    else
      ip_valid =
        if ip_binding do
          session_data.ip == client_ip
        else
          true
        end

      # User-Agent binding não implementado - sempre válido
      ua_valid =
        if ua_binding do
          # Futura implementação: session_data.user_agent == user_agent
          true
        else
          true
        end

      ip_valid and ua_valid
    end
  end

  defp trusted_proxy?(ip) do
    trusted_ranges = Application.get_env(:malachimq, :trusted_proxy_ranges, [])

    if Enum.empty?(trusted_ranges) do
      false
    else
      # Converte IP para formato que inet_cidr aceita
      ip_string = format_ip(ip)

      Enum.any?(trusted_ranges, fn range ->
        try do
          cidr = InetCidr.parse_cidr!(range, true)

          case parse_ip_address(ip_string) do
            {:ok, parsed_ip} -> InetCidr.contains?(cidr, parsed_ip)
            _ -> false
          end
        rescue
          _ ->
            Logger.warning(I18n.t(:invalid_cidr_range, range: range), range: range)
            false
        end
      end)
    end
  end

  defp parse_ip_address(ip_string) when is_binary(ip_string) do
    case :inet.parse_address(String.to_charlist(ip_string)) do
      {:ok, ip} -> {:ok, ip}
      {:error, _} -> :error
    end
  end

  defp format_ip(ip) when is_tuple(ip) do
    case tuple_size(ip) do
      4 -> :inet.ntoa(ip) |> to_string()
      8 -> :inet.ntoa(ip) |> to_string()
      _ -> "invalid"
    end
  end

  defp format_ip(ip) when is_binary(ip), do: ip
  defp format_ip(_), do: "unknown"
end
