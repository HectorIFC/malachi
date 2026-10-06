defmodule Malachi.Console.Access do
  @moduledoc """
  Who may reach which HTTP route, decided in one place for both HTTP endpoints, the legacy dashboard
  (`Malachi.Dashboard`) and the console (`Malachi.Console.Router`). A test that changes a rule here sees
  both endpoints follow, and no route decides access on its own.

  ## Console roles, not wire permissions

  A route requires a **console role** (`Malachi.Auth.ConsoleRole`): `:viewer` reads, `:editor` adds the
  mutations that are not security (storage policies), `:admin` adds users, ACLs and diagnostics. The wire
  permissions `:produce` and `:consume` grant no console role: an account that publishes records cannot
  read the cluster's operational state through either endpoint. The one bridge is the superuser
  (`Malachi.Auth.Authorization.superuser?/1`): a wire `:admin` is a console `:admin` as well, so an
  existing admin keeps everything (`effective_role/2`).

  ## The route table

  `routes/0` lists every route with the level it requires: `:public` (no credentials), `:authenticated`
  (any valid session, with or without a role), or a role. `required_role/2` reads it; a route missing
  from it requires `:admin`, so forgetting a row denies rather than leaks. The read API (#230) adds its
  `/api/v1` rows here.

  ## Resolving a request

  `resolve/3` turns the credentials of a request into a subject. The session only proves identity: the
  role and the permissions are read from the user registry on every request
  (`Malachi.Auth.UserStore.get_principal/1`, a query against the replica on the node that received the
  request), so a role change or a removed account takes effect on the next request each node serves once
  its replica has applied the change, not on the next login. That read is eventually consistent: a
  follower lagging the leader answers with the old role for the replication lag, and a node cut off from
  the leader keeps answering with it until it rejoins. A Server-Sent Events stream is authorized once,
  when it opens; a role taken away while it is open takes effect when it reconnects or its session
  expires.

  The read needs only the local replica, not a leader or a quorum. When that replica is not running (a
  member restarting, or not yet joined at boot), the request is answered 503 (`:principal_unavailable`)
  rather than served from the session, which is the price of the registry being the authority.

  Two rate limit buckets are spent, split by what the request has proven. A token that validates spends
  its session's own `:dashboard_api` budget, keyed by a digest of the token. A token that does not is
  charged to the address's `:dashboard_auth` bucket, the one logins use, **before** it is validated,
  since validation is what writes the expiry and hijack audit events.

  With authentication disabled (`MALACHI_DASHBOARD_AUTH_ENABLED=false`) every request is the anonymous
  subject from `anonymous/0`, which holds `:admin`: the switch exists for local work, and it lets every
  route through as it always has. `GET /api/v1/me` reports it with `authenticated: false`.
  """

  alias Malachi.AuditLog
  alias Malachi.Auth
  alias Malachi.Auth.Authorization
  alias Malachi.Auth.ConsoleRole
  alias Malachi.Auth.UserStore
  alias Malachi.Metrics
  alias Malachi.RateLimiter

  @typedoc "What a route requires."
  @type level :: :public | :authenticated | ConsoleRole.t()

  @typedoc "A path pattern: literal segments, `:param` for any one non-empty segment, or `:any` for any path."
  @type pattern :: [String.t() | :param] | :any

  @typedoc "Who is asking."
  @type subject :: %{
          username: String.t() | nil,
          role: ConsoleRole.t() | nil,
          permissions: [atom()],
          authenticated: boolean(),
          session: String.t() | nil
        }

  @typedoc "The request an `admit/3` decision is about, for its audit event."
  @type request :: %{ip: String.t(), user_agent: String.t(), method: String.t() | atom(), path: String.t()}

  @typedoc "Why a request was not resolved to a subject."
  @type resolve_error ::
          {:error, :authentication_required}
          | {:error, :session_expired | :session_hijack_attempt | :invalid_session | :principal_unavailable}
          | {:error, :rate_limit_exceeded, non_neg_integer()}
          | {:error, :api_rate_limit_exceeded, non_neg_integer(), String.t(), String.t()}

  @capabilities %{
    viewer: [:read_cluster],
    editor: [:manage_policies],
    admin: [:manage_users, :manage_acls, :diagnostics]
  }

  @routes [
    # Public: the login flow, the probes and the logo send no credentials.
    {"GET", ["login"], :public},
    {"POST", ["login"], :public},
    {"GET", ["logout"], :public},
    {"GET", ["health"], :public},
    {"GET", ["ready"], :public},
    {"GET", ["logo.svg"], :public},
    {"OPTIONS", :any, :public},
    # Any valid session: the shell needs to know who it is talking to before it knows what it may show.
    {"GET", ["api", "v1", "me"], :authenticated},
    # Reads of the cluster's operational state.
    {"GET", [], :viewer},
    {"GET", ["stream"], :viewer},
    {"GET", ["metrics"], :viewer},
    {"GET", ["topic"], :viewer},
    {"GET", ["rate_limits"], :viewer},
    {"GET", ["policies"], :viewer},
    {"GET", ["topics", :param, "policy"], :viewer},
    # Mutations that are not security.
    {"PUT", ["policies", :param], :editor},
    {"DELETE", ["policies", :param], :editor},
    {"PUT", ["topics", :param, "policy"], :editor},
    {"DELETE", ["topics", :param, "policy"], :editor},
    # Users and ACLs, read and written.
    {"GET", ["users"], :admin},
    {"POST", ["users"], :admin},
    {"DELETE", ["users", :param], :admin},
    {"PUT", ["users", :param, "password"], :admin},
    {"PUT", ["users", :param, "role"], :admin},
    {"GET", ["users", :param, "acls"], :admin},
    {"POST", ["users", :param, "acls"], :admin},
    {"DELETE", ["users", :param, "acls"], :admin}
  ]

  @doc "Every route as `{method, pattern, level}`, in the order `required_role/2` matches them."
  @spec routes() :: [{String.t(), pattern(), level()}]
  def routes, do: @routes

  @doc """
  The level `method` on `path` requires. `path` is the request path without its query, and `method` the
  upper case method name (an atom is accepted too). A path no row matches requires `:admin`.

  ## Examples

      iex> Malachi.Console.Access.required_role("GET", "/metrics")
      :viewer

      iex> Malachi.Console.Access.required_role(:PUT, "/topics/orders/policy")
      :editor

      iex> Malachi.Console.Access.required_role("GET", "/not-a-route")
      :admin

  """
  @spec required_role(String.t() | atom(), String.t()) :: level()
  def required_role(method, path) when is_atom(method), do: required_role(Atom.to_string(method), path)

  def required_role(method, path) when is_binary(method) and is_binary(path) do
    segments = String.split(path, "/", trim: true)

    Enum.find_value(@routes, :admin, fn {route_method, pattern, level} ->
      if route_method == method and matches?(pattern, segments), do: level
    end)
  end

  defp matches?(:any, _segments), do: true
  defp matches?(pattern, segments) when length(pattern) != length(segments), do: false

  defp matches?(pattern, segments) do
    pattern
    |> Enum.zip(segments)
    |> Enum.all?(fn
      {:param, segment} -> segment != ""
      {literal, segment} -> literal == segment
    end)
  end

  @doc """
  The console role `permissions` and a stored `role` add up to: the stored role, raised to `:admin` for a
  superuser. The wire permissions `:produce` and `:consume` add nothing.

  ## Examples

      iex> Malachi.Console.Access.effective_role([:produce], nil)
      nil

      iex> Malachi.Console.Access.effective_role([:admin], :viewer)
      :admin

      iex> Malachi.Console.Access.effective_role([], :editor)
      :editor

  """
  @spec effective_role([atom()], ConsoleRole.t() | nil) :: ConsoleRole.t() | nil
  def effective_role(permissions, role) do
    ConsoleRole.max(role, if(Authorization.superuser?(permissions), do: :admin))
  end

  @doc """
  What `role` lets an operator do, as the console renders it: the capabilities of the role and of every
  role below it. No role has none.

  ## Examples

      iex> Malachi.Console.Access.capabilities(:editor)
      [:read_cluster, :manage_policies]

      iex> Malachi.Console.Access.capabilities(nil)
      []

  """
  @spec capabilities(ConsoleRole.t() | nil) :: [atom()]
  def capabilities(role) do
    for granted <- ConsoleRole.all(), ConsoleRole.includes?(role, granted), capability <- @capabilities[granted] do
      capability
    end
  end

  @doc """
  Whether `subject` may reach a route that requires `level`: `:ok`, `{:error, :authentication_required}`
  for no subject, or `{:error, {:missing_role, level, role}}` naming what is missing.
  """
  @spec authorize(subject() | nil, level()) ::
          :ok | {:error, :authentication_required} | {:error, {:missing_role, ConsoleRole.t(), ConsoleRole.t() | nil}}
  def authorize(_subject, :public), do: :ok
  def authorize(nil, _level), do: {:error, :authentication_required}
  def authorize(%{}, :authenticated), do: :ok

  def authorize(%{role: role}, required) do
    if ConsoleRole.includes?(role, required), do: :ok, else: {:error, {:missing_role, required, role}}
  end

  @doc "The subject of every request while authentication is disabled: no user, and every role."
  @spec anonymous() :: subject()
  def anonymous, do: %{username: nil, role: :admin, permissions: [], authenticated: false, session: nil}

  @doc "Whether authentication is enabled (`MALACHI_DASHBOARD_AUTH_ENABLED`, on unless set to false)."
  @spec auth_enabled?() :: boolean()
  def auth_enabled?, do: Application.get_env(:malachi, :dashboard_auth_enabled, true)

  @doc """
  The session token in a request: the `Authorization: Bearer` header first, the primary mechanism for
  every client, then the `malachi_token` cookie, which a browser login sets as a convenience. `nil` when
  neither is present. `authorization` and `cookie` are the raw header values, or `nil`.
  """
  @spec token(String.t() | nil, String.t() | nil) :: String.t() | nil
  def token(authorization, cookie), do: bearer_token(authorization) || cookie_token(cookie)

  defp bearer_token("Bearer " <> token) when token != "", do: token
  defp bearer_token(_other), do: nil

  defp cookie_token(nil), do: nil

  defp cookie_token(cookie) do
    cookie
    |> String.split(";")
    |> Enum.find_value(fn pair ->
      case pair |> String.trim() |> String.split("=", parts: 2) do
        ["malachi_token", value] when value != "" -> String.trim(value)
        _other -> nil
      end
    end)
  end

  @doc """
  The whole decision for one request to a route that requires `level` (not `:public`): resolves `token`,
  authorizes the subject, and records the outcome, so both endpoints audit and count alike. Returns
  `{:ok, subject}`, or the refusal `resolve/3` or `authorize/2` gave. With authentication disabled the
  subject is `anonymous/0` and nothing is spent or recorded.

  What is recorded: `:dashboard_access` on success; `:dashboard_auth_failure` and the failure counter for
  an invalid session or a missing role; `:dashboard_api_rate_limited` for a session over its budget and
  `:dashboard_auth_failure` for an address over the login budget, each with its blocked counter. A
  request with no credentials at all records nothing, as a page load before login is not an attack.
  """
  @spec admit(String.t() | nil, level(), request()) ::
          {:ok, subject()} | resolve_error() | {:error, {:missing_role, ConsoleRole.t(), ConsoleRole.t() | nil}}
  def admit(token, level, request) do
    if auth_enabled?() do
      outcome =
        with {:ok, subject} <- resolve(token, request.ip, request.user_agent),
             :ok <- refused_by(authorize(subject, level), subject, request) do
          {:ok, subject}
        end

      record(outcome, request)
    else
      {:ok, anonymous()}
    end
  end

  defp refused_by(:ok, _subject, _request), do: :ok

  defp refused_by({:error, missing} = refusal, subject, request) do
    Metrics.increment_dashboard_auth_failed()
    audit(:dashboard_auth_failure, subject.username, request, :failure, %{reason: missing})
    refusal
  end

  defp record({:ok, subject} = admitted, request) do
    audit(:dashboard_access, subject.username, request, :success, %{method: request.method})
    admitted
  end

  # A role refusal was recorded where it happened, with the user it belongs to.
  defp record({:error, {:missing_role, _required, _role}} = refusal, _request), do: refusal
  defp record({:error, :authentication_required} = refusal, _request), do: refusal

  defp record({:error, :api_rate_limit_exceeded, retry_after_ms, digest, username} = refusal, request) do
    # Counted and audited apart from login throttling, so a busy console never reads as a brute force
    # attempt, and recorded under the session digest (never the token) with the user it belongs to.
    Metrics.increment_rate_limit_blocked(:dashboard_api)

    audit(:dashboard_api_rate_limited, username, request, :rate_limited, %{
      session: digest,
      retry_after_ms: retry_after_ms
    })

    refusal
  end

  defp record({:error, :rate_limit_exceeded, retry_after_ms} = refusal, request) do
    Metrics.increment_dashboard_auth_blocked()
    audit(:dashboard_auth_failure, nil, request, :rate_limited, %{retry_after_ms: retry_after_ms})
    refusal
  end

  defp record({:error, reason} = refusal, request) do
    Metrics.increment_dashboard_auth_failed()
    audit(:dashboard_auth_failure, nil, request, :failure, %{reason: reason})
    refusal
  end

  defp audit(event, username, request, status, metadata) do
    context = if username, do: %{username: username, ip: request.ip}, else: %{ip: request.ip}

    AuditLog.log_event(
      event,
      context,
      "http_#{request.method}_#{request.path}",
      status,
      Map.put(metadata, :path, request.path)
    )
  end

  @doc """
  Resolves `token` (from `token/2`) to the subject behind it, from `client_ip` and `user_agent`, spending
  the rate limit buckets described in the module documentation. The role and permissions come from the
  registry, not the session. Every error says what happened; `Malachi.HTTP.Problem.from_error/1` names its
  response. A request refused for lack of a role still spent its session's budget: it is authenticated work.
  """
  @spec resolve(String.t() | nil, term(), String.t()) :: {:ok, subject()} | resolve_error()
  def resolve(nil, _client_ip, _user_agent), do: {:error, :authentication_required}

  def resolve(token, client_ip, user_agent) do
    if Auth.session_valid?(token, client_ip, user_agent) do
      token |> Auth.validate_token(client_ip, user_agent) |> admit(token)
    else
      case check_login_bucket(client_ip) do
        :ok -> token |> Auth.validate_token(client_ip, user_agent) |> admit(token)
        {:error, :rate_limit_exceeded, retry_after_ms} -> {:error, :rate_limit_exceeded, retry_after_ms}
      end
    end
  end

  # A session that stopped validating between the check above and the validation (it expired in the gap)
  # is answered with the reason, once.
  defp admit({:error, reason}, _token), do: {:error, reason}

  defp admit({:ok, %{username: username}}, token) do
    case check_api_bucket(token) do
      :ok ->
        principal(username, session_digest(token))

      {:error, :rate_limit_exceeded, retry_after_ms} ->
        {:error, :api_rate_limit_exceeded, retry_after_ms, session_digest(token), username}
    end
  end

  # The registry is the authority, not the session: a user removed on another node still holds a session
  # here (sessions are per node), and reads as an invalid session.
  defp principal(username, digest) do
    case UserStore.get_principal(username) do
      {:ok, %{permissions: permissions, role: role}} ->
        {:ok,
         %{
           username: username,
           role: effective_role(permissions, role),
           permissions: permissions,
           authenticated: true,
           session: digest
         }}

      {:error, :user_not_found} ->
        {:error, :invalid_session}

      {:error, _unreachable} ->
        {:error, :principal_unavailable}
    end
  end

  @doc """
  The login bucket's limit, read in one place for the two checks that spend it and for `/rate_limits`.
  Unlike `:dashboard_api` it has no off switch: a limit of 0 does not disable brute force protection.
  """
  @spec login_bucket_config() :: %{limit: non_neg_integer(), window_ms: pos_integer()}
  def login_bucket_config do
    %{
      limit: Application.get_env(:malachi, :dashboard_auth_rate_limit, 10),
      window_ms: Application.get_env(:malachi, :dashboard_auth_rate_window_ms, 60_000)
    }
  end

  @doc "Spends one unit of `client_ip`'s login bucket: `:ok` or `{:error, :rate_limit_exceeded, retry_after_ms}`."
  @spec check_login_bucket(term()) :: :ok | {:error, :rate_limit_exceeded, non_neg_integer()}
  def check_login_bucket(client_ip), do: RateLimiter.check_limit(client_ip, :dashboard_auth, login_bucket_config())

  defp check_api_bucket(token) do
    case RateLimiter.action_config(:dashboard_api) do
      nil -> :ok
      config -> RateLimiter.check_limit(session_digest(token), :dashboard_api, config)
    end
  end

  @doc """
  What the API bucket is keyed by: a digest of the session token, never the token. The limiter's table is
  public, and its blocked identifiers are printed by `/rate_limits`, so a raw token there would hand a live
  session to whoever reads it. 128 bits of SHA-256 cannot be reversed or collide in practice, and it is
  what the `:dashboard_api_rate_limited` audit event records, so the two can be matched.
  """
  @spec session_digest(String.t()) :: String.t()
  def session_digest(token) do
    :sha256 |> :crypto.hash(token) |> binary_part(0, 16) |> Base.url_encode64(padding: false)
  end
end
