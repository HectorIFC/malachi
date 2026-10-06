defmodule Malachi.HTTP.Problem do
  @moduledoc """
  The one error shape of every HTTP surface, the legacy dashboard (`Malachi.Dashboard`) and the console
  (`Malachi.Console.Router`): an RFC 9457 `application/problem+json` body whose `type` is a **translation
  key** and whose data travel in extension members, as section 10.3 of
  `docs/design/operator-interfaces.md` sets. The body carries no `title` or `detail`: `Malachi.I18n.locale/0`
  is node global, so a phrase translated here would come out in the node's language rather than the
  operator's, and the client translates the key itself.

  `from_error/1` is the single map from an error a handler meets to its status, key and members, so two
  endpoints meeting the same error answer it the same way. `build/2` and `for_error/1` are pure and
  return the status, the headers and the encoded body, which each endpoint frames with its own transport.

  ## Example

      iex> {status, type, members} = Malachi.HTTP.Problem.from_error({:missing_role, :editor, :viewer})
      iex> {status, type, members}
      {403, "errors.auth.missing_role", %{"required_role" => "editor", "role" => "viewer"}}

  """

  alias Malachi.Auth.ConsoleRole

  @content_type "application/problem+json"

  # Every key a response can carry, with its status. A client builds its catalog from this list.
  @types %{
    "errors.auth.unauthenticated" => 401,
    "errors.auth.invalid_credentials" => 401,
    "errors.auth.session_expired" => 401,
    "errors.auth.session_rejected" => 401,
    "errors.auth.session_invalid" => 401,
    "errors.auth.missing_role" => 403,
    "errors.auth.unavailable" => 503,
    "errors.http.invalid_request" => 400,
    "errors.http.not_found" => 404,
    "errors.http.method_not_allowed" => 405,
    "errors.http.header_fields_too_large" => 431,
    "errors.http.rate_limited" => 429,
    "errors.users.exists" => 409,
    "errors.users.not_found" => 404,
    "errors.users.invalid_permissions" => 400,
    "errors.users.invalid_role" => 400,
    "errors.users.persist_failed" => 503,
    "errors.acls.invalid_operation" => 400,
    "errors.acls.invalid" => 400,
    "errors.cluster.upgrade_pending" => 409,
    "errors.policies.no_such_policy" => 404,
    "errors.policies.no_such_topic" => 404,
    "errors.policies.policy_in_use" => 409,
    "errors.policies.unsupported_policy_field" => 409,
    "errors.policies.invalid_policy_name" => 400,
    "errors.policies.invalid_topic" => 400,
    "errors.policies.invalid_policy" => 400,
    "errors.policies.unknown_policy_field" => 400,
    "errors.policies.invalid_policy_field" => 400,
    "errors.policies.duplicate_policy_field" => 400,
    "errors.policies.timeout" => 503,
    "errors.http.unavailable" => 503
  }

  @typedoc "A translation key from `types/0`."
  @type type :: String.t()

  @doc "Every translation key a problem response can carry, mapped to its HTTP status."
  @spec types() :: %{type() => pos_integer()}
  def types, do: @types

  @doc """
  The response for `type` with extension `members`, as `{status, headers, body}`. The status comes from
  `types/0`, so a key and its status cannot disagree. Headers are the content type plus what the status
  obliges: `WWW-Authenticate` on a 401 and `Retry-After` (whole seconds, rounded up) on a 429 that carries
  `retry_after_ms`. Raises on a key that is not in `types/0`, which a test meets before a client does.
  """
  @spec build(type(), map()) :: {pos_integer(), [{String.t(), String.t()}], binary()}
  def build(type, members \\ %{}) when is_map(members) do
    status = status(type)
    body = Jason.encode!(Map.merge(members, %{"type" => type, "status" => status}))
    {status, [{"Content-Type", @content_type} | status_headers(status, members)], body}
  end

  @doc "`build/2` for the error `from_error/1` classifies."
  @spec for_error(term()) :: {pos_integer(), [{String.t(), String.t()}], binary()}
  def for_error(error) do
    {_status, type, members} = from_error(error)
    build(type, members)
  end

  defp status_headers(401, _members), do: [{"WWW-Authenticate", ~s(Bearer realm="Malachi")}]

  defp status_headers(429, %{"retry_after_ms" => ms}) when is_integer(ms),
    do: [{"Retry-After", Integer.to_string(div(ms + 999, 1000))}]

  defp status_headers(_status, _members), do: []

  @doc "The status `type` is answered with."
  @spec status(type()) :: pos_integer()
  def status(type), do: Map.fetch!(@types, type)

  @doc """
  The status, translation key and extension members for an error a handler met. Covers the session and
  role refusals of `Malachi.Console.Access`, the user and ACL writes of `Malachi.Auth`, a machine version
  refusal (`Malachi.Cluster.MachineVersion.refusal?/1`) and the policy refusals of `Malachi.Policies`.
  Anything else is a control plane that did not answer usefully, a 503 the same request may pass later.
  """
  @spec from_error(term()) :: {pos_integer(), type(), map()}
  def from_error(error) do
    {type, members} = classify(error)
    {status(type), type, members}
  end

  defp classify(:authentication_required), do: {"errors.auth.unauthenticated", %{}}
  defp classify(:invalid_credentials), do: {"errors.auth.invalid_credentials", %{}}
  defp classify(:session_expired), do: {"errors.auth.session_expired", %{}}
  defp classify(:session_hijack_attempt), do: {"errors.auth.session_rejected", %{}}
  defp classify(:invalid_session), do: {"errors.auth.session_invalid", %{}}
  defp classify(:principal_unavailable), do: {"errors.auth.unavailable", %{}}

  defp classify({:missing_role, required, actual}),
    do: {"errors.auth.missing_role", %{"required_role" => role_string(required), "role" => role_string(actual)}}

  defp classify(:invalid_request), do: {"errors.http.invalid_request", %{}}
  defp classify(:not_found), do: {"errors.http.not_found", %{}}
  defp classify(:method_not_allowed), do: {"errors.http.method_not_allowed", %{}}
  defp classify(:header_fields_too_large), do: {"errors.http.header_fields_too_large", %{}}

  defp classify({:rate_limited, retry_after_ms}),
    do: {"errors.http.rate_limited", %{"retry_after_ms" => retry_after_ms}}

  defp classify(:user_exists), do: {"errors.users.exists", %{}}
  defp classify(:user_not_found), do: {"errors.users.not_found", %{}}
  defp classify(:invalid_permissions), do: {"errors.users.invalid_permissions", %{}}

  defp classify(:invalid_role),
    do: {"errors.users.invalid_role", %{"roles" => Enum.map(ConsoleRole.all(), &to_string/1)}}

  defp classify(:persist_failed), do: {"errors.users.persist_failed", %{}}
  defp classify(:invalid_operation), do: {"errors.acls.invalid_operation", %{}}
  defp classify(:invalid_acl), do: {"errors.acls.invalid", %{}}

  defp classify({:unsupported_command, _key, introduced, effective}), do: upgrade_pending(introduced, effective)
  defp classify({:unknown_command, _key, effective}), do: upgrade_pending(nil, effective)

  defp classify(reason)
       when reason in [
              :no_such_policy,
              :no_such_topic,
              :invalid_policy_name,
              :invalid_topic,
              :invalid_policy,
              :timeout
            ],
       do: {"errors.policies.#{reason}", %{}}

  defp classify({:policy_in_use, topics}), do: {"errors.policies.policy_in_use", %{"topics" => topics}}

  defp classify({:unsupported_policy_field, field, introduced, effective}) do
    {"errors.policies.unsupported_policy_field",
     %{"field" => field, "introduced" => introduced, "effective" => effective}}
  end

  defp classify({field_reason, field})
       when field_reason in [:unknown_policy_field, :invalid_policy_field, :duplicate_policy_field],
       do: {"errors.policies.#{field_reason}", %{"field" => field}}

  defp classify(_unavailable), do: {"errors.http.unavailable", %{}}

  defp upgrade_pending(introduced, effective),
    do: {"errors.cluster.upgrade_pending", %{"introduced" => introduced, "effective" => effective}}

  defp role_string(nil), do: nil
  defp role_string(role), do: Atom.to_string(role)
end
