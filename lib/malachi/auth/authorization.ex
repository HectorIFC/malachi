defmodule Malachi.Auth.Authorization do
  @moduledoc """
  The pure per-topic authorization decision (P5, decision 5-1A) — composes the coarse global RBAC with the
  fine-grained ACL grant into a single allow/deny, so the enforcement boundary stays a one-line check.

  `allow?/4` layers three rules, in order:

    1. `:admin` is a superuser — always allowed.
    2. **Backward compatibility (default):** when strict mode is **off**, a global operation permission
       (`:produce`/`:consume` in the session) grants the operation on **any** topic, exactly as before ACLs
       existed — so enabling ACLs breaks no existing deployment.
    3. Otherwise the decision falls to the per-topic ACL grant (`acl_grant?`). In **strict mode** rules 1 and
       3 are the only paths: global permissions are ignored and access is **deny-by-default** — only an
       explicit ACL (or admin) allows the operation.

  Pure and total; the caller supplies `acl_grant?` (from `Malachi.Auth.AclRegistry.authorized?/4`) and the
  `strict?` flag (from config), so this module has no dependencies and is exhaustively testable.
  """

  @doc "Whether `operation` is allowed given the session `permissions`, the ACL match `acl_grant?`, and `strict?`."
  @spec allow?([atom()], atom(), boolean(), boolean()) :: boolean()
  def allow?(permissions, operation, acl_grant?, strict?) when is_list(permissions) and is_boolean(acl_grant?) do
    cond do
      :admin in permissions -> true
      not strict? and operation in permissions -> true
      true -> acl_grant?
    end
  end
end
