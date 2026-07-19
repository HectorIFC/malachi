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

  The ACL grant is supplied as a **thunk** (`acl_grant_fun`), evaluated only in the third rule — so the
  produce/consume hot path pays for an ACL lookup only when the decision actually needs it (strict mode, or a
  user without the global permission), never when admin or a global permission already settles it. Pure and
  total; `strict?` comes from config, so this module has no dependencies and is exhaustively testable.
  """

  @doc """
  Whether `operation` is allowed given the session `permissions` and `strict?`. `acl_grant_fun` is a
  zero-arity function returning whether a matching per-topic ACL grant exists; it is called only when the
  decision falls through to the ACL (so the caller can skip an ACL store query otherwise).
  """
  @spec allow?([atom()], atom(), boolean(), (-> boolean())) :: boolean()
  def allow?(permissions, operation, strict?, acl_grant_fun)
      when is_list(permissions) and is_boolean(strict?) and is_function(acl_grant_fun, 0) do
    cond do
      :admin in permissions -> true
      not strict? and operation in permissions -> true
      true -> acl_grant_fun.()
    end
  end
end
