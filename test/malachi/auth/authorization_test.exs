defmodule Malachi.Auth.AuthorizationTest do
  use ExUnit.Case, async: true

  alias Malachi.Auth.Authorization

  # An ACL thunk that records whether it was called, so we can assert lazy evaluation.
  defp acl(result) do
    parent = self()

    fn ->
      send(parent, :acl_consulted)
      result
    end
  end

  defp granted, do: fn -> true end
  defp not_granted, do: fn -> false end

  describe "allow?/4 — admin" do
    test "admin is always allowed and never consults the ACL" do
      assert Authorization.allow?([:admin], :produce, false, acl(false))
      assert Authorization.allow?([:admin], :consume, true, acl(false))
      refute_received :acl_consulted
    end
  end

  describe "allow?/4 — non-strict (backward compatible)" do
    test "a global operation permission grants any topic without consulting the ACL" do
      assert Authorization.allow?([:produce], :produce, false, acl(false))
      assert Authorization.allow?([:consume], :consume, false, acl(false))
      refute_received :acl_consulted
    end

    test "without the global permission, the ACL grant decides" do
      assert Authorization.allow?([:consume], :produce, false, granted())
      refute Authorization.allow?([:consume], :produce, false, not_granted())
    end
  end

  describe "allow?/4 — strict (deny-by-default)" do
    test "global permissions are ignored; only an ACL grant (or admin) allows" do
      refute Authorization.allow?([:produce], :produce, true, not_granted())
      assert Authorization.allow?([:produce], :produce, true, granted())
    end

    test "no permission and no grant is denied" do
      refute Authorization.allow?([], :produce, true, not_granted())
      refute Authorization.allow?([], :consume, false, not_granted())
    end
  end
end
