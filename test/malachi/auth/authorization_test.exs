defmodule Malachi.Auth.AuthorizationTest do
  use ExUnit.Case, async: true

  alias Malachi.Auth.Authorization

  describe "allow?/4 — admin" do
    test "admin is always allowed, regardless of strict mode or ACL" do
      assert Authorization.allow?([:admin], :produce, false, false)
      assert Authorization.allow?([:admin], :consume, false, true)
    end
  end

  describe "allow?/4 — non-strict (backward compatible)" do
    test "a global operation permission grants any topic when strict is off" do
      assert Authorization.allow?([:produce], :produce, false, false)
      assert Authorization.allow?([:consume], :consume, false, false)
    end

    test "without the global permission, falls back to the ACL grant" do
      assert Authorization.allow?([:consume], :produce, true, false)
      refute Authorization.allow?([:consume], :produce, false, false)
    end
  end

  describe "allow?/4 — strict (deny-by-default)" do
    test "global permissions are ignored; only an ACL grant (or admin) allows" do
      refute Authorization.allow?([:produce], :produce, false, true)
      assert Authorization.allow?([:produce], :produce, true, true)
    end

    test "no permission and no grant is denied" do
      refute Authorization.allow?([], :produce, false, true)
      refute Authorization.allow?([], :consume, false, false)
    end
  end
end
