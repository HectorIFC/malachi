defmodule Malachi.Auth.ConsoleRoleTest do
  use ExUnit.Case, async: true

  alias Malachi.Auth.ConsoleRole

  doctest ConsoleRole

  test "the three roles, least privileged first" do
    assert ConsoleRole.all() == [:viewer, :editor, :admin]
  end

  test "valid? accepts the roles and nil, nothing else" do
    for role <- [nil, :viewer, :editor, :admin], do: assert(ConsoleRole.valid?(role))
    for other <- [:root, "viewer", :produce, 1], do: refute(ConsoleRole.valid?(other))
  end

  test "parse maps the fixed strings and refuses anything else without creating an atom" do
    assert ConsoleRole.parse("viewer") == {:ok, :viewer}
    assert ConsoleRole.parse("admin") == {:ok, :admin}
    assert ConsoleRole.parse(nil) == {:ok, nil}
    assert ConsoleRole.parse("Viewer") == :error
    assert ConsoleRole.parse(42) == :error

    unseen = "role_never_seen_#{System.unique_integer([:positive])}"
    assert ConsoleRole.parse(unseen) == :error
    assert_raise ArgumentError, fn -> String.to_existing_atom(unseen) end
  end

  test "the roles are strictly nested" do
    for {role, index} <- Enum.with_index(ConsoleRole.all()),
        {required, required_index} <- Enum.with_index(ConsoleRole.all()) do
      assert ConsoleRole.includes?(role, required) == index >= required_index
    end

    for required <- ConsoleRole.all(), do: refute(ConsoleRole.includes?(nil, required))
  end

  test "max picks the more privileged role in either order" do
    assert ConsoleRole.max(:admin, :viewer) == :admin
    assert ConsoleRole.max(:editor, :editor) == :editor
    assert ConsoleRole.max(:viewer, nil) == :viewer
    assert ConsoleRole.max(nil, nil) == nil
  end
end
