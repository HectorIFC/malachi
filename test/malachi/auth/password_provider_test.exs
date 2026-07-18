defmodule Malachi.Auth.PasswordProviderTest do
  use ExUnit.Case, async: true

  alias Malachi.Auth.PasswordProvider

  test "resolves valid credentials to an identity" do
    context = %{verify: fn "alice", "pw" -> {:ok, [:produce, :consume]} end}

    assert {:ok, %{username: "alice", permissions: [:produce, :consume]}} =
             PasswordProvider.authenticate({"alice", "pw"}, context)
  end

  test "passes through an invalid-password error (no session, no masking)" do
    context = %{verify: fn _username, _password -> {:error, :invalid_password} end}
    assert {:error, :invalid_password} = PasswordProvider.authenticate({"alice", "bad"}, context)
  end

  test "passes through an unknown-user error" do
    context = %{verify: fn _username, _password -> {:error, :user_not_found} end}
    assert {:error, :user_not_found} = PasswordProvider.authenticate({"ghost", "pw"}, context)
  end
end
