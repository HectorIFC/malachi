defmodule Malachi.WireUserFramesTest do
  @moduledoc """
  The console role api keys (22 and 23) against the golden frames both clients read
  (`test/support/fixtures/wire/user_frames.json`; the Node side is `scripts/lib/wire.selftest.js`). A
  shipped frame is frozen, so a difference here is a wire break, not a fixture to regenerate.
  """
  use ExUnit.Case, async: true

  alias Malachi.Wire

  @fixture "test/support/fixtures/wire/user_frames.json"
  @external_resource @fixture
  @cases @fixture |> File.read!() |> Jason.decode!() |> Map.fetch!("cases")

  defp encode("set_role_req", v), do: Wire.encode_set_role_req(v["username"], v["role"])
  defp encode("list_users_with_roles_req", nil), do: <<>>

  defp encode("list_users_with_roles_resp", users) do
    Wire.encode_list_users_with_roles_resp(
      Enum.map(users, fn u ->
        %{
          username: u["username"],
          permissions: Enum.map(u["permissions"], &String.to_existing_atom/1),
          role: u["role"] && String.to_existing_atom(u["role"])
        }
      end)
    )
  end

  defp decode("set_role_req", bin) do
    {username, role} = Wire.decode_set_role_req(bin)
    %{"username" => username, "role" => role}
  end

  defp decode("list_users_with_roles_req", <<>>), do: nil

  defp decode("list_users_with_roles_resp", bin) do
    for u <- Wire.decode_list_users_with_roles_resp(bin),
        do: %{"username" => u.username, "permissions" => u.permissions, "role" => u.role}
  end

  test "the fixture covers every console role codec, both directions" do
    codecs = @cases |> Enum.map(& &1["codec"]) |> Enum.uniq() |> Enum.sort()
    assert codecs == ~w(list_users_with_roles_req list_users_with_roles_resp set_role_req)
  end

  for %{"name" => name, "codec" => codec, "value" => value, "hex" => hex} <- @cases do
    test "#{codec}: #{name}" do
      bytes = Base.decode16!(unquote(hex), case: :lower)
      assert encode(unquote(codec), unquote(Macro.escape(value))) == bytes
      assert decode(unquote(codec), bytes) == unquote(Macro.escape(value))
    end
  end

  test "the api keys are the next free numbers after the storage policy keys" do
    assert {Wire.set_role_key(), Wire.list_users_with_roles_key()} == {22, 23}
  end

  test "a malformed payload raises, which the connection answers as malformed_request" do
    request = Wire.encode_set_role_req("alice", "viewer")
    assert_raise MatchError, fn -> Wire.decode_set_role_req(request <> <<0>>) end

    response = Wire.encode_list_users_with_roles_resp([%{username: "a", permissions: [], role: nil}])
    assert_raise MatchError, fn -> Wire.decode_list_users_with_roles_resp(response <> <<0>>) end
  end
end
