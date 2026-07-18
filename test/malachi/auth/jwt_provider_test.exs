defmodule Malachi.Auth.JwtProviderTest do
  use ExUnit.Case, async: true

  alias Malachi.Auth.JwtProvider
  alias Malachi.Test.JwtFixtures

  @issuer "https://idp.example"
  @audience "malachi"

  # A fake user-store lookup: `users` maps username => permissions; anything else is :user_not_found.
  defp lookup(users) do
    fn username ->
      case Map.fetch(users, username) do
        {:ok, permissions} -> {:ok, {username, "hash", permissions}}
        :error -> {:error, :user_not_found}
      end
    end
  end

  setup do
    {sign, verify} = JwtFixtures.rs256_keypair()
    now = System.system_time(:second)
    claims = %{"iss" => @issuer, "aud" => @audience, "sub" => "alice", "exp" => now + 300}
    context = %{signer: verify, issuer: @issuer, audience: @audience, identity_claim: "sub"}
    {:ok, sign: sign, context: context, claims: claims, now: now}
  end

  test "maps a valid token's identity claim to a provisioned user's identity + permissions", ctx do
    token = JwtFixtures.sign(ctx.sign, ctx.claims)
    context = Map.put(ctx.context, :lookup, lookup(%{"alice" => [:produce]}))
    assert {:ok, %{username: "alice", permissions: [:produce]}} = JwtProvider.authenticate(token, context)
  end

  test "honors a configurable identity claim", ctx do
    token = JwtFixtures.sign(ctx.sign, Map.put(ctx.claims, "preferred_username", "svc"))
    context = ctx.context |> Map.put(:identity_claim, "preferred_username") |> Map.put(:lookup, lookup(%{"svc" => [:admin]}))
    assert {:ok, %{username: "svc", permissions: [:admin]}} = JwtProvider.authenticate(token, context)
  end

  test "a valid token whose identity is not a provisioned user is :unknown_identity", ctx do
    token = JwtFixtures.sign(ctx.sign, ctx.claims)
    context = Map.put(ctx.context, :lookup, lookup(%{}))
    assert {:error, :unknown_identity} = JwtProvider.authenticate(token, context)
  end

  test "a token missing the identity claim is :no_identity", ctx do
    token = JwtFixtures.sign(ctx.sign, Map.delete(ctx.claims, "sub"))
    context = Map.put(ctx.context, :lookup, lookup(%{}))
    assert {:error, :no_identity} = JwtProvider.authenticate(token, context)
  end

  test "validation errors pass through as themselves (expired, bad signature)", ctx do
    context = Map.put(ctx.context, :lookup, lookup(%{"alice" => [:produce]}))

    expired = JwtFixtures.sign(ctx.sign, %{ctx.claims | "exp" => ctx.now - 10})
    assert {:error, :token_expired} = JwtProvider.authenticate(expired, context)

    {other_sign, _} = JwtFixtures.rs256_keypair()
    forged = JwtFixtures.sign(other_sign, ctx.claims)
    assert {:error, :invalid_signature} = JwtProvider.authenticate(forged, context)
  end

  test "a user-store error is passed through (not masked as unknown_identity)", ctx do
    token = JwtFixtures.sign(ctx.sign, ctx.claims)
    context = Map.put(ctx.context, :lookup, fn _username -> {:error, :timeout} end)
    assert {:error, :timeout} = JwtProvider.authenticate(token, context)
  end

  test "fails closed if the lookup returns a record for a different username", ctx do
    token = JwtFixtures.sign(ctx.sign, ctx.claims)
    context = Map.put(ctx.context, :lookup, fn _username -> {:ok, {"someone-else", "hash", [:admin]}} end)
    assert {:error, :unknown_identity} = JwtProvider.authenticate(token, context)
  end

  test "a non-binary token is rejected", ctx do
    assert {:error, :invalid_token} = JwtProvider.authenticate(nil, ctx.context)
  end
end
