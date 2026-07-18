defmodule Malachi.Auth.MtlsProviderTest do
  use ExUnit.Case, async: true

  alias Malachi.Auth.MtlsProvider
  alias Malachi.Test.CertFixtures

  # A fake user-store lookup: `users` maps username => permissions; anything else is :user_not_found.
  defp lookup(users) do
    fn username ->
      case Map.fetch(users, username) do
        {:ok, permissions} -> {:ok, {username, "argon2-hash", permissions}}
        :error -> {:error, :user_not_found}
      end
    end
  end

  test "maps a cert CN to a provisioned user's identity and permissions" do
    context = %{policy: :cn, lookup: lookup(%{"svc-producer" => [:produce]})}

    assert {:ok, %{username: "svc-producer", permissions: [:produce]}} =
             MtlsProvider.authenticate(CertFixtures.der("svc_producer"), context)
  end

  test "resolves identity from a SAN URI when the policy selects it" do
    context = %{policy: {:san, :uri}, lookup: lookup(%{"spiffe://malachi/svc-producer" => [:admin]})}

    assert {:ok, %{username: "spiffe://malachi/svc-producer", permissions: [:admin]}} =
             MtlsProvider.authenticate(CertFixtures.der("svc_producer"), context)
  end

  test "defaults to the :cn policy when none is given" do
    context = %{lookup: lookup(%{"svc-producer" => [:consume]})}
    assert {:ok, %{username: "svc-producer"}} = MtlsProvider.authenticate(CertFixtures.der("svc_producer"), context)
  end

  test "a valid cert whose identity is not a provisioned user is :unknown_identity" do
    context = %{policy: :cn, lookup: lookup(%{})}
    assert {:error, :unknown_identity} = MtlsProvider.authenticate(CertFixtures.der("svc_producer"), context)
  end

  test "a cert with no usable identity for the policy is :no_identity" do
    # plain_admin carries no SAN, so a SAN-URI policy finds nothing to map.
    context = %{policy: {:san, :uri}, lookup: lookup(%{})}
    assert {:error, :no_identity} = MtlsProvider.authenticate(CertFixtures.der("plain_admin"), context)
  end

  test "a malformed certificate is rejected distinctly from a policy miss" do
    assert {:error, :malformed_certificate} = MtlsProvider.authenticate(<<0, 1, 2, 3>>, %{lookup: lookup(%{})})
  end

  test "no peer certificate is rejected" do
    assert {:error, :no_peer_certificate} = MtlsProvider.authenticate(nil, %{})
  end

  test "a user-store error is passed through (not masked as unknown_identity)" do
    context = %{policy: :cn, lookup: fn _username -> {:error, :timeout} end}
    assert {:error, :timeout} = MtlsProvider.authenticate(CertFixtures.der("svc_producer"), context)
  end

  test "fails closed if the lookup returns a record for a different username" do
    # Defense in depth: never grant another user's permissions to the cert identity.
    context = %{policy: :cn, lookup: fn _username -> {:ok, {"someone-else", "hash", [:admin]}} end}
    assert {:error, :unknown_identity} = MtlsProvider.authenticate(CertFixtures.der("svc_producer"), context)
  end
end
