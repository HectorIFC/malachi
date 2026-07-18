defmodule Malachi.Auth.CertIdentityTest do
  use ExUnit.Case, async: true

  alias Malachi.Auth.CertIdentity
  alias Malachi.Test.CertFixtures

  describe "common_name/1" do
    test "extracts the subject CN" do
      assert {:ok, "svc-producer"} = CertIdentity.common_name(CertFixtures.der("svc_producer"))
      assert {:ok, "plain-admin"} = CertIdentity.common_name(CertFixtures.der("plain_admin"))
    end

    test "rejects malformed input rather than raising" do
      assert {:error, :malformed_certificate} = CertIdentity.common_name(<<0, 1, 2, 3>>)
      assert {:error, :malformed_certificate} = CertIdentity.common_name(<<>>)
      assert {:error, :malformed_certificate} = CertIdentity.common_name(:not_a_binary)
    end
  end

  describe "sans/1" do
    test "returns the string-valued SANs as {kind, value}, in certificate order" do
      assert {:ok, sans} = CertIdentity.sans(CertFixtures.der("svc_producer"))
      assert sans == [uri: "spiffe://malachi/svc-producer", dns: "producer.internal"]
    end

    test "returns every SAN of a repeated kind (order preserved)" do
      assert {:ok, sans} = CertIdentity.sans(CertFixtures.der("multi_uri"))

      assert sans == [
               uri: "spiffe://malachi/first",
               uri: "spiffe://malachi/second",
               dns: "multi.internal"
             ]
    end

    test "returns an empty list when the certificate has no SAN extension" do
      assert {:ok, []} = CertIdentity.sans(CertFixtures.der("plain_admin"))
    end

    test "rejects malformed input" do
      assert {:error, :malformed_certificate} = CertIdentity.sans(<<0, 1, 2>>)
    end
  end

  describe "identity/2" do
    test ":cn policy yields the Common Name" do
      assert {:ok, "svc-producer"} = CertIdentity.identity(CertFixtures.der("svc_producer"), :cn)
    end

    test "{:san, kind} yields the first matching SAN" do
      cert = CertFixtures.der("svc_producer")
      assert {:ok, "spiffe://malachi/svc-producer"} = CertIdentity.identity(cert, {:san, :uri})
      assert {:ok, "producer.internal"} = CertIdentity.identity(cert, {:san, :dns})
    end

    test "{:san, kind} returns the first of several SANs of that kind (deterministic, certificate order)" do
      # multi_uri carries two URI SANs; the policy must resolve to a single, stable identity (the first).
      assert {:ok, "spiffe://malachi/first"} = CertIdentity.identity(CertFixtures.der("multi_uri"), {:san, :uri})
    end

    test "{:san, kind} with no matching SAN is an error" do
      assert {:error, :no_matching_san} = CertIdentity.identity(CertFixtures.der("svc_producer"), {:san, :email})
      assert {:error, :no_matching_san} = CertIdentity.identity(CertFixtures.der("plain_admin"), {:san, :uri})
    end

    test "propagates malformed-certificate errors" do
      assert {:error, :malformed_certificate} = CertIdentity.identity(<<9, 9, 9>>, :cn)
      assert {:error, :malformed_certificate} = CertIdentity.identity(<<9, 9, 9>>, {:san, :uri})
    end
  end
end
