defmodule Malachi.Auth.CertIdentity do
  @moduledoc """
  Pure extraction of a client **identity string** from a DER-encoded X.509 certificate, the deterministic
  core of the mTLS auth provider (P4). Given a peer certificate, it reads the subject Common Name (CN) or a
  Subject Alternative Name (SAN), per a configured policy, yielding the string the mTLS provider maps to a
  malachi user (policy 2A: CN = username, permissions from the replicated user store).

  Pure and side-effect free: it parses the certificate the TLS layer already verified (chain/validity are
  **not** re-checked here: that is the acceptor's `verify_peer`), so it never touches the network or a
  clock. Malformed input yields `{:error, :malformed_certificate}` rather than raising.
  """
  require Record

  # The X.509 records live in public_key's OTP-PUB-KEY.hrl; extracting them lets us read fields by name
  # (robust across OTP versions) instead of by tuple position.
  Record.defrecordp(
    :otp_cert,
    :OTPCertificate,
    Record.extract(:OTPCertificate, from_lib: "public_key/include/OTP-PUB-KEY.hrl")
  )

  Record.defrecordp(
    :otp_tbs,
    :OTPTBSCertificate,
    Record.extract(:OTPTBSCertificate, from_lib: "public_key/include/OTP-PUB-KEY.hrl")
  )

  # id-at-commonName and id-ce-subjectAltName (RFC 5280).
  @cn_oid {2, 5, 4, 3}
  @san_oid {2, 5, 29, 17}

  @typedoc "A friendly SAN kind. Only string-valued SANs usable as an identity are surfaced."
  @type san_kind :: :uri | :dns | :email

  @typedoc "Which certificate field names the identity: the CN, or the first SAN of a given kind."
  @type policy :: :cn | {:san, san_kind()}

  @doc "The subject Common Name, or `{:error, :no_common_name | :malformed_certificate}`."
  @spec common_name(binary()) :: {:ok, String.t()} | {:error, atom()}
  def common_name(der) do
    with {:ok, tbs} <- decode(der) do
      {:rdnSequence, rdns} = otp_tbs(tbs, :subject)

      value =
        rdns
        |> List.flatten()
        |> Enum.find_value(fn
          {:AttributeTypeAndValue, @cn_oid, raw} -> directory_string(raw)
          _other_attribute -> nil
        end)

      case value do
        nil -> {:error, :no_common_name}
        "" -> {:error, :no_common_name}
        cn -> {:ok, cn}
      end
    end
  end

  @doc """
  The Subject Alternative Names as `{kind, value}` (only the string-valued kinds `:uri`/`:dns`/`:email`).
  `{:ok, []}` when the certificate has no SAN extension. `{:error, :malformed_certificate}` on bad input.
  """
  @spec sans(binary()) :: {:ok, [{san_kind(), String.t()}]} | {:error, atom()}
  def sans(der) do
    with {:ok, tbs} <- decode(der) do
      sans =
        tbs
        |> otp_tbs(:extensions)
        |> san_entries()
        |> Enum.flat_map(&normalize_san/1)

      {:ok, sans}
    end
  end

  @doc """
  The identity string named by `policy`: the CN (`:cn`) or a SAN of the given kind (`{:san, kind}`).

  For `{:san, kind}` the **first** SAN of that kind is returned, in **certificate order** (the order the CA
  encoded them), so a cert bearing several SANs of one kind (e.g. multiple SPIFFE URIs) resolves to a single
  deterministic identity. Use a certificate with exactly one SAN of the policy kind when the mapping must be
  unambiguous. Returns `{:ok, identity}`, or `{:error, :no_common_name | :no_matching_san | :malformed_certificate}`.
  """
  @spec identity(binary(), policy()) :: {:ok, String.t()} | {:error, atom()}
  def identity(der, :cn), do: common_name(der)

  def identity(der, {:san, kind}) when kind in [:uri, :dns, :email] do
    with {:ok, sans} <- sans(der) do
      case Enum.find(sans, fn {k, _value} -> k == kind end) do
        {_kind, value} -> {:ok, value}
        nil -> {:error, :no_matching_san}
      end
    end
  end

  # --- internals ---

  defp decode(der) when is_binary(der) do
    otp = :public_key.pkix_decode_cert(der, :otp)
    {:ok, otp_cert(otp, :tbsCertificate)}
  rescue
    _error -> {:error, :malformed_certificate}
  catch
    _kind, _reason -> {:error, :malformed_certificate}
  end

  defp decode(_not_binary), do: {:error, :malformed_certificate}

  # The SAN extension's value (a list of `{san_type, value}`) or `[]` when absent (`:asn1_NOVALUE`).
  defp san_entries(extensions) when is_list(extensions) do
    Enum.find_value(extensions, [], fn
      {:Extension, @san_oid, _critical, entries} when is_list(entries) -> entries
      _other_extension -> false
    end)
  end

  defp san_entries(_no_extensions), do: []

  # Only string-valued SANs are surfaced (iPAddress etc. carry raw bytes, not an identity string, and are
  # dropped). Values arrive as charlists from the ASN.1 decoder.
  defp normalize_san({:uniformResourceIdentifier, value}), do: [{:uri, to_string(value)}]
  defp normalize_san({:dNSName, value}), do: [{:dns, to_string(value)}]
  defp normalize_san({:rfc822Name, value}), do: [{:email, to_string(value)}]
  defp normalize_san(_other_kind), do: []

  # A DirectoryString is `{string_type, value}` (utf8String/printableString/...); normalize to a binary.
  defp directory_string({_string_type, value}), do: to_string(value)
  defp directory_string(value) when is_binary(value) or is_list(value), do: to_string(value)
  defp directory_string(_other), do: nil
end
