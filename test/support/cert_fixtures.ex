defmodule Malachi.Test.CertFixtures do
  @moduledoc """
  Loads the test X.509 certificate fixtures under `test/support/fixtures/certs/`.

  These are **public** self-signed certificates only: a certificate carries no secret (just the public key,
  subject and SANs), so committing them is safe (unlike a private key, which is never checked in). They are
  throwaway test material generated with `openssl req -x509`.

  Fixtures:

    * `svc_producer`, CN `svc-producer`, SAN `URI:spiffe://malachi/svc-producer` + `DNS:producer.internal`
    * `plain_admin`, CN `plain-admin`, no SAN extension
    * `multi_uri`, CN `multi-svc`, SANs `URI:spiffe://malachi/first`, `URI:spiffe://malachi/second`, `DNS:multi.internal`
  """

  @certs_dir Path.join(__DIR__, "fixtures/certs")

  @doc "The DER-encoded certificate `name` (e.g. `\"svc_producer\"`)."
  @spec der(String.t()) :: binary()
  def der(name) do
    [{:Certificate, der, :not_encrypted}] = name |> pem() |> :public_key.pem_decode()
    der
  end

  @doc "The raw PEM text of certificate `name`."
  @spec pem(String.t()) :: String.t()
  def pem(name), do: File.read!(Path.join(@certs_dir, "#{name}.pem"))

  @doc """
  Generates a throwaway self-signed server certificate and key (CN `localhost`, one day) under `dir`, and
  returns their paths. Generated rather than committed because a private key is never checked in; openssl
  ships on the CI image and is what the project's own cert scripts use.
  """
  @spec server_cert!(Path.t()) :: %{certfile: Path.t(), keyfile: Path.t()}
  def server_cert!(dir) do
    File.mkdir_p!(dir)
    certfile = Path.join(dir, "cert.pem")
    keyfile = Path.join(dir, "key.pem")

    args = ~w(req -x509 -newkey rsa:2048 -nodes -keyout #{keyfile} -out #{certfile} -days 1 -subj /CN=localhost)
    {_out, 0} = System.cmd("openssl", args, stderr_to_stdout: true)
    %{certfile: certfile, keyfile: keyfile}
  end
end
