#!/bin/bash
# Generates DEVELOPMENT certificates for Malachi inter-node TLS (Erlang distribution): a shared CA and one
# CA-signed node certificate (usable by every node, since inet_tls_dist verifies the chain, not the
# hostname), plus a ready-to-use ssl_dist options file. NOT for production, use your real PKI there.
set -e

CERT_DIR="priv/dist_cert"
DAYS=825
mkdir -p "$CERT_DIR"

echo "🔐 Generating development inter-node TLS certificates in $CERT_DIR ..."

# Shared CA
openssl genrsa -out "$CERT_DIR/ca.key" 4096
openssl req -new -x509 -days "$DAYS" -key "$CERT_DIR/ca.key" -out "$CERT_DIR/ca.crt" \
  -subj "/O=Malachi/CN=Malachi Dev Distribution CA"

# Node certificate, signed by the CA, valid for both TLS roles (each node is server and client)
openssl genrsa -out "$CERT_DIR/node.key" 2048
openssl req -new -key "$CERT_DIR/node.key" -out "$CERT_DIR/node.csr" -subj "/O=Malachi/CN=malachi-node"
cat > "$CERT_DIR/node.ext" <<EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:malachi-node,DNS:localhost,IP:127.0.0.1
EOF
openssl x509 -req -days "$DAYS" -in "$CERT_DIR/node.csr" \
  -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" -CAcreateserial \
  -extfile "$CERT_DIR/node.ext" -out "$CERT_DIR/node.crt"

rm -f "$CERT_DIR/node.csr" "$CERT_DIR/node.ext" "$CERT_DIR/ca.srl"
chmod 600 "$CERT_DIR"/*.key

# A ready-to-use ssl_dist options file pointing at the generated certs (absolute paths).
ABS="$(cd "$CERT_DIR" && pwd)"
cat > "$CERT_DIR/dist_tls.conf" <<EOF
[
  {server, [
    {certfile, "$ABS/node.crt"},
    {keyfile, "$ABS/node.key"},
    {cacertfile, "$ABS/ca.crt"},
    {verify, verify_peer},
    {fail_if_no_peer_cert, true}
  ]},
  {client, [
    {certfile, "$ABS/node.crt"},
    {keyfile, "$ABS/node.key"},
    {cacertfile, "$ABS/ca.crt"},
    {verify, verify_peer}
  ]}
].
EOF

echo "✅ CA + node cert generated, and a ready options file at $CERT_DIR/dist_tls.conf"
echo ""
echo "Run a release with inter-node TLS:"
echo "  MALACHIMQ_DIST_TLS=true MALACHIMQ_DIST_TLS_OPTFILE=$ABS/dist_tls.conf bin/malachi start"
