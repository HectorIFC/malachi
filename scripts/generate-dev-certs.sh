#!/bin/bash

# Malachi - Development TLS Certificate Generator
# Generates self-signed certificates for local development

set -e

echo "🔐 Generating development TLS certificates for Malachi..."
echo ""

# Create certificate directory
CERT_DIR="priv/cert"
mkdir -p "$CERT_DIR"

# Certificate settings
DAYS=365
SUBJECT="/C=US/ST=Development/L=Local/O=Malachi/CN=localhost"

echo "📁 Certificate directory: $CERT_DIR"
echo "⏰ Validity: $DAYS days"
echo "📋 Subject: $SUBJECT"
echo ""

# Generate private key
echo "🔑 Generating private key..."
openssl genrsa -out "$CERT_DIR/server.key" 2048
chmod 600 "$CERT_DIR/server.key"  # Restrict private key permissions

# Generate certificate signing request
echo "📝 Generating certificate signing request..."
openssl req -new \
  -key "$CERT_DIR/server.key" \
  -out "$CERT_DIR/server.csr" \
  -subj "$SUBJECT"

# Create extensions file for Subject Alternative Names
cat > "$CERT_DIR/server.ext" <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = *.localhost
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

# Generate self-signed certificate
echo "📜 Generating self-signed certificate..."
openssl x509 -req \
  -in "$CERT_DIR/server.csr" \
  -signkey "$CERT_DIR/server.key" \
  -out "$CERT_DIR/server.crt" \
  -days "$DAYS" \
  -sha256 \
  -extfile "$CERT_DIR/server.ext"

chmod 644 "$CERT_DIR/server.crt"  # Public certificate can be readable

# Clean up temporary files
rm "$CERT_DIR/server.csr"
rm "$CERT_DIR/server.ext"

echo ""
echo "✅ Certificates generated successfully!"
echo ""
echo "📂 Files created:"
echo "  - $CERT_DIR/server.key (private key)"
echo "  - $CERT_DIR/server.crt (certificate)"
echo ""

# Show certificate details
echo "📋 Certificate details:"
openssl x509 -in "$CERT_DIR/server.crt" -noout -subject -dates -fingerprint -sha256
echo ""

# Test certificate
echo "🔍 Verifying certificate..."
openssl verify -CAfile "$CERT_DIR/server.crt" "$CERT_DIR/server.crt" 2>&1 | grep -q "OK" && \
  echo "✅ Certificate verification: OK" || \
  echo "⚠️  Self-signed certificate (expected for development)"
echo ""

echo "🚀 Ready to use TLS! Set these environment variables:"
echo ""
echo "  export MALACHI_ENABLE_TLS=true"
echo "  export MALACHI_TLS_CERTFILE=$CERT_DIR/server.crt"
echo "  export MALACHI_TLS_KEYFILE=$CERT_DIR/server.key"
echo ""
echo "⚠️  WARNING: These are development certificates only!"
echo "   DO NOT use in production. Use Let's Encrypt or a commercial CA."
echo ""
