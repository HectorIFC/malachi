#!/usr/bin/env bash
# Installs Lefthook and wires up this repository's git hooks, so every contributor commits through the
# same checks.
#
# The install downloads a PINNED release binary and verifies its sha256 against scripts/lefthook.checksums
# before anything is executed or unpacked. It used to pipe a remote setup script straight into `sudo bash`
# (issue #69): whoever controlled that URL, or held a TLS-stripping network position, ran arbitrary code as
# root on a contributor's laptop. Do not "simplify" this back into a pipe-to-shell; CI enforces the rule via
# scripts/check-no-pipe-to-shell.sh.
#
# The binary lands inside the clone (.lefthook/, already gitignored), so no sudo, no apt repository, and no
# third-party package feed is involved, and nothing is written outside the working copy.
#
# To bump the version: change LEFTHOOK_VERSION below, then replace the checksum table wholesale with
#   curl -fsSL https://github.com/evilmartians/lefthook/releases/download/v<version>/lefthook_checksums.txt \
#     -o scripts/lefthook.checksums
# The vendored copy deliberately drops the .txt extension: .gitignore ignores *.txt repo-wide, so a file
# named lefthook_checksums.txt would be silently untracked and CI would clone without it.
#
# Prefer to manage Lefthook yourself (Homebrew, go install, a distro package)? Skip this script entirely and
# run `lefthook install` from the repository root. This script deliberately does not consult a Lefthook on
# PATH: pinning a version and then running whatever happens to be installed would be decorative pinning.
#
# Usage: scripts/setup-dev.sh
set -euo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)"

LEFTHOOK_VERSION=2.1.12
CHECKSUMS=scripts/lefthook.checksums
BIN=.lefthook/bin/lefthook

WORK=""
cleanup() {
  if [ -n "$WORK" ]; then rm -rf "$WORK"; fi
}
trap cleanup EXIT

die() {
  echo "❌ $*" >&2
  exit 1
}

# Prints the sha256 of a file. Aborts rather than falling through when neither tool is available: a setup
# that "verifies" without a hash function is worse than one that refuses, because it looks like it checked.
sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | cut -d' ' -f1
  else
    die "no sha256 tool found (need sha256sum or shasum). Refusing to install an unverified binary."
  fi
}

# Linux arm64 and aarch64 are the same upstream build (identical hash in the checksum table); both names
# appear because uname disagrees across distributions.
asset_for_platform() {
  local os arch
  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os:$arch" in
    Darwin:arm64) echo "lefthook_${LEFTHOOK_VERSION}_MacOS_arm64" ;;
    Darwin:x86_64) echo "lefthook_${LEFTHOOK_VERSION}_MacOS_x86_64" ;;
    Linux:x86_64 | Linux:amd64) echo "lefthook_${LEFTHOOK_VERSION}_Linux_x86_64" ;;
    Linux:aarch64 | Linux:arm64) echo "lefthook_${LEFTHOOK_VERSION}_Linux_aarch64" ;;
    *)
      die "unsupported platform $os $arch. Install Lefthook manually and run \`lefthook install\`:
   https://github.com/evilmartians/lefthook#install"
      ;;
  esac
}

# Exact field match rather than a regex: asset names contain dots, and a missing entry has to be an error
# instead of an empty string that would then "match" nothing and pass.
expected_sha() {
  local sha
  sha="$(awk -v name="$1" '$2 == name { print $1 }' "$CHECKSUMS")"
  if [ -z "$sha" ]; then
    die "no checksum recorded for $1 in $CHECKSUMS. Refusing to use an unverifiable download."
  fi
  echo "$sha"
}

# $1 is the file on disk, $2 the asset name whose recorded hash it must match.
verify_sha() {
  local expected actual
  expected="$(expected_sha "$2")"
  actual="$(sha256_of "$1")"

  if [ "$expected" != "$actual" ]; then
    die "checksum mismatch for $2
   expected: $expected
   actual:   $actual
   The download does not match the pinned release. Nothing was installed."
  fi
}

ensure_lefthook() {
  if [ -x "$BIN" ] && [ "$("$BIN" version 2>/dev/null | head -1)" = "$LEFTHOOK_VERSION" ]; then
    echo "✅ Lefthook $LEFTHOOK_VERSION already installed at $BIN"
    return
  fi

  local asset url
  asset="$(asset_for_platform)"
  url="https://github.com/evilmartians/lefthook/releases/download/v${LEFTHOOK_VERSION}/${asset}.gz"

  WORK="$(mktemp -d)"

  echo "📦 Downloading Lefthook $LEFTHOOK_VERSION ($asset.gz)..."
  curl -fsSL --proto '=https' --tlsv1.2 --retry 3 --retry-connrefused --max-time 120 \
    "$url" -o "$WORK/$asset.gz" || die "download failed: $url"

  # Verify before unpacking, so gzip never processes a byte we have not vouched for, then verify again
  # after: a corrupt decompression is cheap to catch and both hashes are already in the table.
  verify_sha "$WORK/$asset.gz" "$asset.gz"
  gunzip -c "$WORK/$asset.gz" >"$WORK/$asset" || die "could not decompress $asset.gz"
  verify_sha "$WORK/$asset" "$asset"

  chmod +x "$WORK/$asset"
  mkdir -p "$(dirname "$BIN")"
  mv "$WORK/$asset" "$BIN"
  echo "✅ Lefthook $LEFTHOOK_VERSION verified and installed at $BIN"
}

echo "🔧 Setting up development environment..."
echo ""

ensure_lefthook

echo ""
echo "🔗 Installing git hooks..."

"$BIN" install

echo ""
echo "✅ Setup complete!"
echo ""
echo "Git hooks are now configured. The pre-commit hook will:"
echo "  - Run mix format"
echo "  - Re-stage any Elixir files it reformats"
echo ""
echo "To skip the hook temporarily:"
echo "  git commit --no-verify"
echo ""
