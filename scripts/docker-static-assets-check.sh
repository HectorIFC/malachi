#!/usr/bin/env bash
# Asserts that a running Malachi container ships and serves its static assets.
#
#   scripts/docker-static-assets-check.sh <container> <dashboard_url>
#
# Two failure modes are silent without it: an image built without priv/static answers 404 for every
# asset, and an image built from a copy of priv as a whole carries whatever gitignored material the
# build context had there (development CA and node keys, dialyzer PLTs). So this checks both that the
# dashboard serves the logo byte for byte as it is in the repository, and that the release's priv
# directory holds nothing but static.
#
# Exits 0 when every check passes, 1 on the first failed check, 2 on a usage error.
set -euo pipefail

if [ "$#" -ne 2 ] || [ -z "$1" ] || [ -z "$2" ]; then
  echo "usage: $0 <container> <dashboard_url>" >&2
  exit 2
fi

container="$1"
dashboard_url="${2%/}"
expected_logo="$(cd "$(dirname "$0")/.." && pwd)/priv/static/logo.svg"

fail() {
  echo "static assets check failed: $*" >&2
  exit 1
}

body="$(mktemp)"
trap 'rm -f "$body"' EXIT

response="$(curl -sS -o "$body" -w '%{http_code} %{content_type}' "$dashboard_url/logo.svg")" ||
  fail "could not reach $dashboard_url/logo.svg"
status="${response%% *}"
content_type="${response#* }"

[ "$status" = "200" ] || fail "GET /logo.svg answered status $status, expected 200"

case "$content_type" in
  image/svg+xml*) ;;
  *) fail "GET /logo.svg answered content type '$content_type', expected image/svg+xml" ;;
esac

cmp -s "$body" "$expected_logo" || fail "GET /logo.svg body differs from $expected_logo"
echo "logo served: 200, $content_type, identical to the repository copy"

listing="$(docker exec "$container" sh -c 'ls -1A /app/lib/malachi-*/priv')" ||
  fail "could not list the release priv directory in container $container"

[ "$listing" = "static" ] ||
  fail "release priv directory holds '$(printf '%s' "$listing" | tr '\n' ' ')', expected only static"
echo "release priv directory holds only static"
