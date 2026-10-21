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
# Exits 0 when every check passes, 1 on the first failed check, 2 on a usage error: a wrong number of
# arguments or an empty one, a timeout that is not a whole number of seconds from 1 to 99999 written
# without leading zeros, or no coreutils timeout on PATH.
#
# Both calls are bounded, so a dashboard that accepts a connection and never answers, or a container
# that does not respond to exec, fails the check instead of holding the CI job until its own limit:
# STATIC_ASSETS_HTTP_TIMEOUT (seconds, default 10) caps the whole logo request and
# STATIC_ASSETS_EXEC_TIMEOUT (seconds, default 15) caps the priv listing. Requires coreutils timeout.
set -euo pipefail

if [ "$#" -ne 2 ] || [ -z "$1" ] || [ -z "$2" ]; then
  echo "usage: $0 <container> <dashboard_url>" >&2
  exit 2
fi

# Parameter expansion rather than dirname, which lives on the PATH this check may be run without.
case "$0" in
  */*) script_dir="${0%/*}" ;;
  *) script_dir=. ;;
esac
# shellcheck source=scripts/docker_check_lib.sh
source "$script_dir/docker_check_lib.sh"

container="$1"
dashboard_url="${2%/}"
# The default applies only when a variable is unset: set but empty is a mistake, refused below.
http_timeout="${STATIC_ASSETS_HTTP_TIMEOUT-10}"
exec_timeout="${STATIC_ASSETS_EXEC_TIMEOUT-15}"

require_positive_seconds STATIC_ASSETS_HTTP_TIMEOUT "$http_timeout"
require_positive_seconds STATIC_ASSETS_EXEC_TIMEOUT "$exec_timeout"
require_coreutils_timeout

expected_logo="$(cd "$script_dir/.." && pwd)/priv/static/logo.svg"

fail() {
  echo "static assets check failed: $*" >&2
  exit 1
}

body="$(mktemp)"
trap 'rm -f "$body"' EXIT

curl_status=0
response="$(curl -sS --connect-timeout "$http_timeout" --max-time "$http_timeout" -o "$body" \
  -w '%{http_code} %{content_type}' "$dashboard_url/logo.svg")" || curl_status=$?
# 28 is curl's operation timeout.
[ "$curl_status" -ne 28 ] || fail "GET /logo.svg did not complete within ${http_timeout}s"
[ "$curl_status" -eq 0 ] || fail "could not reach $dashboard_url/logo.svg"
status="${response%% *}"
content_type="${response#* }"

[ "$status" = "200" ] || fail "GET /logo.svg answered status $status, expected 200"

case "$content_type" in
  image/svg+xml*) ;;
  *) fail "GET /logo.svg answered content type '$content_type', expected image/svg+xml" ;;
esac

cmp -s "$body" "$expected_logo" || fail "GET /logo.svg body differs from $expected_logo"
echo "logo served: 200, $content_type, identical to the repository copy"

exec_status=0
listing="$(timeout "$exec_timeout" docker exec "$container" sh -c 'ls -1A /app/lib/malachi-*/priv')" ||
  exec_status=$?
# 124 is what timeout exits with when it had to stop the command.
[ "$exec_status" -ne 124 ] ||
  fail "listing the release priv directory in container $container did not finish within ${exec_timeout}s"
[ "$exec_status" -eq 0 ] || fail "could not list the release priv directory in container $container"

[ "$listing" = "static" ] ||
  fail "release priv directory holds '$(printf '%s' "$listing" | tr '\n' ' ')', expected only static"
echo "release priv directory holds only static"
