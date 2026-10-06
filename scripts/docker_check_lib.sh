# shellcheck shell=bash
# Shared by the checks that assert on a running Malachi container (scripts/docker-static-assets-check.sh,
# scripts/docker-image-health-check.sh). Sourced, never run: it defines functions and runs nothing.
#
# Both checks bound every call they make, and both read their limits from the environment, so both need
# the same refusal of a limit that would mean no limit at all, and the same up front check for the
# coreutils timeout that enforces them. Each failure here exits 2, the usage error of both checks.

# Refuses anything but a whole number of seconds from 1 to 99999 written without leading zeros:
#
#   require_positive_seconds <variable name> <value>
#
# Both curl and timeout read 0 as no limit at all, so 0 is refused along with anything not a number.
# Leading zeros are refused rather than normalized, which would bring in bash's octal reading, and the
# five digit cap keeps the value inside what curl accepts: a longer one it rejects outright, which
# would surface as an unreachable dashboard.
require_positive_seconds() {
  case "$2" in
    '' | *[!0-9]* | 0* | ??????*)
      echo "$1 must be a whole number of seconds from 1 to 99999 without leading zeros, got '$2'" >&2
      exit 2
      ;;
  esac
}

# Checked up front: missing, the first bounded call would fail as if the container were at fault.
require_coreutils_timeout() {
  if ! command -v timeout > /dev/null 2>&1; then
    echo "this check requires coreutils timeout, which is not on PATH" >&2
    exit 2
  fi
}
