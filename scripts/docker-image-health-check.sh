#!/usr/bin/env bash
# Asserts that a running Malachi container turns healthy under the image's own HEALTHCHECK.
#
#   scripts/docker-image-health-check.sh <container>
#
# Every compose file used to override the probe, so a broken HEALTHCHECK in the image went unnoticed:
# it probed localhost, which resolves to ::1 first inside the container while the dashboard listens on
# IPv4 only, and a node that served fine reported itself unhealthy to anyone running the image as built
# (issue #282). So this refuses a container whose probe is not the image's, and then waits for Docker to
# report the container healthy.
#
# The wait is bounded by the image's own timings: its start period, plus one interval for the first probe
# to run, plus the probe's timeout for that probe to answer. A zero timing reads as Docker's default
# (interval 30s, timeout 30s, start period 0). IMAGE_HEALTH_TIMEOUT (seconds) replaces that budget.
# It fails at once, without waiting the budget out, when Docker reports the container unhealthy or the
# container stops running. Those two failures, and a spent budget, also print the output of the last
# probe Docker recorded (or that no probe has run yet), when the probe log can still be read.
#
# Exits 0 when the container reports healthy, 1 on a failed check, 2 on a usage error: a wrong number of
# arguments or an empty one, a limit that is not a whole number of seconds from 1 to 99999 written
# without leading zeros, or no coreutils timeout on PATH.
#
# Every docker call is bounded by IMAGE_HEALTH_EXEC_TIMEOUT (seconds, default 15), so a daemon that does
# not answer fails the check instead of holding the CI job until its own limit. IMAGE_HEALTH_POLL
# (seconds, default 1) is the pause between two reads of the health status.
set -euo pipefail

if [ "$#" -ne 1 ] || [ -z "$1" ]; then
  echo "usage: $0 <container>" >&2
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
# The defaults apply only when a variable is unset: set but empty is a mistake, refused below.
exec_timeout="${IMAGE_HEALTH_EXEC_TIMEOUT-15}"
poll="${IMAGE_HEALTH_POLL-1}"

require_positive_seconds IMAGE_HEALTH_EXEC_TIMEOUT "$exec_timeout"
require_positive_seconds IMAGE_HEALTH_POLL "$poll"
if [ -n "${IMAGE_HEALTH_TIMEOUT+set}" ]; then
  require_positive_seconds IMAGE_HEALTH_TIMEOUT "$IMAGE_HEALTH_TIMEOUT"
fi
require_coreutils_timeout

fail() {
  echo "image health check failed: $*" >&2
  exit 1
}

# Runs one bounded docker call and prints its output; fails the check, naming what was asked, when the
# call does not finish in time or exits non-zero. Called inside a command substitution, so the callers
# pass its exit status on with `|| exit "$?"`.
docker_or_fail() {
  local what="$1" status=0 out
  shift
  out="$(timeout "$exec_timeout" docker "$@")" || status=$?
  # 124 is what timeout exits with when it had to stop the command.
  [ "$status" -ne 124 ] || fail "$what did not finish within ${exec_timeout}s"
  [ "$status" -eq 0 ] || fail "could not read $what"
  printf '%s' "$out"
}

# Fails the check after printing the output of the last probe Docker recorded. Reading it is best
# effort: a container that cannot be inspected any more still fails with the reason given.
fail_with_last_probe() {
  local log
  # Each entry is prefixed with a marker, since a template cannot index the last element of a list.
  if log="$(timeout "$exec_timeout" docker inspect \
    -f '{{with .State.Health}}{{range .Log}}<probe>{{.Output}}{{end}}{{end}}' "$container" 2> /dev/null)"; then
    if [ -n "$log" ]; then
      echo "last probe output: ${log##*<probe>}" >&2
    else
      echo "no probe has run yet" >&2
    fi
  fi
  fail "$@"
}

image="$(docker_or_fail "the image of container $container" inspect -f '{{.Image}}' "$container")" || exit "$?"

container_test="$(docker_or_fail "the healthcheck of container $container" inspect \
  -f '{{with .Config.Healthcheck}}{{json .Test}}{{end}}' "$container")" || exit "$?"

# The timings first: the probe itself is a JSON list with spaces in it, so it goes last, where `read`
# hands it over whole. printf %d gives the durations in nanoseconds rather than as Go duration strings.
image_healthcheck="$(docker_or_fail "the HEALTHCHECK of image $image" image inspect \
  -f '{{with .Config.Healthcheck}}{{printf "%d %d %d" .StartPeriod .Interval .Timeout}} {{json .Test}}{{end}}' \
  "$image")" || exit "$?"

read -r start_period_ns interval_ns timeout_ns image_test <<< "$image_healthcheck" || true

case "${image_test:-}" in
  '' | '["NONE"]') fail "image $image has no HEALTHCHECK" ;;
esac

# Docker prints each timing as a whole number of nanoseconds. Anything else, from a CLI that formats the
# template differently, would otherwise reach the arithmetic below: a word there is read as an unset
# variable, and in the zero test it fails over to Docker's default, so the check would wait the wrong
# budget and pass.
for timing in "$start_period_ns" "$interval_ns" "$timeout_ns"; do
  case "$timing" in
    '' | *[!0-9]*)
      fail "could not read the HEALTHCHECK timings of image $image, got '$start_period_ns $interval_ns $timeout_ns'"
      ;;
  esac
done

[ "$container_test" = "$image_test" ] ||
  fail "container $container overrides the image HEALTHCHECK: it probes ${container_test:-nothing}, the image probes $image_test"

if [ -n "${IMAGE_HEALTH_TIMEOUT+set}" ]; then
  budget="$IMAGE_HEALTH_TIMEOUT"
else
  [ "$interval_ns" -ne 0 ] || interval_ns=30000000000
  [ "$timeout_ns" -ne 0 ] || timeout_ns=30000000000
  # Rounded up, so a timing below a whole second still waits for it.
  budget=$(((start_period_ns + interval_ns + timeout_ns + 999999999) / 1000000000))
fi

echo "waiting up to ${budget}s for container $container to report healthy under the image HEALTHCHECK"

started=$SECONDS
while :; do
  state="$(docker_or_fail "the health status of container $container" inspect \
    -f '{{.State.Status}} {{with .State.Health}}{{.Status}}{{end}}' "$container")" || exit "$?"
  status="${state%% *}"
  health="${state#* }"
  elapsed=$((SECONDS - started))

  [ "$status" = "running" ] || fail_with_last_probe "container $container is $status, not running, after ${elapsed}s"

  case "$health" in
    healthy)
      echo "container $container reported healthy after ${elapsed}s"
      exit 0
      ;;
    unhealthy) fail_with_last_probe "container $container reported unhealthy after ${elapsed}s" ;;
  esac

  [ "$elapsed" -lt "$budget" ] ||
    fail_with_last_probe "container $container is still ${health:-without a health status} after ${elapsed}s, past the ${budget}s budget"

  sleep "$poll"
done
