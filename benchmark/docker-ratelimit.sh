#!/usr/bin/env bash
# What does enforcing the publish quota cost on the produce path? The limiter is an ETS token bucket, but
# produce is the hottest path in the system, so "an ETS lookup is free" is a claim to measure, not to
# assume. This sweep answers it on the 3-node cluster, with the same client and cpuset separation as the
# other benchmarks.
#
# The quota is keyed by AUTHENTICATED USER and the load generator authenticates as one user, so every
# connection in the run contends on a single hot bucket. That is deliberate: it is the worst case for
# ETS bucket-lock contention, and the only case worth reporting.
#
#   off           - the limiter switched off entirely, the baseline
#   unconfigured  - limiter on, publish limit 0 (the SHIPPED DEFAULT): the cost every deployment pays,
#                   which should be one config read and a branch
#   high          - limiter on, publish limit far above the offered load: the cost of actually checking
#                   the bucket on every produce, with nothing ever refused
#
# The number that decides the design is `high` vs `off`. Anything inside the run-to-run noise floor
# (roughly 15% on the reference machine) means the check is affordable; rate_limited must be 0 in every
# case, since a refusal would mean the limit bit and the throughput number is measuring the wrong thing.
#
# Usage: benchmark/docker-ratelimit.sh   (override DUR/WARM/CONNS/TOPICS/BATCH/REPEATS/LIMIT via env)
set -uo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)" || exit 1
COMPOSE="docker compose -f docker-compose.cluster.yml"
DUR="${DUR:-8}"
WARM="${WARM:-5}"
CONNS="${CONNS:-48}"
BATCH="${BATCH:-100}"
TOPICS="${TOPICS:-24}"
REPEATS="${REPEATS:-3}"
# Far above anything the client can offer: the point is to pay for the check, never to be refused by it.
LIMIT="${LIMIT:-100000000}"
export SRV_CPUSET="${SRV_CPUSET:-4,5,6,7}" LT_CPUSET="${LT_CPUSET:-0,1,2,3}"
export RF="${RF:-1}"
FAILED=0

wait_healthy() {
  for _ in $(seq 1 36); do
    healthy=$(docker ps --filter "name=malachi-cluster" --filter "health=healthy" --format '{{.Names}}' | wc -l | tr -d ' ')
    [ "$healthy" = "3" ] && return 0
    sleep 5
  done
  return 1
}

# Recreates the three nodes with the current rate-limit env and waits for them. Checked, because a failed
# recreate is the one failure this harness cannot see downstream: the PREVIOUS containers are still up and
# still healthy, so wait_healthy passes and the run reports a number measured under the old configuration
# as if it were the new one.
restart_cluster() {
  if ! $COMPOSE up -d --force-recreate malachi1 malachi2 malachi3 >"$WORK/up.log" 2>&1; then
    echo "cluster recreation failed"; cat "$WORK/up.log"; exit 1
  fi
  wait_healthy || { echo "cluster did not converge to healthy"; cat "$WORK/up.log"; exit 1; }
}

run_case() {
  $COMPOSE run --rm loadtest \
    --host malachi1,malachi2,malachi3 --scenario produce \
    --connections "$CONNS" --batch "$BATCH" --topics "$TOPICS" \
    --duration "$DUR" --warmup "$WARM" --record-size 256 --json 2>/dev/null \
    | grep -E '^\{' | tail -1
}

apply_case() {
  case "$1" in
    off) export MALACHI_RATE_LIMIT_ENABLED=false MALACHI_PUBLISH_RATE_LIMIT=0 ;;
    unconfigured) export MALACHI_RATE_LIMIT_ENABLED=true MALACHI_PUBLISH_RATE_LIMIT=0 ;;
    high) export MALACHI_RATE_LIMIT_ENABLED=true MALACHI_PUBLISH_RATE_LIMIT="$LIMIT" ;;
  esac
  export MALACHI_PUBLISH_RATE_WINDOW_MS=1000
  # Switching the limiter on switches the AUTH limit on with it, and the generator opens CONNS
  # connections from one ip against a shipped limit of 10 a minute. Left alone it does not slow the run
  # down, it starves it of connections, and the case reports no run at all. This is a benchmark of the
  # publish quota, so the auth limit is lifted clear of it.
  export MALACHI_AUTH_RATE_LIMIT=$(( CONNS * 1000 ))
}

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "Building images..."
$COMPOSE build >/dev/null 2>&1 || { echo "build failed"; exit 1; }
$COMPOSE down -v >/dev/null 2>&1

CASES="off unconfigured high"
declare -A best_recs best_p50 best_p99 errors dropped limited nojson
for case_name in $CASES; do
  best_recs[$case_name]=0; best_p50[$case_name]=0; best_p99[$case_name]=0
  errors[$case_name]=0; dropped[$case_name]=0; limited[$case_name]=0; nojson[$case_name]=0
done

# Rounds are INTERLEAVED rather than run case by case, so that any drift over the sweep (a warming
# laptop, a filling tmpfs) is spread across the cases instead of landing on whichever goes last.
for round in $(seq 1 "$REPEATS"); do
  echo "round $round/$REPEATS..."
  for case_name in $CASES; do
    apply_case "$case_name"
    restart_cluster
    json=$(run_case)

    if [ -z "$json" ]; then
      # A run that produced no JSON did not happen: the client crashed, or the server never came up. That
      # is not the run-to-run noise best-of-N exists to absorb, so it fails the sweep even when a sibling
      # repeat succeeds. Otherwise a crashed run is reported in the table and still exits 0.
      nojson[$case_name]=$(( ${nojson[$case_name]} + 1 ))
      FAILED=1
      continue
    fi

    read -r recs p50 p99 err drop rl < <(echo "$json" \
      | jq -r '[.records_per_s,.latency_ms.p50,.latency_ms.p99,.errors,.dropped,.rate_limited]|@tsv')
    errors[$case_name]=$(( ${errors[$case_name]} + err ))
    dropped[$case_name]=$(( ${dropped[$case_name]} + drop ))
    limited[$case_name]=$(( ${limited[$case_name]} + rl ))
    # Best of N: these runs share a laptop with Docker, so the slow ones measure the noise floor.
    if [ "$recs" -gt "${best_recs[$case_name]}" ]; then
      best_recs[$case_name]=$recs; best_p50[$case_name]=$p50; best_p99[$case_name]=$p99
    fi
  done
done

printf "\n%-14s | %10s %8s %8s %7s %7s %9s %7s | %s\n" \
  case "rec/s" "p50 ms" "p99 ms" errors dropped "rate_lim" "no json" "vs off"
printf -- "-------------------------------------------------------------------------------------------------\n"

baseline=${best_recs[off]}

for case_name in $CASES; do
  recs=${best_recs[$case_name]}

  if [ "$recs" = "0" ]; then
    printf "%-14s | %s\n" "$case_name" "(no successful run)"; FAILED=1; continue
  fi

  if [ "$case_name" = "off" ]; then
    delta="baseline"
  else
    delta=$(LC_NUMERIC=C awk -v a="$recs" -v b="$baseline" 'BEGIN{printf "%+.1f%%", (a-b)*100/b}')
  fi

  printf "%-14s | %10s %8s %8s %7s %7s %9s %7s | %s\n" \
    "$case_name" "$recs" "${best_p50[$case_name]}" "${best_p99[$case_name]}" \
    "${errors[$case_name]}" "${dropped[$case_name]}" "${limited[$case_name]}" "${nojson[$case_name]}" "$delta"

  # A run that refused produces was not measuring the check, it was measuring the limit. Never silent.
  [ "${errors[$case_name]}" != "0" ] && FAILED=1
  [ "${limited[$case_name]}" != "0" ] && { echo "  ^ $case_name refused produces: raise LIMIT, this row is not a throughput measurement"; FAILED=1; }
done

$COMPOSE down -v >/dev/null 2>&1
exit "$FAILED"
