#!/usr/bin/env bash
# What does the integrity scrub cost under load? The scrub reads sealed segments from disk in the
# background, so the honest question is what that steals from the produce path. This sweep answers it
# on the 3-node cluster, with the same client and cpuset separation as the other benchmarks.
#
# The cluster runs on PERSISTENT volumes (not the benchmark tmpfs) with small segments, so the warmup
# leaves behind a large set of SEALED segments: without those the scrub has nothing to verify and any
# measurement would be vacuous. The three cases share those volumes, so each one scrubs the same data:
#
#   off        - MALACHI_SCRUB_ENABLED=false, the baseline
#   default    - the shipped cadence (one segment a minute)
#   aggressive - 200 segments every 2s, i.e. hundreds of times the shipped rate, to show what the
#                scrub costs when it is actually working hard (an operator raising the rate on a big
#                node is buying exactly this)
#
# The default is deliberately slow, so "default" is expected to land within run-to-run noise of the
# baseline; the aggressive row is what makes the cost visible at all.
#
# Usage: benchmark/docker-scrub.sh   (override DUR/WARM/CONNS/TOPICS/BATCH/REPEATS via env)
set -uo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)" || exit 1
COMPOSE="docker compose -f docker-compose.cluster.yml"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
DUR="${DUR:-8}"
WARM="${WARM:-5}"
CONNS="${CONNS:-48}"
BATCH="${BATCH:-100}"
TOPICS="${TOPICS:-24}"
REPEATS="${REPEATS:-3}"
# Enough produce to seal a few hundred segments before the measured runs.
SEED_DUR="${SEED_DUR:-20}"
export SRV_CPUSET="${SRV_CPUSET:-4,5,6,7}" LT_CPUSET="${LT_CPUSET:-0,1,2,3}"
export RF="${RF:-1}"
export MALACHI_DATA_ROOT=/data
export MALACHI_SEGMENT_MAX_BYTES="${MALACHI_SEGMENT_MAX_BYTES:-262144}"
FAILED=0

wait_healthy() {
  for _ in $(seq 1 36); do
    healthy=$(docker ps --filter "name=malachi-cluster" --filter "health=healthy" --format '{{.Names}}' | wc -l | tr -d ' ')
    [ "$healthy" = "3" ] && return 0
    sleep 5
  done
  return 1
}

# Recreates the three nodes with the current scrub env and waits for them.
restart_cluster() {
  # Checked, because a failed recreate is the one failure this harness cannot see downstream: the
  # PREVIOUS containers are still up and still healthy, so wait_healthy passes and the run reports a
  # number measured under the old scrub configuration as if it were the new one.
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

echo "Building images..."
$COMPOSE build >/dev/null 2>&1 || { echo "build failed"; exit 1; }

echo "Seeding sealed segments (${SEED_DUR}s of produce with ${MALACHI_SEGMENT_MAX_BYTES}-byte segments)..."
$COMPOSE down -v >/dev/null 2>&1
MALACHI_SCRUB_ENABLED=false restart_cluster
DUR="$SEED_DUR" $COMPOSE run --rm loadtest \
  --host malachi1,malachi2,malachi3 --scenario produce --connections "$CONNS" --batch "$BATCH" \
  --topics "$TOPICS" --duration "$SEED_DUR" --warmup 2 --record-size 256 --json >/dev/null 2>&1
sealed=$(docker exec malachi-cluster-1 sh -c 'ls -d /data/malachi_log/*/ 2>/dev/null | wc -l' | tr -d ' ')
echo "node 1 holds $sealed segment directories to verify"

# Rounds are INTERLEAVED (off, default, aggressive, then again) rather than run case by case. The
# cases share persistent volumes that keep growing as the client produces into them, so running each
# case to completion in turn systematically penalises whichever goes last: a first attempt at this
# measured -7.7% for a cadence that can only scan ONE 256KB segment in the whole window, which is
# physically impossible and was pure ordering drift.
apply_case() {
  case "$1" in
    off) export MALACHI_SCRUB_ENABLED=false MALACHI_SCRUB_INTERVAL_MS= MALACHI_SCRUB_SEGMENTS_PER_TICK= ;;
    default) export MALACHI_SCRUB_ENABLED=true MALACHI_SCRUB_INTERVAL_MS= MALACHI_SCRUB_SEGMENTS_PER_TICK= ;;
    aggressive) export MALACHI_SCRUB_ENABLED=true MALACHI_SCRUB_INTERVAL_MS=2000 MALACHI_SCRUB_SEGMENTS_PER_TICK=200 ;;
  esac
}

declare -A best_recs best_p50 best_p99 errors dropped
for case_name in off default aggressive; do
  best_recs[$case_name]=0; best_p50[$case_name]=0; best_p99[$case_name]=0
  errors[$case_name]=0; dropped[$case_name]=0
done

for round in $(seq 1 "$REPEATS"); do
  echo "round $round/$REPEATS..."
  for case_name in off default aggressive; do
    apply_case "$case_name"
    restart_cluster
    json=$(run_case)

    if [ -z "$json" ]; then
      dropped[$case_name]=$(( ${dropped[$case_name]} + 1 ))
      continue
    fi

    read -r recs p50 p99 err < <(echo "$json" | jq -r '[.records_per_s,.latency_ms.p50,.latency_ms.p99,.errors]|@tsv')
    errors[$case_name]=$(( ${errors[$case_name]} + err ))
    # Best of N: these runs share a laptop with Docker, so the slow ones measure the noise floor.
    if [ "$recs" -gt "${best_recs[$case_name]}" ]; then
      best_recs[$case_name]=$recs; best_p50[$case_name]=$p50; best_p99[$case_name]=$p99
    fi
  done
done

printf "\n%-12s | %10s %8s %8s %8s %8s | %s\n" case "rec/s" "p50 ms" "p99 ms" errors "no json" "vs off"
printf -- "--------------------------------------------------------------------------------\n"

baseline=${best_recs[off]}

for case_name in off default aggressive; do
  recs=${best_recs[$case_name]}

  if [ "$recs" = "0" ]; then
    printf "%-12s | %s\n" "$case_name" "(no successful run)"; FAILED=1; continue
  fi

  if [ "$case_name" = "off" ]; then
    delta="baseline"
  else
    delta=$(LC_NUMERIC=C awk -v a="$recs" -v b="$baseline" 'BEGIN{printf "%+.1f%%", (a-b)*100/b}')
  fi

  printf "%-12s | %10s %8s %8s %8s %8s | %s\n" \
    "$case_name" "$recs" "${best_p50[$case_name]}" "${best_p99[$case_name]}" \
    "${errors[$case_name]}" "${dropped[$case_name]}" "$delta"

  [ "${errors[$case_name]}" != "0" ] && FAILED=1
done

$COMPOSE down -v >/dev/null 2>&1

echo
if [ "$FAILED" != "0" ]; then
  echo "done, WITH FAILED CASES (see above)"
  exit 1
fi
echo "done (best of $REPEATS runs per case; RF=$RF, ${MALACHI_SEGMENT_MAX_BYTES}-byte segments on persistent volumes)"
