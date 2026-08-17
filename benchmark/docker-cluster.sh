#!/usr/bin/env bash
# 3-node cluster benchmark: can a Malachi cluster of three nodes behind ONE metadata vnode reach 1M rec/s
# aggregate? The client (pinned to its own cores) drives all three nodes at once via multi-host
# round-robin, over enough topics that segment placement spreads primaries across the nodes.
#
# Two regimes are measured, each on a FRESH cluster (recreated, empty tmpfs):
# - RF=1: segments striped across the three primaries, group commit ACTIVE on each node. The
#   throughput regime, the one with a real shot at 1M.
# - RF=3: every batch quorum-fsynced across the nodes and group commit gated off (rf > 1). The
#   full-durability regime; expected far lower, reported honestly.
#
# A `docker stats` snapshot is taken mid-window so a CPU-saturated node set is visible evidence (the
# three servers share the 4 cores of SRV_CPUSET).
#
# Usage: benchmark/docker-cluster.sh   (override DUR/WARM/CONNS/TOPICS/BATCH/RFS via env)
set -uo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)" || exit 1
COMPOSE="docker compose -f docker-compose.cluster.yml"
# Private scratch dir (not a predictable /tmp path a local attacker could pre-create as a symlink).
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
DUR="${DUR:-4}"
# Cluster warmup must cover cross-node metadata propagation: a topic created via one node reaches the
# other nodes' caches on the periodic refresh (~1s), so a 1s warmup leaks no_such_topic transients into
# the measured window. 5s puts the window fully in steady state.
WARM="${WARM:-5}"
CONNS="${CONNS:-192}"
BATCH="${BATCH:-100}"
TOPICS="${TOPICS:-64}"
RFS="${RFS:-1 3}"
export SRV_CPUSET="${SRV_CPUSET:-4,5,6,7}" LT_CPUSET="${LT_CPUSET:-0,1,2,3}"
FAILED=0

echo "Building images (first run compiles all deps; slow)..."
$COMPOSE build || { echo "build failed"; exit 1; }

printf "%-4s | %10s %8s %8s %8s %8s %8s\n" rf "rec/s" "p50 ms" "p99 ms" dropped overload reconn
printf -- "----------------------------------------------------------------\n"

# Waits until all three nodes report healthy. `up --wait` cannot be used: the formation race makes a
# node exit once before its restart converges, and --wait fails fast on that first exit even though the
# restart policy will bring it back healthy seconds later.
wait_healthy() {
  for _ in $(seq 1 36); do
    healthy=$(docker ps --filter "name=malachi-cluster" --filter "health=healthy" --format '{{.Names}}' | wc -l | tr -d ' ')
    [ "$healthy" = "3" ] && return 0
    sleep 5
  done
  return 1
}

for rf in $RFS; do
  RF="$rf" $COMPOSE up -d --force-recreate malachi1 malachi2 malachi3 >"$WORK/up.log" 2>&1

  if ! wait_healthy; then
    echo "  cluster did not converge to healthy (RF=$rf); node 1 log tail:"
    RF="$rf" $COMPOSE logs --tail 25 malachi1 2>&1 | tail -25
    RF="$rf" $COMPOSE down >/dev/null 2>&1
    FAILED=1
    continue
  fi

  # Snapshot CPU% of every container mid-window (background; the run below takes warmup+duration secs).
  ( sleep $((WARM + DUR / 2)); docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' > "$WORK/stats.txt" 2>/dev/null ) &
  stats_pid=$!

  json=$(RF="$rf" $COMPOSE run --rm loadtest \
           --host malachi1,malachi2,malachi3 --scenario produce \
           --connections "$CONNS" --batch "$BATCH" --topics "$TOPICS" \
           --duration "$DUR" --warmup "$WARM" --record-size 256 --json 2>/dev/null \
         | grep -E '^\{' | tail -1)

  wait "$stats_pid" 2>/dev/null
  RF="$rf" $COMPOSE down >/dev/null 2>&1

  if [ -z "$json" ]; then
    # A caseless client must fail the run, not blend in as a blank row.
    printf "%-4s | %s\n" "$rf" "(no json)"; FAILED=1; continue
  fi
  read -r recs p50 p99 err drop over recon < <(echo "$json" \
    | jq -r '[.records_per_s,.latency_ms.p50,.latency_ms.p99,.errors,.dropped,.overloaded,.reconnects]|@tsv')
  [ "$err" != "0" ] && drop="$drop(err=$err)"
  printf "%-4s | %10s %8s %8s %8s %8s %8s\n" "$rf" "$recs" "$p50" "$p99" "$drop" "$over" "$recon"
  if [ -s "$WORK/stats.txt" ]; then
    echo "     mid-window CPU: $(tr '\n' ' ' < "$WORK/stats.txt")"
  fi
done

echo
if [ "$FAILED" != "0" ]; then
  echo "done, WITH FAILED CASES (see above)"
  exit 1
fi
echo "done (target: 1M rec/s aggregate at RF=1; the three servers share SRV_CPUSET=$SRV_CPUSET)"
