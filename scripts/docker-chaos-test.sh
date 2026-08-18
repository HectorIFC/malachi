#!/usr/bin/env bash
# Chaos certification harness (the NorthGuard certification pipeline, scaled to this repo): a 3-node
# RF=3 cluster takes synthetic traffic while real failures are injected, and three invariants are
# certified:
#   1. An acknowledged write is NEVER lost (rf=3 quorum durability survives any single-node fault).
#   2. The cluster reconverges to 3/3 healthy after every event.
#   3. Availability recovers: errors DURING an event are expected, but a clean produce+fetch must
#      pass after the chaos ends.
#
# Events, in order (each followed by reconvergence):
#   a. power pull    - docker kill (SIGKILL) of node 3, then restart
#   b. partition     - network disconnect of node 2, then reconnect
#   c. stalled node  - docker pause (SIGSTOP) of node 1: worse than dead, sockets open but mute
#   d. rolling restart of all three nodes
#
# The acked-durability checker (scripts/chaos_checker.exs) produces sequential values through the
# whole window, retrying through faults, and only records CONFIRMED writes; verify at the end proves
# every one of them is still readable.
#
# Usage: scripts/docker-chaos-test.sh    (requires the Docker cluster images; builds them if needed)
set -uo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)" || exit 1
COMPOSE="docker compose -f docker-compose.cluster.yml"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
export SRV_CPUSET="${SRV_CPUSET:-2,3,4,5,6,7}" LT_CPUSET="${LT_CPUSET:-0,1}"
export RF=3
# Persistent per-node volumes instead of the benchmark tmpfs: durability-through-restart is exactly
# what this harness certifies, and tmpfs is remounted empty on every container restart.
export MALACHI_DATA_ROOT=/data
CHECKER_WINDOW_S="${CHECKER_WINDOW_S:-150}"
FAILED=0

say() { printf '\n== %s ==\n' "$1"; }
fail() { echo "FAIL: $1"; FAILED=1; }

wait_healthy() {
  for _ in $(seq 1 48); do
    h=$(docker ps --filter "name=malachi-cluster" --filter "health=healthy" --format '{{.Names}}' | wc -l | tr -d ' ')
    [ "$h" = "3" ] && return 0
    sleep 5
  done
  return 1
}

say "building images"
$COMPOSE build >/dev/null 2>&1 || { echo "build failed"; exit 1; }

say "starting 3-node RF=3 cluster (fresh persistent volumes)"
$COMPOSE down -v >/dev/null 2>&1
$COMPOSE up -d --force-recreate malachi1 malachi2 malachi3 >"$WORK/up.log" 2>&1
wait_healthy || { echo "cluster never converged; aborting"; cat "$WORK/up.log"; exit 1; }
NET=$(docker inspect malachi-cluster-1 --format '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')
echo "cluster healthy; network: $NET"

say "starting acked-durability checker (${CHECKER_WINDOW_S}s window) + background traffic"
# The checker runs inside the compose network (the cluster publishes no host ports); the acked file
# and the checker script are mounted in from the host.
$COMPOSE run --rm -v "$WORK:/chaos" -v "$PWD/scripts:/chaos_scripts" --entrypoint sh loadtest \
  -c "cd /app && mix run --no-start /chaos_scripts/chaos_checker.exs produce malachi1,malachi2,malachi3 chaos_acked $CHECKER_WINDOW_S /chaos/acked.log" \
  >"$WORK/checker.log" 2>&1 &
CHECKER=$!

$COMPOSE run --rm loadtest \
  --host malachi1,malachi2,malachi3 --scenario produce --connections 24 --batch 10 --topics 12 \
  --duration $((CHECKER_WINDOW_S - 20)) --warmup 5 --record-size 256 --json \
  >"$WORK/traffic.log" 2>&1 &
TRAFFIC=$!

sleep 15

say "event a: power pull (SIGKILL node 3)"
docker kill malachi-cluster-3 >/dev/null 2>&1
sleep 10
docker start malachi-cluster-3 >/dev/null 2>&1
wait_healthy && echo "reconverged after power pull" || fail "cluster did not reconverge after power pull"

say "event b: network partition (node 2)"
docker network disconnect "$NET" malachi-cluster-2 >/dev/null 2>&1
sleep 10
docker network connect "$NET" malachi-cluster-2 >/dev/null 2>&1
wait_healthy && echo "reconverged after partition" || fail "cluster did not reconverge after partition"

say "event c: stalled node (SIGSTOP node 1)"
docker pause malachi-cluster-1 >/dev/null 2>&1
sleep 8
docker unpause malachi-cluster-1 >/dev/null 2>&1
wait_healthy && echo "reconverged after stall" || fail "cluster did not reconverge after stall"

say "event d: rolling restart"
for node in malachi3 malachi2 malachi1; do
  $COMPOSE restart "$node" >/dev/null 2>&1
  wait_healthy || fail "cluster did not reconverge after restarting $node"
done
echo "rolling restart complete"

say "waiting for the checker window to close"
wait "$CHECKER" 2>/dev/null
wait "$TRAFFIC" 2>/dev/null
acked=$(wc -l < "$WORK/acked.log" 2>/dev/null | tr -d ' ')
echo "checker acked $acked writes through the chaos (log tail:)"
tail -3 "$WORK/checker.log"
[ "${acked:-0}" -gt 0 ] || fail "checker acked nothing: no availability at any point"

say "invariant 1: every acknowledged write survived"
$COMPOSE run --rm -v "$WORK:/chaos" -v "$PWD/scripts:/chaos_scripts" --entrypoint sh loadtest \
  -c "cd /app && mix run --no-start /chaos_scripts/chaos_checker.exs verify malachi1,malachi2,malachi3 chaos_acked /chaos/acked.log" \
  >"$WORK/verify.log" 2>&1
tail -3 "$WORK/verify.log"
if grep -q "VERIFY OK" "$WORK/verify.log"; then
  echo "durability invariant holds"
else
  fail "acked writes were lost (see verify output above)"
fi

say "invariant 2: final convergence"
wait_healthy && echo "3/3 healthy" || fail "cluster is not fully healthy at the end"

say "invariant 3: clean produce+fetch after the chaos"
post=$($COMPOSE run --rm loadtest \
        --host malachi1,malachi2,malachi3 --scenario produce --connections 12 --batch 10 --topics 6 \
        --duration 3 --warmup 3 --record-size 256 --json 2>/dev/null | grep -E '^\{' | tail -1)
post_errs=$(echo "$post" | jq -r '[.errors,.dropped]|add' 2>/dev/null)
post_recs=$(echo "$post" | jq -r .records_per_s 2>/dev/null)
if [ "${post_errs:-1}" = "0" ] && [ "${post_recs:-0}" -gt 0 ]; then
  echo "post-chaos produce clean: ${post_recs} rec/s, 0 errors/drops"
else
  fail "post-chaos produce not clean: $post"
fi

$COMPOSE down >/dev/null 2>&1

say "result"
if [ "$FAILED" != "0" ]; then
  echo "CHAOS CERTIFICATION FAILED (see FAIL lines above)"
  exit 1
fi
echo "CHAOS CERTIFICATION PASSED: durability, convergence and availability held through all events"
