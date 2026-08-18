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
# every one of them is still readable. Storage-level faults (corruption, truncation, file loss) are
# scripts/docker-storage-chaos.sh; shared plumbing lives in scripts/chaos_lib.sh.
#
# Usage: scripts/docker-chaos-test.sh    (requires the Docker cluster images; builds them if needed)
set -uo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)" || exit 1
export SRV_CPUSET="${SRV_CPUSET:-2,3,4,5,6,7}" LT_CPUSET="${LT_CPUSET:-0,1}"
export RF=3
# Persistent per-node volumes instead of the benchmark tmpfs: durability-through-restart is exactly
# what this harness certifies, and tmpfs is remounted empty on every container restart.
export MALACHI_DATA_ROOT=/data
CHECKER_WINDOW_S="${CHECKER_WINDOW_S:-150}"
CHAOS_TOPIC=chaos_acked
source "$(dirname "$0")/chaos_lib.sh"

build_images
start_cluster
start_checker "$CHECKER_WINDOW_S"
start_traffic $((CHECKER_WINDOW_S - 20))
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

close_window
verify_acked
check_convergence
check_clean_produce
finish "CHAOS CERTIFICATION"
