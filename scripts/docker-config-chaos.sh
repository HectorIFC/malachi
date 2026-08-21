#!/usr/bin/env bash
# Config-deploy certification: the deploy the build / roll back the build / config deployments
# scenarios of the NorthGuard certification pipeline, applied to what this repo deploys (one image,
# config via env). Runs against the real 3-node RF=3 cluster with the acked-durability checker
# producing through every step.
#
# Events:
#   h. rolling config deploy - a harmless setting (MALACHI_GROUP_COMMIT_INTERVAL_MS 2 -> 5) is
#      applied node by node (recreate, wait healthy, next). Certifies: availability holds between
#      steps (the checker keeps acking), 3/3 healthy at the end, and the new config is effective
#      on every node.
#   i. bad config + rollback - a config that fails fast at boot (MALACHI_CLUSTER_STRATEGY=banana,
#      which runtime.exs rejects with a raise) is deployed to ONE node: it must crash-loop, never
#      go healthy, while the other two keep serving quorum writes. Rolling the env back must bring
#      the node home. Certifies: the bad node stays down, exactly 2/3 stay healthy, acks keep
#      flowing through the crash-loop, and post-rollback the cluster reconverges to 3/3.
#
# Plus the fatia 1 closing invariants: every acked write survives, final 3/3 convergence, and a
# clean produce+fetch after the chaos.
#
# Usage: scripts/docker-config-chaos.sh
set -uo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)" || exit 1
export SRV_CPUSET="${SRV_CPUSET:-2,3,4,5,6,7}" LT_CPUSET="${LT_CPUSET:-0,1}"
export RF=3
export MALACHI_DATA_ROOT=/data
# Long enough to cover three sequential node recreations plus the crash-loop and rollback.
CHECKER_WINDOW_S="${CHECKER_WINDOW_S:-210}"
CHAOS_TOPIC=chaos_acked
source "$(dirname "$0")/chaos_lib.sh"

acked_count() { wc -l < "$WORK/acked.log" 2>/dev/null | tr -d ' '; }

# Requires the acked count to have grown past $1 (availability held through the step named $2).
require_progress() {
  now=$(acked_count)
  if [ "${now:-0}" -gt "$1" ]; then
    echo "acks kept flowing through $2 ($1 -> $now)"
  else
    fail "no produce was acknowledged through $2 (stuck at ${now:-0})"
  fi
}

build_images
start_cluster
start_checker "$CHECKER_WINDOW_S"
sleep 10

say "event h: rolling config deploy (group commit interval 2 -> 5)"
export MALACHI_GROUP_COMMIT_INTERVAL_MS=5
for node in malachi3 malachi2 malachi1; do
  before=$(acked_count)
  $COMPOSE up -d "$node" >/dev/null 2>&1
  wait_healthy || fail "cluster did not reconverge after deploying to $node"
  require_progress "$before" "the $node deploy"
done

for n in 1 2 3; do
  effective=$(docker exec "malachi-cluster-$n" sh -c 'echo $MALACHI_GROUP_COMMIT_INTERVAL_MS')
  [ "$effective" = "5" ] || fail "node $n did not pick up the new config (interval=$effective)"
done
echo "new config effective on all three nodes"

say "event i: bad config on node 3 (crash-loop), quorum keeps serving, then rollback"
before=$(acked_count)
export MALACHI_CLUSTER_STRATEGY=banana
$COMPOSE up -d malachi3 >/dev/null 2>&1
sleep 30

bad_healthy=$(docker ps --filter "name=malachi-cluster-3" --filter "health=healthy" -q | wc -l | tr -d ' ')
healthy=$(docker ps --filter "name=malachi-cluster" --filter "health=healthy" --format '{{.Names}}' | wc -l | tr -d ' ')
if [ "$bad_healthy" = "0" ] && [ "$healthy" = "2" ]; then
  echo "bad config held node 3 down; 2/3 healthy as expected"
else
  fail "unexpected health during the crash-loop (node3 healthy=$bad_healthy, total healthy=$healthy)"
fi
require_progress "$before" "the crash-loop (quorum on 2/3)"

echo "rolling the config back"
export MALACHI_CLUSTER_STRATEGY=epmd
$COMPOSE up -d malachi3 >/dev/null 2>&1
wait_healthy && echo "node 3 recovered after the rollback" || fail "cluster did not reconverge after the rollback"

close_window
verify_acked
check_convergence
check_clean_produce
finish "CONFIG DEPLOY CERTIFICATION"
