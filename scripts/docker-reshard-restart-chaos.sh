#!/usr/bin/env bash
# Reshard-durability certification: grow the metadata sharding on a live cluster, then take every node
# down at once and bring them all back. Runs against the real 3-node RF=3 cluster with the
# acked-durability checker producing throughout.
#
# Events:
#   j. grow the ring while serving - `mix malachi.reshard --to 6` on a cluster booted with
#      MALACHI_LOG_VNODES=4. Certifies: the reshard completes, the recorded ring reports 6 vnodes,
#      two of them created by splitting (names the environment cannot produce), and acks keep
#      flowing through the migrations.
#   k. full-cluster restart - all three nodes are stopped together (so no node survives to gossip the
#      ring) and started again. Certifies: the recorded ring still has the same 6 vnodes, including
#      the split-created ones, and the cluster reconverges to 3/3.
#
# This is the drill that proves what an in-process test cannot: the ring survives the operating-system
# processes, their volumes being remounted, and the loss of every copy held in memory. Before the ring
# was durable, step k came back believing MALACHI_LOG_VNODES=4 and orphaned the migrated metadata.
#
# Plus the closing invariants shared with the other drills: every acked write survives, final 3/3
# convergence, and a clean produce+fetch after the chaos.
#
# KNOWN RED: the two data-plane invariants (acked-write survival and the post-chaos produce) currently
# FAIL here, and not because of the ring. A sharded control plane comes back from a full-cluster
# restart healthy but unable to serve metadata writes, which is issue #136: it reproduces on main, it
# reproduces with no reshard involved, and it does not reproduce unsharded. Events j and k, which are
# what this drill exists to certify, pass. Expect a red run until #136 is fixed.
#
# Usage: scripts/docker-reshard-restart-chaos.sh
set -uo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)" || exit 1
export SRV_CPUSET="${SRV_CPUSET:-2,3,4,5,6,7}" LT_CPUSET="${LT_CPUSET:-0,1}"
export RF=3
export MALACHI_DATA_ROOT=/data
# Segment preallocation at the production default. These drills certify what a real deployment does
# under failure, and preallocation changes the shape of a crashed segment: past its last write the
# file is a tail of zeros, which recovery has to read as unwritten space rather than as corruption
# (Malachi.Storage.ElixirStore.classify_tail/2). At 64MB the active segment never fills inside a
# drill window, so that tail is present at every restart these harnesses inject, which is the point.
# The compose defaults it to 0 because its own default data root is a tmpfs; here the data root is a
# real volume, so it is set back on.
export MALACHI_SEGMENT_PREALLOC_BYTES="${MALACHI_SEGMENT_PREALLOC_BYTES:-67108864}"
# The cluster must boot already sharded: the ring only exists for a sharded control plane, and growing
# it is what this drill certifies survives.
export MALACHI_LOG_VNODES=4
# Long enough to cover the reshard's migrations plus a full stop and start of all three nodes.
CHECKER_WINDOW_S="${CHECKER_WINDOW_S:-240}"
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

# Runs an operator mix task inside node $1, targeting that same node. The cluster uses short names, so
# the task has to run from a VM that is itself short-named with the same cookie; a separate long-named
# container could not connect to it at all.
node_task() {
  idx=$1
  shift
  docker exec "malachi-cluster-$idx" sh -c \
    "cd /app && elixir --sname chaoscli --cookie malachi_bench -S mix $* --node malachi@malachi$idx" 2>&1
}

# Re-sharding is lease-gated and the lease is elected, so an operator has to issue it on the node that
# currently holds it; refusing elsewhere with "try another node" is the task's documented behaviour.
# Walking the three nodes is what an operator does, and it keeps the drill from failing on an election
# that simply landed somewhere other than node 1.
reshard_on_lease_holder() {
  for idx in 1 2 3; do
    out=$(node_task "$idx" "malachi.reshard --to $1")
    status=$?
    if [ $status -eq 0 ]; then
      echo "reshard ran on node $idx (the lease holder)"
      return 0
    fi
    if ! echo "$out" | grep -q "does not hold the cluster lease"; then
      echo "$out" | sed 's/^/    /'
      return 1
    fi
  done
  echo "    no node accepted the reshard; none reported holding the lease"
  return 1
}

# The durable ring as the cluster records it. Read from node 1; the query is linearizable, so ra routes
# it to the leader wherever that is. Retried, because right after a full restart the ring store may need
# a moment to elect before it can answer, and an unreadable store is not the same as a changed ring.
ring_show() {
  for _ in $(seq 1 20); do
    out=$(node_task 1 "malachi.ring --show")
    if echo "$out" | grep -q "durable ring: version"; then
      echo "$out"
      return 0
    fi
    sleep 3
  done
  echo "$out"
  return 1
}

ring_vnode_count() {
  ring_show | sed -n 's/.*, \([0-9][0-9]*\) vnodes .*/\1/p' | head -1
}

# The recorded ring as token, vnode id and placement, one line per vnode. All three, not just the id:
# a vnode keeping its name while its token or its placement moved would change metadata routing just as
# surely, and comparing ids alone would call that unchanged.
ring_vnode_topology() {
  ring_show | awk '/^  [0-9]+\t/ { print $1 "\t" $2 "\t" $3 }' | sort
}

build_images
start_cluster
start_checker "$CHECKER_WINDOW_S"
sleep 10

before_ring=$(ring_vnode_count)
[ "$before_ring" = "4" ] || fail "expected a 4-vnode ring at boot, got '${before_ring:-none}'"
echo "booted with a 4-vnode ring"

event "j: growing the metadata sharding from 4 to 6 while serving"
before=$(acked_count)
reshard_on_lease_holder 6 || fail "the reshard did not complete"
require_progress "$before" "the reshard"

grown_count=$(ring_vnode_count)
ring_vnode_topology > "$WORK/vnodes_before.txt"
split_born=$(awk '$2 ~ /^vn_/ { count++ } END { print count + 0 }' "$WORK/vnodes_before.txt")
if [ "$grown_count" = "6" ] && [ "$split_born" = "2" ]; then
  echo "ring grew to 6 vnodes, 2 of them created by splitting"
else
  fail "expected a 6-vnode ring with 2 split-created vnodes, got '${grown_count:-none}' with $split_born"
fi

event "k: stopping every node at once, then bringing them all back"
$COMPOSE stop malachi1 malachi2 malachi3 >/dev/null 2>&1
running=$(docker ps --filter "name=malachi-cluster" -q | wc -l | tr -d ' ')
[ "$running" = "0" ] || fail "expected every node down before the restart, $running still running"
echo "all three nodes down; no node survives to gossip the ring"

$COMPOSE start malachi1 malachi2 malachi3 >/dev/null 2>&1
wait_healthy || fail "the cluster did not reconverge after the full restart"
# Healthy is the HTTP check; a cold cluster still has every vnode's Raft group to recover from disk and
# a leader to elect for each. The invariant under test is that the ring survived, not how fast it comes
# back, so give the control plane time to finish before reading it or producing to it.
sleep "${RESTART_SETTLE_S:-30}"

restored_count=$(ring_vnode_count)
[ "$restored_count" = "6" ] || \
  fail "the ring was not durable: expected 6 vnodes after the restart, got '${restored_count:-none}' (MALACHI_LOG_VNODES is 4)"

ring_vnode_topology > "$WORK/vnodes_after.txt"
if diff -q "$WORK/vnodes_before.txt" "$WORK/vnodes_after.txt" >/dev/null; then
  echo "the same 6 vnodes came back with the same tokens and placements, split-created ones included"
else
  echo "--- before"; cat "$WORK/vnodes_before.txt"
  echo "--- after";  cat "$WORK/vnodes_after.txt"
  fail "the ring changed across the restart"
fi

close_window
verify_acked
check_convergence
check_clean_produce
finish "RESHARD DURABILITY CERTIFICATION"
