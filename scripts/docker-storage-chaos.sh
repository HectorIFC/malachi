#!/usr/bin/env bash
# Storage chaos certification: the "different types of corruption" scenarios of the NorthGuard
# certification pipeline, run against the real 3-node RF=3 Docker cluster. Corruption always targets a
# FOLLOWER copy (the topology mode of scripts/chaos_checker.exs names each segment's primary):
# corruption of a primary copy needs seal-on-failure of its own, a separate roadmap item. A storage
# FAILURE (event j below) does not, since #147: the node takes the copy out of service and the
# segment is sealed on the other replicas.
#
# Events, each injected with the node STOPPED (damage races the live server otherwise: an early
# run's truncation was refilled to full size by in-flight pushes before the restart, hiding a
# zero-hole the probe could not see), each followed by reconvergence:
#   e. torn write  - cut a follower's segment copy to 3/4 and append garbage (a partial, corrupt
#                    trailing frame: the classic crash-mid-write shape). Recovery clamps the copy
#                    at the last CRC-valid frame; the write path's catch-up (still active) or the
#                    integrity probe (sealed meanwhile) repairs the tail.
#   e2. torn write inside the PREALLOCATED region - the same crash, in the shape it actually takes
#                    once segments are preallocated: the file does not shrink, because the space was
#                    already there. Cut the records short and put the file back to its full size, so
#                    a half-written frame runs out into the zeros that were behind it all along.
#                    This is the case that used to be indistinguishable from bit rot, since the
#                    signal recovery relied on (the file ending inside a frame) no longer exists.
#   f. truncation  - cut a follower's segment copy to half. Same repair paths.
#   g. file loss   - delete a follower's SEALED segment directory. The self-healing integrity
#                    probe must detect the silent under-replication and re-backfill the copy
#                    (metadata still says RF=3, so only a physical probe sees it).
#   h. bit rot     - flip bytes INSIDE a follower's sealed copy, keeping the file's exact size. No
#                    size probe can see this one: the copy looks perfect and answers reads with the
#                    records before the damage and nothing after, silently. Only the integrity
#                    scrub (Malachi.Cluster.Scrubber, checksum verification) catches it, and it
#                    must repair the copy from an intact replica.
#   i. rotted index- corrupt a follower's sparse-index sidecar (.idx), not its records. The index is
#                    DERIVED data, so the repair must be local: rebuilt from the segment, without
#                    consulting a peer and without touching the .log. Reads stay whole throughout,
#                    because a read that does not find what the index promised rescans the segment.
#   j. full volume - a SECOND phase, on a fresh cluster whose third node has a small log volume
#                    (docker-compose.storage-full.yml). The volume is filled while the checker keeps
#                    producing. Before #147 the first ENOSPC crashed the one process holding every log
#                    on the node, and crashed it again on every restart until the application gave up.
#                    Certified here: the node stays up (no restart, no replication server crash,
#                    healthy), the failures really happened (the node logged them), a segment whose
#                    copy failed is sealed on the other replicas, and after freeing the space the
#                    closing invariants hold.
#
# Invariants certified on top of the fatia-1 set (acked durability, convergence, clean produce):
#   4. The damaged copies physically reconverge: byte-identical segment files across all 3 nodes.
#      With preallocation on, this certifies one thing more: recovery ZEROES the bytes a torn write
#      left behind instead of truncating them away (truncating would give back the preallocated
#      region). A node that crashed and one that never did therefore hold identical files, dead
#      zone included, and this check is what proves it.
#
# Usage: scripts/docker-storage-chaos.sh
set -uo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)" || exit 1
export SRV_CPUSET="${SRV_CPUSET:-2,3,4,5,6,7}" LT_CPUSET="${LT_CPUSET:-0,1}"
export RF=3
export MALACHI_DATA_ROOT=/data
# Small segments so the checker's own traffic seals segments (repairable sealed copies) in-window.
export MALACHI_SEGMENT_MAX_BYTES="${MALACHI_SEGMENT_MAX_BYTES:-4096}"
# And a tiny INTERNAL roll, so each segment's log rolls and writes its sparse-index sidecar. With the
# library default (1GB / 1h) a run of seconds has no `.idx` at all and event i would be vacuous.
export MALACHI_LOG_ROLL_MAX_BYTES="${MALACHI_LOG_ROLL_MAX_BYTES:-2048}"
# Preallocation ON, and small: segments are sized ahead of their contents in production (64MB), and
# the whole point of this drill is the recovery path that a preallocated tail changes. Small enough
# that invariant 4 can md5 the files whole.
#
# The effective size is clamped to the internal roll above, so the ask here is an upper bound and the
# file is really preallocated to MALACHI_LOG_ROLL_MAX_BYTES. That is the right size: the segment
# cannot grow past the roll, so anything more would be room it can never use, and the blank tail this
# drill needs still exists for everything below the roll point.
export MALACHI_SEGMENT_PREALLOC_BYTES="${MALACHI_SEGMENT_PREALLOC_BYTES:-8192}"
# The scrub at production cadence revisits a segment about weekly, which no test window can wait
# for, so the drill runs it aggressively: the point is to certify that it detects and repairs, not
# to measure its pace (that is benchmark/docker-scrub.sh).
export MALACHI_SCRUB_INTERVAL_MS="${MALACHI_SCRUB_INTERVAL_MS:-2000}"
export MALACHI_SCRUB_SEGMENTS_PER_TICK="${MALACHI_SCRUB_SEGMENTS_PER_TICK:-200}"
CHECKER_WINDOW_S="${CHECKER_WINDOW_S:-150}"
CHAOS_TOPIC=chaos_acked
source "$(dirname "$0")/chaos_lib.sh"

DATA_DIR=/data/malachi_log

topology() { checker_run "topology $CHAOS_HOSTS $CHAOS_TOPIC" 2>/dev/null | grep '^SEGMENT'; }

# SEGMENT range=N seq=M state=S start=O primary=malachi@malachiX replicas=a,b,c -> field value
seg_field() { sed -n "s/.*$2=\([^ ]*\).*/\1/p" <<<"$1"; }

# malachi@malachi2 -> malachi-cluster-2 / malachi2
container_of() { echo "malachi-cluster-${1##*malachi}"; }
service_of() { echo "malachi${1##*malachi}"; }

# First replica of the SEGMENT line $1 that is not its primary.
follower_of() {
  primary=$(seg_field "$1" primary)
  seg_field "$1" replicas | tr ',' '\n' | grep -v "^${primary}$" | head -1
}

seg_dir() { echo "$DATA_DIR/${CHAOS_TOPIC}-r$(seg_field "$1" range)-s$(seg_field "$1" seq)"; }

# Damages a follower copy per $2 (a shell fragment run with $dir set), with the follower node
# STOPPED: damage is injected through a one-off container on the node's data volume, so no live
# server can race the injection (append past a truncation, reopen a deleted file's descriptor).
# Then restarts the node and waits for reconvergence. Prints what it picked.
damage_follower() {
  line=$(topology | grep "state=$1" | head -1)
  if [ -z "$line" ]; then
    fail "no $1 segment found to damage"
    return 1
  fi

  follower=$(follower_of "$line")
  dir=$(seg_dir "$line")
  container=$(container_of "$follower")
  echo "target: $dir on $follower (primary $(seg_field "$line" primary))"
  DAMAGED_LINE="$line" DAMAGED_DIR="$dir" DAMAGED_PRIMARY=$(container_of "$(seg_field "$line" primary)") DAMAGED_FOLLOWER="$container"

  volume=$(docker inspect "$container" --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Name}}{{end}}{{end}}')
  image=$(docker inspect "$container" --format '{{.Config.Image}}')

  docker stop "$container" >/dev/null 2>&1
  docker run --rm -v "$volume:/data" --entrypoint sh "$image" -c "dir=$dir; $2" ||
    { fail "damage command failed for $container"; docker start "$container" >/dev/null 2>&1; return 1; }
  docker start "$container" >/dev/null 2>&1
  wait_healthy || fail "cluster did not reconverge after restarting $follower"
}

# Waits until the damaged dir's files matching $2 (default the segment's *.log) have the same md5s on
# the damaged follower and on the primary. Replicas hold identical bytes for both the records and the
# derived index, so byte equality is the honest check for "repaired" either way.
wait_copy_repaired() {
  glob="${2:-*.log}"

  for _ in $(seq 1 12); do
    a=$(docker exec "$DAMAGED_PRIMARY" sh -c "md5sum $DAMAGED_DIR/$glob 2>/dev/null | sort" | awk '{print $1}')
    b=$(docker exec "$DAMAGED_FOLLOWER" sh -c "md5sum $DAMAGED_DIR/$glob 2>/dev/null | sort" | awk '{print $1}')
    if [ -n "$a" ] && [ "$a" = "$b" ]; then
      echo "copy repaired: follower matches primary byte for byte ($glob)"
      return 0
    fi
    sleep 5
  done
  fail "$1"
}

build_images
start_cluster
start_checker "$CHECKER_WINDOW_S"
sleep 25

event "e: torn write on a follower's active-segment copy (cut to 3/4 + garbage tail)"
damage_follower active 'f=$(ls $dir/*.log | head -1); sz=$(wc -c <$f); truncate -s $((sz * 3 / 4)) $f; head -c 50 /dev/urandom >> $f' &&
  echo "torn write injected and node restarted"

event "e2: torn write INSIDE a follower's preallocated region (the file keeps its size)"
# No garbage appended, on purpose: a crash does not invent bytes, it stops writing them. Cutting the
# records short and restoring the length leaves exactly what an interrupted flush leaves on a
# preallocated file, a frame that runs out into the zeros behind it. Recovery must read that as a
# torn tail (drop it, zero it, keep the room) and NOT as bit rot, which is what event h injects.
#
# The cut point is computed from the WRITTEN prefix, never a constant, and it takes three bytes off
# the END of it rather than a fraction. Both parts were wrong once. A fixed offset lands in the blank
# tail of a segment that has just rolled, so the cut removes nothing but zeros and the restore puts
# the same zeros back: the drill reports a torn frame it never injected. A fraction is no better with
# uniform records, which is what the checker produces: three quarters of four equal frames is exactly
# a frame boundary, leaving whole frames followed by zeros, which recovery reads as `:blank` and not
# as torn. One run in four would have certified nothing.
#
# Three bytes off the end always lands strictly inside the last frame, because the smallest frame
# this format can encode is 39 bytes (a 10-byte header plus 29 bytes of fixed payload fields). The
# guard requires a written prefix bigger than that, so an empty segment fails the event instead of
# passing it.
damage_follower active 'f=$(ls $dir/*.log | head -1); before=$(wc -c <$f); written=$(od -An -v -tx1 $f | awk "{for(i=1;i<=NF;i++){n++; if(\$i!=\"00\") last=n}} END{print last+0}"); [ "$written" -gt 40 ] || { echo "no written records in $f to tear (written=$written)"; exit 1; }; cut=$((written - 3)); truncate -s $cut $f; truncate -s $before $f; [ "$(wc -c <$f)" = "$before" ]' &&
  echo "torn frame injected inside the preallocated region, size unchanged, node restarted"

event "f: truncate a follower's active-segment copy to half"
damage_follower active 'f=$(ls $dir/*.log | head -1); sz=$(wc -c <$f); truncate -s $((sz / 2)) $f' &&
  echo "truncation injected and node restarted"

event "g: delete a follower's sealed-segment directory, then restart it"
if damage_follower sealed 'rm -rf $dir'; then
  echo "sealed copy deleted and node restarted; waiting for the integrity probe to re-backfill"
  wait_copy_repaired "lost sealed copy was not re-backfilled (silent under-replication)"
fi

event "h: bit rot inside a follower's sealed copy, keeping the file's exact size"
# dd with conv=notrunc overwrites in place: the file keeps its length and its size probe stays
# happy, so nothing but a checksum scan can tell this copy from a good one.
if damage_follower sealed 'f=$(ls $dir/*.log | head -1); before=$(wc -c <$f); dd if=/dev/urandom of=$f bs=32 count=1 seek=1 conv=notrunc 2>/dev/null; [ "$(wc -c <$f)" = "$before" ]'; then
  echo "bit rot injected (size unchanged) and node restarted; waiting for the scrub to repair"
  wait_copy_repaired "rotted sealed copy was not repaired by the integrity scrub"
fi

event "i: corrupt a follower's sparse-index sidecar, leaving its records untouched"
# The .idx is derived from the records, so this must be repaired WITHOUT a peer and without touching
# the .log: the scrub rebuilds it from the segment the node already holds. The records' md5 is captured
# before and after to prove the segment itself was never rewritten.
if damage_follower sealed 'f=$(ls $dir/*.idx 2>/dev/null | head -1); [ -n "$f" ] || { echo "no sidecar in $dir"; exit 1; }; before=$(wc -c <$f); dd if=/dev/urandom of=$f bs=8 count=1 seek=1 conv=notrunc 2>/dev/null; [ "$(wc -c <$f)" = "$before" ]'; then
  logs_before=$(docker exec "$DAMAGED_FOLLOWER" sh -c "md5sum $DAMAGED_DIR/*.log | sort")
  echo "index corrupted (records untouched) and node restarted; waiting for the scrub to rebuild it"
  wait_copy_repaired "rotted sparse index was not rebuilt by the integrity scrub" '*.idx'

  logs_after=$(docker exec "$DAMAGED_FOLLOWER" sh -c "md5sum $DAMAGED_DIR/*.log | sort")
  if [ "$logs_before" = "$logs_after" ]; then
    echo "records untouched by the index repair, as they must be"
  else
    fail "repairing the index rewrote the segment's records"
  fi
fi

close_window

say "invariant 4: physical convergence of every chaos-topic segment copy"
converged=0
for _ in $(seq 1 12); do
  h1=$(docker exec malachi-cluster-1 sh -c "md5sum $DATA_DIR/${CHAOS_TOPIC}-*/*.log 2>/dev/null | sort")
  h2=$(docker exec malachi-cluster-2 sh -c "md5sum $DATA_DIR/${CHAOS_TOPIC}-*/*.log 2>/dev/null | sort")
  h3=$(docker exec malachi-cluster-3 sh -c "md5sum $DATA_DIR/${CHAOS_TOPIC}-*/*.log 2>/dev/null | sort")
  if [ -n "$h1" ] && [ "$h1" = "$h2" ] && [ "$h1" = "$h3" ]; then
    converged=1
    echo "all $(echo "$h1" | wc -l | tr -d ' ') segment files byte-identical across the 3 nodes"
    break
  fi
  sleep 5
done
[ "$converged" = "1" ] || fail "segment copies did not physically reconverge across the nodes"

verify_acked
check_convergence
check_clean_produce

# --- phase 2, event j: a node's log volume fills up (issue #147) ------------------------------------------
#
# A cluster of its own, on purpose. The small volume is a tmpfs, and a tmpfs comes back EMPTY when its
# container restarts, which is what every event above does to a follower: sharing the cluster would turn
# each of those restarts into the loss of every copy on that node, a different drill. For the same reason
# nothing below restarts the full node.
FULL_NODE=malachi-cluster-3
FULL_WINDOW_S="${FULL_WINDOW_S:-180}"
COMPOSE="$COMPOSE -f docker-compose.storage-full.yml"

say "phase 2: a fresh cluster whose $FULL_NODE has a small log volume"
rm -f "$WORK/acked.log"
start_cluster
start_checker "$FULL_WINDOW_S"
sleep 25

event "j: fill $FULL_NODE's log volume until its writes fail with ENOSPC"
restarts_before=$(docker inspect "$FULL_NODE" --format '{{.RestartCount}}')
active_before=$(topology | grep 'state=active')

# dd stops at ENOSPC and exits non-zero, which is the point; df shows the volume really is full.
docker exec "$FULL_NODE" sh -c 'dd if=/dev/zero of=/data/malachi_log/filler bs=1M 2>/dev/null; df -h /data/malachi_log | tail -1'

# The failures have to actually happen, or this event certifies nothing: the node logs each copy it takes
# out of service (Malachi.Cluster.ReplicationServer, "failed in storage").
failed_copies=0
for _ in $(seq 1 12); do
  failed_copies=$(docker logs "$FULL_NODE" 2>&1 | grep -c "failed in storage")
  [ "$failed_copies" -gt 0 ] && break
  sleep 5
done

if [ "$failed_copies" -gt 0 ]; then
  echo "$FULL_NODE took $failed_copies segment copies out of service"
else
  fail "filling $FULL_NODE's volume produced no storage failure: the event injected nothing"
fi

# Keep producing against the full volume for a while: a crash that takes a few failures to build up (the
# supervisor's 3 restarts in 5s) must have time to show.
sleep 20

# Up means all three, because each alone can lie: `mix run --no-halt` keeps the container running after the
# application stops, a replication server crash is restarted fast enough to look healthy, and a restart
# count only sees the container.
restarts_after=$(docker inspect "$FULL_NODE" --format '{{.RestartCount}}')
health=$(docker inspect "$FULL_NODE" --format '{{.State.Health.Status}}')
crashes=$(docker logs "$FULL_NODE" 2>&1 | grep -c "Malachi.LogReplication terminating")

if [ "$restarts_after" = "$restarts_before" ] && [ "$health" = "healthy" ] && [ "$crashes" = "0" ]; then
  echo "$FULL_NODE stayed up through the full volume: no restart, no replication server crash, healthy"
else
  fail "$FULL_NODE went down with a full volume (restarts $restarts_before -> $restarts_after, health $health, replication server crashes $crashes)"
fi

# A segment that was active when the volume filled must be sealed by now: its copy on the full node failed,
# and the heal pass seals it on the other two so producers move to a new segment.
sealed_since=0
for _ in $(seq 1 12); do
  topology_now=$(topology)
  sealed_since=0

  while read -r line; do
    [ -n "$line" ] || continue
    range=$(seg_field "$line" range)
    seq_no=$(seg_field "$line" seq)
    grep -q "range=$range seq=$seq_no state=sealed" <<<"$topology_now" && sealed_since=$((sealed_since + 1))
  done <<<"$active_before"

  [ "$sealed_since" -gt 0 ] && break
  sleep 5
done

if [ "$sealed_since" -gt 0 ]; then
  echo "$sealed_since segment(s) active when the volume filled are sealed now: producers moved on"
else
  fail "no segment active when $FULL_NODE's volume filled was sealed: seal-on-failure did not happen"
fi

docker exec "$FULL_NODE" rm -f /data/malachi_log/filler && echo "space freed on $FULL_NODE"

close_window
verify_acked
check_convergence
check_clean_produce
finish "STORAGE CHAOS CERTIFICATION"
