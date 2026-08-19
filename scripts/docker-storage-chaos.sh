#!/usr/bin/env bash
# Storage chaos certification: the "different types of corruption" scenarios of the NorthGuard
# certification pipeline, run against the real 3-node RF=3 Docker cluster. Damage always targets a
# FOLLOWER copy (the topology mode of scripts/chaos_checker.exs names each segment's primary):
# primary damage needs seal-on-failure, a separate roadmap item.
#
# Events, each injected with the node STOPPED (damage races the live server otherwise: an early
# run's truncation was refilled to full size by in-flight pushes before the restart, hiding a
# zero-hole the probe could not see), each followed by reconvergence:
#   e. torn write  - cut a follower's segment copy to 3/4 and append garbage (a partial, corrupt
#                    trailing frame: the classic crash-mid-write shape). Recovery clamps the copy
#                    at the last CRC-valid frame; the write path's catch-up (still active) or the
#                    integrity probe (sealed meanwhile) repairs the tail.
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
#
# Invariants certified on top of the fatia-1 set (acked durability, convergence, clean produce):
#   4. The damaged copies physically reconverge: byte-identical segment files across all 3 nodes.
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

say "event e: torn write on a follower's active-segment copy (cut to 3/4 + garbage tail)"
damage_follower active 'f=$(ls $dir/*.log | head -1); sz=$(wc -c <$f); truncate -s $((sz * 3 / 4)) $f; head -c 50 /dev/urandom >> $f' &&
  echo "torn write injected and node restarted"

say "event f: truncate a follower's active-segment copy to half"
damage_follower active 'f=$(ls $dir/*.log | head -1); sz=$(wc -c <$f); truncate -s $((sz / 2)) $f' &&
  echo "truncation injected and node restarted"

say "event g: delete a follower's sealed-segment directory, then restart it"
if damage_follower sealed 'rm -rf $dir'; then
  echo "sealed copy deleted and node restarted; waiting for the integrity probe to re-backfill"
  wait_copy_repaired "lost sealed copy was not re-backfilled (silent under-replication)"
fi

say "event h: bit rot inside a follower's sealed copy, keeping the file's exact size"
# dd with conv=notrunc overwrites in place: the file keeps its length and its size probe stays
# happy, so nothing but a checksum scan can tell this copy from a good one.
if damage_follower sealed 'f=$(ls $dir/*.log | head -1); before=$(wc -c <$f); dd if=/dev/urandom of=$f bs=32 count=1 seek=1 conv=notrunc 2>/dev/null; [ "$(wc -c <$f)" = "$before" ]'; then
  echo "bit rot injected (size unchanged) and node restarted; waiting for the scrub to repair"
  wait_copy_repaired "rotted sealed copy was not repaired by the integrity scrub"
fi

say "event i: corrupt a follower's sparse-index sidecar, leaving its records untouched"
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
finish "STORAGE CHAOS CERTIFICATION"
