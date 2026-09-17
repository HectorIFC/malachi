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
#   4. The damaged copies physically reconverge: every node's copy of every chaos-topic segment is
#      readable and holds the same records, byte for byte over the valid part of each file, and a
#      segment the control plane sealed holds exactly its sealed length (ChaosChecker.Copies).
#      Not byte-identical FILES, which this invariant once required and which the log model does not
#      promise (issue #152): only a fenced copy has its preallocated tail trimmed, and each node rolls
#      its internal files at its own sync points, so healthy copies differ as files on every run.
#      That recovery zeroes a torn write inside the preallocated region instead of truncating it is
#      pinned where it is decided, in test/malachi/storage/elixir_store_test.exs.
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
# that invariant 4 can md5 the files whole. A copy's blank tail is trimmed only when that copy's store
# is sealed or closed, and a produce roll fences only the primary, so copies of one segment need not
# have the same length on every node: that is what the per-copy report of invariant 4 separates out.
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

  volume=$(data_volume_of "$container")
  image=$(image_of "$container")

  docker stop "$container" >/dev/null 2>&1
  docker run --rm -v "$volume:/data" --entrypoint sh "$image" -c "dir=$dir; $2" ||
    { fail "damage command failed for $container"; docker start "$container" >/dev/null 2>&1; return 1; }
  docker start "$container" >/dev/null 2>&1
  wait_healthy || fail "cluster did not reconverge after restarting $follower"
}

# Waits until the damaged follower's copy of the damaged segment holds the same records as the primary's,
# by invariant 4's rule (the files themselves need not match: see the header). Fails with $1 otherwise,
# printing what the two copies held on the last try.
wait_segment_repaired() {
  if copies 12 5000 "segment=$(basename "$DAMAGED_DIR")" "$DAMAGED_PRIMARY" "$DAMAGED_FOLLOWER" >"$WORK/repair.txt" 2>&1; then
    echo "copy repaired: follower holds the primary's records ($(sed -n 's/^COPIES verdict=\([a-z_]*\) .*/\1/p' "$WORK/repair.txt"))"
  else
    disagreeing_copies "$WORK/repair.txt"
    fail "$1"
  fi
}

# Waits until the damaged dir's sparse index has the same md5s on the damaged follower and on the
# primary. The index is derived from the records and rebuilt locally, so byte equality is the honest
# check for "rebuilt".
wait_index_rebuilt() {
  for _ in $(seq 1 12); do
    a=$(docker exec "$DAMAGED_PRIMARY" sh -c "md5sum $DAMAGED_DIR/*.idx 2>/dev/null | sort" | awk '{print $1}')
    b=$(docker exec "$DAMAGED_FOLLOWER" sh -c "md5sum $DAMAGED_DIR/*.idx 2>/dev/null | sort" | awk '{print $1}')
    if [ -n "$a" ] && [ "$a" = "$b" ]; then
      echo "index rebuilt: follower matches primary byte for byte (*.idx)"
      return 0
    fi
    sleep 5
  done
  fail "$1"
}

# The checker's copies mode (ChaosChecker.Copies) over the data volumes of the given containers, mounted
# read-only. --no-deps because a copy on a STOPPED node is a copy too, and `compose run` would otherwise start
# the node first. Args: attempts interval_ms [segment=<dir>] container... Exit status is the checker's: 0
# when every segment's copies hold the same records.
copies() {
  attempts=$1 interval_ms=$2
  shift 2
  filter=""
  case "${1:-}" in segment=*) filter="$1 " && shift ;; esac

  roots=""
  CHECKER_RUN_ARGS=(--no-deps)
  for c in "$@"; do
    node="malachi${c##*-}"
    CHECKER_RUN_ARGS+=(-v "$(data_volume_of "$c"):/copies/$node:ro")
    roots="$roots $node=/copies/$node/${DATA_DIR#/data/}"
  done

  checker_run "copies $CHAOS_HOSTS $CHAOS_TOPIC $attempts $interval_ms $filter${roots# }"
  copies_status=$?
  CHECKER_RUN_ARGS=()
  return "$copies_status"
}

# The COPY and COPIES lines of every segment in report $1 whose copies are not identical, which is the part of a
# report worth reading: on a long run most segments agree to the byte.
disagreeing_copies() {
  awk 'NR == FNR { if ($1 == "COPIES" && $2 != "verdict=identical" && $3 ~ /^segment=/) keep[$3] = 1; next }
       ($1 == "COPY" && ($2 in keep)) || ($1 == "COPIES" && ($3 in keep))' "$1" "$1"
}

# Keeps what a failed comparison saw, before phase 2's start_cluster recreates the volumes and it is gone: the
# report, the substrate and host load, and each node's copy of every segment the report names.
keep_copies_evidence() {
  mkdir -p "$EVIDENCE_DIR" || { fail "cannot create the evidence directory $EVIDENCE_DIR"; return 1; }
  cp "$1" "$EVIDENCE_DIR/copies.txt"
  {
    date -u +%Y-%m-%dT%H:%M:%SZ
    uptime
    docker info --format 'docker kernel {{.KernelVersion}}, {{.NCPU}} cpus, {{.OperatingSystem}}'
  } >"$EVIDENCE_DIR/substrate.txt" 2>&1

  dirs=$(disagreeing_copies "$1" | sed -n 's/^COPIES .* segment=\([^ ]*\) .*/\1/p' | tr '\n' ' ')
  for c in malachi-cluster-1 malachi-cluster-2 malachi-cluster-3; do
    mkdir -p "$EVIDENCE_DIR/$c"
    docker run --rm -v "$(data_volume_of "$c"):/data:ro" -v "$EVIDENCE_DIR/$c:/out" --entrypoint sh "$(image_of "$c")" \
      -c "cd $DATA_DIR && for d in $dirs; do if [ -d \$d ]; then cp -a \$d /out/; fi; done" ||
      echo "could not copy the segment directories out of $c"
  done

  EVIDENCE_KEPT="$EVIDENCE_DIR"
  echo "evidence kept in $EVIDENCE_DIR"
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
  wait_segment_repaired "lost sealed copy was not re-backfilled (silent under-replication)"
fi

event "h: bit rot inside a follower's sealed copy, keeping the file's exact size"
# dd with conv=notrunc overwrites in place: the file keeps its length and its size probe stays
# happy, so nothing but a checksum scan can tell this copy from a good one.
if damage_follower sealed 'f=$(ls $dir/*.log | head -1); before=$(wc -c <$f); dd if=/dev/urandom of=$f bs=32 count=1 seek=1 conv=notrunc 2>/dev/null; [ "$(wc -c <$f)" = "$before" ]'; then
  echo "bit rot injected (size unchanged) and node restarted; waiting for the scrub to repair"
  wait_segment_repaired "rotted sealed copy was not repaired by the integrity scrub"
fi

event "i: corrupt a follower's sparse-index sidecar, leaving its records untouched"
# The .idx is derived from the records, so this must be repaired WITHOUT a peer and without touching
# the .log: the scrub rebuilds it from the segment the node already holds. The records' md5 is captured
# before and after to prove the segment itself was never rewritten.
if damage_follower sealed 'f=$(ls $dir/*.idx 2>/dev/null | head -1); [ -n "$f" ] || { echo "no sidecar in $dir"; exit 1; }; before=$(wc -c <$f); dd if=/dev/urandom of=$f bs=8 count=1 seek=1 conv=notrunc 2>/dev/null; [ "$(wc -c <$f)" = "$before" ]'; then
  logs_before=$(docker exec "$DAMAGED_FOLLOWER" sh -c "md5sum $DAMAGED_DIR/*.log | sort")
  echo "index corrupted (records untouched) and node restarted; waiting for the scrub to rebuild it"
  wait_index_rebuilt "rotted sparse index was not rebuilt by the integrity scrub"

  logs_after=$(docker exec "$DAMAGED_FOLLOWER" sh -c "md5sum $DAMAGED_DIR/*.log | sort")
  if [ "$logs_before" = "$logs_after" ]; then
    echo "records untouched by the index repair, as they must be"
  else
    fail "repairing the index rewrote the segment's records"
  fi
fi

close_window

say "invariant 4: physical convergence of every chaos-topic segment copy"
# Retried for a minute, like every repair wait: a copy still being caught up converges inside the window.
if copies 12 5000 malachi-cluster-1 malachi-cluster-2 malachi-cluster-3 >"$WORK/copies.txt" 2>&1; then
  grep '^COPIES segments=' "$WORK/copies.txt"
  echo "every segment's copies hold the same records on the 3 nodes"
else
  grep '^COPIES segments=' "$WORK/copies.txt" || { echo "per-copy report unavailable:"; tail -5 "$WORK/copies.txt"; }
  echo "segments whose copies are not identical, per node:"
  disagreeing_copies "$WORK/copies.txt"
  keep_copies_evidence "$WORK/copies.txt"
  fail "segment copies did not reconverge to the same records across the nodes (see the per-copy report above)"
fi

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

# Every segment whose copy failed on the full node must be sealed by now: the heal pass seals it on the other
# two so producers move to a new segment. Keyed on the failed copies themselves, not on whatever was active
# when the volume filled: under the checker's traffic a segment rolls every few seconds, so an assertion
# that any pre-fill segment got sealed holds with or without seal-on-failure and certifies nothing.
#
# The node names each copy it takes out of service, as in
#   segment {{"chaos_acked", 0}, 14}'s copy on this node failed in storage (:enospc)
# which is range 0, seq 14 in the topology's terms.
failed_segments=$(docker logs "$FULL_NODE" 2>&1 |
  sed -n "s/.*segment {{\"$CHAOS_TOPIC\", \([0-9]*\)}, \([0-9]*\)}'s copy on this node failed in storage.*/range=\1 seq=\2/p" |
  sort -u)

if [ -z "$failed_segments" ]; then
  fail "$FULL_NODE logged storage failures, but none named a $CHAOS_TOPIC segment this drill can check"
else
  unsealed="$failed_segments"
  for _ in $(seq 1 12); do
    topology_now=$(topology)
    unsealed=$(while read -r key; do
      grep -q "$key state=sealed" <<<"$topology_now" || echo "$key"
    done <<<"$failed_segments")

    [ -z "$unsealed" ] && break
    sleep 5
  done

  if [ -z "$unsealed" ]; then
    echo "all $(wc -l <<<"$failed_segments" | tr -d ' ') segment(s) whose copy failed on $FULL_NODE are sealed: producers moved on"
  else
    fail "segment(s) whose copy failed on $FULL_NODE were never sealed: $(tr '\n' ';' <<<"$unsealed")"
  fi
fi

docker exec "$FULL_NODE" rm -f /data/malachi_log/filler && echo "space freed on $FULL_NODE"

close_window
verify_acked
check_convergence
check_clean_produce
finish "STORAGE CHAOS CERTIFICATION"
