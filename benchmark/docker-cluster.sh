#!/usr/bin/env bash
# 3-node cluster benchmark: can a Malachi cluster of three nodes behind ONE metadata vnode reach 1M rec/s
# aggregate? The client (pinned to its own cores) drives all three nodes at once via multi-host
# round-robin, over enough topics that segment placement spreads primaries across the nodes.
#
# Two regimes are measured, each on a FRESH cluster (recreated, volumes removed before and after):
# - RF=1: segments striped across the three primaries, group commit ACTIVE on each node. The
#   throughput regime, the one with a real shot at 1M.
# - RF=3: every batch quorum-fsynced across the nodes and group commit gated off (rf > 1). The
#   full-durability regime; expected far lower, reported honestly.
#
# Two data modes, one per invocation, chosen by REAL_DISK:
# - REAL_DISK=0 (default): data on each node's 1g tmpfs /tmp, preallocation off. fsync costs nothing
#   there, which is what keeps runs comparable to each other; it is NOT the durable path.
# - REAL_DISK=1: data on each node's named volume /data, preallocation at DISK_PREALLOC_BYTES (64MB,
#   the production default). The durable path, paid in full. A case fails unless /data is a real
#   filesystem (not tmpfs) and the preallocated bytes are on it afterwards: the store falls back to an
#   unpreallocated segment when preallocation fails (ENOSPC, say), which would otherwise measure the
#   wrong path without a word.
# Comparing the two takes two invocations; alternate them (tmpfs, disk, tmpfs, disk, ...) so the spread
# between repetitions of one mode is the noise floor a difference has to clear.
#
# Every segment is created before the measured window: each topic starts with one range whose segment
# opens on its first produce, and on a real disk that open writes the whole preallocation (about 249ms
# for 64MB, benchmark/README.md). `--prepopulate $BATCH` sends one batch per topic during setup, so those
# stalls land there and not in the warmup or the window. Both modes pass it, so the modes differ only in
# where the data lives.
#
# Per-flush latency, the most direct cost of the durable path, is not reported: the flush telemetry is
# not on main yet. Porting it and scraping it here is #164.
#
# A `docker stats` snapshot is taken mid-window so a CPU-saturated node set is visible evidence (the
# three servers share the cores of SRV_CPUSET). The window is the one the generator marks with
# `--measure-marker`, never a guess from its start time: setup length varies with the data mode.
#
# Linux only, like Malachi: the measurement counts only there. On any other host the script refuses to
# run unless ALLOW_NON_LINUX=1, which runs it as a smoke test whose numbers are not comparable.
#
# Knobs (env): DUR WARM CONNS BATCH RSIZE TOPICS RFS SRV_CPUSET LT_CPUSET, and
#   REAL_DISK            0 or 1, see above
#   DISK_PREALLOC_BYTES  preallocation in disk mode, 1..67108864 (the broker clamps it to the 64MB
#                        segment size, and the disk check relies on that bound)
#   CASE_TIMEOUT         seconds one load generator run may take, setup included (default 300); a run
#                        that exceeds it is killed, reported as a timeout, and the next case still runs
#   OUT                  when set, one JSON object per case is APPENDED to this file (JSON lines)
#
# Usage: benchmark/docker-cluster.sh
#        REAL_DISK=1 OUT=results/docker-cluster.jsonl benchmark/docker-cluster.sh
set -uo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)" || exit 1
COMPOSE="docker compose -f docker-compose.cluster.yml"

# --- knobs, all refused (exit 2) before anything runs ---

usage_error() { echo "$*" >&2; exit 2; }

require_positive() {
  case "$2" in
    '' | *[!0-9]* | 0*) usage_error "$1 must be a positive integer, got '$2'" ;;
  esac
}

DUR="${DUR:-4}"
# Cluster warmup must cover cross-node metadata propagation: a topic created via one node reaches the
# other nodes' caches on the periodic refresh (~1s), so a 1s warmup leaks no_such_topic transients into
# the measured window. 5s puts the window fully in steady state.
WARM="${WARM:-5}"
CONNS="${CONNS:-192}"
BATCH="${BATCH:-100}"
RSIZE="${RSIZE:-256}"
TOPICS="${TOPICS:-64}"
RFS="${RFS:-1 3}"
# Only an unset REAL_DISK means tmpfs: an empty one (a blank CI variable, say) is refused below rather
# than silently running the mode nobody asked for.
REAL_DISK="${REAL_DISK-0}"
# The size the broker preallocates at most: `segment_prealloc_bytes/0` in lib/malachi/application.ex
# clamps the setting to the segment size, 64MB unless MALACHI_SEGMENT_MAX_BYTES says otherwise.
MAX_PREALLOC_BYTES=67108864
DISK_PREALLOC_BYTES="${DISK_PREALLOC_BYTES:-$MAX_PREALLOC_BYTES}"
CASE_TIMEOUT="${CASE_TIMEOUT:-300}"
OUT="${OUT:-}"
export SRV_CPUSET="${SRV_CPUSET:-4,5,6,7}" LT_CPUSET="${LT_CPUSET:-0,1,2,3}"

for knob in DUR WARM CONNS BATCH RSIZE TOPICS CASE_TIMEOUT DISK_PREALLOC_BYTES; do
  require_positive "$knob" "${!knob}"
done
[ -n "${RFS// /}" ] || usage_error "RFS is empty"
for rf in $RFS; do
  case "$rf" in
    1 | 2 | 3) ;;
    *) usage_error "RFS entries must be 1, 2 or 3 (the cluster has three nodes), got '$rf'" ;;
  esac
done
case "$REAL_DISK" in
  0 | 1) ;;
  *) usage_error "REAL_DISK must be 0 or 1, got '$REAL_DISK'" ;;
esac
if [ "$DISK_PREALLOC_BYTES" -gt "$MAX_PREALLOC_BYTES" ]; then
  usage_error "DISK_PREALLOC_BYTES must be at most $MAX_PREALLOC_BYTES (the segment size the broker clamps it to), got $DISK_PREALLOC_BYTES"
fi
# Both change the clamp above, which the disk check depends on, and nothing here needs them.
for knob in MALACHI_SEGMENT_MAX_BYTES MALACHI_LOG_ROLL_MAX_BYTES; do
  [ -z "${!knob:-}" ] || usage_error "$knob is not supported by this benchmark (it changes the preallocation clamp the disk check relies on)"
done

SMOKE=0
if [ "$(uname -s)" != "Linux" ]; then
  [ "${ALLOW_NON_LINUX:-0}" = "1" ] ||
    usage_error "this benchmark runs on Linux only; ALLOW_NON_LINUX=1 runs it as a smoke test whose numbers are not comparable"
  SMOKE=1
fi
required_tools="docker jq timeout"
[ "$SMOKE" = 1 ] || required_tools="$required_tools findmnt lsblk"
for tool in $required_tools; do
  command -v "$tool" > /dev/null 2>&1 || usage_error "$tool is required to run this benchmark"
done

if [ "$REAL_DISK" = 1 ]; then
  DATA_MODE=disk
  export MALACHI_DATA_ROOT=/data MALACHI_SEGMENT_PREALLOC_BYTES="$DISK_PREALLOC_BYTES"
else
  DATA_MODE=tmpfs
  export MALACHI_DATA_ROOT=/tmp MALACHI_SEGMENT_PREALLOC_BYTES=0
fi

if [ -n "$OUT" ]; then
  mkdir -p "$(dirname "$OUT")" || usage_error "cannot create the directory of OUT=$OUT"
fi

# Private scratch dir (not a predictable /tmp path a local attacker could pre-create as a symlink).
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
NODES="malachi1 malachi2 malachi3"
LOADTEST_CONTAINER="malachi-cluster-loadtest-$$"
# Created by the generator, inside its own container, the moment the measured window opens.
MEASURE_MARKER=/tmp/malachi-measure-window
FAILED=0

# --- where the numbers come from ---

# The disk under a mounted filesystem, from its MAJ:MIN: the partition's parent when there is one.
describe_disk() {
  lsblk -J -l -o NAME,PKNAME,MAJ:MIN,ROTA,MODEL 2> /dev/null | jq -r --arg mm "$1" '
    .blockdevices as $all
    | ([$all[] | select(.["maj:min"] == $mm)] | first) as $part
    | if $part == null then "disk unknown"
      else ([$all[] | select(.name == ($part.pkname // $part.name))] | first // $part)
        | "disk \(.name) rota=\(.rota | tostring) model=\((.model // "unknown") | tostring | gsub("^\\s+|\\s+$"; ""))"
      end' 2> /dev/null || echo "disk unknown"
}

# What backs the Docker root on the host, where the named volumes live.
describe_docker_root() {
  local root="$1" fstype options source majmin
  if [ "$SMOKE" = 1 ]; then
    echo "unknown (not a Linux host)"
    return
  fi
  if ! read -r fstype options source majmin < <(findmnt -no FSTYPE,OPTIONS,SOURCE,MAJ:MIN --target "$root" 2> /dev/null); then
    echo "unknown (findmnt could not resolve $root)"
    return
  fi
  echo "$fstype $options on $source ($(describe_disk "$majmin"))"
}

HOST="$(uname -srm)"
DOCKER_DESC="$(docker info --format '{{.OperatingSystem}}, kernel {{.KernelVersion}}, storage driver {{.Driver}}' 2> /dev/null)" ||
  usage_error "docker info failed; is the Docker daemon running?"
DOCKER_ROOT="$(docker info --format '{{.DockerRootDir}}' 2> /dev/null)"
BACKING="$(describe_docker_root "$DOCKER_ROOT")"

echo "Building images (first run compiles all deps; slow)..."
$COMPOSE build || { echo "build failed"; exit 1; }

# The broker turns group commit off above RF 1 whatever MALACHI_GROUP_COMMIT says
# (`group_commit_flag and replication_factor == 1` in lib/malachi/broker_server.ex).
group_commit_for_rf() { if [ "$1" = 1 ]; then echo true; else echo false; fi; }

# Regime names come from the same formatter the ceiling results use, run in the load generator image.
# They name the preallocation too, so a tmpfs case and a disk case never carry the same label.
declare -A LABELS
for rf in $RFS; do
  label="$($COMPOSE run --rm --no-deps --entrypoint mix loadtest malachi.loadtest.ceiling label \
             --batch "$BATCH" --record-size "$RSIZE" --group-commit "$(group_commit_for_rf "$rf")" \
             --segment-prealloc-bytes "$MALACHI_SEGMENT_PREALLOC_BYTES" \
             2> "$WORK/label.err" | tail -1)"
  if [ -z "$label" ]; then
    echo "could not name the regime for RF=$rf; its stderr:"
    tail -15 "$WORK/label.err" | sed 's/^/  /'
    exit 1
  fi
  LABELS[$rf]="$label"
done

echo "data:    $DATA_MODE (MALACHI_DATA_ROOT=$MALACHI_DATA_ROOT, preallocation $MALACHI_SEGMENT_PREALLOC_BYTES bytes)"
echo "host:    $HOST"
echo "docker:  $DOCKER_DESC"
echo "volumes: $DOCKER_ROOT on $BACKING"
if [ "$SMOKE" = 1 ]; then
  echo "SMOKE TEST: not a Linux host, these numbers are not comparable to anything"
fi
echo

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

# Removes the containers AND the volumes, so no case ever starts on another case's data.
teardown() { RF="$1" $COMPOSE down -v > /dev/null 2>&1; }

# "<fstype> <options>" of the filesystem holding the data root inside a node, as the broker sees it: the
# longest mount point in /proc/self/mounts that contains the data root.
node_mount() {
  RF="$1" $COMPOSE exec -T "$2" awk -v p="$MALACHI_DATA_ROOT/" '
    { mp = ($2 == "/") ? "/" : $2 "/" }
    index(p, mp) == 1 && length(mp) > best { best = length(mp); fs = $3; opts = $4 }
    END { if (fs == "") exit 1; print fs, opts }' /proc/self/mounts 2> /dev/null
}

# Bytes the case needs on the host disk: every topic's segment on every replica, fully preallocated.
# The three volumes share that disk, so the total is what matters.
needed_bytes() { echo $((TOPICS * $1 * MALACHI_SEGMENT_PREALLOC_BYTES)); }

# Snapshots CPU% of every container around the middle of the measured window, into $WORK/stats.txt. The
# window opens when the generator creates MEASURE_MARKER in its container, which is polled until then,
# and the sampler gives up once the run is over ($WORK/run.done) without having seen it. Timing the
# snapshot from the generator's start instead put it inside setup whenever setup was long: in the disk
# mode, where setup writes the whole preallocation first, it read 6 to 16% per node against about 100%
# on tmpfs (dispatch run 35125794435), which said nothing about the window.
#
# `docker stats --no-stream` samples for about two seconds before it prints (2.4s measured on Docker
# Desktop), so it starts that much before the middle: started at the middle of a 4s window it ended
# after the window, and the generator, already gone, was missing from the reading.
sample_cpu() {
  until [ -e "$WORK/run.done" ]; do
    if [ "$(docker exec "$LOADTEST_CONTAINER" sh -c "test -e $MEASURE_MARKER && echo yes" 2> /dev/null)" = yes ]; then
      sleep "$((DUR > 2 ? (DUR - 2) / 2 : 0))"
      docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' > "$WORK/stats.txt" 2> /dev/null
      return
    fi
    sleep 0.5
  done
}

# Appends the case to OUT; a no-op without it.
record_case() {
  local rf="$1" outcome="$2" loadtest="${3:-null}" du_bytes="${4:-null}" cpu="${5:-}"
  [ -n "$OUT" ] || return 0
  jq -cn \
    --arg data_mode "$DATA_MODE" --argjson rf "$rf" --arg regime_label "${LABELS[$rf]}" \
    --arg outcome "$outcome" --argjson prealloc_bytes "$MALACHI_SEGMENT_PREALLOC_BYTES" \
    --argjson du_bytes "$du_bytes" --argjson nodes "$MOUNTS_JSON" --arg host "$HOST" \
    --arg docker "$DOCKER_DESC" --arg docker_root_backing "$BACKING" --argjson smoke "$SMOKE" \
    --arg mid_window_cpu "$cpu" --argjson loadtest "$loadtest" \
    '{data_mode: $data_mode, rf: $rf, regime_label: $regime_label, outcome: $outcome,
      prealloc_bytes: $prealloc_bytes, du_bytes: $du_bytes, nodes: $nodes, host: $host, docker: $docker,
      docker_root_backing: $docker_root_backing, smoke_test: ($smoke == 1),
      mid_window_cpu: (if $mid_window_cpu == "" then null else $mid_window_cpu end), loadtest: $loadtest}' \
    >> "$OUT"
}

# A case that stops before the load generator ran: reported, recorded, torn down, counted as failed.
fail_case() {
  local rf="$1" outcome="$2" message="$3"
  printf "%-4s | %s\n" "$rf" "($outcome)"
  echo "     $message"
  record_case "$rf" "$outcome"
  teardown "$rf"
  FAILED=1
}

for rf in $RFS; do
  teardown "$rf"
  RF="$rf" $COMPOSE up -d --force-recreate malachi1 malachi2 malachi3 > "$WORK/up.log" 2>&1
  MOUNTS_JSON='{}'

  if ! wait_healthy; then
    echo "  cluster did not converge to healthy (RF=$rf); node 1 log tail:"
    RF="$rf" $COMPOSE logs --tail 25 malachi1 2>&1 | tail -25
    fail_case "$rf" "unhealthy" "the cluster never reported three healthy nodes"
    continue
  fi

  # Where each node's data really lives. In disk mode a tmpfs here means the mode did nothing.
  mount_error=""
  for node in $NODES; do
    if ! read -r fstype options < <(node_mount "$rf" "$node"); then
      mount_error="could not read the mount holding $MALACHI_DATA_ROOT on $node"
      break
    fi
    MOUNTS_JSON="$(jq -c --arg n "$node" --arg f "$fstype" --arg o "$options" \
      '. + {($n): {fstype: $f, mount_options: $o}}' <<< "$MOUNTS_JSON")"
    if [ "$DATA_MODE" = disk ] && [ "$fstype" = tmpfs ]; then
      mount_error="$MALACHI_DATA_ROOT on $node is tmpfs, so this is not a real-disk run"
      break
    fi
  done
  if [ -n "$mount_error" ]; then
    fail_case "$rf" "wrong filesystem" "$mount_error"
    continue
  fi
  echo "     data on: $(jq -r 'to_entries | map("\(.key) \(.value.fstype)") | join(", ")' <<< "$MOUNTS_JSON")"

  if [ "$DATA_MODE" = disk ]; then
    needed=$(( $(needed_bytes "$rf") + 1024 * 1024 * 1024 ))
    free_kb="$(RF="$rf" $COMPOSE exec -T malachi1 df -Pk "$MALACHI_DATA_ROOT" 2> /dev/null | awk 'NR == 2 { print $4 }')"
    if [ -z "$free_kb" ]; then
      fail_case "$rf" "no disk space reading" "df could not read $MALACHI_DATA_ROOT on malachi1"
      continue
    fi
    if [ $((free_kb * 1024)) -lt "$needed" ]; then
      fail_case "$rf" "not enough disk" \
        "need $needed bytes ($TOPICS topics x RF $rf x $MALACHI_SEGMENT_PREALLOC_BYTES preallocated + 1GB margin), $((free_kb * 1024)) free"
      continue
    fi
  fi

  # The CPU snapshot runs beside the generator; it is evidence of who saturated, not a measurement.
  rm -f "$WORK/stats.txt" "$WORK/run.done"
  sample_cpu &
  stats_pid=$!

  # stderr goes to a file rather than /dev/null. It used to be discarded, and the cost of that showed
  # up twice: a run reported `(no json)` with the reason already thrown away, and diagnosing it needed
  # the whole step reproduced by hand. It turned out to be `compose run` racing a teardown that had not
  # finished draining, which the first line of that stderr says outright.
  RF="$rf" timeout --kill-after=10 "$CASE_TIMEOUT" $COMPOSE run --rm --name "$LOADTEST_CONTAINER" loadtest \
    --host malachi1,malachi2,malachi3 --scenario produce \
    --connections "$CONNS" --batch "$BATCH" --topics "$TOPICS" --prepopulate "$BATCH" \
    --duration "$DUR" --warmup "$WARM" --record-size "$RSIZE" --measure-marker "$MEASURE_MARKER" --json \
    > "$WORK/loadtest.out" 2> "$WORK/loadtest-rf$rf.err"
  status=$?
  touch "$WORK/run.done"
  wait "$stats_pid" 2> /dev/null
  cpu=""
  [ -s "$WORK/stats.txt" ] && cpu="$(tr '\n' ' ' < "$WORK/stats.txt" | sed 's/ $//')"

  if [ "$status" = 124 ] || [ "$status" = 137 ]; then
    # Killing the compose client can leave its container running.
    docker rm -f "$LOADTEST_CONTAINER" > /dev/null 2>&1
    fail_case "$rf" "timeout after ${CASE_TIMEOUT}s" "the load generator did not finish; raise CASE_TIMEOUT if setup is legitimately slow"
    continue
  fi

  json="$(grep -E '^\{' "$WORK/loadtest.out" | tail -1)"
  if [ -z "$json" ] || ! jq -e 'type == "object"' > /dev/null 2>&1 <<< "$json"; then
    # A caseless client must fail the run, not blend in as a blank row.
    printf "%-4s | %s\n" "$rf" "(no json)"
    echo "     the load generator produced no result; its stderr:"
    tail -15 "$WORK/loadtest-rf$rf.err" | sed 's/^/       /'
    record_case "$rf" "no json" null null "$cpu"
    teardown "$rf"
    FAILED=1
    continue
  fi

  read -r recs p50 p99 err drop over recon < <(jq -r \
    '[.records_per_s,.latency_ms.p50,.latency_ms.p99,.errors,.dropped,.overloaded,.reconnects]|@tsv' <<< "$json")
  outcome="ok"
  # Errors are a lower number, not a failed run: RF=3 on a real disk is expected to time some out.
  if [ "$err" != "0" ]; then
    drop="$drop(err=$err)"
    outcome="ok with errors"
  fi
  printf "%-4s | %10s %8s %8s %8s %8s %8s\n" "$rf" "$recs" "$p50" "$p99" "$drop" "$over" "$recon"
  echo "     regime: ${LABELS[$rf]}"
  [ -n "$cpu" ] && echo "     mid-window CPU: $cpu"

  du_bytes=null
  if [ "$DATA_MODE" = disk ]; then
    du_kb=0
    for node in $NODES; do
      kb="$(RF="$rf" $COMPOSE exec -T "$node" du -sk "$MALACHI_DATA_ROOT/malachi_log" 2> /dev/null | awk '{ print $1 }')"
      du_kb=$((du_kb + ${kb:-0}))
    done
    du_bytes=$((du_kb * 1024))
    expected="$(needed_bytes "$rf")"
    echo "     on disk: $du_bytes bytes across the nodes (at least $expected expected)"
    if [ "$du_bytes" -lt "$expected" ]; then
      echo "     preallocation did not land on the volume: $TOPICS topics x RF $rf x $MALACHI_SEGMENT_PREALLOC_BYTES bytes is $expected"
      outcome="preallocation missing"
      FAILED=1
    fi
  fi

  record_case "$rf" "$outcome" "$json" "$du_bytes" "$cpu"
  teardown "$rf"
done

echo
if [ "$FAILED" != "0" ]; then
  echo "done, WITH FAILED CASES (see above)"
  exit 1
fi
echo "done ($DATA_MODE; target: 1M rec/s aggregate at RF=1; the three servers share SRV_CPUSET=$SRV_CPUSET)"
